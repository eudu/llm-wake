# llm-wake

A Wake-on-LAN proxy for a local LLM server on a PC that is often off. Point your clients (coding agents, scripts,
cron jobs) at the proxy instead of the PC: a request to the model wakes the PC, waits until the model server answers
and goes through as if the PC had been on all along.

One Python file, standard library only, no configuration file. Run it on an always-on machine in the same LAN as the
PC (a Raspberry Pi, a NAS, a small LXC container).

```text
client --> llm-wake (always on) --magic packet--> PC with LM Studio / Ollama / llama.cpp
                     \--------- HTTP, streamed ---------^
```

## Why

Wake-on-LAN alone is not enough for an LLM server:

- The client has to wait for the boot instead of failing at once.
- Agents and editors poll the model list (`GET /v1/models`) on their own every few seconds. If that wakes the PC, it
  never stays off.
- A magic packet sent while Windows is still shutting down is lost; one packet is not always enough.
- The server's port opens before the model is ready: the first completion right after the boot can fail.
- Completions are streamed (SSE) and must not be buffered.

## How it works

- **Requests that use the model** (`POST` and every other method except `GET` and `HEAD`): if the upstream answers
  `GET /v1/models`, the request goes through at once. Otherwise llm-wake sends a magic packet, repeats it every
  `--resend` seconds, waits up to `--wake-timeout` seconds for `/v1/models` to answer, waits `--grace` seconds more
  and forwards the request. Requests that arrive at the same time share one wake. If the PC does not answer in time,
  the client gets `504` with an OpenAI-style error body.
- **`GET` and `HEAD` never wake the PC.** While the PC answers, llm-wake keeps a copy of the last answer of each path
  in `--cache-dir`; while it sleeps, a `GET` gets that copy (or `503` if there is none). Model pickers keep working
  and the PC stays off.
- **`GET /llm-wake/health`** answers `{"upstream": true|false, "url": ...}` without waking the PC.
- **API key (optional, recommended).** When `LLM_WAKE_API_KEY` is set, every request except the health check needs
  `Authorization: Bearer <key>`. Without it the answer is `401` and the PC is not woken. The key belongs to the proxy
  and is not passed on to the LLM server.
- Everything else is proxied as is: method, path, body and headers go to the upstream unchanged, the response is
  streamed back chunk by chunk. llm-wake does not parse the OpenAI API, so any HTTP server works; the PC counts as up
  when `GET /v1/models` returns `200` (LM Studio, Ollama and llama.cpp `llama-server` all serve it).

## Requirements

- Python 3.8 or newer on the proxy machine (tested on 3.13 and 3.14).
- Wake-on-LAN working on the PC: enabled in the BIOS/UEFI, a wired network card with wake on magic packet. On Windows
  also turn off Fast Startup (`powercfg /h off`) and check the card's advanced properties (for Intel cards
  "Enable PME" had to be on to wake from power off).
- The LLM server starting on its own after boot, without anyone logging in. For LM Studio: a scheduled task at
  startup that runs `lms daemon up` and `lms server start --port 1234 --bind 0.0.0.0`.

## Usage

```sh
python3 llm_wake.py \
  --listen 0.0.0.0:1234 \
  --upstream http://192.168.1.50:1234 \
  --mac 00:11:22:33:44:55 \
  --broadcast 192.168.1.255 \
  --cache-dir /var/lib/llm-wake
```

Then use `http://<proxy>:1234/v1` as the base URL of an OpenAI-compatible provider in your client, with the key as its
API key. To require a key, start the proxy with it in the environment (not as a flag, flags show in the process list):

```sh
LLM_WAKE_API_KEY="$(cat /etc/llm-wake/api-key)" python3 llm_wake.py ...
curl -H "Authorization: Bearer $(cat /etc/llm-wake/api-key)" http://<proxy>:1234/v1/chat/completions ...
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `0.0.0.0:1234` | address and port to serve |
| `--upstream` | (required) | the LLM server on the PC |
| `--mac` | (required) | MAC address of the PC's network card |
| `--broadcast` | `255.255.255.255` | where the magic packet goes; use the subnet broadcast (e.g. `192.168.1.255`) on macOS and when the proxy has several interfaces |
| `--wake-timeout` | `240` | seconds to hold a request while the PC boots; keep it below your client's own timeout |
| `--grace` | `15` | seconds to wait after the PC first answers |
| `--resend` | `15` | seconds between magic packets while waiting |
| `--cache-dir` | none | where copies of `GET` answers are kept; without it a `GET` to a sleeping PC gets `503` |

### systemd

```ini
[Unit]
Description=Wake-on-LAN proxy for the LLM server
After=network-online.target
Wants=network-online.target

[Service]
DynamicUser=yes
StateDirectory=llm-wake
# one line LLM_WAKE_API_KEY=<key>, mode 0600, owned by root
EnvironmentFile=/etc/llm-wake/env
ExecStart=/usr/bin/python3 -I /opt/llm-wake/llm_wake.py --listen 0.0.0.0:1234 --upstream http://192.168.1.50:1234 --mac 00:11:22:33:44:55 --broadcast 192.168.1.255 --cache-dir /var/lib/llm-wake
Restart=on-failure
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX

[Install]
WantedBy=multi-user.target
```

The proxy uses about 15 MB of memory.

## Security

Without `LLM_WAKE_API_KEY` anyone who can reach the port can use the model and wake the PC, and a web page opened in a
browser on the same LAN can send it requests blindly (llm-wake adds no CORS headers, so the page cannot read the
answers). Set a key. llm-wake speaks plain HTTP and has no rate limits: keep it inside the LAN or a VPN and do not
expose it to the internet. It can only reach the one `--upstream` given at start; nothing about it can be changed over
the network.

## Comparison

Other ways to wake a machine on demand, as of November 2026:

| | llm-wake | [Wakezilla](https://github.com/guibeira/wakezilla) | [caddy-wol](https://github.com/dulli/caddy-wol) | [wol-proxy of llama-swap](https://github.com/mostlygeek/llama-swap/tree/main/cmd/wol-proxy) |
| --- | --- | --- | --- | --- |
| What it is | one Python file | Rust service with web dashboard, TUI, tray, network scanner, remote shutdown | Caddy plugin | Go tool in the llama-swap repo |
| Works with | any HTTP server with `/v1/models` | any TCP service | any HTTP service behind Caddy | llama-swap only (needs its `/api/events` and `/wol-health`) |
| Proxies | HTTP, streamed | TCP | HTTP (Caddy `reverse_proxy`) | HTTP |
| Ready when | `/v1/models` answers, plus a grace period | the TCP port accepts a connection | the upstream accepts a connection (Caddy retry) | llama-swap's event stream is connected |
| Wait while booting | `--wake-timeout`, 240 s by default | 60 s, fixed in code | `lb_try_duration` in the Caddyfile | `-timeout`, 60 s by default |
| Repeats the magic packet | every 15 s while waiting | one burst before the wait (count configurable) | at most once per 10 minutes per host | not described in its README |
| Requests that do not wake | all `GET`/`HEAD`, answered from a cache | none | none (any request that fails with an error) | `/api/events` only |
| Authentication | optional API key | none on the dashboard API (v0.2.14); shutdown requests to the client are authenticated | whatever Caddy is configured with | llama-swap's own `apiKeys` |
| Extra features | none | dashboard, auto shutdown after inactivity, network discovery | everything Caddy does (TLS, auth) | model swapping in llama-swap |
| Setup | `python3 llm_wake.py ...` | installer, Docker or Homebrew | build Caddy with `xcaddy` | `go build` |

Pick something else when: you want a dashboard and automatic shutdown (Wakezilla); you already run Caddy and need TLS
or authentication in front (caddy-wol); you serve models through llama-swap (its wol-proxy).

Sources: Wakezilla v0.2.14 (`src/forward.rs` for the 60 s wait and the TCP check); caddy-wol at commit `2d37979`
(`middleware.go`, README); llama-swap `cmd/wol-proxy/README.md` at commit `9cc0aec`.

## Tests

```sh
python3 -m unittest discover -s tests
```

The tests start llm-wake against a fake LM Studio on `127.0.0.1` and "wake" it by starting that server.

## License

MIT
