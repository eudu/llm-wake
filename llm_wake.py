#!/usr/bin/env python3
"""Wake-on-LAN proxy in front of a local LLM server (LM Studio, Ollama, llama.cpp, any HTTP API) on a PC that sleeps.

Clients (coding agents, scripts, cron jobs) talk to this proxy instead of the PC. A request goes through at once
while the PC answers; otherwise the proxy sends a magic packet (again every few seconds: a packet that arrives while the
PC is still shutting down is lost), holds the request until the model list answers and forwards it. Right after a
wake the server may still fail while it loads the model, so for --retry-window seconds a 5xx, a dropped connection or
a stream that opens with an error about an unloaded model (LM Studio unloads a model whose idle TTL ran out during
the sleep right as the PC wakes) is retried: nothing has reached the client yet. A server that was up all along gets
no retries. Responses are streamed, so SSE completions arrive as
they are generated. Only a request that uses the model (POST and the like) wakes the PC: GET is how clients read the
model lists on their own (/v1/models, /api/v1/models), so while the PC sleeps a GET gets the last copy of that path.
GET /llm-wake/health tells whether the PC answers, without waking it. With an API key (LLM_WAKE_API_KEY) every other
request needs "Authorization: Bearer <key>"; without it the answer is 401 and the PC is not woken. The key is not
passed on to the PC.

Standard library only, one file; run it as a service on an always-on machine in the same LAN as the PC.
https://github.com/eudu/llm-wake
"""
import argparse
import hmac
import http.client
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re
import socket
import threading
import time
import urllib.parse

HEALTH_PATH = '/llm-wake/health'
# headers of one connection, not of the request or the response (RFC 9110, 7.6.1)
HOP_BY_HOP = {
    'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'proxy-connection',
    'te', 'trailer', 'transfer-encoding', 'upgrade', 'host',
}
log = logging.getLogger('llm-wake')


def magic_packet(mac):
    digits = re.sub(r'[^0-9a-fA-F]', '', mac)
    if len(digits) != 12:
        raise ValueError(f'not a MAC address: {mac}')
    return b'\xff' * 6 + bytes.fromhex(digits) * 16


def send_magic_packet(mac, broadcast, port=9):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.sendto(magic_packet(mac), (broadcast, port))


class Waker:
    """Checks the upstream and wakes it once for all the requests that wait at the same time."""

    def __init__(self, host, port, ready_path, wake, wake_timeout, grace, poll, resend, retry_window):
        self.host, self.port, self.ready_path = host, port, ready_path
        self.wake, self.wake_timeout, self.grace, self.poll, self.resend = wake, wake_timeout, grace, poll, resend
        self.retry_window = retry_window
        # until this moment (time.monotonic) failures of the upstream are taken for a model still loading after a wake
        self.retry_until = 0.0
        self.lock = threading.Lock()

    def just_woken(self):
        return time.monotonic() < self.retry_until

    def answers(self, timeout=2.0):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        try:
            conn.request('GET', self.ready_path)
            response = conn.getresponse()
            response.read()
            return response.status == 200
        except OSError:
            return False
        finally:
            conn.close()

    def ensure_up(self):
        if self.answers():
            return True
        with self.lock:
            # another request may have woken it while this one waited for the lock
            if self.answers():
                return True
            log.info('upstream %s:%s does not answer, sending Wake-on-LAN', self.host, self.port)
            self.wake()
            sent = time.monotonic()
            deadline = sent + self.wake_timeout
            while time.monotonic() < deadline:
                time.sleep(self.poll)
                if time.monotonic() - sent >= self.resend:
                    self.wake()
                    sent = time.monotonic()
                if self.answers():
                    log.info('upstream answers after %.0f s', time.monotonic() - deadline + self.wake_timeout)
                    time.sleep(self.grace)
                    self.retry_until = time.monotonic() + self.retry_window
                    return True
            log.warning('upstream did not answer within %.0f s', self.wake_timeout)
            return False


class FirstChunk:
    """The first piece of a streamed body, read aside: LM Studio sends nothing while it reads a long prompt, so the
    wait for it ends with the retry window, and the rest of the stream goes to the client as usual."""

    def __init__(self, response, wait):
        self.data, self.error = None, None
        self.reader = threading.Thread(target=self.read, args=(response,), daemon=True)
        self.reader.start()
        self.reader.join(max(wait, 0.0))

    def read(self, response):
        try:
            self.data = response.read1(65536)
        except OSError as caught:
            self.error = caught

    def unloaded(self):
        # LM Studio: event: error, data: {"error":{"message":"Model is unloaded."}, ...}
        return bool(self.data) and self.data.startswith(b'event: error') and b'unload' in self.data.lower()

    def get(self):
        self.reader.join()
        if self.error is not None:
            raise self.error
        return self.data


class Handler(BaseHTTPRequestHandler):
    # HTTP/1.0: every response ends by closing the connection, so a streamed body needs no re-chunking
    server_version = 'llm-wake'

    def log_message(self, fmt, *args):
        log.info('%s %s', self.address_string(), fmt % args)

    def do_GET(self):
        self.handle_any()

    do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = do_GET

    def reply_json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    def handle_any(self):
        try:
            self.route()
        except (BrokenPipeError, ConnectionResetError):
            log.info('%s went away before the answer', self.address_string())

    def route(self):
        server = self.server
        path = urllib.parse.urlsplit(self.path).path
        length = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(length) if length else None
        if path == HEALTH_PATH:
            self.reply_json(200, {'upstream': server.waker.answers(), 'url': server.upstream})
            return
        if not self.authorized():
            self.reply_json(401, {'error': {'message': 'missing or wrong API key of llm-wake',
                                            'type': 'llm_wake_unauthorized'}})
            return
        if self.command in ('GET', 'HEAD'):
            self.read_only(path, body)
            return
        if not server.waker.ensure_up():
            self.reply_json(504, {'error': {
                'message': f'LM Studio at {server.upstream} did not answer within {server.waker.wake_timeout:.0f} s '
                           'after Wake-on-LAN',
                'type': 'llm_wake_timeout',
            }})
            return
        self.forward(body)

    def authorized(self):
        key = self.server.api_key
        if not key:
            return True
        given = self.headers.get('Authorization') or ''
        return given.startswith('Bearer ') and hmac.compare_digest(given[7:].encode(), key.encode())

    def read_only(self, path, body):
        """GET from the PC when it answers (and keep a copy of the path), else the copy; never wakes the PC."""
        cache = None
        if self.server.cache_dir:
            cache = self.server.cache_dir / (re.sub(r'[^\w.-]', '_', path.strip('/')) + '.body')
        if self.server.waker.answers():
            self.forward(body, keep=cache)
            return
        if cache and cache.is_file():
            data = cache.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(data)
            return
        self.reply_json(503, {'error': {'message': f'LM Studio sleeps and {path} is not cached yet; GET does not wake it',
                                        'type': 'llm_wake_sleeping'}})

    def forward(self, body, keep=None):
        waker = self.server.waker
        skip = HOP_BY_HOP | ({'authorization'} if self.server.api_key else set())
        headers = {k: v for k, v in self.headers.items() if k.lower() not in skip}
        headers['Host'] = f'{waker.host}:{waker.port}'
        attempt = 0
        while True:
            attempt += 1
            conn = http.client.HTTPConnection(waker.host, waker.port, timeout=self.server.read_timeout)
            try:
                conn.request(self.command, self.path, body=body, headers=headers)
                response = conn.getresponse()
                error = None
            except OSError as caught:
                conn.close()
                response, error = None, caught
            failed = error is not None or response.status >= 500
            first = None
            if not failed and waker.just_woken() and 'event-stream' in (response.getheader('Content-Type') or ''):
                first = FirstChunk(response, waker.retry_until - time.monotonic())
                failed = first.unloaded()
            if not failed or not waker.just_woken():
                break
            # the model is still loading after a wake: the client has got nothing yet, ask again
            reason = 'unloaded the model' if first else error or response.status
            log.info('upstream %s after a wake, retrying (attempt %d)', reason, attempt)
            if response is not None:
                if not first:
                    response.read()
                conn.close()
            time.sleep(waker.poll)
        if error is not None:
            self.reply_json(502, {'error': {'message': f'LM Studio at {self.server.upstream}: {error}',
                                            'type': 'llm_wake_upstream'}})
            return
        try:
            self.send_response(response.status, response.reason)
            chunked = (response.getheader('Transfer-Encoding') or '').lower() == 'chunked'
            for name, value in response.getheaders():
                if name.lower() in HOP_BY_HOP or (chunked and name.lower() == 'content-length'):
                    continue
                self.send_header(name, value)
            self.send_header('Connection', 'close')
            self.end_headers()
            if self.command == 'HEAD':
                return
            if first is not None and (chunk := first.get()):
                self.wfile.write(chunk)
                self.wfile.flush()
            if keep is not None and response.status == 200:
                data = response.read()
                keep.parent.mkdir(parents=True, exist_ok=True)
                keep.write_bytes(data)
                self.wfile.write(data)
                return
            while chunk := response.read1(65536):
                self.wfile.write(chunk)
                self.wfile.flush()
        finally:
            conn.close()


def make_server(listen, upstream, wake, ready_path='/v1/models', wake_timeout=240.0, grace=0.0, poll=2.0, resend=15.0,
                cache_dir=None, read_timeout=900.0, api_key=None, retry_window=60.0):
    parsed = urllib.parse.urlsplit(upstream)
    server = ThreadingHTTPServer(listen, Handler)
    server.daemon_threads = True
    server.upstream = upstream
    server.waker = Waker(parsed.hostname, parsed.port or 80, ready_path, wake, wake_timeout, grace, poll, resend,
                         retry_window)
    server.cache_dir = Path(cache_dir) if cache_dir else None
    server.read_timeout = read_timeout
    server.api_key = api_key or None
    return server


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--listen', default='0.0.0.0:1234', help='address:port to serve')
    parser.add_argument('--upstream', required=True, help='the LLM server on the PC, e.g. http://192.168.1.50:1234')
    parser.add_argument('--mac', required=True, help='MAC of the PC network card')
    parser.add_argument('--broadcast', default='255.255.255.255', help='where the magic packet goes')
    # opencode gives up on response headers after 300 s: answer 504 before that
    parser.add_argument('--wake-timeout', type=float, default=240.0, help='seconds to hold a request while the PC boots')
    parser.add_argument('--grace', type=float, default=0.0, help='seconds to wait after the PC first answers')
    parser.add_argument('--retry-window', type=float, default=60.0,
                        help='seconds after a wake in which a 5xx, a dropped connection or an unloaded model is retried')
    parser.add_argument('--resend', type=float, default=15.0, help='seconds between magic packets while waiting')
    parser.add_argument('--cache-dir', default=None, help='copies of GET answers served while the PC sleeps')
    args = parser.parse_args(argv)
    # from the environment, not a flag: a flag shows in the process list
    api_key = os.environ.get('LLM_WAKE_API_KEY', '').strip()
    logging.basicConfig(level=logging.INFO, format='%(name)s: %(message)s')
    host, _, port = args.listen.rpartition(':')
    magic_packet(args.mac)  # refuse a bad MAC at start, not at the first wake
    server = make_server(
        (host, int(port)),
        args.upstream,
        wake=lambda: send_magic_packet(args.mac, args.broadcast),
        wake_timeout=args.wake_timeout,
        grace=args.grace,
        resend=args.resend,
        retry_window=args.retry_window,
        cache_dir=args.cache_dir,
        api_key=api_key,
    )
    log.info('listening on %s, upstream %s, MAC %s, API key %s', args.listen, args.upstream, args.mac,
             'required' if api_key else 'off')
    server.serve_forever()


if __name__ == '__main__':
    main()
