"""Tests of llm_wake.py: the proxy is started against a fake LM Studio on 127.0.0.1, and "waking" starts that server.

Run: python3 -m unittest discover -s tests
"""
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('llm_wake', ROOT / 'llm_wake.py')
llm_wake = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(llm_wake)

MODELS = {'object': 'list', 'data': [{'id': 'qwen/qwen3.8-27b', 'object': 'model'}]}


class FakeLmStudio(BaseHTTPRequestHandler):
    """/v1/models, and an echo of the request streamed back in two chunks like an SSE completion."""

    # headers of the last request, to see what the proxy passes on
    last_headers = None

    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path not in ('/v1/models', '/api/v1/models'):
            self.send_error(404)
            return
        body = json.dumps(MODELS).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        FakeLmStudio.last_headers = dict(self.headers)
        body = self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Transfer-Encoding', 'chunked')
        self.end_headers()
        for part in [b'data: ' + body + b'\n\n', b'data: [DONE]\n\n']:
            self.wfile.write(b'%x\r\n%s\r\n' % (len(part), part))
            self.wfile.flush()
            time.sleep(0.05)
        self.wfile.write(b'0\r\n\r\n')


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Upstream:
    def __init__(self, port):
        self.port = port
        self.server = None

    def start(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', self.port), FakeLmStudio)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.server = None


class ProxyTest(unittest.TestCase):
    def setUp(self):
        self.upstream = Upstream(free_port())
        self.wakes = []
        self.cache = Path(tempfile.mkdtemp())
        self.addCleanup(self.upstream.stop)

    def start_proxy(self, wake=None, wake_timeout=5.0, grace=0.0, resend=10.0, api_key=None):
        def default_wake():
            self.wakes.append(time.monotonic())
            # the PC boots for a while before the server answers
            threading.Timer(0.5, self.upstream.start).start()

        proxy = llm_wake.make_server(
            ('127.0.0.1', 0),
            f'http://127.0.0.1:{self.upstream.port}',
            wake=wake or default_wake,
            wake_timeout=wake_timeout,
            grace=grace,
            poll=0.1,
            resend=resend,
            cache_dir=self.cache,
            api_key=api_key,
        )
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        self.addCleanup(proxy.server_close)
        self.addCleanup(proxy.shutdown)
        return f'http://127.0.0.1:{proxy.server_address[1]}'

    def post(self, base, payload, headers=None):
        request = urllib.request.Request(
            base + '/v1/chat/completions',
            data=json.dumps(payload).encode(),
            headers={'Content-Type': 'application/json', **(headers or {})},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, response.headers.get('Content-Type'), response.read()

    def get(self, base, path, headers=None):
        try:
            with urllib.request.urlopen(urllib.request.Request(base + path, headers=headers or {}), timeout=20) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def test_magic_packet_is_six_ff_and_sixteen_macs(self):
        packet = llm_wake.magic_packet('00:11:22:33:44:55')
        self.assertEqual(len(packet), 102)
        self.assertEqual(packet[:6], b'\xff' * 6)
        self.assertEqual(packet[6:], bytes.fromhex('001122334455') * 16)

    def test_forwards_a_streamed_completion_when_the_pc_is_up(self):
        self.upstream.start()
        base = self.start_proxy()
        status, content_type, body = self.post(base, {'model': 'qwen/qwen3.8-27b', 'stream': True})
        self.assertEqual(status, 200)
        self.assertEqual(content_type, 'text/event-stream')
        self.assertEqual(body, b'data: {"model": "qwen/qwen3.8-27b", "stream": true}\n\ndata: [DONE]\n\n')
        self.assertEqual(self.wakes, [])

    def test_wakes_the_pc_and_holds_the_request_until_it_answers(self):
        base = self.start_proxy(grace=0.3)
        started = time.monotonic()
        status, _, body = self.post(base, {'model': 'qwen/qwen3.8-27b'})
        self.assertEqual(status, 200)
        self.assertIn(b'qwen/qwen3.8-27b', body)
        self.assertEqual(len(self.wakes), 1)
        # boot (0.5 s) plus the grace after the first answer
        self.assertGreaterEqual(time.monotonic() - started, 0.8)

    def test_parallel_requests_share_one_wake(self):
        base = self.start_proxy()
        with ThreadPoolExecutor(4) as pool:
            results = list(pool.map(lambda i: self.post(base, {'n': i}), range(4)))
        self.assertEqual([r[0] for r in results], [200] * 4)
        self.assertEqual(len(self.wakes), 1)

    def test_gives_up_with_504_when_the_pc_does_not_answer(self):
        base = self.start_proxy(wake=lambda: self.wakes.append(time.monotonic()), wake_timeout=0.5)
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.post(base, {'model': 'qwen/qwen3.8-27b'})
        self.assertEqual(caught.exception.code, 504)
        self.assertIn('error', json.loads(caught.exception.read()))
        self.assertEqual(len(self.wakes), 1)

    def test_repeats_the_magic_packet_while_waiting(self):
        # a packet sent while the PC is still shutting down is lost; the next one wakes it
        def wake_on_second_packet():
            self.wakes.append(time.monotonic())
            if len(self.wakes) == 2:
                self.upstream.start()

        base = self.start_proxy(wake=wake_on_second_packet, wake_timeout=3.0, resend=0.3)
        status, _, _ = self.post(base, {'model': 'qwen/qwen3.8-27b'})
        self.assertEqual(status, 200)
        self.assertEqual(len(self.wakes), 2)

    def test_model_lists_are_served_from_cache_without_waking(self):
        # opencode and bb read the lists on their own; only a request for the model may wake the PC
        self.upstream.start()
        base = self.start_proxy()
        for path in ['/v1/models', '/api/v1/models']:
            self.assertEqual(self.get(base, path), (200, json.dumps(MODELS).encode()))
        self.upstream.stop()
        for path in ['/v1/models', '/api/v1/models']:
            status, body = self.get(base, path)
            self.assertEqual((status, json.loads(body)), (200, MODELS))
        self.assertEqual(self.wakes, [])

    def test_get_without_cache_does_not_wake(self):
        base = self.start_proxy()
        for path in ['/v1/models', '/api/v1/models', '/api/v0/models']:
            status, _ = self.get(base, path)
            self.assertEqual(status, 503)
        self.assertEqual(self.wakes, [])

    def test_health_reports_the_upstream_without_waking(self):
        base = self.start_proxy()
        status, body = self.get(base, '/llm-wake/health')
        self.assertEqual((status, json.loads(body)['upstream']), (200, False))
        self.upstream.start()
        status, body = self.get(base, '/llm-wake/health')
        self.assertEqual((status, json.loads(body)['upstream']), (200, True))
        self.assertEqual(self.wakes, [])


    def test_with_a_key_a_request_without_it_gets_401_and_does_not_wake(self):
        base = self.start_proxy(api_key='secret')
        for headers in [None, {'Authorization': 'Bearer wrong'}, {'Authorization': 'secret'}]:
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.post(base, {'model': 'qwen/qwen3.8-27b'}, headers=headers)
            self.assertEqual(caught.exception.code, 401)
            self.assertEqual(json.loads(caught.exception.read())['error']['type'], 'llm_wake_unauthorized')
        status, _ = self.get(base, '/v1/models')
        self.assertEqual(status, 401)
        self.assertEqual(self.wakes, [])

    def test_with_the_key_the_request_goes_through_without_it(self):
        base = self.start_proxy(api_key='secret')
        status, _, body = self.post(base, {'model': 'qwen/qwen3.8-27b'}, headers={'Authorization': 'Bearer secret'})
        self.assertEqual(status, 200)
        self.assertIn(b'qwen/qwen3.8-27b', body)
        self.assertEqual(len(self.wakes), 1)
        # the key is the proxy's own; the server on the PC does not get it
        self.assertNotIn('Authorization', FakeLmStudio.last_headers)

    def test_health_needs_no_key(self):
        base = self.start_proxy(api_key='secret')
        status, body = self.get(base, '/llm-wake/health')
        self.assertEqual((status, json.loads(body)['upstream']), (200, False))


if __name__ == '__main__':
    unittest.main()
