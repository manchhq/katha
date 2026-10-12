"""Test-only HTTP proxy. Inject a lost reply after a real remote commit.

Invoked by the ignored Rust integration test. Credentials come from the SDK's
request headers; this process never logs headers, bodies or remote URLs.
"""
import http.server
import os
import socket
import threading
import time
import urllib.error
import urllib.request

remote = os.environ["KATHA_TEST_TURSO_URL"].replace("libsql://", "https://").replace("turso://", "https://").rstrip("/")
mode = os.environ.get("KATHA_TEST_PROXY_FAULT", "drop")
lock = threading.Lock()
fired = False

class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def forward(self):
        global fired
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        headers = {key: value for key, value in self.headers.items()
                   if key.lower() not in ("host", "connection", "content-length", "accept-encoding")}
        request = urllib.request.Request(remote + self.path, data=body if body else None,
                                         headers=headers, method=self.command)
        try:
            response = urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        except Exception:
            self.send_error(502, "Upstream unavailable")
            return
        with response:
            content = response.read()
            status = response.status
            response_headers = list(response.headers.items())
        inject = False
        if 200 <= status < 300 and "pipeline" in self.path and b"katha_events" in body and b"INSERT" in body:
            with lock:
                if not fired:
                    fired = True
                    inject = True
        if inject:
            print("COMMITTED", flush=True)
            if mode == "drop":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            time.sleep(2)
        self.send_response(status)
        for key, value in response_headers:
            if key.lower() not in ("transfer-encoding", "content-length", "connection", "server", "date"):
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(content)
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True

    do_GET = forward
    do_POST = forward

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
print(f"http://127.0.0.1:{server.server_port}", flush=True)
server.serve_forever()
