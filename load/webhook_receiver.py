"""Local load-test sink only; no auth, persistence or production guarantees."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        if self.path != "/payments":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1_048_576:
            self.send_error(413)
            return
        self.rfile.read(length)
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *_args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
