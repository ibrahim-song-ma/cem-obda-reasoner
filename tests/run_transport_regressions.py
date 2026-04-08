#!/usr/bin/env python3
"""Transport regressions for localhost proxy bypass in the OBDA client."""

from __future__ import annotations

import json
import os
import socketserver
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CLIENT = ROOT / ".agents" / "skills" / "obda-query" / "scripts" / "obda_api.py"
PYTHON = ROOT / ".venv" / "bin" / "python"


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


class RealSchemaHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/schema":
            payload = {
                "classes": [],
                "data_properties": [],
                "object_properties": [],
                "class_hierarchy": [],
            }
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        body = b'{"detail":"Not Found"}'
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


class FakeProxy404Handler(BaseHTTPRequestHandler):
    def do_CONNECT(self) -> None:  # noqa: N802
        body = b'{"detail":"Not Found"}'
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        body = b'{"detail":"Not Found"}'
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def start_server(handler: type[BaseHTTPRequestHandler]) -> tuple[ThreadedTCPServer, int]:
    server = ThreadedTCPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, int(server.server_address[1])


def main() -> int:
    real_server, real_port = start_server(RealSchemaHandler)
    proxy_server, proxy_port = start_server(FakeProxy404Handler)
    try:
        env = os.environ.copy()
        env["HTTP_PROXY"] = f"http://127.0.0.1:{proxy_port}"
        env["http_proxy"] = env["HTTP_PROXY"]
        env["HTTPS_PROXY"] = env["HTTP_PROXY"]
        env["https_proxy"] = env["HTTP_PROXY"]
        env["ALL_PROXY"] = env["HTTP_PROXY"]
        env["all_proxy"] = env["HTTP_PROXY"]
        env["NO_PROXY"] = ""
        env["no_proxy"] = ""

        result = subprocess.run(
            [
                str(PYTHON),
                str(CLIENT),
                "--base-url",
                f"http://127.0.0.1:{real_port}",
                "schema",
            ],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            sys.stderr.write(result.stderr or result.stdout)
            raise SystemExit(result.returncode or 1)

        payload = json.loads(result.stdout)
        summary = payload.get("schema_summary")
        if not isinstance(summary, dict):
            raise SystemExit("missing schema_summary in output")
        print("PASS transport_loopback_proxy_bypass")
        return 0
    finally:
        real_server.shutdown()
        real_server.server_close()
        proxy_server.shutdown()
        proxy_server.server_close()


if __name__ == "__main__":
    raise SystemExit(main())
