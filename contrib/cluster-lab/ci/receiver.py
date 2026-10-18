#!/usr/bin/env python3
"""Webhook receiver of the CI lab (compose.ci.yaml).

Appends one JSON line per delivery to the file named on the command line:
the delivery id, the event, the ref or the action, the repository and the
issue number when there is one. GET /health answers 200. The end-to-end
suite reads the file to prove each event was delivered exactly once.

Usage: receiver.py <host> <port> <file>
"""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST, PORT, OUT = sys.argv[1], int(sys.argv[2]), sys.argv[3]
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if self.path == "/health" else 404)
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw or b"{}")
        except ValueError:
            body = {}
        issue = body.get("issue") or body.get("pull_request") or {}
        record = {
            "delivery": self.headers.get("X-Forgejo-Delivery") or self.headers.get("X-Gitea-Delivery"),
            "event": self.headers.get("X-Forgejo-Event") or self.headers.get("X-Gitea-Event"),
            "ref": body.get("ref"),
            "after": body.get("after"),
            "action": body.get("action"),
            "repo": (body.get("repository") or {}).get("full_name"),
            "number": issue.get("number"),
            "title": issue.get("title"),
        }
        with LOCK, open(OUT, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, fmt, *args):
        sys.stderr.write("receiver: " + (fmt % args) + "\n")


open(OUT, "a", encoding="utf-8").close()
ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
