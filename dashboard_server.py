#!/usr/bin/env python3
"""
Sonderling Monitor — local dashboard server.
Proxies GitHub API calls server-side (PAT never in browser).
Runs at http://localhost:8765
"""
import json
import os
import threading
import urllib.error
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

PORT        = 9182
SCRIPT_DIR  = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / ".dashboard_config.json"
GITHUB_API  = "https://api.github.com"
OWNER       = "connorgu"
REPO        = "sonderling-monitor"


def get_token() -> str:
    if tok := os.environ.get("GITHUB_PAT", ""):
        return tok
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text()).get("token", "")
        except Exception:
            pass
    return ""


def save_token(token: str) -> None:
    CONFIG_FILE.write_text(json.dumps({"token": token}))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress default stdout logging

    def do_GET(self):
        if self.path in ("/", "/dashboard.html"):
            self._serve_file("dashboard.html", "text/html")
        elif self.path.startswith("/github-api/"):
            self._proxy_github()
        elif self.path == "/health":
            self._json(200, {"ok": True})
        else:
            self.send_error(404)

    def _serve_file(self, name: str, ctype: str) -> None:
        f = SCRIPT_DIR / name
        if not f.exists():
            self.send_error(404, f"{name} not found")
            return
        data = f.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", f"{ctype}; charset=utf-8")
        self.send_header("Content-Length", len(data))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    def _proxy_github(self) -> None:
        gh_path = self.path[len("/github-api"):]   # strip prefix
        token   = get_token()
        url     = GITHUB_API + gh_path

        req = urllib.request.Request(url, headers={
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent":           "SonderlingMonitor/1.0",
        })

        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                body   = r.read()
                status = r.status
        except urllib.error.HTTPError as e:
            body   = e.read()
            status = e.code

        self.send_response(status)
        self.send_header("Content-Type",                "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length",              len(body))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, data: dict) -> None:
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type",                "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length",              len(body))
        self.end_headers()
        self.wfile.write(body)


def ensure_token() -> None:
    """Prompt for PAT on first run; save to .dashboard_config.json."""
    if get_token():
        return
    print("\n  No GitHub token found.")
    print("  Enter your PAT (repo scope) — stored locally in .dashboard_config.json")
    print("  (This is only asked once)\n")
    tok = input("  Token: ").strip()
    if tok:
        save_token(tok)
        print("  ✓ Token saved.\n")
    else:
        print("  ✗ No token entered — API calls will fail.\n")


def main() -> None:
    ensure_token()

    class ReusableServer(HTTPServer):
        allow_reuse_address = True
    server = ReusableServer(("127.0.0.1", PORT), Handler)
    url    = f"http://localhost:{PORT}/dashboard.html"

    print(f"\n  ┌─────────────────────────────────────────┐")
    print(f"  │  SONDERLING MONITOR DASHBOARD            │")
    print(f"  │  {url:<40} │")
    print(f"  │  Ctrl+C to stop                          │")
    print(f"  └─────────────────────────────────────────┘\n")

    # Open browser after short delay so server is ready
    threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Dashboard stopped.")


if __name__ == "__main__":
    main()
