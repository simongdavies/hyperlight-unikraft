#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.

import json
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tools" / "run-workerd-vfs-demo.sh"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        bodies = {
            "/evidence/vfs-bundle": {
                "outcome": "pass",
                "readable": True,
                "entries": [],
                "immutable": True,
                "writeError": {"code": "EROFS"},
            },
            "/evidence/vfs-tmp": {
                "outcome": "pass",
                "previousExists": False,
                "body": "tmp-read-write-ok",
            },
            "/evidence/vfs-dev-null": {
                "outcome": "pass",
                "written": 10,
                "read": 0,
            },
            "/evidence/vfs-dev-zero": {
                "outcome": "pass",
                "read": 32,
                "allZero": True,
            },
            "/evidence/vfs-dev-random": {
                "outcome": "pass",
                "firstRead": 32,
                "secondRead": 32,
                "firstNonZero": True,
                "secondNonZero": True,
                "distinct": True,
            },
        }
        body = bodies.get(self.path, {"outcome": "fail"})
        if self.path == "/evidence/vfs-dev-zero" and self.server.fail_zero:
            body = {"outcome": "fail", "read": 32, "allZero": False}
        payload = json.dumps(body).encode()
        self.send_response(200 if self.path in bodies else 404)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        pass


def run(base_url, output_dir, *args, stdin=None):
    return subprocess.run(
        [
            "bash",
            str(RUNNER),
            "--base-url",
            base_url,
            "--output-dir",
            str(output_dir),
            *args,
        ],
        cwd=ROOT,
        stdin=stdin,
        text=True,
        capture_output=True,
        check=False,
    )


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.fail_zero = False
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        listed = subprocess.run(
            ["bash", str(RUNNER), "--list"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert listed.returncode == 0, listed.stderr
        assert "tmp-reset" in listed.stdout
        assert "dev-random" in listed.stdout

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            complete = run(base_url, output, "all")
            assert complete.returncode == 0, complete.stderr
            assert "PASS: 5" in complete.stdout
            assert (output / "bundle.json").is_file()
            assert (output / "tmp-reset-first.json").is_file()
            assert (output / "tmp-reset-second.json").is_file()
            assert (output / "dev-random.json").is_file()

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            selected = run(base_url, output, "bundle", "dev-null")
            assert selected.returncode == 0, selected.stderr
            assert "PASS: 2" in selected.stdout
            assert not (output / "dev-zero.json").exists()

        with tempfile.TemporaryDirectory() as directory:
            server.fail_zero = True
            failed = run(base_url, Path(directory), "dev-zero", "dev-random")
            assert failed.returncode != 0
            assert "PASS: 1" in failed.stdout
            assert "FAIL: 1" in failed.stdout
            assert (Path(directory) / "dev-random.json").is_file()
            server.fail_zero = False

        unknown = run(base_url, Path(tempfile.gettempdir()), "not-a-check")
        assert unknown.returncode == 2
        assert "unknown check" in unknown.stderr

        paused = run(
            base_url,
            Path(tempfile.gettempdir()),
            "--pause",
            "bundle",
            stdin=subprocess.DEVNULL,
        )
        assert paused.returncode == 2
        assert "--pause requires terminal stdin" in paused.stderr
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    print("run-workerd-vfs-demo tests passed")


if __name__ == "__main__":
    main()
