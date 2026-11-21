"""Helper that boots a real uvicorn server on an ephemeral port (tests only)."""

from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager

import uvicorn

from .config import Settings
from .main import create_app


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextmanager
def run_server(database_path: str):
    port = _free_port()
    config = uvicorn.Config(
        create_app(settings=Settings(database_path=database_path)),
        host="127.0.0.1",
        port=port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    while not server.started:
        if time.time() > deadline:  # pragma: no cover
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)
