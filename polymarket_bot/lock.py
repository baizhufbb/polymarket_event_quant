from __future__ import annotations

import socket


LOCK_HOST = "127.0.0.1"
LOCK_PORT = 47831


class SingleInstance:
    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def __enter__(self) -> "SingleInstance":
        try:
            self.socket.bind((LOCK_HOST, LOCK_PORT))
            self.socket.listen(1)
        except OSError as exc:
            self.socket.close()
            raise RuntimeError("another bot instance is already running") from exc
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.socket.close()
