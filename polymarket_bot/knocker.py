"""Knocking runs in a Go library loaded into this process (knocker/).

The Go side sends each market's signed orders on the fleet's timetable over
its own HTTP/2 connections, judges every reply, stops when the venue has
registered the orders, and collects the replies still in flight. Python
signs the orders beforehand and decides afterwards which one to keep.

The library ships built: polymarket_bot/libknocker.so for the Linux server,
committed together with the sources it was built from, and
knocker/build/knocker.dll for tests on Windows, built by knocker/build.ps1
and never committed.

Every call passes JSON text through ctypes, which lets go of the GIL for as
long as the call runs, so a knock blocks only the thread that made it.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import logging
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

PACKAGE_DIR = Path(__file__).resolve().parent
KNOCKER_DIR = PACKAGE_DIR.parent / "knocker"
LINUX_LIBRARY = PACKAGE_DIR / "libknocker.so"
WINDOWS_LIBRARY = KNOCKER_DIR / "build" / "knocker.dll"
ORDER_URL = "https://clob.polymarket.com/order"
# How long one collection of replies for the trace waits for the first.
_TRACE_WAIT_MS = 200


class KnockerError(RuntimeError):
    """The library refused a call or could not be loaded."""


@dataclass(frozen=True)
class Hooks:
    """Where one account's replies go: the attempt trace, and the fix for a
    reply saying the venue wants another order version."""

    trace: Callable[[dict], None] | None = None
    version_mismatch: Callable[[], None] | None = None


@dataclass(frozen=True)
class Verdict:
    trace: str
    accepted: bool
    order_id: str | None
    duplicate_id: str | None
    not_ready: bool


def library_path() -> Path:
    return WINDOWS_LIBRARY if sys.platform == "win32" else LINUX_LIBRARY


def source_hash(root: Path = KNOCKER_DIR) -> str:
    """Hash of the Go sources the library is built from.

    The build stamps it into the library, so a library that is behind its
    sources shows. Line endings are read as \\n so a Windows checkout and
    the server agree.
    """
    digest = hashlib.sha256()
    sources = sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and _library_source(path.relative_to(root).as_posix())
    )
    for relative in sources:
        digest.update(relative.encode() + b"\n")
        digest.update((root / relative).read_bytes().replace(b"\r\n", b"\n"))
        digest.update(b"\n")
    return digest.hexdigest()


def _library_source(relative: str) -> bool:
    if relative in ("go.mod", "go.sum"):
        return True
    if not relative.endswith(".go") or relative.endswith("_test.go"):
        return False
    return not relative.startswith(("cmd/", "internal/fakevenue/", "build/"))


_lock = threading.Lock()
_library: ctypes.CDLL | None = None
_hooks: dict[str, Hooks] = {}
_trace_thread: threading.Thread | None = None


def _lib() -> ctypes.CDLL:
    global _library
    with _lock:
        if _library is None:
            path = library_path()
            if not path.exists():
                raise KnockerError(
                    f"knock library not found at {path}; build it with knocker/build.ps1"
                )
            library = ctypes.CDLL(str(path))
            for name in ("KnockerKnock", "KnockerClassify"):
                function = getattr(library, name)
                function.argtypes = [ctypes.c_char_p]
                function.restype = ctypes.c_void_p
            library.KnockerNextAttempts.argtypes = [ctypes.c_int]
            library.KnockerNextAttempts.restype = ctypes.c_void_p
            for name in ("KnockerVersion", "KnockerStop"):
                function = getattr(library, name)
                function.argtypes = []
                function.restype = ctypes.c_void_p
            library.KnockerFree.argtypes = [ctypes.c_void_p]
            library.KnockerFree.restype = None
            _library = library
        return _library


def _call(name: str, *args) -> object:
    library = _lib()
    pointer = getattr(library, name)(*args)
    try:
        text = ctypes.string_at(pointer)
    finally:
        library.KnockerFree(pointer)
    answer = json.loads(text)
    if "error" in answer:
        raise KnockerError(answer["error"])
    return answer["ok"]


def version() -> dict:
    return _call("KnockerVersion")


def classify(reply: object) -> Verdict:
    """Judge a reply the way the knock judges one."""
    got = _call("KnockerClassify", json.dumps(reply, default=str).encode())
    return Verdict(
        trace=got["trace"],
        accepted=got["accepted"],
        order_id=got["order_id"] or None,
        duplicate_id=got["duplicate_id"] or None,
        not_ready=got["not_ready"],
    )


def knock(plan: dict, hooks: dict[str, Hooks]) -> dict:
    """Knock one market for every member of plan; returns once all are done.

    hooks, by account, receive that account's replies - also the ones that
    land after this call has returned.

    The knock runs on a thread of its own while this one waits, so Ctrl+C
    (the launcher's stop) still lands here at once. It then has every member
    stop sending and waits for what they registered - their replies in
    flight take at most the three-second drain - and returns that, so the
    orders are recorded and cancelled on the way out rather than left at the
    venue unknown. take_interrupt() then reports the stop, for the caller to
    act on once the result is written down.
    """
    with _lock:
        _hooks.update(hooks)
    _start_trace_thread()
    payload = json.dumps(plan).encode()
    outcome: dict = {}

    def run() -> None:
        try:
            outcome["result"] = _call("KnockerKnock", payload)
        except BaseException as exc:  # noqa: BLE001 - handed to the caller
            outcome["error"] = exc

    worker = threading.Thread(target=run, name="knock", daemon=True)
    worker.start()
    interrupted = stopped = False
    while True:
        try:
            if interrupted and not stopped:
                _call("KnockerStop")
                stopped = True
            worker.join(0.1)
            if not worker.is_alive():
                break
        except KeyboardInterrupt:
            interrupted = True
    if interrupted:
        _interrupted.set()
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


_interrupted = threading.Event()


def take_interrupt() -> bool:
    """Whether a stop arrived during a knock since the last call."""
    if _interrupted.is_set():
        _interrupted.clear()
        return True
    return False


def _start_trace_thread() -> None:
    global _trace_thread
    with _lock:
        if _trace_thread is None or not _trace_thread.is_alive():
            _trace_thread = threading.Thread(
                target=_collect_replies, name="knock-trace", daemon=True
            )
            _trace_thread.start()


def _collect_replies() -> None:
    while True:
        try:
            batch = _call("KnockerNextAttempts", _TRACE_WAIT_MS)
        except Exception:  # noqa: BLE001 - the trace must outlive a bad call
            logger.exception("[knocker] collecting replies failed")
            time.sleep(1.0)
            continue
        for attempt in batch:
            try:
                _record(attempt)
            except Exception:  # noqa: BLE001 - one bad hook must not stop the rest
                logger.exception("[knocker] recording a reply failed")


def _record(attempt: dict) -> None:
    status = attempt.get("status", 0)
    if status == 0:
        logger.error("[knocker] request error: %s", attempt.get("error") or "no reply")
    elif status != 200:
        logger.error(
            "[knocker] request error status=%s url=%s body=%s",
            status,
            ORDER_URL,
            attempt.get("body", ""),
        )
    with _lock:
        hooks = _hooks.get(attempt["account"])
    if hooks is None:
        return
    if hooks.trace is not None:
        hooks.trace(
            {
                "attempt": attempt["attempt"],
                "legs": attempt["legs"],
                "sent_ts_ms": attempt["sent_ts_ms"],
                "returned_ts_ms": attempt["returned_ts_ms"],
                "results": attempt["results"],
            }
        )
    if attempt.get("version_mismatch") and hooks.version_mismatch is not None:
        hooks.version_mismatch()


# The knock reports replies raw; these word them the way the Python sender
# did, so the errors stored with a placement read as they always have.


def reply_value(status: int, body: str) -> object:
    """A reply as the sender held it: a 200 is its JSON (or its text), any
    other status folds into {"errorMsg": ..., "success": False}."""
    if status == 200:
        return _parsed(body)
    error_msg = _parsed(body)
    payload = error_msg if isinstance(error_msg, dict) else {}
    message = str(payload.get("error") or error_msg or _exception_text(status, error_msg))
    return {"errorMsg": message, "success": False}


def error_text(item: dict) -> str:
    """One way a knock ended without an order."""
    if "text" in item:
        return item["text"]
    return str(reply_value(item["status"], item["body"])) or "partial placement"


def ambiguous_text(item: dict) -> str:
    """A send that came back without a verdict, or replies that disagree."""
    if "text" in item:
        return item["text"]
    status = item["status"]
    if status == 0:
        return (
            "PolyApiException: "
            "PolyApiException[status_code=None, error_message=Request exception!]"
        )
    return f"PolyApiException: {_exception_text(status, _parsed(item['body']))}"


def _parsed(body: str) -> object:
    try:
        return json.loads(body)
    except ValueError:
        return body


def _exception_text(status: int, error_msg: object) -> str:
    return f"PolyApiException[status_code={status}, error_message={error_msg}]"
