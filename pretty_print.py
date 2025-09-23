import os
import sys
import time


def _detect_color_support() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    stream = sys.stdout
    is_tty = hasattr(stream, "isatty") and stream.isatty()
    if not is_tty:
        return False
    return True


_SUPPORTS_COLOR = _detect_color_support()


def _c(code: str) -> str:
    return code if _SUPPORTS_COLOR else ""


RESET = _c("\033[0m")
BOLD = _c("\033[1m")
DIM = _c("\033[2m")
ITAL = _c("\033[3m")

FG = {
    "grey": _c("\033[90m"),
    "red": _c("\033[91m"),
    "green": _c("\033[92m"),
    "yellow": _c("\033[93m"),
    "blue": _c("\033[94m"),
    "magenta": _c("\033[95m"),
    "cyan": _c("\033[96m"),
    "white": _c("\033[97m"),
}


def info(msg: str) -> None:
    print(f"{FG['blue']}ℹ{RESET} {msg}")


def ok(msg: str) -> None:
    print(f"{FG['green']}✔{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"{FG['yellow']}⚠{RESET} {msg}")


def err(msg: str) -> None:
    print(f"{FG['red']}✖{RESET} {msg}")


class StepTimer:
    """Контекстный менеджер для печати времени шага."""

    def __init__(self, start_msg: str, done_label: str = "done"):
        self.start_msg = start_msg
        self.done_label = done_label
        self.t0 = None

    def __enter__(self):
        info(self.start_msg)
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        dt = time.perf_counter() - self.t0
        if exc_type is None:
            ok(f"{self.done_label} {DIM}({dt:.2f}s){RESET}")
        else:
            err(f"failed {DIM}({dt:.2f}s){RESET}")
        # не подавляем исключения
        return False
