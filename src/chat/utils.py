from .protocoltypes import Command
from typing import Any
from time import time
import msgpack


def mk_pack(
    command: Command | int,
    id: int,
    name: str,
    content: Any,
    chan: int = 0,
    to: int = 0,
    ts: float | None = None,
) -> bytes:
    if ts is None:
        ts = time()
    return msgpack.packb((command, id, name, content, chan, to, ts))  # type: ignore
