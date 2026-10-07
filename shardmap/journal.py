"""Append-only JSON-lines journal for crash recovery."""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path


class Journal:
    """Durable operation journal.

    Each record is one JSON object per line, appended fsync'd so that after
    a process crash the executor can reconstruct which job was in flight
    and decide to resume or safely abort it.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: dict) -> None:
        line = json.dumps(record, sort_keys=True) + "\n"
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())

    def read_all(self) -> list[dict]:
        with self._lock:
            if not self.path.exists():
                return []
            out = []
            with open(self.path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        out.append(json.loads(line))
            return out

    def clear(self) -> None:
        with self._lock:
            self.path.unlink(missing_ok=True)
