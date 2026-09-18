"""只增事件存储：内存索引 + JSONL 持久化。

- 事件按全局 seq 顺序追加，凭证内容永不修改、永不删除。
- event_id / idempotency_key 全局唯一，重复提交（断网补传、服务恢复后重放）
  返回同一份原始回执，保证幂等。
- 写操作在单把锁内串行化；读侧随时可基于事件流做折叠重算。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from .models import Event


class EventStore:
    def __init__(self, path: str | os.PathLike[str] | None = None):
        self._lock = threading.RLock()
        self._events: list[Event] = []
        self._by_event_id: dict[str, Event] = {}
        self._by_idem: dict[str, Event] = {}
        self._path = Path(path) if path else None
        if self._path:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._load()

    # ------------------------------------------------------------ 持久化
    def _load(self) -> None:
        assert self._path is not None
        if not self._path.exists():
            return
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                evt = Event(**row)
                self._index(evt)

    def _index(self, evt: Event) -> None:
        self._events.append(evt)
        self._by_event_id[evt.event_id] = evt
        if evt.idempotency_key:
            self._by_idem[evt.idempotency_key] = evt

    def _append(self, evt: Event) -> None:
        with self._lock:
            if self._path is not None:
                with self._path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(evt.to_dict(), ensure_ascii=False) + "\n")
            self._index(evt)

    # ------------------------------------------------------------ 接口
    def lookup(self, event_id: str | None = None, idem: str | None = None) -> Event | None:
        with self._lock:
            if event_id and event_id in self._by_event_id:
                return self._by_event_id[event_id]
            if idem and idem in self._by_idem:
                return self._by_idem[idem]
            return None

    def append(self, evt: Event) -> Event:
        with self._lock:
            existing = self.lookup(evt.event_id, evt.idempotency_key)
            if existing is not None:
                return existing  # 幂等：原样返回首次回执
            evt = Event(**{**evt.to_dict(), "seq": len(self._events) + 1})
            self._append(evt)
            return evt

    def events_for(self, invoice_ref: str) -> list[Event]:
        with self._lock:
            return [e for e in self._events if e.invoice_ref == invoice_ref]

    def all_events(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    @property
    def lock(self) -> threading.RLock:
        return self._lock
