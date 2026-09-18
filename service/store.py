"""只增不改的凭证流水存储（WAL）。

- 所有业务事件以 JSON Lines 顺序追加，落盘前 fsync，保证断电/崩溃后可重放；
- 每条记录携带 seq 与哈希链（本记录哈希 = sha256(上一条哈希 + 本记录正文)），
  原始凭证摘要一旦写入即不可修改，任何篡改都会在重放校验时暴露；
- 进程内加线程锁，跨进程通过侧车锁文件 fcntl 互斥，支持断网补传并发写入。
"""
from __future__ import annotations

import hashlib
import json
import os
import threading

try:  # fcntl 仅 POSIX 提供，演练环境为 Linux
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


class CorruptLogError(RuntimeError):
    """哈希链断裂或记录缺省时抛出，代表原始凭证可能被改动。"""


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), default=str)


class WalStore:
    def __init__(self, path: str):
        self.path = path
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock_path = os.path.join(directory, f".{os.path.basename(path)}.lock")
        self._tl = threading.Lock()
        self._flock = open(self._lock_path, "a+")
        self._next_seq = 1
        self._prev_hash = ""
        for rec in self._iter_raw(verify=False):
            self._next_seq = rec["seq"] + 1
            self._prev_hash = rec["hash"]

    # ---- 底层读写 -------------------------------------------------------
    def _iter_raw(self, verify: bool):
        if not os.path.exists(self.path):
            return
        prev = ""
        expected_seq = 1
        with open(self.path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                    seq = rec["seq"]
                    digest = rec.pop("hash")
                except (KeyError, json.JSONDecodeError) as exc:
                    raise CorruptLogError(f"第{line_no}行记录结构损坏: {exc}") from exc
                if verify:
                    if seq != expected_seq:
                        raise CorruptLogError(
                            f"第{line_no}行序号不连续: 期望 {expected_seq}, 实际 {seq}")
                    body = canonical(rec)
                    expect = hashlib.sha256((prev + body).encode("utf-8")).hexdigest()
                    if not _const_time_eq(expect, digest):
                        raise CorruptLogError(f"第{line_no}行哈希校验失败，凭证已被改动")
                    prev = digest
                else:
                    prev = digest
                rec["hash"] = digest
                expected_seq += 1
                yield rec

    def read_all(self, verify: bool = True) -> list[dict]:
        """读出全部记录（自行加锁）；verify=True 时逐条校验哈希链。"""
        with self._file_lock():
            return self._read_all_unlocked(verify)

    def _read_all_unlocked(self, verify: bool) -> list[dict]:
        """调用方已持有文件锁时使用。"""
        return list(self._iter_raw(verify=verify))

    def append(self, envelope: dict) -> dict:
        """追加一条记录（自行加锁），返回含 seq/hash 的完整记录副本。"""
        with self._tl, self._file_lock():
            return self._append_unlocked(envelope)

    def _append_unlocked(self, envelope: dict) -> dict:
        """调用方已持有文件锁时使用，避免嵌套 flock 被提前释放。

        持锁后重新确认链尾，避免另一进程/本进程其他实例先写入导致断链。
        """
        next_seq, prev_hash = 1, ""
        for raw in self._iter_raw(verify=False):
            next_seq, prev_hash = raw["seq"] + 1, raw["hash"]
        rec = dict(envelope)
        rec["seq"] = next_seq
        body = canonical(rec)
        digest = hashlib.sha256((prev_hash + body).encode("utf-8")).hexdigest()
        rec["hash"] = digest
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        self._prev_hash, self._next_seq = digest, next_seq + 1
        return dict(rec)

    def close(self):
        try:
            self._flock.close()
        except Exception:  # noqa: BLE001
            pass

    def _file_lock(self):
        return _FileLock(self._flock)


class _FileLock:
    def __init__(self, fh):
        self._fh = fh

    def __enter__(self):
        if fcntl is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        if fcntl is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        return False


def _const_time_eq(a: str, b: str) -> bool:
    if len(a) != len(b):
        return False
    result = 0
    for x, y in zip(a, b):
        result |= ord(x) ^ ord(y)
    return result == 0
