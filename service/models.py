"""领域模型：命令、不可变凭证事件与错误。

原始凭证一经入账即不可修改（无更新接口），任何后续变化都以追加事件表达，
例如迟到的承运人回执、退货、部分回款，全部形成可重算的流水。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any


# --------------------------------------------------------------------------- 角色

class Role(str, Enum):
    FINANCE_MANAGER = "finance_manager"   # 融资经理：放款
    RISK_OFFICER = "risk_officer"         # 风控：冻结 / 解冻
    OPERATOR = "operator"                 # 单证操作员：登记凭证


# --------------------------------------------------------------------------- 错误

class LedgerError(Exception):
    """业务拒绝：携带拒绝依据 code 与可读 reason。"""

    def __init__(self, code: str, reason: str, basis: dict[str, Any] | None = None):
        super().__init__(reason)
        self.code = code
        self.reason = reason
        self.basis = basis or {}

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "reason": self.reason, "basis": self.basis}


# --------------------------------------------------------------------------- 命令

@dataclass(frozen=True)
class Command:
    event_id: str
    role: str
    payload: dict[str, Any] = field(default_factory=dict)
    occurred_at: str | None = None       # ISO8601；业务发生日，汇率/先后判定用
    idempotency_key: str | None = None   # 断网补传可用与 event_id 相同的键


# --------------------------------------------------------------------------- 事件（不可变凭证回执）

@dataclass(frozen=True)
class Event:
    event_id: str
    command_type: str
    invoice_ref: str
    seq: int
    ts: str                       # 入账时间（系统时钟）
    occurred_at: str              # 业务发生时间
    role: str
    accepted: bool
    data: dict[str, Any]          # 凭证内容；含币种金额/汇率/影响金额
    reject: dict[str, Any] | None = None
    supersedes: str | None = None  # 迟到回执标注其影响/覆盖的前事件
    idempotency_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "command_type": self.command_type,
            "invoice_ref": self.invoice_ref,
            "seq": self.seq,
            "ts": self.ts,
            "occurred_at": self.occurred_at,
            "role": self.role,
            "accepted": self.accepted,
            "data": self.data,
            "reject": self.reject,
            "supersedes": self.supersedes,
            "idempotency_key": self.idempotency_key,
        }


def D(value: Any) -> Decimal:
    """金额一律以 Decimal 处理，禁止 float 进入运算。"""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
