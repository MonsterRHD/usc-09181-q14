"""异常演练测试。

场景：
1. 并发提交同票融资——不得超额放款，拒绝须带额度依据；
2. 同一发票分别受让给保理商和银行——重复质押被识别并冻结；
3. 先回款后退货——生成对出口商的追索（应退回款）；
4. 迟到的承运人回执——标注对当前余额的影响，短少触发追索；
5. 断网补传 / 服务恢复——同一 event_id 幂等，状态可由流水完整重算；
6. 发票拆分、部分回款、币种换算、冻结授权。
"""
from __future__ import annotations

import tempfile
import threading
import unittest
from decimal import Decimal
from pathlib import Path

from service.ledger import Ledger
from service.models import Command, Role
from service.store import EventStore


def cmd(event_id: str, role: Role | str, payload: dict, occurred_at: str | None = None,
        idem: str | None = None) -> Command:
    return Command(event_id=event_id, role=getattr(role, "value", role),
                   payload=payload, occurred_at=occurred_at, idempotency_key=idem)


OP = Role.OPERATOR.value
FM = Role.FINANCE_MANAGER.value
RO = Role.RISK_OFFICER.value


def base_ledger(tmp: Path) -> Ledger:
    led = Ledger(EventStore(tmp / "ledger.jsonl"))
    led.handle(cmd("fx-1", OP, {"type": "FX_RATE", "currency": "USD", "rate": "7.20",
                                "effective_date": "2026-09-01"}, "2026-09-01T08:00:00Z"))
    led.handle(cmd("inv-1", OP, {
        "type": "INVOICE", "invoice_ref": "INV-1001", "seller": "出口商甲",
        "debtor": "买方乙", "currency": "CNY", "face": "100000", "qty": "100",
    }, "2026-09-01T09:00:00Z"))
    led.handle(cmd("ship-1", OP, {"type": "SHIPMENT", "invoice_ref": "INV-1001",
                                  "bl_no": "BL-9", "shipped_at": "2026-09-02"},
                   "2026-09-02T09:00:00Z"))
    led.handle(cmd("notice-1", OP, {"type": "ASSIGNMENT_NOTICE", "invoice_ref": "INV-1001",
                                    "assignee": "FACTOR-A", "notice_id": "N-1"},
                   "2026-09-02T10:00:00Z"))
    return led


class DrillTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # ------------------------------------------------------------------ 1
    def test_concurrent_same_invoice_loans_never_overlend(self) -> None:
        led = base_ledger(self.tmp)
        st = led.statement("INV-1001")
        # 装运后、承运人回执前：按临时比例 0.7，限额 70,000
        self.assertEqual(st["finance_limit"], "70000.00")
        self.assertEqual(st["available"], "70000.00")

        results: list = []
        barrier = threading.Barrier(8)

        def borrow(i: int) -> None:
            barrier.wait()  # 最大化竞态：8 笔 20,000 同时进来
            evt = led.handle(cmd(f"loan-c{i}", FM, {
                "type": "LOAN", "invoice_ref": "INV-1001", "amount": "20000", "currency": "CNY"}))
            results.append(evt)

        threads = [threading.Thread(target=borrow, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        accepted = [e for e in results if e.accepted]
        rejected = [e for e in results if not e.accepted]
        # 限额 70,000，笔均 20,000：恰好 3 笔成功，5 笔拒绝
        self.assertEqual(len(accepted), 3)
        self.assertEqual(len(rejected), 5)
        self.assertTrue(all(e.reject["code"] == "LIMIT_EXCEEDED" for e in rejected))

        st = led.statement("INV-1001")
        self.assertEqual(st["outstanding"], "60000.00")
        self.assertEqual(st["available"], "10000.00")
        self.assertLessEqual(Decimal(st["outstanding"]), Decimal(st["finance_limit"]))
        # 拒绝依据：限额构成完整可核
        basis = rejected[0].reject["basis"]
        self.assertEqual(basis["finance_limit"], "70000.00")
        self.assertEqual(basis["outstanding"], "60000.00")
        self.assertEqual(basis["available"], "10000.00")
        self.assertEqual(basis["converted_request"], "20000.00")

    def test_loan_requires_finance_manager(self) -> None:
        led = base_ledger(self.tmp)
        evt = led.handle(cmd("loan-x", OP, {"type": "LOAN", "invoice_ref": "INV-1001",
                                            "amount": "1", "currency": "CNY"}))
        self.assertFalse(evt.accepted)
        self.assertEqual(evt.reject["code"], "FORBIDDEN")

    # ------------------------------------------------------------------ 2
    def test_duplicate_assignment_factor_and_bank_is_frozen(self) -> None:
        led = base_ledger(self.tmp)
        dup = led.handle(cmd("notice-2", OP, {"type": "ASSIGNMENT_NOTICE",
                                              "invoice_ref": "INV-1001", "assignee": "BANK-B",
                                              "notice_id": "N-2"}, "2026-09-03T10:00:00Z"))
        self.assertFalse(dup.accepted)
        self.assertEqual(dup.reject["code"], "DUPLICATE_ASSIGNMENT")
        self.assertEqual(dup.reject["basis"]["first_assignee"], "FACTOR-A")
        self.assertEqual(dup.reject["basis"]["first_notice_event_id"], "notice-1")

        st = led.statement("INV-1001")
        self.assertTrue(st["frozen"])
        self.assertEqual(st["recourse"]["dup_party"], "BANK-B")

        # 冻结期间融资经理也不能放款
        loan = led.handle(cmd("loan-f", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                             "amount": "1000", "currency": "CNY"}))
        self.assertFalse(loan.accepted)
        self.assertEqual(loan.reject["code"], "INVOICE_FROZEN")

        # 即使风控来解冻，重复质押这一系统风险未消除，仍然冻结
        unf = led.handle(cmd("unfreeze-1", RO, {"type": "UNFREEZE", "invoice_ref": "INV-1001"}))
        self.assertTrue(unf.accepted)
        self.assertTrue(unf.data["effective_frozen"])
        self.assertTrue(led.statement("INV-1001")["frozen"])

    def test_no_finance_before_assignment_notice(self) -> None:
        led = Ledger(EventStore(self.tmp / "l.jsonl"))
        led.handle(cmd("inv-2", OP, {"type": "INVOICE", "invoice_ref": "INV-2",
                                     "seller": "S", "debtor": "D", "currency": "CNY",
                                     "face": "50000", "qty": "10"}))
        led.handle(cmd("ship-2", OP, {"type": "SHIPMENT", "invoice_ref": "INV-2", "bl_no": "B"}))
        evt = led.handle(cmd("loan-2", FM, {"type": "LOAN", "invoice_ref": "INV-2",
                                            "amount": "1000", "currency": "CNY"}))
        self.assertFalse(evt.accepted)
        self.assertEqual(evt.reject["code"], "NOTICE_MISSING")

    # ------------------------------------------------------------------ 3
    def test_repayment_then_return_creates_recourse(self) -> None:
        led = base_ledger(self.tmp)
        # 全额放款 70,000，随后买方全额回款
        self.assertTrue(led.handle(cmd("loan-1", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                                      "amount": "70000", "currency": "CNY"})).accepted)
        self.assertTrue(led.handle(cmd("repay-1", OP, {"type": "REPAYMENT", "invoice_ref": "INV-1001",
                                                       "amount": "70000", "currency": "CNY"})).accepted)
        st = led.statement("INV-1001")
        self.assertEqual(st["outstanding"], "0.00")

        # 之后退货 20,000（先回款后退货）
        ret = led.handle(cmd("ret-1", OP, {"type": "RETURN", "invoice_ref": "INV-1001",
                                           "amount": "20000", "currency": "CNY"}))
        self.assertTrue(ret.accepted)
        self.assertEqual(ret.data["cashback_due_from_seller"], "20000.00")
        self.assertTrue(ret.data["recourse_triggered"])
        st = led.statement("INV-1001")
        self.assertTrue(st["recourse"]["active"])
        # 限额随退货下降：80,000 合格票面 × 0.7 = 56,000
        self.assertEqual(st["finance_limit"], "56000.00")

    def test_over_repayment_rejected(self) -> None:
        led = base_ledger(self.tmp)
        led.handle(cmd("loan-3", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                      "amount": "10000", "currency": "CNY"}))
        evt = led.handle(cmd("repay-3", OP, {"type": "REPAYMENT", "invoice_ref": "INV-1001",
                                             "amount": "10001", "currency": "CNY"}))
        self.assertFalse(evt.accepted)
        self.assertEqual(evt.reject["code"], "REPAYMENT_EXCEEDED")

    # ------------------------------------------------------------------ 4
    def test_late_carrier_receipt_short_shipment_annotates_impact(self) -> None:
        led = base_ledger(self.tmp)
        # 先按临时比例放款 70,000
        self.assertTrue(led.handle(cmd("loan-4", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                                      "amount": "70000", "currency": "CNY"})).accepted)
        # 迟到的承运人回执：100 件只确认 80 件
        rc = led.handle(cmd("rc-1", OP, {"type": "CARRIER_RECEIPT", "invoice_ref": "INV-1001",
                                         "carrier_ref": "CR-1", "confirmed_qty": "80"}))
        self.assertTrue(rc.accepted)
        self.assertTrue(rc.data["late"])
        impact = rc.data["impact_on_current_balance"]
        # 临时限额 70,000 → 确认后 100,000×0.8×0.8=64,000；未偿 70,000 → 超额 6,000
        self.assertEqual(impact["limit_before"], "70000.00")
        self.assertEqual(impact["limit_after"], "64000.00")
        self.assertEqual(impact["excess_after"], "6000.00")
        self.assertTrue(impact["recourse_triggered"])
        st = led.statement("INV-1001")
        self.assertTrue(st["recourse"]["active"])
        self.assertEqual(st["recourse"]["loan_excess"], "6000.00")
        self.assertTrue(st["frozen"])  # 系统冻结，禁止继续放款

    def test_on_time_full_carrier_receipt_raises_limit(self) -> None:
        led = base_ledger(self.tmp)
        rc = led.handle(cmd("rc-ok", OP, {"type": "CARRIER_RECEIPT", "invoice_ref": "INV-1001",
                                          "carrier_ref": "CR-0", "confirmed_qty": "100"}))
        self.assertFalse(rc.data["late"])
        self.assertEqual(rc.data["impact_on_current_balance"]["limit_after"], "80000.00")
        self.assertEqual(led.statement("INV-1001")["finance_limit"], "80000.00")

    # ------------------------------------------------------------------ 5
    def test_idempotent_replay_and_recovery_from_journal(self) -> None:
        path = self.tmp / "rec.jsonl"
        led = base_ledger_tmp(path)
        # 断网补传：同一 event_id + 幂等键重复发，含一次曾经被拒绝的命令
        ok1 = led.handle(cmd("loan-r1", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                             "amount": "999999", "currency": "CNY"}, idem="K-1"))
        self.assertFalse(ok1.accepted)
        replay = led.handle(cmd("loan-r1", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                                "amount": "999999", "currency": "CNY"}, idem="K-1"))
        self.assertIs(replay, ok1)  # 原样返回首次回执

        ok2 = led.handle(cmd("loan-r2", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                             "amount": "5000", "currency": "CNY"}))
        replay2 = led.handle(cmd("loan-r2", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                                 "amount": "5000", "currency": "CNY"}))
        self.assertIs(replay2, ok2)

        before = led.statement("INV-1001")
        # 服务恢复：丢弃内存，仅凭 JSONL 重放
        led2 = Ledger(EventStore(path))
        after = led2.statement("INV-1001")
        self.assertEqual(before, after)
        self.assertEqual(after["outstanding"], "5000.00")
        # 恢复后再次补传旧 event_id，依旧是同一条拒绝留痕
        again = led2.handle(cmd("loan-r1", FM, {"type": "LOAN", "invoice_ref": "INV-1001",
                                                "amount": "999999", "currency": "CNY"}, idem="K-1"))
        self.assertFalse(again.accepted)
        self.assertEqual(again.reject["code"], "LIMIT_EXCEEDED")
        n = len(led2.journal())
        self.assertEqual(n, len(led.journal()))  # 重放未产生任何新流水

    # ------------------------------------------------------------------ 6
    def test_split_partial_repayment_and_fx(self) -> None:
        led = base_ledger(self.tmp)
        led.handle(cmd("rc-full", OP, {"type": "CARRIER_RECEIPT", "invoice_ref": "INV-1001",
                                       "carrier_ref": "CR", "confirmed_qty": "100"}))
        # 拆出 30% 给子票
        sp = led.handle(cmd("split-1", OP, {"type": "SPLIT", "invoice_ref": "INV-1001",
                                            "child_ref": "INV-1001-A", "ratio": "0.30"}))
        self.assertTrue(sp.accepted)
        self.assertEqual(sp.data["child_face"], "30000.00")
        self.assertTrue(sp.data["notice_inherited"])

        child = led.statement("INV-1001-A")
        parent = led.statement("INV-1001")
        self.assertEqual(child["face"], "30000.00")
        # 母票剩余基数 70,000，限额 56,000；子票 30,000，限额 24,000
        self.assertEqual(parent["finance_limit"], "56000.00")
        self.assertEqual(child["finance_limit"], "24000.00")
        self.assertEqual(child["assignee"], "FACTOR-A")  # 受让地位随拆分承继

        # 外币（USD）放款 1,000，按 7.20 换算 = 7,200 CNY
        loan = led.handle(cmd("loan-usd", FM, {"type": "LOAN", "invoice_ref": "INV-1001-A",
                                               "amount": "1000", "currency": "USD"},
                              "2026-09-05T00:00:00Z"))
        self.assertTrue(loan.accepted)
        self.assertEqual(loan.data["converted"], "7200.00")
        self.assertEqual(loan.data["rate"], "7.20")

        # 部分回款 4,000 CNY
        rep = led.handle(cmd("repay-p", OP, {"type": "REPAYMENT", "invoice_ref": "INV-1001-A",
                                             "amount": "4000", "currency": "CNY"}))
        self.assertTrue(rep.accepted)
        self.assertEqual(rep.data["outstanding_after"], "3200.00")

        # 母票与子票额度独立：子票未偿不占用母票
        self.assertEqual(led.statement("INV-1001")["outstanding"], "0.00")

    def test_freeze_requires_risk_officer(self) -> None:
        led = base_ledger(self.tmp)
        bad = led.handle(cmd("fz-bad", FM, {"type": "FREEZE", "invoice_ref": "INV-1001",
                                            "reason": "调查"}))
        self.assertEqual(bad.reject["code"], "FORBIDDEN")
        ok = led.handle(cmd("fz-ok", RO, {"type": "FREEZE", "invoice_ref": "INV-1001",
                                          "reason": "调查"}))
        self.assertTrue(ok.accepted)
        self.assertTrue(led.statement("INV-1001")["manual_frozen"])
        # 非风控不能解冻
        self.assertEqual(led.handle(cmd("uf-bad", FM, {"type": "UNFREEZE",
                                                       "invoice_ref": "INV-1001"}), ).reject["code"],
                         "FORBIDDEN")
        unf = led.handle(cmd("uf-ok", RO, {"type": "UNFREEZE", "invoice_ref": "INV-1001"}))
        self.assertTrue(unf.accepted)
        self.assertFalse(unf.data["effective_frozen"])
        self.assertFalse(led.statement("INV-1001")["frozen"])

    def test_original_voucher_is_immutable(self) -> None:
        led = base_ledger(self.tmp)
        # 没有任何修改接口；用相同 event_id 只能拿回原始回执
        first = led.handle(cmd("inv-1", OP, {"type": "INVOICE", "invoice_ref": "INV-1001",
                                             "seller": "出口商甲", "debtor": "买方乙",
                                             "currency": "CNY", "face": "999999", "qty": "100"}))
        self.assertEqual(first.to_dict(), led.journal("INV-1001")[0])  # 原始凭证未被改写
        # 用新 event_id 重复登记同一发票号：拒绝
        again = led.handle(cmd("inv-dup", OP, {"type": "INVOICE", "invoice_ref": "INV-1001",
                                               "seller": "X", "debtor": "Y", "currency": "CNY",
                                               "face": "1", "qty": "1"}))
        self.assertFalse(again.accepted)
        self.assertEqual(again.reject["code"], "INVOICE_EXISTS")


def base_ledger_tmp(path: Path) -> Ledger:
    led = Ledger(EventStore(path))
    led.handle(cmd("fx-1", OP, {"type": "FX_RATE", "currency": "USD", "rate": "7.20",
                                "effective_date": "2026-09-01"}, "2026-09-01T08:00:00Z"))
    led.handle(cmd("inv-1", OP, {"type": "INVOICE", "invoice_ref": "INV-1001", "seller": "出口商甲",
                                 "debtor": "买方乙", "currency": "CNY", "face": "100000",
                                 "qty": "100"}, "2026-09-01T09:00:00Z"))
    led.handle(cmd("ship-1", OP, {"type": "SHIPMENT", "invoice_ref": "INV-1001", "bl_no": "BL-9",
                                  "shipped_at": "2026-09-02"}, "2026-09-02T09:00:00Z"))
    led.handle(cmd("notice-1", OP, {"type": "ASSIGNMENT_NOTICE", "invoice_ref": "INV-1001",
                                    "assignee": "FACTOR-A", "notice_id": "N-1"},
                   "2026-09-02T10:00:00Z"))
    return led


if __name__ == "__main__":
    unittest.main()
