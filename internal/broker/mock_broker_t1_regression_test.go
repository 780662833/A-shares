package broker

import (
"testing"

"ashare-quant/internal/model"
)

// T1.2.3/T1.2.9/T1.2.10 补充回归：针对历史 P0 缺陷（T+1 结转误解锁当日持仓、
// isFirstRunOfDay 判据错误导致次日漏结转）的专项防回归用例。

// TestCarryOverDoesNotUnlockSameDayPosition 撮合 d2 时，BuyDate=d2 的持仓不得被结转解锁。
func TestCarryOverDoesNotUnlockSameDayPosition(t *testing.T) {
h := newHarness(t)
const d1, d2 = "2026-10-12", "2026-10-13"
h.seedKline("600519", d1, 100.00, 110.00, 90.00)
h.seedKline("600519", d2, 101.00, 111.10, 90.90)

buy1 := h.seedOrder("600519", model.DirectionBuy, 101.00, 1000, d1)
if _, err := h.broker().SimulateNextDayOpen(d1); err != nil {
t.Fatalf("d1: %v", err)
}
if st := h.mustOrder(buy1.OrderID).Status; st != model.OrderStatusFilled {
t.Fatalf("d1 buy should fill, got %s", st)
}
// d1 买入后：Quantity=1000, Available=0
if p := h.mustPosition("600519"); p.Quantity != 1000 || p.AvailableQuantity != 0 {
t.Fatalf("after d1 buy: %+v", p)
}

// d2 首跑前再塞一笔 d2 当天成交的买单（PENDING，TradeDate=d2）
buy2 := h.seedOrder("600519", model.DirectionBuy, 102.00, 1000, d2)
if _, err := h.broker().SimulateNextDayOpen(d2); err != nil {
t.Fatalf("d2: %v", err)
}
if st := h.mustOrder(buy2.OrderID).Status; st != model.OrderStatusFilled {
t.Fatalf("d2 buy should fill, got %s", st)
}
// d2 收盘后：总量 2000；可卖只应包含 d1 解锁的 1000（d2 新买不可卖）
p := h.mustPosition("600519")
if p.Quantity != 2000 {
t.Fatalf("quantity = %d want 2000", p.Quantity)
}
if p.AvailableQuantity != 1000 {
t.Fatalf("available = %d want 1000 (T+1: same-day 1000 must stay locked)", p.AvailableQuantity)
}

// d3：剩余 1000 解锁
const d3 = "2026-10-14"
h.seedKline("600519", d3, 100.00, 110.00, 90.00)
sell := h.seedOrder("600519", model.DirectionSell, 90.00, 2000, d3)
if _, err := h.broker().SimulateNextDayOpen(d3); err != nil {
t.Fatalf("d3: %v", err)
}
if st := h.mustOrder(sell.OrderID).Status; st != model.OrderStatusFilled {
t.Fatalf("d3 full sell should fill after unlock, got %s (%s)", st, h.mustOrder(sell.OrderID).RejectReason)
}
}

// TestLatePendingOrderDoesNotBlockNextDayCarryOver 复现原缺陷场景：
// d1 撮合完成后，若已存在 TradeDate=d2 的 PENDING 订单，d2 首跑仍必须执行结转。
func TestLatePendingOrderDoesNotBlockNextDayCarryOver(t *testing.T) {
h := newHarness(t)
const d1, d2 = "2026-10-12", "2026-10-13"
h.seedKline("600519", d1, 100.00, 110.00, 90.00)
h.seedKline("600519", d2, 101.00, 111.10, 90.90)

if _, err := h.broker().SimulateNextDayOpen(d1); err != nil { // 空跑 d1
t.Fatalf("d1 empty run: %v", err)
}
buy := h.seedOrder("600519", model.DirectionBuy, 101.00, 1000, d1)
if _, err := h.broker().SimulateNextDayOpen(d1); err != nil {
t.Fatalf("d1 buy: %v", err)
}
if h.mustOrder(buy.OrderID).Status != model.OrderStatusFilled {
t.Fatal("buy not filled")
}

// 日终复盘：d2 的卖单在 d2 撮合之前就已落库（真实系统的常态时序）
sellNext := h.seedOrder("600519", model.DirectionSell, 90.00, 1000, d2)
sum, err := h.broker().SimulateNextDayOpen(d2)
if err != nil {
t.Fatalf("d2: %v", err)
}
if sum.Filled != 1 {
t.Fatalf("d2 sell must fill via carry-over, summary=%+v reason=%s",
sum, h.mustOrder(sellNext.OrderID).RejectReason)
}
}
