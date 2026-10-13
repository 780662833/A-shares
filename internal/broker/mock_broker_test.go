package broker

import (
	"testing"

	"ashare-quant/internal/db"
	"ashare-quant/internal/model"
)

// ---------------------------------------------------------------------------
// 测试脚手架
// ---------------------------------------------------------------------------

type harness struct {
	t   *testing.T
	acc *model.Account
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	// 每个用例独立临时库，避免测试间状态污染（InitDB 是进程级单例）。
	dir := t.TempDir()
	path := dir + "/trading.db"
	gdb, err := db.InitDB(path)
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var acc model.Account
	if err := gdb.Order("id ASC").First(&acc).Error; err != nil {
		t.Fatalf("load account: %v", err)
	}
	return &harness{t: t, acc: &acc}
}

// seedKline 写入当日 K 线（含涨跌停价）。
func (h *harness) seedKline(code, date string, open, limitUp, limitDown float64) {
	h.t.Helper()
	k := model.DailyKline{
		Code: code, Date: date,
		Open: open, High: open, Low: open, Close: open,
		Volume: 1_000_000, Amount: open * 1_000_000,
		LimitUpPrice: limitUp, LimitDownPrice: limitDown,
	}
	if err := db.MustGetDB().Create(&k).Error; err != nil {
		h.t.Fatalf("seed kline: %v", err)
	}
}

// seedPosition 写入持仓。
func (h *harness) seedPosition(code string, qty, avail int64, cost float64, buyDate string) {
	h.t.Helper()
	p := model.Position{Code: code, Quantity: qty, AvailableQuantity: avail, CostPrice: cost, BuyDate: buyDate}
	if err := db.MustGetDB().Create(&p).Error; err != nil {
		h.t.Fatalf("seed position: %v", err)
	}
}

// seedOrder 写入 PENDING 订单。
func (h *harness) seedOrder(code, dir string, price float64, qty int64, tradeDate string) model.Order {
	h.t.Helper()
	o := model.Order{
		OrderID: model.NewOrderID(uint64(h.seq())), Code: code, Direction: dir,
		Price: price, Quantity: qty, Status: model.OrderStatusPending, TradeDate: tradeDate,
	}
	if err := db.MustGetDB().Create(&o).Error; err != nil {
		h.t.Fatalf("seed order: %v", err)
	}
	return o
}

var orderSeq int

func (h *harness) seq() int { orderSeq++; return orderSeq }

func (h *harness) mustOrder(orderID string) model.Order {
	h.t.Helper()
	var o model.Order
	if err := db.MustGetDB().Where("order_id = ?", orderID).First(&o).Error; err != nil {
		h.t.Fatalf("load order %s: %v", orderID, err)
	}
	return o
}

func (h *harness) mustPosition(code string) model.Position {
	h.t.Helper()
	var p model.Position
	if err := db.MustGetDB().Where("code = ?", code).First(&p).Error; err != nil {
		h.t.Fatalf("load position %s: %v", code, err)
	}
	return p
}

func (h *harness) mustAccount() model.Account {
	h.t.Helper()
	var a model.Account
	if err := db.MustGetDB().Order("id ASC").First(&a).Error; err != nil {
		h.t.Fatalf("load account: %v", err)
	}
	return a
}

func (h *harness) broker() *MockBroker {
	h.t.Helper()
	b, err := NewMockBroker(db.MustGetDB())
	if err != nil {
		h.t.Fatalf("NewMockBroker: %v", err)
	}
	return b
}

func approxEq(a, b float64) bool { return absF(a-b) < 0.005 }

func absF(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// ---------------------------------------------------------------------------
// 纯计算函数单测
// ---------------------------------------------------------------------------

func TestCalcFillPrice(t *testing.T) {
	cases := []struct {
		open float64
		dir  string
		want float64
	}{
		{10.00, model.DirectionBuy, 10.02},   // 10*1.002=10.02
		{10.00, model.DirectionSell, 9.98},   // 10*0.998=9.98
		{3.45, model.DirectionBuy, 3.46},     // 3.456 -> 3.46
		{7.41, model.DirectionSell, 7.40},    // 7.39818 -> 7.40
		{123.45, model.DirectionBuy, 123.70}, // 123.6969 -> 123.70
	}
	for _, c := range cases {
		if got := calcFillPrice(c.open, c.dir); !approxEq(got, c.want) {
			t.Errorf("calcFillPrice(%v,%s)=%v want %v", c.open, c.dir, got, c.want)
		}
	}
}

func TestCalcFees(t *testing.T) {
	// 买入：佣金万2.5，最低5元
	if got := calcBuyFee(10000); !approxEq(got, 5.00) { // 2.5 -> 最低5
		t.Errorf("buy fee small amount: got %v want 5", got)
	}
	if got := calcBuyFee(200000); !approxEq(got, 50.00) { // 200000*0.00025=50
		t.Errorf("buy fee big amount: got %v want 50", got)
	}
	// 卖出：佣金 + 印花税千0.5
	if got := calcSellFee(10000); !approxEq(got, 5+5) { // max(2.5,5)+5
		t.Errorf("sell fee small: got %v want 10", got)
	}
	if got := calcSellFee(200000); !approxEq(got, 50+100) { // 50 + 100
		t.Errorf("sell fee big: got %v want 150", got)
	}
}

// ---------------------------------------------------------------------------
// 场景 1：正常买入成交（滑点 + 佣金 + T+1 锁定）
// ---------------------------------------------------------------------------

func TestSimulateBuyFill(t *testing.T) {
	h := newHarness(t)
	const d1 = "2026-10-12"
	h.seedKline("600519", d1, 100.00, 110.00, 90.00)
	o := h.seedOrder("600519", model.DirectionBuy, 101.00, 1000, d1)

	sum, err := h.broker().SimulateNextDayOpen(d1)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if sum.Filled != 1 || sum.Rejected != 0 {
		t.Fatalf("summary = %+v", sum)
	}

	got := h.mustOrder(o.OrderID)
	if got.Status != model.OrderStatusFilled {
		t.Fatalf("status = %s reason=%s", got.Status, got.RejectReason)
	}
	// 成交价 = 100*1.002 = 100.20；成交额 = 100200；佣金 = 100200*0.00025 = 25.05
	if !approxEq(got.AvgPrice, 100.20) {
		t.Errorf("avg price = %v want 100.20", got.AvgPrice)
	}
	if !approxEq(got.Fee, 25.05) {
		t.Errorf("fee = %v want 25.05", got.Fee)
	}

	pos := h.mustPosition("600519")
	if pos.Quantity != 1000 {
		t.Errorf("qty = %d want 1000", pos.Quantity)
	}
	if pos.AvailableQuantity != 0 {
		t.Errorf("T+1 violated: available = %d want 0 (当日买入不可卖)", pos.AvailableQuantity)
	}
	if !approxEq(pos.CostPrice, 100.20) {
		t.Errorf("cost = %v want 100.20", pos.CostPrice)
	}

	acc := h.mustAccount()
	wantCash := 1_000_000 - 100_200 - 25.05
	if !approxEq(acc.AvailableCash, wantCash) {
		t.Errorf("cash = %v want %v", acc.AvailableCash, wantCash)
	}
}

// ---------------------------------------------------------------------------
// 场景 2：涨停一字板拒买 / 跌停一字板拒卖
// ---------------------------------------------------------------------------

func TestLimitUpDownOneWordBoard(t *testing.T) {
	h := newHarness(t)
	const d1 = "2026-10-12"
	// 600519 涨停一字板：开=高=低=收=涨停价 110
	h.seedKline("600519", d1, 110.00, 110.00, 90.00)
	// 000001 跌停一字板：开盘=跌停价 9.00
	h.seedKline("000001", d1, 9.00, 11.00, 9.00)
	// 持有一笔可卖的 000001
	h.seedPosition("000001", 1000, 1000, 9.50, "2026-10-10")

	buy := h.seedOrder("600519", model.DirectionBuy, 110.00, 1000, d1) // 委托价==涨停价 => 应拒
	sell := h.seedOrder("000001", model.DirectionSell, 9.00, 1000, d1) // 委托价==跌停价 => 应拒

	sum, err := h.broker().SimulateNextDayOpen(d1)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if sum.Rejected != 2 || sum.Filled != 0 {
		t.Fatalf("summary = %+v", sum)
	}

	gb := h.mustOrder(buy.OrderID)
	if gb.Status != model.OrderStatusRejected || !contains(gb.RejectReason, "涨停一字板") {
		t.Errorf("buy rejection wrong: %s / %s", gb.Status, gb.RejectReason)
	}
	gs := h.mustOrder(sell.OrderID)
	if gs.Status != model.OrderStatusRejected || !contains(gs.RejectReason, "跌停一字板") {
		t.Errorf("sell rejection wrong: %s / %s", gs.Status, gs.RejectReason)
	}
	// 废单不应产生任何资金/持仓变动
	acc := h.mustAccount()
	if !approxEq(acc.AvailableCash, 1_000_000) {
		t.Errorf("cash changed on rejection: %v", acc.AvailableCash)
	}
	pos := h.mustPosition("000001")
	if pos.Quantity != 1000 || pos.AvailableQuantity != 1000 {
		t.Errorf("position changed on rejection: %+v", pos)
	}
}

// contains 简易子串判断（避免引入 strings 仅为测试）。
func contains(s, sub string) bool {
	return indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// 场景 3：T+1 —— 当日买入当日卖出必须被拒；次日结转后可卖
// ---------------------------------------------------------------------------

func TestTPlus1SameDaySellRejectedThenNextDayOK(t *testing.T) {
	h := newHarness(t)
	const d1, d2 = "2026-10-12", "2026-10-13"
	h.seedKline("600519", d1, 100.00, 110.00, 90.00)
	h.seedKline("600519", d2, 101.00, 111.10, 90.90)

	buy := h.seedOrder("600519", model.DirectionBuy, 101.00, 1000, d1)
	if _, err := h.broker().SimulateNextDayOpen(d1); err != nil {
		t.Fatalf("day1 simulate: %v", err)
	}
	if st := h.mustOrder(buy.OrderID).Status; st != model.OrderStatusFilled {
		t.Fatalf("day1 buy should fill, got %s", st)
	}

	// 同日再挂卖单（模拟策略违规）：撮合必须因 T+1 拒绝。
	// 委托价给足安全边际（90 < 开盘含滑点 99.80），确保先通过限价校验、命中 T+1 拦截。
	sellToday := h.seedOrder("600519", model.DirectionSell, 90.00, 1000, d1)
	sum, err := h.broker().SimulateNextDayOpen(d1) // 同日补跑
	if err != nil {
		t.Fatalf("day1 re-run: %v", err)
	}
	if sum.Total != 1 || sum.Rejected != 1 {
		t.Fatalf("re-run summary = %+v", sum)
	}
	if r := h.mustOrder(sellToday.OrderID); r.Status != model.OrderStatusRejected ||
		!contains(r.RejectReason, "T+1") {
		t.Errorf("same-day sell must be rejected by T+1: %+v", r)
	}

	// 次日：结转解锁后卖出成功
	sellNext := h.seedOrder("600519", model.DirectionSell, 90.00, 1000, d2)
	if _, err := h.broker().SimulateNextDayOpen(d2); err != nil {
		t.Fatalf("day2 simulate: %v", err)
	}
	g := h.mustOrder(sellNext.OrderID)
	if g.Status != model.OrderStatusFilled {
		t.Fatalf("next-day sell should fill, got %s (%s)", g.Status, g.RejectReason)
	}
	// 成交价 = 101*0.998 = 100.80；成交额 100800；费用 = 佣金25.20 + 印花税50.40 = 75.60
	if !approxEq(g.AvgPrice, 100.80) {
		t.Errorf("sell avg price = %v want 100.80", g.AvgPrice)
	}
	if !approxEq(g.Fee, 75.60) {
		t.Errorf("sell fee = %v want 75.60", g.Fee)
	}
	pos := h.mustPosition("600519")
	if pos.Quantity != 0 || pos.AvailableQuantity != 0 {
		t.Errorf("position after full sell: %+v", pos)
	}
}

// ---------------------------------------------------------------------------
// 场景 4：限价单价格穿越 / 非整手 / 资金不足
// ---------------------------------------------------------------------------

func TestRejectionsMisc(t *testing.T) {
	h := newHarness(t)
	const d1 = "2026-10-12"
	h.seedKline("600519", d1, 100.00, 110.00, 90.00)

	limitTooLow := h.seedOrder("600519", model.DirectionBuy, 99.00, 1000, d1) // 委托价 < 开盘含滑点100.20
	oddLot := h.seedOrder("600519", model.DirectionBuy, 101.00, 150, d1)      // 非 100 整数倍
	noPosSell := h.seedOrder("600519", model.DirectionSell, 90.00, 1000, d1)  // 无持仓（价格给足安全边际）

	sum, err := h.broker().SimulateNextDayOpen(d1)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if sum.Rejected != 3 || sum.Filled != 0 {
		t.Fatalf("summary = %+v", sum)
	}
	if r := h.mustOrder(limitTooLow.OrderID); !contains(r.RejectReason, "限价单无法成交") {
		t.Errorf("limit order reason wrong: %s", r.RejectReason)
	}
	if r := h.mustOrder(oddLot.OrderID); !contains(r.RejectReason, "整数倍") {
		t.Errorf("odd lot reason wrong: %s", r.RejectReason)
	}
	if r := h.mustOrder(noPosSell.OrderID); !contains(r.RejectReason, "无持仓") {
		t.Errorf("no position reason wrong: %s", r.RejectReason)
	}
}

func TestInsufficientFunds(t *testing.T) {
	h := newHarness(t)
	const d1 = "2026-10-12"
	h.seedKline("600519", d1, 100.00, 110.00, 90.00)
	// 两笔买单：第一笔 5000 股花费约 501,000+125.25；第二笔再来 5000 股 => 现金不够
	o1 := h.seedOrder("600519", model.DirectionBuy, 101.00, 5000, d1)
	o2 := h.seedOrder("600519", model.DirectionBuy, 101.00, 5000, d1)

	sum, err := h.broker().SimulateNextDayOpen(d1)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if sum.Filled != 1 || sum.Rejected != 1 {
		t.Fatalf("summary = %+v", sum)
	}
	if h.mustOrder(o1.OrderID).Status != model.OrderStatusFilled {
		t.Errorf("first buy should fill")
	}
	if r := h.mustOrder(o2.OrderID); r.Status != model.OrderStatusRejected || !contains(r.RejectReason, "资金不足") {
		t.Errorf("second buy should be rejected for funds: %+v", r)
	}
	acc := h.mustAccount()
	if acc.AvailableCash < 0 {
		t.Fatalf("cash went negative: %v", acc.AvailableCash)
	}
}

// ---------------------------------------------------------------------------
// 场景 5：无行情（停牌）拒单 + 幂等重跑
// ---------------------------------------------------------------------------

func TestNoKlineAndIdempotentRerun(t *testing.T) {
	h := newHarness(t)
	const d1 = "2026-10-12"
	o := h.seedOrder("300750", model.DirectionBuy, 200.00, 100, d1) // 未 seed K 线

	sum, err := h.broker().SimulateNextDayOpen(d1)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if sum.Rejected != 1 || !contains(h.mustOrder(o.OrderID).RejectReason, "无行情") {
		t.Fatalf("expect no-kline rejection, summary=%+v", sum)
	}

	// 重跑：已 REJECTED 的单不再处理
	sum2, err := h.broker().SimulateNextDayOpen(d1)
	if err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if sum2.Total != 0 {
		t.Errorf("rerun should find nothing pending, got %+v", sum2)
	}
}

// ---------------------------------------------------------------------------
// 场景 6：非法入参
// ---------------------------------------------------------------------------

func TestInvalidInputs(t *testing.T) {
	h := newHarness(t)
	if _, err := h.broker().SimulateNextDayOpen("2026/10/12"); err == nil {
		t.Errorf("expect error for bad date format")
	}
	if _, err := h.broker().SimulateNextDayOpen(""); err == nil {
		t.Errorf("expect error for empty date")
	}
	if _, err := NewMockBroker(nil); err == nil {
		t.Errorf("expect error for nil db")
	}
}
