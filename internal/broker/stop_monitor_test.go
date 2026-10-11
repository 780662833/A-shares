package broker

import (
	"strings"
	"testing"

	"ashare-quant/internal/db"
	"ashare-quant/internal/model"
)

// ---------------------------------------------------------------------------
// EvaluateStop 纯函数单测（无 DB 依赖）
// ---------------------------------------------------------------------------

func mkPos(code string, stop float64, qty, avail int64, stopSetDate string) model.Position {
	return model.Position{
		Code: code, Quantity: qty, AvailableQuantity: avail,
		CostPrice: 10, BuyDate: "2026-10-08",
		StopPrice: stop, StopSetDate: stopSetDate,
	}
}

func mkKline(low, close float64) *model.DailyKline {
	return &model.DailyKline{Low: low, Close: close, Open: close, High: close}
}

func TestEvaluateStop_Table(t *testing.T) {
	cases := []struct {
		name      string
		pos       model.Position
		kline     *model.DailyKline
		wantSkip  bool
		wantWhy   string
		wantQty   int64 // OrderQty（skip 时忽略）
		wantLotOK bool  // OrderQty 应为整手或零股清仓全额
	}{
		{
			name:     "击穿风控线_整手全卖",
			pos:      mkPos("300750", 200.0, 500, 500, "2026-10-08"),
			kline:    mkKline(199.5, 198.0),
			wantSkip: false, wantQty: 500, wantLotOK: true,
		},
		{
			name:    "未设置风控线",
			pos:     mkPos("300750", 0, 500, 500, "2026-10-08"),
			kline:   mkKline(10, 10),
			wantSkip: true, wantWhy: "no_stop_line",
		},
		{
			name:     "行情缺失",
			pos:      mkPos("300750", 200, 500, 500, "2026-10-08"),
			kline:    nil,
			wantSkip: true, wantWhy: "no_quote",
		},
		{
			name:     "未击穿",
			pos:      mkPos("300750", 200, 500, 500, "2026-10-08"),
			kline:    mkKline(200.01, 205),
			wantSkip: true, wantWhy: "not_breached",
		},
		{
			name:     "恰好等于风控线_触发",
			pos:      mkPos("300750", 200, 100, 100, "2026-10-08"),
			kline:    mkKline(200, 199),
			wantSkip: false, wantQty: 100, wantLotOK: true,
		},
		{
			name:     "T+1锁定_当日买入不可卖",
			pos:      mkPos("300750", 200, 500, 0, "2026-10-08"),
			kline:    mkKline(150, 150),
			wantSkip: true, wantWhy: "t_plus_1_locked",
		},
		{
			name:     "零股清仓_不足一手全额卖出",
			pos:      mkPos("300750", 200, 60, 60, "2026-10-08"),
			kline:    mkKline(150, 150),
			wantSkip: false, wantQty: 60, wantLotOK: false, // 零股一次性清仓
		},
		{
			name:     "非整手可卖量_向下取整保留零股",
			pos:      mkPos("300750", 200, 550, 550, "2026-10-08"),
			kline:    mkKline(150, 150),
			wantSkip: false, wantQty: 500, wantLotOK: true,
		},
		{
			name:     "数据异常_可卖超过总量_截断",
			pos:      mkPos("300750", 200, 300, 400, "2026-10-08"),
			kline:    mkKline(150, 150),
			wantSkip: false, wantQty: 300, wantLotOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := EvaluateStop(tc.pos, tc.kline)
			if tr.Skipped != tc.wantSkip {
				t.Fatalf("Skipped=%v want %v (why=%q)", tr.Skipped, tc.wantSkip, tr.SkipWhy)
			}
			if tc.wantSkip {
				if tr.SkipWhy != tc.wantWhy {
					t.Fatalf("SkipWhy=%q want %q", tr.SkipWhy, tc.wantWhy)
				}
				return
			}
			if tr.OrderQty != tc.wantQty {
				t.Fatalf("OrderQty=%d want %d", tr.OrderQty, tc.wantQty)
			}
			if tc.wantLotOK && tr.OrderQty%LotSize != 0 {
				t.Fatalf("OrderQty=%d 应为整手数", tr.OrderQty)
			}
			if !strings.HasPrefix(tr.Reason, stopLossReasonPrefix) {
				t.Fatalf("Reason 缺少止损前缀: %q", tr.Reason)
			}
		})
	}
}

func TestStopMarketSellPrice(t *testing.T) {
	if got := stopMarketSellPrice(0); got != 0 {
		t.Fatalf("prevClose<=0 应返回 0, got %v", got)
	}
	// 100 × 0.902 = 90.2，贴近跌停价上沿的等效市价限价单
	if got := stopMarketSellPrice(100); !approxEq(got, 90.20) {
		t.Fatalf("stopMarketSellPrice(100)=%v want 90.20", got)
	}
}

// ---------------------------------------------------------------------------
// EnforceStops 集成测试（sqlite 临时库）
// ---------------------------------------------------------------------------

func seedStopPosition(t *testing.T, code string, qty, avail int64, stop float64, stopSetDate string) {
	t.Helper()
	p := mkPos(code, stop, qty, avail, stopSetDate)
	if err := db.MustGetDB().Create(&p).Error; err != nil {
		t.Fatalf("seed position: %v", err)
	}
}

func countOrders(t *testing.T, code, tradeDate string) int64 {
	t.Helper()
	var n int64
	if err := db.MustGetDB().Model(&model.Order{}).
		Where("code = ? AND trade_date = ?", code, tradeDate).Count(&n).Error; err != nil {
		t.Fatalf("count orders: %v", err)
	}
	return n
}

func TestEnforceStops_TriggerAndIdempotent(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	const date = "2026-10-09"

	// 300750：击穿 → 应生成 1 张止损 SELL 单
	h.seedKline("300750", date, 195, 234, 191)
	if err := db.MustGetDB().Model(&model.DailyKline{}).
		Where("code = ? AND date = ?", "300750", date).
		Update("low", 198).Error; err != nil {
		t.Fatalf("update low: %v", err)
	}
	seedStopPosition(t, "300750", 500, 500, 200, "2026-10-08")

	// 600000：未击穿 → 静默
	h.seedKline("600000", date, 10.5, 11.55, 9.45)
	seedStopPosition(t, "600000", 1000, 1000, 9.0, "2026-10-08")

	// 000001：风控线设定日=今日（T+1 建仓当日）→ SQL 过滤直接排除
	h.seedKline("000001", date, 8, 8.8, 7.2)
	seedStopPosition(t, "000001", 300, 300, 9, date)

	res, err := b.EnforceStops(date)
	if err != nil {
		t.Fatalf("EnforceStops: %v", err)
	}
	if res.Triggered != 1 {
		t.Fatalf("Triggered=%d want 1 (scanned=%d)", res.Triggered, res.Scanned)
	}
	if res.Scanned != 2 { // 000001 被 stop_set_date < tradeDate 过滤
		t.Fatalf("Scanned=%d want 2", res.Scanned)
	}
	if n := countOrders(t, "300750", date); n != 1 {
		t.Fatalf("300750 止损单数=%d want 1", n)
	}
	if n := countOrders(t, "600000", date); n != 0 {
		t.Fatalf("600000 不应有订单, got %d", n)
	}

	// 校验委托参数：SELL / PENDING / 数量整手 / Reason 前缀落在 RejectReason
	var ord model.Order
	if err := db.MustGetDB().Where("code = ? AND trade_date = ?", "300750", date).First(&ord).Error; err != nil {
		t.Fatalf("load stop order: %v", err)
	}
	if ord.Direction != model.DirectionSell || ord.Status != model.OrderStatusPending {
		t.Fatalf("direction/status 非法: %+v", ord)
	}
	if ord.Quantity != 500 || ord.Quantity%LotSize != 0 {
		t.Fatalf("quantity=%d 非法", ord.Quantity)
	}
	if !strings.HasPrefix(ord.RejectReason, stopLossReasonPrefix) {
		t.Fatalf("RejectReason 缺少止损标记: %q", ord.RejectReason)
	}
	// 委托价 = 当日收盘 195 × 0.902 = 175.89（MVP 口径，seedKline 以 open 填充 close）
	if !approxEq(ord.Price, 175.89) {
		t.Fatalf("price=%.2f want≈175.89", ord.Price)
	}

	// 幂等：同日重复调用不叠加
	res2, err := b.EnforceStops(date)
	if err != nil {
		t.Fatalf("EnforceStops rerun: %v", err)
	}
	if res2.Triggered != 0 || res2.SkippedAlreadyQueued != 1 {
		t.Fatalf("rerun triggered=%d queued_skip=%d want 0/1", res2.Triggered, res2.SkippedAlreadyQueued)
	}
	if n := countOrders(t, "300750", date); n != 1 {
		t.Fatalf("重复调用后止损单数=%d want 1", n)
	}
}

func TestEnforceStops_SellsTakePriorityOverManualPlan(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	const date = "2026-10-09"

	h.seedKline("300750", date, 195, 234, 191)
	seedStopPosition(t, "300750", 500, 500, 200, "2026-10-08")

	// 人工卖出计划已在途 → 止损监控跳过该持仓
	h.seedOrder("300750", model.DirectionSell, 210, 300, date)

	res, err := b.EnforceStops(date)
	if err != nil {
		t.Fatalf("EnforceStops: %v", err)
	}
	if res.Triggered != 0 || res.SkippedAlreadyQueued != 1 {
		t.Fatalf("triggered=%d queued=%d want 0/1", res.Triggered, res.SkippedAlreadyQueued)
	}
	if n := countOrders(t, "300750", date); n != 1 {
		t.Fatalf("订单总数=%d want 1（仅人工单）", n)
	}
}

func TestEnforceStops_NoQuoteSkipped(t *testing.T) {
	h := newHarness(t)
	b := h.broker()
	const date = "2026-10-09"

	// 停牌：无 K 线
	seedStopPosition(t, "301999", 500, 500, 200, "2026-10-08")

	res, err := b.EnforceStops(date)
	if err != nil {
		t.Fatalf("EnforceStops: %v", err)
	}
	if res.Triggered != 0 || res.SkippedNoQuote != 1 {
		t.Fatalf("triggered=%d no_quote=%d want 0/1", res.Triggered, res.SkippedNoQuote)
	}
}
