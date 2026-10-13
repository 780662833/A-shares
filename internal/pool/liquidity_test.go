// liquidity_test.go —— 阶段 4（流动性过滤与交易容量）专项验收测试。
// 覆盖计划中三条硬规则 + 双门槛 + 容量三态 + MANUAL 保护 + 全灭退化。全部离线。
package pool

import (
	"context"
	"strings"
	"testing"
	"time"
)

func amtRows(v float64, n int) []DailyAmountRow {
	rows := make([]DailyAmountRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, DailyAmountRow{Date: "d", Amount: v})
	}
	return rows
}

// ---- R1：成交额缺失不能自动视为零，也不能删分母假装达标 ----

func TestAdv20MissingNotZeroAndFixedDenominator(t *testing.T) {
	// 20 行观测，其中 5 行缺失（Amount<=0），其余 15 行均为 1 亿元。
	rows := amtRows(1e8, 15)
	rows = append(rows, amtRows(0, 3)...)
	rows = append(rows, DailyAmountRow{Date: "x", Amount: -1}) // 负值同样视为缺失
	rows = append(rows, DailyAmountRow{Date: "y", Amount: 0})
	res := adv20(rows)
	if res.Samples != 15 || res.Observed != 20 {
		t.Fatalf("R1 样本/观测计数错误: samples=%d observed=%d", res.Samples, res.Observed)
	}
	if res.Valid {
		t.Fatal("R1 有效样本 15<20 却判 Valid")
	}
	// 关键断言：分母恒为 20 —— 若"删掉缺失分母"则均值会是 1e8（假装达标），
	// 正确口径是 15*1e8/20 = 7.5e7（诚实拉低）。
	if got := res.Adv; got != 7.5e7 {
		t.Fatalf("R1 分母必须固定为 20: got=%.0f want=7.5e7", got)
	}
}

func TestAdv20IgnoresOlderThanWindow(t *testing.T) {
	// 超过 20 行的旧数据不参与计算（只看最近 20 个观测行）。
	rows := append(amtRows(9e8, 30), amtRows(6e7, 20)...)
	res := adv20(rows)
	if res.Observed != 20 || res.Adv != 6e7 {
		t.Fatalf("窗口截断错误: observed=%d adv=%.0f", res.Observed, res.Adv)
	}
}

// ---- R2：有效交易日不足 20 个 → DATA_INSUFFICIENT ----

func TestLiquidityDataInsufficientUnder20Samples(t *testing.T) {
	f := newFake()
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return amtRows(2e8, 19) // 19 个有效样本 < 20
	}
	g := NewGenerator(f, DefaultConfig())
	pool := []AggEntry{{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector}}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, time.Now())
	if len(out) != 1 {
		t.Fatalf("DATA_INSUFFICIENT 不得剔除记录: len=%d", len(out))
	}
	if out[0].Liquidity != LiqDataInsufficient {
		t.Fatalf("R2 期望 DATA_INSUFFICIENT, got %v", out[0].Liquidity)
	}
	out[0].ComputeTradability()
	if out[0].Tradability != TradUnknown || out[0].BlockedBy != "LIQUIDITY_DATA" {
		t.Fatalf("R2 数据不足应 UNKNOWN(LIQUIDITY_DATA): %s/%s", out[0].Tradability, out[0].BlockedBy)
	}
}

// ---- 双门槛：股票 5e7 / ETF 2e7，互不套用 ----

func TestDualThresholdStockVsETF(t *testing.T) {
	f := newFake()
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return amtRows(3e7, 20) // 3000 万：ETF 达标、股票不达标
	}
	g := NewGenerator(f, DefaultConfig())
	pool := []AggEntry{
		{Symbol: "510300.SH", PrimarySource: sourceETF, SourceType: sourceETF},
		{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector},
	}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, time.Now())
	if len(out) != 1 || out[0].Symbol != "510300.SH" {
		t.Fatalf("股票条目应按股票门槛被剔除, ETF 按自身门槛保留: %+v", out)
	}
	if out[0].Liquidity != LiqPass {
		t.Fatalf("ETF 3e7 >= 2e7 应 PASS, got %v", out[0].Liquidity)
	}
	if !strings.Contains(strings.Join(warns, "|"), "剔除 1 只") {
		t.Fatalf("缺少聚合 warning: %v", warns)
	}
}

func TestLiqWarnNearThreshold(t *testing.T) {
	f := newFake()
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return amtRows(5.5e7, 20) // 达标但余量 <20%
	}
	g := NewGenerator(f, DefaultConfig())
	pool := []AggEntry{{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector}}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, time.Now())
	if len(out) != 1 || out[0].Liquidity != LiqWarn {
		t.Fatalf("期望 WARN(余量<20%%), got %+v", out)
	}
	out[0].ComputeTradability()
	if out[0].Tradability != TradEligible {
		t.Fatalf("WARN 不应阻塞交易资格: %s", out[0].Tradability)
	}
}

// ---- 4.3 交易容量：Participation = plan/ADV20，上限 1% ----

func TestCapacityParticipation(t *testing.T) {
	// 用户示例：ADV=8000万, plan=120万 → 1.5% > 1% → OVER
	if s := capacityStatus(1.2e6, 8e7, 1.0); s != CapacityOver {
		t.Fatalf("120万/8000万=1.5%% 应 OVER, got %s", s)
	}
	// 恰好 1% 边界：不超过 → OK
	if s := capacityStatus(8e5, 8e7, 1.0); s != CapacityOK {
		t.Fatalf("参与率恰 1%% 应 OK, got %s", s)
	}
	// 候选池阶段无计划金额 → UNCHECKED（绝不伪装成通过）
	if s := capacityStatus(0, 8e7, 1.0); s != CapacityUnchecked {
		t.Fatalf("plan=0 应 UNCHECKED, got %s", s)
	}
	if s := capacityStatus(-1, 8e7, 1.0); s != CapacityUnchecked {
		t.Fatalf("plan<0 应 UNCHECKED, got %s", s)
	}
	if s := capacityStatus(1e6, 0, 1.0); s != CapacityUnchecked {
		t.Fatalf("adv<=0 无法计算参与率, 应 UNCHECKED, got %s", s)
	}
}

func TestCapacityGateMarksEntries(t *testing.T) {
	f := newFake()
	cfg := DefaultConfig()
	cfg.PlanNotional = 1.2e6 // 120 万计划单
	g := NewGenerator(f, cfg)
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		if code == "600001.SH" {
			return amtRows(8e7, 20) // ADV 8000万 → 1.5% OVER
		}
		return amtRows(8e8, 20) // ADV 8亿 → 0.15% OK
	}
	pool := []AggEntry{
		{Symbol: "600001.SH", PrimarySource: sourceSector, SourceType: sourceSector},
		{Symbol: "600002.SH", PrimarySource: sourceSector, SourceType: sourceSector},
	}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, time.Now())
	bySym := map[string]AggEntry{}
	for _, e := range out {
		bySym[e.Symbol] = e
	}
	if bySym["600001.SH"].CapacityStatus != CapacityOver {
		t.Fatalf("600001 应 OVER: %+v", bySym["600001.SH"])
	}
	if bySym["600002.SH"].CapacityStatus != CapacityOK {
		t.Fatalf("600002 应 OK: %+v", bySym["600002.SH"])
	}
	e := bySym["600001.SH"]
	e.ComputeTradability()
	if e.Tradability != TradBlocked || e.BlockedBy != "CAPACITY" {
		t.Fatalf("OVER 应 BLOCKED(CAPACITY): %s/%s", e.Tradability, e.BlockedBy)
	}
	// 注意：闸门返回的 map 副本不影响原 slice；重取原条目校验 OK 路径
	out[1].ComputeTradability()
	if out[1].Tradability != TradEligible {
		t.Fatalf("OK 容量不应阻塞: %s/%s", out[1].Tradability, out[1].BlockedBy)
	}
}

// ---- MANUAL 永不因流动性闸门剔除 ----

func TestManualNeverDroppedByLiquidity(t *testing.T) {
	f := newFake()
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return amtRows(1e5, 20) // 20 日均成交额仅 10 万元，远低于任何门槛
	}
	g := NewGenerator(f, DefaultConfig())
	pool := []AggEntry{
		{Symbol: "600519.SH", PrimarySource: sourceManual, SourceType: sourceManual},
	}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, time.Now())
	if len(out) != 1 {
		t.Fatalf("MANUAL 条目不得被流动性剔除: %+v", out)
	}
	if out[0].Liquidity != LiqFail {
		t.Fatalf("状态仍应诚实标 FAIL: %v", out[0].Liquidity)
	}
	out[0].ComputeTradability()
	if out[0].Tradability != TradBlocked || out[0].BlockedBy != "LIQUIDITY" {
		t.Fatalf("FAIL 应 BLOCKED(LIQUIDITY) 但记录保留: %s/%s", out[0].Tradability, out[0].BlockedBy)
	}
}

// ---- 全军覆没 = 数据事故 → 退化放行 ----

func TestLiquidityMassFailureDegenerates(t *testing.T) {
	f := newFake()
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return amtRows(1e3, 20) // 全部远低于门槛
	}
	g := NewGenerator(f, DefaultConfig())
	pool := []AggEntry{
		{Symbol: "600001.SH", PrimarySource: sourceSector, SourceType: sourceSector},
		{Symbol: "600002.SH", PrimarySource: sourceFund, SourceType: sourceFund},
	}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, time.Now())
	if len(out) != 2 {
		t.Fatalf("全军覆没应视为数据事故退化放行: %+v", out)
	}
	if !strings.Contains(strings.Join(warns, "|"), "事故") {
		t.Fatalf("退化路径必须留 warning: %v", warns)
	}
}

// ---- 开关关闭 = 行为回退到阶段 3 ----

func TestLiquidityDisabledPassthrough(t *testing.T) {
	f := newFake()
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		t.Fatal("开关关闭时不得调用 StockAmounts")
		return nil
	}
	cfg := DefaultConfig()
	cfg.LiquidityEnabled = false
	g := NewGenerator(f, cfg)
	pool := []AggEntry{{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector}}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, time.Now())
	if len(out) != 1 || out[0].Liquidity != "" {
		t.Fatalf("关闭时应原样透传: %+v", out)
	}
}

// ---- R3：单位口径——适配层换算后筛选逻辑只见"元" ----

func TestYiUnitFlowsThroughGateAsYuan(t *testing.T) {
	f := newFake()
	// 模拟上游返回"亿元"口径经适配层 normalizeYiAmount 换算后的结果：
	// 6000 万元 = 0.6e8 元 → 股票门槛 5e7 应 PASS。
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		v := normalizeYiAmount(0.6) // 0.6 亿 -> 6e7 元
		return amtRows(v, 20)
	}
	g := NewGenerator(f, DefaultConfig())
	pool := []AggEntry{{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector}}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, time.Now())
	if len(out) != 1 || out[0].Liquidity != LiqPass {
		t.Fatalf("0.6亿(换算后 6e7 元) 应 PASS: %+v", out)
	}
	if out[0].AvgAmount20D == nil || *out[0].AvgAmount20D != 6e7 {
		t.Fatalf("快照应以元保存: %+v", out[0].AvgAmount20D)
	}
}
