package pool

import "testing"

// ---------------------------------------------------------------------------
// 阶段 0 数据契约验收：枚举语义、单位换算、参数表对齐
// ---------------------------------------------------------------------------

func TestSourceStatusSemantics(t *testing.T) {
	// SUCCESS_EMPTY 是正常空结果：可用（生成器据此产出合法空来源）
	if !StatusSuccessEmpty.Usable() {
		t.Fatal("SUCCESS_EMPTY must be usable")
	}
	// FAILED 绝不能被当作"通过/空通过"
	if StatusFailed.Usable() {
		t.Fatal("FAILED must never be usable")
	}
	for _, s := range []SourceStatus{StatusOK, StatusPartial, StatusStale} {
		if !s.Usable() {
			t.Fatalf("%s should be usable", s)
		}
	}
}

func TestAmountUnitNormalization(t *testing.T) {
	// 亿元 -> 元 唯一入口，筛选逻辑只见"元"
	if got := normalizeYiAmount(1); got != YuanPerYi {
		t.Fatalf("1亿 should be 1e8元, got %v", got)
	}
	if got := normalizeYiAmount(0.5); got != 5e7 {
		t.Fatalf("0.5亿 should be 5e7, got %v", got)
	}
	// 与配置阈值口径一致性: LHBMinNetBuy=5000万 == normalizeYiAmount(0.5)
	p := Params()
	if p.LHBMinNetBuy != normalizeYiAmount(0.5) {
		t.Fatalf("LHB threshold unit mismatch: %v vs %v", p.LHBMinNetBuy, normalizeYiAmount(0.5))
	}
	if p.MinMarketCap != normalizeYiAmount(50) {
		t.Fatalf("market cap threshold unit mismatch: %v", p.MinMarketCap)
	}
	if formatYi(5e9) != "50.00亿" {
		t.Fatalf("formatYi diag output wrong: %s", formatYi(5e9))
	}
}

func TestParamsContractTable(t *testing.T) {
	p := Params()
	// 第一版参数表逐项核对（均为"元"口径）
	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"SectorTopN", float64(p.SectorTopN), 3},
		{"SectorLeadersK", float64(p.SectorLeadersK), 3},
		{"ETFTopM", float64(p.ETFTopM), 8},
		{"FundFlowMinNetBuy", p.LHBMinNetBuy, 5e7},    // 5000 万元
		{"FundFlowMinMarketCap", p.MinMarketCap, 5e9}, // 50 亿元
		{"StockMinPrice", p.MinPrice, 1.0},            // 1 元
		{"MinListingDays", float64(p.MinListedDays), 120},
		{"RSWindow(R60)", float64(p.RSWindow), 60},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	// ETF 与股票流动性门槛必须分开定义（不允许共用单一常量）
	if ContractStockMinADV20 == ContractETFMinADV20 {
		t.Error("stock and ETF ADV thresholds must differ")
	}
	if ContractStockMinADV20 != 5e7 || ContractETFMinADV20 != 2e7 {
		t.Errorf("ADV contract values wrong: stock=%v etf=%v", ContractStockMinADV20, ContractETFMinADV20)
	}
	// StrengthTopN 灰度期不截断
	if p.RSTopK != 0 {
		t.Errorf("RSTopK default should be 0 (no truncation in gray release), got %d", p.RSTopK)
	}
}

func TestEnumValuesAreFixedStrings(t *testing.T) {
	// 契约锁定：枚举字面量不可变更（下游落库/日志解析依赖）
	type kv struct{ got, want string }
	list := []kv{
		{string(StatusOK), "OK"}, {string(StatusSuccessEmpty), "SUCCESS_EMPTY"},
		{string(StatusPartial), "PARTIAL"}, {string(StatusStale), "STALE"}, {string(StatusFailed), "FAILED"},
		{string(EligPass), "PASS"}, {string(EligBlocked), "BLOCKED"}, {string(EligUnknown), "UNKNOWN"},
		{string(LiqPass), "PASS"}, {string(LiqWarn), "WARN"}, {string(LiqFail), "FAIL"}, {string(LiqDataInsufficient), "DATA_INSUFFICIENT"},
		{string(FinPass), "PASS"}, {string(FinWarn), "WARN"}, {string(FinBlocked), "BLOCKED"}, {string(FinDataInsufficient), "DATA_INSUFFICIENT"},
		{string(StrengthA), "A"}, {string(StrengthB), "B"}, {string(StrengthNone), "NONE"},
	}
	for _, p := range list {
		if p.got != p.want {
			t.Errorf("enum literal changed: %q != %q", p.got, p.want)
		}
	}
}
