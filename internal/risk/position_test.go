package risk

import "testing"

func baseReq() PositionRequest {
	return PositionRequest{
		TotalAssets:   1_000_000,
		AvailableCash: 1_000_000,
		EntryPrice:    100.0,
		EffectiveStop: 95.0, // 止损距离 5 元 = 5%
		Confidence:    80,
	}
}

// 风险预算主公式: 100万 × 1% / 5元 = 2000 股（单票上限 20% 恰好同为 2000）。
func TestCalculatePosition_RiskBudget(t *testing.T) {
	res, err := CalculatePosition(baseReq(), DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Quantity != 2000 {
		t.Errorf("quantity = %d, want 2000", res.Quantity)
	}
	if res.CappedBy != "risk" && res.CappedBy != "single_cap" {
		t.Errorf("capped_by = %q, want risk/single_cap", res.CappedBy)
	}
	if res.RiskAmount > 10_000+1e-9 {
		t.Errorf("risk_amount %.2f exceeds 1%% budget 10000", res.RiskAmount)
	}
	if res.Quantity%LotSize != 0 {
		t.Errorf("quantity %d not multiple of lot", res.Quantity)
	}
}

// 止损贴得太近（<1.5%）→ 拒绝，防止仓位畸大。
func TestCalculatePosition_StopTooTight(t *testing.T) {
	req := baseReq()
	req.EffectiveStop = 99.0 // 距离 1% < 1.5%
	if _, err := CalculatePosition(req, DefaultConfig()); err == nil {
		t.Fatal("expected error for tight stop")
	}
}

// confidence < 60 → 降级观察，不建仓（决策 #2：LLM 无权给数量）。
func TestCalculatePosition_LowConfidence(t *testing.T) {
	req := baseReq()
	req.Confidence = 55
	if _, err := CalculatePosition(req, DefaultConfig()); err == nil {
		t.Fatal("expected error for low confidence")
	}
}

// effective_stop >= entry 属于数据错误，必须报错而不是算出负仓位。
func TestCalculatePosition_StopAboveEntry(t *testing.T) {
	req := baseReq()
	req.EffectiveStop = 101.0
	if _, err := CalculatePosition(req, DefaultConfig()); err == nil {
		t.Fatal("expected error for stop above entry")
	}
}

// 现金约束生效: 只有 5 万现金时, qtyCash=500 < qtyRisk=200? 反例校验 capped_by。
func TestCalculatePosition_CashCap(t *testing.T) {
	req := baseReq()
	req.EntryPrice = 200.0
	req.EffectiveStop = 180.0 // 距离 20 元 → qtyRisk = 10000/20 = 500 股
	req.AvailableCash = 30_000
	res, err := CalculatePosition(req, DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.CappedBy != "cash" {
		t.Errorf("capped_by = %q, want cash", res.CappedBy)
	}
	if float64(res.Quantity)*req.EntryPrice*1.003 > req.AvailableCash+1e-6 {
		t.Errorf("position %.0f exceeds cash %.0f", float64(res.Quantity)*req.EntryPrice, req.AvailableCash)
	}
}

// 单票上限约束: 已持有 18% 市值时, 单票预算只剩 2% → single_cap 生效。
func TestCalculatePosition_SingleCap(t *testing.T) {
	req := baseReq()
	req.HeldValue = 180_000
	res, err := CalculatePosition(req, DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.CappedBy != "single_cap" {
		t.Errorf("capped_by = %q, want single_cap", res.CappedBy)
	}
	if res.PositionValue+req.HeldValue > 200_000+1e-6 {
		t.Errorf("single position %.2f breaches 20%% cap", res.PositionValue+req.HeldValue)
	}
}

// BEAR 市全局闸门（决策 #3）: GlobalMaxPct=0.3 且无持仓时, 全局预算 30 万;
// 若风险预算本身小于它则不生效。构造一个风险预算更大的场景验证 global 生效。
func TestCalculatePosition_GlobalGate(t *testing.T) {
	req := baseReq()        // entry 100, stop 95 → qtyRisk = 20000 股 (市值 200 万, 被 single_cap 削到 2000)
	req.GlobalMaxPct = 0.01 // BEAR 闸门: 全局最多 1% 总资产 = 1000 股 < single_cap 2000 → global 生效
	res, err := CalculatePosition(req, DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.CappedBy != "global" {
		t.Errorf("capped_by = %q, want global", res.CappedBy)
	}
	if res.PositionValue > 1_000_000*0.01+1e-6 {
		t.Errorf("global breach: %.2f", res.PositionValue)
	}
}

// 结果不足一手 → 明确报错, 调用方降级 OBSERVE。
func TestCalculatePosition_BelowOneLot(t *testing.T) {
	req := baseReq()
	req.TotalAssets = 10_000 // 风险预算 100 元 / 5 元距离 = 20 股 < 100
	if _, err := CalculatePosition(req, DefaultConfig()); err == nil {
		t.Fatal("expected below-lot error")
	}
}

// 零值 Config 应回退缺省而不是除零：结果与 DefaultConfig 完全一致
// （100万 × 1% / 5元 = 2000 股，单票 20% 上限同为 2000）。
func TestCalculatePosition_ZeroConfigFallback(t *testing.T) {
	res, err := CalculatePosition(baseReq(), Config{})
	if err != nil {
		t.Fatalf("zero config fallback failed: %v", err)
	}
	if res.Quantity != 2000 {
		t.Errorf("quantity = %d, want 2000 (same as DefaultConfig)", res.Quantity)
	}
	defRes, defErr := CalculatePosition(baseReq(), DefaultConfig())
	if defErr != nil {
		t.Fatalf("default config failed: %v", defErr)
	}
	if res.Quantity != defRes.Quantity || res.CappedBy != defRes.CappedBy {
		t.Errorf("zero config result %+v differs from default %+v", res, defRes)
	}
}
