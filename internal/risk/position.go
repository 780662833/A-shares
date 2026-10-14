// Package risk 实现漏斗 Layer 3 的硬风控：风险预算仓位计算与组合级闸门。
//
// 设计决策（对应统一执行计划 Step 4 / 关键决策 #2）：
//   - LLM 只输出 action/confidence，禁止输出 quantity；股数一律由本包的
//     风险预算公式硬编码计算（CalculatePosition）。
//   - 单笔风险 = 总资产 × RiskPerTradePct；止损距离 = entry − stop(effective_stop)。
//     quantity = floor(单笔风险 / 止损距离 / 100) × 100。
//   - 同时受三层上限约束：单票市值上限、可用现金、BEAR 市总仓位动态闸门
//     （来自 /api/market/environment 的 max_position_pct，决策 #3 硬闸门部分）。
package risk

import (
	"fmt"
	"math"
)

// LotSize A 股一手 = 100 股，所有数量向下取整到手的整数倍。
const LotSize int64 = 100

// Config 风险预算参数。零值字段使用 DefaultConfig 的安全缺省。
type Config struct {
	// RiskPerTradePct 单笔交易最大可承受亏损占总资产比例（0.01 = 1%）。
	RiskPerTradePct float64
	// MaxPositionPct 单票持仓市值占总资产上限（0.20 = 20%）。
	MaxPositionPct float64
	// MinStopDistancePct 止损距离保护：若 (entry-stop)/entry 小于该值，
	// 说明止损贴得太近，计算出的仓位会畸大，直接拒绝下单（返回错误）。
	MinStopDistancePct float64
}

// DefaultConfig 保守缺省：单笔风险 1%，单票 20%，止损距离至少 1.5%。
func DefaultConfig() Config {
	return Config{
		RiskPerTradePct:    0.01,
		MaxPositionPct:     0.20,
		MinStopDistancePct: 0.015,
	}
}

// PositionRequest 仓位计算输入。
type PositionRequest struct {
	TotalAssets   float64 // 账户总资产（现金+冻结+持仓市值）
	AvailableCash float64 // 可用现金
	HeldValue     float64 // 该票当前持仓市值（加仓时传入，用于单票上限约束）
	EntryPrice    float64 // 计划买入价（一般用最新收盘价或昨收）
	EffectiveStop float64 // 实际风控线 = max(stop_loss, invalidation)，Step 1 契约
	GlobalMaxPct  float64 // BEAR 市总仓位闸门（0 表示不启用，用 1.0）
	Confidence    int     // LLM 信心分 0-100：<60 视为观察，不允许建仓
}

// PositionResult 仓位计算输出（quantity 为最终硬编码结果，LLM 无权覆盖）。
type PositionResult struct {
	Quantity      int64   `json:"quantity"`       // 建议股数（100 的整数倍）
	RiskAmount    float64 `json:"risk_amount"`    // 单笔预算风险金额（元）
	StopDistance  float64 `json:"stop_distance"`  // 每股止损距离（元）
	PositionValue float64 `json:"position_value"` // 预计持仓市值（元）
	CappedBy      string  `json:"capped_by"`      // 生效的最紧约束: risk/cash/single_cap/global/no_cap
}

// CalculatePosition 风险预算公式计算股数。任何不满足硬约束的请求返回 error，
// 调用方（Layer 3 节点）应将失败决策降级为 OBSERVE 并留痕。
func CalculatePosition(req PositionRequest, cfg Config) (*PositionResult, error) {
	if cfg.RiskPerTradePct <= 0 {
		cfg.RiskPerTradePct = DefaultConfig().RiskPerTradePct
	}
	if cfg.MaxPositionPct <= 0 {
		cfg.MaxPositionPct = DefaultConfig().MaxPositionPct
	}
	if cfg.MinStopDistancePct <= 0 {
		cfg.MinStopDistancePct = DefaultConfig().MinStopDistancePct
	}

	switch {
	case req.TotalAssets <= 0:
		return nil, fmt.Errorf("risk: total assets must be positive, got %.2f", req.TotalAssets)
	case req.EntryPrice <= 0:
		return nil, fmt.Errorf("risk: entry price must be positive, got %.2f", req.EntryPrice)
	case req.EffectiveStop <= 0 || req.EffectiveStop >= req.EntryPrice:
		return nil, fmt.Errorf("risk: effective_stop %.2f must be in (0, entry %.2f)", req.EffectiveStop, req.EntryPrice)
	case req.Confidence < 60:
		return nil, fmt.Errorf("risk: confidence %d < 60, downgrade to observe", req.Confidence)
	}

	stopDistance := req.EntryPrice - req.EffectiveStop
	distPct := stopDistance / req.EntryPrice
	if distPct < cfg.MinStopDistancePct {
		return nil, fmt.Errorf("risk: stop distance %.2f%% < min %.2f%%, reject (stop too tight)",
			distPct*100, cfg.MinStopDistancePct*100)
	}

	// 约束 1 (主): 单笔风险预算。
	riskAmount := req.TotalAssets * cfg.RiskPerTradePct
	qtyByRisk := int64(math.Floor(riskAmount/stopDistance/100.0)) * LotSize

	// 约束 2: 可用现金（含费用缓冲 0.3%：佣金万2.5+滑点余量）
	cashBudget := req.AvailableCash / (req.EntryPrice * 1.003)
	qtyCash := int64(math.Floor(cashBudget/100.0)) * LotSize

	// 约束 3: 单票市值上限（扣除已有持仓）。与风险预算共同构成"第一约束"，
	// 二者取小者即最终基础仓位；外层闸门只比较 cash / global。
	singleBudget := math.Max(req.TotalAssets*cfg.MaxPositionPct-req.HeldValue, 0)
	qtySingle := int64(math.Floor(singleBudget/req.EntryPrice/100.0)) * LotSize
	qty := qtyByRisk
	cappedBy := "risk"
	if qtySingle < qty {
		qty = qtySingle
		cappedBy = "single_cap"
	}

	// 约束 4: BEAR 市全局总仓位闸门（决策 #3 硬校验部分）
	qtyGlobal := int64(math.MaxInt64) // 无闸门时的哨兵（不参与取小）
	if req.GlobalMaxPct > 0 && req.GlobalMaxPct < 1.0 {
		globalBudget := math.Max(req.TotalAssets*req.GlobalMaxPct, 0)
		qtyGlobal = int64(math.Floor(globalBudget/req.EntryPrice/100.0)) * LotSize
	}

	// 外层闸门: 现金与全局仓位上限可与第一约束进一步取小。
	for _, c := range []struct {
		qty int64
		tag string
	}{
		{qtyCash, "cash"},
		{qtyGlobal, "global"},
	} {
		if c.qty < qty {
			qty = c.qty
			cappedBy = c.tag
		}
	}

	if qty < LotSize {
		return nil, fmt.Errorf("risk: computed quantity %d below one lot (capped by %s)", qty, cappedBy)
	}

	return &PositionResult{
		Quantity:      qty,
		RiskAmount:    round2(float64(qty) * stopDistance),
		StopDistance:  round2(stopDistance),
		PositionValue: round2(float64(qty) * req.EntryPrice),
		CappedBy:      cappedBy,
	}, nil
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
