// Package risk 实现漏斗模型 Layer3 的「结构风控与仓位计算」硬编码模块。
//
// 设计裁决（架构师确认版）：
//  1. LLM 只输出 action/confidence/reason，禁止输出股数 —— 股数由本包的风险预算公式计算；
//  2. 双止损线取较高者：
//     - invalidation(结构失效线, 如三买的中枢上沿 ZG)：逻辑死亡判定；
//     - price_stop(价格止损线, 如回抽低点×0.99)：资金保护线；
//     实际风控线 stop = max(invalidation, price_stop)，保证单笔亏损不超过预设风险预算；
//  3. ConfidenceScore 作为风险折扣系数（ConfidenceDiscount），信心越低承担的单笔风险越小。
//
// 风险预算法核心公式：
//
//	单笔最大允许亏损额 = TotalAssets × effectiveRiskPct
//	每股风险          = buyPrice − stop
//	理论股数          = 单笔最大允许亏损额 ÷ 每股风险
//	最终股数          = 理论股数向下取整到 100 股(1手)，再受现金/单票/总仓三重上限约束
package risk

import (
	"errors"
	"fmt"
	"math"
)

// ---------------------------------------------------------------------------
// 常量与配置
// ---------------------------------------------------------------------------

// LotSize A 股一手 = 100 股，所有委托数量必须是其整数倍。
const LotSize int64 = 100

// 默认参数（可由 Config 覆盖）。
const (
	// DefaultRiskPerTrade 单笔交易最大亏损占总资产比例：2%。
	DefaultRiskPerTrade = 0.02
	// DefaultMaxPositionRatioPerStock 单只股票市值占总资产上限：20%。
	DefaultMaxPositionRatioPerStock = 0.20
	// DefaultMaxTotalPositionRatio 全部持仓市值占总资产上限：80%。
	DefaultMaxTotalPositionRatio = 0.80
	// PriceStopBufferPct 价格止损缓冲：回抽低点再下移 1% 作为触发线，
	// 避免贴着最低点被盘中毛刺打掉。
	PriceStopBufferPct = 0.01
)

// ErrNoValidStop 两条止损线均不可用（缺失或 >= 买入价），拒绝交易。
var ErrNoValidStop = errors.New("risk: no valid stop-loss below buy price")

// Config 仓位计算器参数。零值字段使用上面的默认常量。
type Config struct {
	RiskPerTrade             float64 // 单笔风险预算比例（占净值），默认 0.02
	MaxPositionRatioPerStock float64 // 单票市值上限比例，默认 0.20
	MaxTotalPositionRatio    float64 // 总仓位上限比例，默认 0.80
}

// Normalize 填充缺省值并校验区间合法性。
func (c *Config) Normalize() error {
	if c == nil {
		return errors.New("risk: nil config")
	}
	if c.RiskPerTrade <= 0 {
		c.RiskPerTrade = DefaultRiskPerTrade
	}
	if c.MaxPositionRatioPerStock <= 0 {
		c.MaxPositionRatioPerStock = DefaultMaxPositionRatioPerStock
	}
	if c.MaxTotalPositionRatio <= 0 {
		c.MaxTotalPositionRatio = DefaultMaxTotalPositionRatio
	}
	if c.RiskPerTrade > 0.10 {
		return fmt.Errorf("risk: RiskPerTrade=%.4f too large (cap 0.10)", c.RiskPerTrade)
	}
	if c.MaxPositionRatioPerStock > 1 || c.MaxTotalPositionRatio > 1 {
		return fmt.Errorf("risk: position ratios must be in (0,1]: per_stock=%.2f total=%.2f",
			c.MaxPositionRatioPerStock, c.MaxTotalPositionRatio)
	}
	if c.MaxPositionRatioPerStock > c.MaxTotalPositionRatio {
		return fmt.Errorf("risk: MaxPositionRatioPerStock(%.2f) must <= MaxTotalPositionRatio(%.2f)",
			c.MaxPositionRatioPerStock, c.MaxTotalPositionRatio)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 输入 / 输出类型
// ---------------------------------------------------------------------------

// AccountSnapshot 账户快照（调用方从 accounts 表读取后传入，本包不触库）。
type AccountSnapshot struct {
	TotalAssets   float64 // 总资产 = 可用现金 + 冻结 + 持仓市值
	AvailableCash float64 // 可用现金
	HoldValue     float64 // 当前全部持仓市值之和
}

// StopPlan 双止损线。任一可为 0（表示该线缺失），两者皆 0/无效则拒单。
type StopPlan struct {
	InvalidationPrice float64 // 结构失效线（如中枢 ZG）
	PriceStop         float64 // 价格止损线（如回抽低点×0.99）
}

// EffectiveStop 按裁决规则返回实际风控线：两线都存在时取较高者；
// 只有一条且低于 buyPrice 时用该条；都不可用返回 ok=false。
//
// 返回值：(实际止损价, 来源标签 invalidation/price_stop, 是否有效)。
func (sp StopPlan) EffectiveStop(buyPrice float64) (float64, string, bool) {
	type cand struct {
		v   float64
		src string
	}
	cands := []cand{
		{sp.InvalidationPrice, "invalidation"},
		{sp.PriceStop, "price_stop"},
	}

	best := 0.0
	bestSrc := ""
	for _, c := range cands {
		if c.v > 0 && c.v < buyPrice && c.v > best {
			best = c.v
			bestSrc = c.src
		}
	}
	if best <= 0 {
		return 0, "", false
	}
	return best, bestSrc, true
}

// SizeInput 一次仓位计算的完整输入。
type SizeInput struct {
	Code            string
	BuyPrice        float64 // 预估成交价（最新收盘价口径，撮合日会再加滑点）
	Account         AccountSnapshot
	Stop            StopPlan
	ConfidenceScore int // LLM 输出的 0-100 信心分
}

// SizeResult 仓位计算结果。Quantity==0 时 Rejected=true 且 Reason 说明原因。
type SizeResult struct {
	Code          string  `json:"code"`
	Quantity      int64   `json:"quantity"`        // 最终股数（100 的整数倍）
	StopPrice     float64 `json:"stop_price"`      // 实际采用的风控线
	StopSource    string  `json:"stop_source"`     // invalidation / price_stop
	PerShareRisk  float64 `json:"per_share_risk"`  // buyPrice - stopPrice
	MaxLossAmount float64 `json:"max_loss_amount"` // 本笔最大理论亏损（含折扣后）
	EstimateCost  float64 `json:"estimate_cost"`   // quantity × buyPrice
	RatioOfAssets float64 `json:"ratio_of_assets"` // estimateCost / TotalAssets
	Rejected      bool    `json:"rejected"`
	Reason        string  `json:"reason,omitempty"`
}

// ---------------------------------------------------------------------------
// 置信度折扣
// ---------------------------------------------------------------------------

// ConfidenceDiscount 把 0-100 信心分映射为单笔风险预算折扣系数 ∈ [0,1]。
//
//	>=80  -> 1.00（满额风险预算）
//	60-79 -> 0.50（半额）
//	<60   -> 0.00（低于风控门槛，直接拒单——与 saveDecision 的 strong_buys>=60 口径一致）
func ConfidenceDiscount(score int) float64 {
	switch {
	case score >= 80:
		return 1.00
	case score >= 60:
		return 0.50
	default:
		return 0.00
	}
}

// ---------------------------------------------------------------------------
// 核心函数：CalculatePosition
// ---------------------------------------------------------------------------

// CalculatePosition 依据风险预算法计算最终可买股数。
//
// 步骤（每一步都可能产生拒单原因，记录在 result.Reason 中；函数本身只在
// 配置非法时返回 error，业务性拒单一律通过 SizeResult.Rejected 表达）：
//  1. 基础参数校验（价格/总资产/可用现金必须为正）；
//  2. 确定实际风控线 stop = max(invalidation, price_stop)（仅接受 < buyPrice 的线）；
//  3. 每股风险 perShareRisk = buyPrice − stop；<=0 拒单（止损价设置错误）；
//  4. 有效风险预算 = TotalAssets × RiskPerTrade × ConfidenceDiscount(score)；
//  5. 理论股数 = 预算 ÷ perShareRisk，向下取整到 100 股整数倍；
//  6. 三重上限钳制（每步重新向下取整到手）：
//     a. 现金上限：可用现金 ÷ (买价×1.003)（预留佣金+滑点缓冲，防次日废单）；
//     b. 单票上限：TotalAssets × MaxPositionRatioPerStock ÷ 买价；
//     c. 总仓上限：(TotalAssets × MaxTotalPositionRatio − HoldValue) ÷ 买价；
//  7. 取整后不足 1 手 => 拒单（小额碎钱不做无意义交易）。
func CalculatePosition(cfg *Config, in SizeInput) (*SizeResult, error) {
	var c Config
	if cfg != nil {
		c = *cfg
	}
	if err := c.Normalize(); err != nil {
		return nil, fmt.Errorf("risk: normalize config: %w", err)
	}

	res := &SizeResult{Code: in.Code}

	// ---- 1. 基础参数校验 ----
	if in.BuyPrice <= 0 {
		res.Rejected = true
		res.Reason = fmt.Sprintf("invalid buy_price %.4f", in.BuyPrice)
		return res, nil
	}
	if in.Account.TotalAssets <= 0 {
		res.Rejected = true
		res.Reason = fmt.Sprintf("invalid total_assets %.4f", in.Account.TotalAssets)
		return res, nil
	}
	if in.Account.AvailableCash <= 0 {
		res.Rejected = true
		res.Reason = fmt.Sprintf("no available cash %.4f", in.Account.AvailableCash)
		return res, nil
	}

	// ---- 2. 实际风控线 ----
	stop, stopSrc, ok := in.Stop.EffectiveStop(in.BuyPrice)
	if !ok {
		res.Rejected = true
		res.Reason = fmt.Sprintf("%v (buy=%.2f invalidation=%.2f price_stop=%.2f)",
			ErrNoValidStop, in.BuyPrice, in.Stop.InvalidationPrice, in.Stop.PriceStop)
		return res, nil
	}
	res.StopPrice = roundCent(stop)
	res.StopSource = stopSrc

	// ---- 3. 每股风险 ----
	perShareRisk := in.BuyPrice - stop
	if perShareRisk <= 0 {
		res.Rejected = true
		res.Reason = fmt.Sprintf("%v: per_share_risk=%.4f", ErrNoValidStop, perShareRisk)
		return res, nil
	}
	res.PerShareRisk = roundCent(perShareRisk)

	// ---- 4. 有效风险预算（含置信度折扣）----
	discount := ConfidenceDiscount(in.ConfidenceScore)
	if discount <= 0 {
		res.Rejected = true
		res.Reason = fmt.Sprintf("confidence_score=%d below threshold 60, reject by risk policy",
			in.ConfidenceScore)
		return res, nil
	}
	riskBudget := in.Account.TotalAssets * c.RiskPerTrade * discount
	res.MaxLossAmount = roundCent(riskBudget)

	// ---- 5. 理论股数，向下取整到 100 股整数倍 ----
	theoryQty := riskBudget / perShareRisk
	qty := floorToLot(theoryQty)

	// ---- 6a. 现金上限（预留 0.3% 费用/滑点缓冲）----
	cashCap := in.Account.AvailableCash / (in.BuyPrice * 1.003)
	if float64(qty) > cashCap {
		qty = floorToLot(cashCap)
	}

	// ---- 6b. 单票市值上限 ----
	singleCap := in.Account.TotalAssets * c.MaxPositionRatioPerStock / in.BuyPrice
	if float64(qty) > singleCap {
		qty = floorToLot(singleCap)
	}

	// ---- 6c. 总仓位上限（已有持仓市值计入）----
	headroom := in.Account.TotalAssets*c.MaxTotalPositionRatio - in.Account.HoldValue
	if headroom <= 0 {
		res.Rejected = true
		res.Reason = fmt.Sprintf("total position ratio already at cap (%.2f%% held of %.2f%% limit)",
			pct(in.Account.HoldValue, in.Account.TotalAssets), c.MaxTotalPositionRatio*100)
		return res, nil
	}
	totalCap := headroom / in.BuyPrice
	if float64(qty) > totalCap {
		qty = floorToLot(totalCap)
	}

	// ---- 7. 最终检查 ----
	if qty < LotSize {
		res.Rejected = true
		res.Reason = fmt.Sprintf("computed quantity %d < one lot(%d): risk budget %.2f / per-share %.2f too small for current account size",
			qty, LotSize, riskBudget, perShareRisk)
		return res, nil
	}

	res.Quantity = qty
	res.EstimateCost = roundCent(float64(qty) * in.BuyPrice)
	res.RatioOfAssets = round4(res.EstimateCost / in.Account.TotalAssets)
	return res, nil
}

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

// floorToLot 向下取整到 100 股整数倍；负数归零。
func floorToLot(v float64) int64 {
	if v <= 0 {
		return 0
	}
	return int64(math.Floor(v/float64(LotSize))) * LotSize
}

// roundCent 四舍五入到分。
func roundCent(v float64) float64 { return math.Round(v*100) / 100 }

// round4 保留 4 位小数（比例字段用）。
func round4(v float64) float64 { return math.Round(v*10000) / 10000 }

// pct part/total 百分比。
func pct(part, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return part / total * 100
}
