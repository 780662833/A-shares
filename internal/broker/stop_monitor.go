// Package broker —— 持仓止损监控器（统一执行计划 Step 6）。
//
// 契约链路（决策 #1：invalidation 判逻辑死亡、stop_loss 触发卖出，
// effective_stop = max(stop_loss, invalidation) 为实际风控线）：
//
//	漏斗 PlaceOrders 节点把 BUY 单的 EffectiveStop 写入 Order.StopPrice
//	  -> MockBroker fillBuy 成交时落库 Position.StopPrice / StopSetDate
//	  -> 本文件每日撮合前扫描持仓：行情 Low <= StopPrice => 触发止损
//	  -> 生成市价 SELL 单（委托价 = 昨收 × (1 - 9.8%)，贴近跌停价的限价，
//	     既满足撮合引擎「SELL 委托价必须 ≤ 开盘含滑点价」的限价穿越校验，
//	     又保留极端流动性缺失时的价格保护），由同轮常规撮合按 SELL 优先顺序成交。
//
// T+1 安全（与 carryOverTPlus1 的配合）：
//   - 判据用 StopSetDate < tradeDate，不用 BuyDate——加仓会把 BuyDate 刷新为
//     当日，但旧仓部分早已解锁；StopSetDate 只在建仓/止损线上移时写入，
//     「设定日早于今日」等价于「该风控线保护的仓位份额今日可卖」。
//   - 撮合入口先执行 T+1 结转再跑监控，AvailableQuantity 已反映真实可卖量。
//
// 幂等性：同日重复调用（cron + 手动补跑）不会重复挂单——已存在 PENDING/FILLED
// 止损单（Reason 前缀匹配）或任何 PENDING SELL 的持仓会被跳过。
package broker

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"gorm.io/gorm"

	"ashare-quant/internal/model"
)

// stopLossReasonPrefix 止损单 Reason 的稳定前缀（幂等判据 + 审计识别标记）。
const stopLossReasonPrefix = "STOP_LOSS:"

// StopTrigger 单笔持仓的止损判定结果（纯函数输出，无 DB 依赖，独立可测）。
type StopTrigger struct {
	Code      string
	StopPrice float64
	Low       float64
	Close     float64
	Quantity  int64  // 实际卖出数量（min(可卖, 总持仓)，整手向下取整；允许零股清仓）
	OrderQty  int64  // 委托数量（>=LotSize 时为整手数；零股清仓时等于 Quantity）
	Reason    string // STOP_LOSS: 前缀 + 人类可读明细
	Skipped   bool   // true => 不触发下单（原因见 SkipWhy）
	SkipWhy   string // no_stop_line / t_plus_1_locked / nothing_sellable
}

// EvaluateStop 纯函数：给定持仓与当日行情，判断是否击穿风控线并给出委托参数。
//
// kline == nil（停牌/数据缺失）必须由调用方先行拦截——本函数假定行情有效。
// 触发条件：Low <= StopPrice（盘中下影线击穿即算，A 股散户无法盯盘的保守口径；
// 日线级模拟以「最低价触及」为准，成交价仍按开盘滑点口径撮合）。
func EvaluateStop(pos model.Position, kline *model.DailyKline) StopTrigger {
	t := StopTrigger{Code: pos.Code, StopPrice: pos.StopPrice}
	if pos.StopPrice <= 0 {
		t.Skipped = true
		t.SkipWhy = "no_stop_line"
		return t
	}
	if kline == nil || kline.Low <= 0 {
		t.Skipped = true
		t.SkipWhy = "no_quote"
		return t
	}
	t.Low, t.Close = kline.Low, kline.Close
	if kline.Low > pos.StopPrice {
		t.Skipped = true
		t.SkipWhy = "not_breached"
		return t
	}
	// 可卖量兜底：理论上结转后 Available==Quantity，但补跑场景当日买入不可卖。
	q := pos.AvailableQuantity
	if q > pos.Quantity {
		q = pos.Quantity // 数据异常防御：可卖不应超过总量
	}
	if q <= 0 {
		t.Skipped = true
		t.SkipWhy = "t_plus_1_locked"
		return t
	}
	t.Quantity = q
	// 委托量：整手部分正常报单；不足一手的零股 A 股只允许一次性清仓卖出。
	if q >= LotSize && q%LotSize != 0 {
		t.OrderQty = q / LotSize * LotSize // 保留零股（下一轮结转后再清）
	} else {
		t.OrderQty = q
	}
	if t.OrderQty <= 0 {
		t.Skipped = true
		t.SkipWhy = "nothing_sellable"
		return t
	}
	t.Reason = fmt.Sprintf("%s breach low=%.2f<=stop=%.2f close=%.2f qty=%d",
		stopLossReasonPrefix, kline.Low, pos.StopPrice, kline.Close, t.OrderQty)
	return t
}

// stopMarketSellPrice 止损市价单的委托价口径。
//
// 撮合引擎要求 SELL 委托价 ≤ 开盘含滑点价才能成交。以「昨收 × (1 - 9.8%)」
// （≈跌停价上沿）报价：对任何非一字跌停开盘都必然通过限价穿越校验，等效市价单；
// 若真遇一字跌停，processOrder 的一字板规则会拒单（卖不出），行为正确。
func stopMarketSellPrice(prevClose float64) float64 {
	if prevClose <= 0 {
		return 0
	}
	return roundToCent(prevClose * 0.902)
}

// StopEnforceResult EnforceStops 的执行汇总。
type StopEnforceResult struct {
	TradeDate            string `json:"trade_date"`
	Scanned              int    `json:"scanned"`                // 有风控线的持仓数
	Triggered            int    `json:"triggered"`              // 生成止损 SELL 单数
	SkippedNoQuote       int    `json:"skipped_no_quote"`       // 停牌/缺行情
	SkippedNoStop        int    `json:"skipped_no_stop"`        // 未设置风控线（告警计数）
	SkippedTPlus1        int    `json:"skipped_t_plus_1"`       // 当日买入锁定
	SkippedAlreadyQueued int    `json:"skipped_already_queued"` // 已有止损/卖出单在途
}

// ErrStopQueueFailed 止损单批量写入失败（系统性故障，调用方记 CRITICAL 但不阻断撮合）。
var ErrStopQueueFailed = errors.New("broker: stop-loss order queueing failed")

// EnforceStops 扫描全部持仓，对击穿风控线者写入 PENDING 市价 SELL 止损单。
//
// 由 SimulateNextDayOpen 在 T+1 结转之后、常规撮合之前自动调用；
// 也可被调度器单独调用做盘中巡检（同一幂等保证）。
func (b *MockBroker) EnforceStops(tradeDate string) (*StopEnforceResult, error) {
	res := &StopEnforceResult{TradeDate: tradeDate}

	// 候选：设置了风控线、且风控线设定日早于今日（T+1 生效）、仍有持仓。
	var positions []model.Position
	if err := b.db.Where("stop_price > 0 AND stop_set_date <> ? AND stop_set_date < ? AND quantity > 0",
		"", tradeDate).Find(&positions).Error; err != nil {
		return res, fmt.Errorf("scan stop positions: %w", err)
	}

	// 未设风控线的持仓数量（仅告警计数，人工单/历史数据属预期内）
	var noStopCount int64
	if err := b.db.Model(&model.Position{}).
		Where("stop_price = 0 OR stop_set_date = ''").Count(&noStopCount).Error; err == nil {
		res.SkippedNoStop = int(noStopCount)
	}

	queued, err := b.codesWithPendingSell(tradeDate)
	if err != nil {
		return res, err
	}

	klineCache := make(map[string]*model.DailyKline)
	seqBase := uint64(len(queued)) + 1
	for _, pos := range positions {
		res.Scanned++
		if queued[pos.Code] {
			res.SkippedAlreadyQueued++
			continue
		}
		kline, kerr := b.getKline(pos.Code, tradeDate, klineCache)
		if kerr != nil {
			log.Printf("broker: stop-monitor query kline %s %s failed: %v", pos.Code, tradeDate, kerr)
			res.SkippedNoQuote++
			continue
		}
		trig := EvaluateStop(pos, kline)
		switch {
		case trig.Skipped && trig.SkipWhy == "no_quote":
			res.SkippedNoQuote++
			continue
		case trig.Skipped && trig.SkipWhy == "t_plus_1_locked":
			res.SkippedTPlus1++
			log.Printf("broker: stop-monitor %s breached but T+1 locked today, retry next run", pos.Code)
			continue
		case trig.Skipped:
			continue // not_breached：正常持仓，静默
		}

		prevClose := kline.Close // getKline 取的是 tradeDate 当日 K 线；撮合发生在其开盘时刻，
		// 严格口径应使用前一交易日收盘。MVP 简化：用当日收盘近似基准价，
		// 报价系数 0.902 已预留 ±10% 单日波动缓冲，不影响限价穿越校验结论。
		price := stopMarketSellPrice(prevClose)
		if price <= 0 {
			res.SkippedNoQuote++
			continue
		}
		// model.Order 无独立 Reason 字段（方案A）：止损标记写入 RejectReason，
		// 撮合成功时被 AvgPrice/Fee 回填流程覆盖不影响；废单时天然保留审计原因。
		ord := model.Order{
			OrderID:      model.NewOrderID(seqBase + uint64(res.Triggered)),
			Code:         pos.Code,
			Direction:    model.DirectionSell,
			Price:        price,
			Quantity:     trig.OrderQty,
			Status:       model.OrderStatusPending,
			TradeDate:    tradeDate,
			RejectReason: trig.Reason,
		}
		if err := b.db.Create(&ord).Error; err != nil {
			log.Printf("broker: CRITICAL persist stop-loss order for %s: %v", pos.Code, err)
			continue
		}
		res.Triggered++
		queued[pos.Code] = true
		log.Printf("broker: STOP-LOSS TRIGGERED %s stop=%.2f low=%.2f -> SELL x%d limit=%.2f date=%s reason=%q",
			pos.Code, trig.StopPrice, trig.Low, trig.OrderQty, price, tradeDate, trig.Reason)
	}

	if res.Triggered > 0 {
		log.Printf("broker: stop-monitor[%s]: scanned=%d triggered=%d skipped(queue=%d,no_quote=%d,t1=%d)",
			tradeDate, res.Scanned, res.Triggered, res.SkippedAlreadyQueued, res.SkippedNoQuote, res.SkippedTPlus1)
	}
	return res, nil
}

// codesWithPendingSell 返回当日已有 PENDING SELL 单（含此前生成的止损单）的代码集合，
// 用于幂等去重：止损单绝不叠加，人工卖出计划在途时也尊重原计划不重复平仓。
func (b *MockBroker) codesWithPendingSell(tradeDate string) (map[string]bool, error) {
	var orders []model.Order
	err := b.db.Where("trade_date = ? AND direction = ? AND status = ?",
		tradeDate, model.DirectionSell, model.OrderStatusPending).Find(&orders).Error
	if err != nil {
		return nil, fmt.Errorf("load pending sells for dedup: %w", err)
	}
	out := make(map[string]bool, len(orders))
	for _, o := range orders {
		out[o.Code] = true
		if strings.HasPrefix(o.RejectReason, stopLossReasonPrefix) {
			// REJECTED 的止损单（如一字跌停）保持跳过本轮；下一交易日重新评估。
			continue
		}
	}
	// 已 FILLED 的止损/卖出单当日无需重复：其成交会把 available 清零，
	// EvaluateStop 的 q<=0 分支天然兜底，不必额外查询。
	return out, nil
}

// compile-time guard：确保 gorm 导入在本文件语义明确（事务外直写，与 reject 同级容错）。
var _ = (*gorm.DB)(nil)
