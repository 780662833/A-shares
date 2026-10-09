// Package broker 实现 A 股日线级别的模拟撮合引擎（Mock Broker）。
//
// 定位：本引擎不接任何真实券商柜台，仅基于「当日 DailyKline 开盘价」对
// 前一日收盘后挂起的 PENDING 订单做规则化模拟成交，是 AI 策略与数据库
// 之间唯一的资金/持仓写入通道。
//
// 已实现的 A 股核心交易规则：
//  1. T+1           —— SELL 单只能卖 Position.AvailableQuantity（当日买入不可卖）；
//     每日撮合开始前执行结转 AvailableQuantity = Quantity。
//  2. 涨跌停/一字板 —— BUY  委托价 >= 涨停价 且 开盘价 == 涨停价 => 拒单（买不到）；
//     SELL 委托价 <= 跌停价 且 开盘价 == 跌停价 => 拒单（卖不出）。
//  3. 滑点           —— 成交价以开盘价为基准：BUY ×(1+0.002)，SELL ×(1-0.002)。
//  4. 费用           —— 佣金 = 成交额×0.00025（最低 5 元，买卖双向）；
//     印花税 = 成交额×0.0005（仅卖出单边）。
//     另补充交易所硬性约束：整手（100 股整数倍）、限价单价格穿越校验、
//     买入资金充足性校验（可用现金 + 本单冻结额口径），拒绝一切透支路径。
//
// 并发模型：撮合任务由 cron 于每日 09:25 单线程触发；为防御手动补跑与
// 定时任务重叠，引擎内部用互斥锁保证 SimulateNextDayOpen 进程内串行。
// 每笔订单的「更新订单 + 更新持仓 + 更新账户」在一个 GORM 事务内完成，全或无，
// 任何一步失败都会整体回滚，绝不出现资金与持仓不一致的中间态。
package broker

import (
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"

	"ashare-quant/internal/model"
)

// ---------------------------------------------------------------------------
// 交易规则参数（集中定义，便于复盘/回测时统一调整）
// ---------------------------------------------------------------------------

const (
	// SlippageRate 滑点比例：千分之二。
	// BUY 成交价 = 开盘价 × (1 + SlippageRate)；SELL 成交价 = 开盘价 × (1 - SlippageRate)。
	SlippageRate = 0.002

	// CommissionRate 佣金费率：万分之 2.5（买卖双向收取）。
	CommissionRate = 0.00025

	// MinCommission 单笔佣金最低 5 元（A 股券商通行规则）。
	MinCommission = 5.0

	// StampTaxRate 印花税率：千分之 0.5，仅卖出单边收取。
	StampTaxRate = 0.0005

	// LotSize 一手 = 100 股，委托数量必须为其整数倍。
	LotSize = 100

	// priceEpsilon 价格比较容差（半个“分”）。
	// 涨停价/开盘价均为四舍五入到分的 float64，直接用 == 判定一字板
	// 存在二进制浮点误差风险，统一以 |a-b| < epsilon 判定“相等”。
	priceEpsilon = 0.005

	// dateLayout DailyKline.Date / Order.TradeDate 的统一格式。
	dateLayout = "2006-01-02"
)

// ---------------------------------------------------------------------------
// 错误定义
// ---------------------------------------------------------------------------

var (
	// ErrNilDB 未注入数据库句柄。
	ErrNilDB = errors.New("broker: nil gorm.DB")

	// ErrInvalidTradeDate tradeDate 格式非法（要求 YYYY-MM-DD）。
	ErrInvalidTradeDate = errors.New("broker: invalid trade date, expect YYYY-MM-DD")

	// ErrAccountNotFound 默认账户不存在（Phase 2 的 db.InitDB 未执行）。
	ErrAccountNotFound = errors.New("broker: default account not found, run db.InitDB first")
)

// ---------------------------------------------------------------------------
// 撮合结果类型
// ---------------------------------------------------------------------------

// FillResult 单笔订单撮合结果明细。
type FillResult struct {
	OrderID   string  `json:"order_id"`
	Code      string  `json:"code"`
	Direction string  `json:"direction"`
	Status    string  `json:"status"`    // FILLED / REJECTED / PENDING(事务失败待重试)
	AvgPrice  float64 `json:"avg_price"` // 成交均价（含滑点，四舍五入到分）
	Fee       float64 `json:"fee"`       // 总费用（佣金[+印花税]）
	CashFlow  float64 `json:"cash_flow"` // 对可用现金的净影响：买入为负、卖出为正（仅 FILLED 有意义）
	Reason    string  `json:"reason,omitempty"`
}

// SimulationSummary SimulateNextDayOpen 的整体结果，供调度层写日志/推送复盘。
type SimulationSummary struct {
	TradeDate string        `json:"trade_date"`
	Total     int           `json:"total"`    // 参与撮合的 PENDING 订单数
	Filled    int           `json:"filled"`   // 成交数
	Rejected  int           `json:"rejected"` // 拒单数
	Retried   int           `json:"retried"`  // 事务系统性失败、保持 PENDING 待重试的订单数
	Results   []FillResult  `json:"results"`  // 逐单明细（按撮合顺序）
	Elapsed   time.Duration `json:"elapsed"`  // 耗时
}

// String 输出人类可读的单行摘要（log 友好）。
func (s *SimulationSummary) String() string {
	return fmt.Sprintf("simulate[%s]: total=%d filled=%d rejected=%d retrying=%d elapsed=%s",
		s.TradeDate, s.Total, s.Filled, s.Rejected, s.Retried, s.Elapsed)
}

// ---------------------------------------------------------------------------
// MockBroker 撮合引擎
// ---------------------------------------------------------------------------

// MockBroker 模拟撮合引擎。零值不可用，请通过 NewMockBroker 构造。
type MockBroker struct {
	db *gorm.DB
	mu sync.Mutex // 进程内串行锁（防御 cron 重叠触发 / 手动补跑并发）
}

// NewMockBroker 构造撮合引擎。db 必须是已完成 AutoMigrate 的 *gorm.DB。
func NewMockBroker(db *gorm.DB) (*MockBroker, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	return &MockBroker{db: db}, nil
}

// ---------------------------------------------------------------------------
// 对外核心接口
// ---------------------------------------------------------------------------

// SimulateNextDayOpen 在次日 09:25（集合竞价结束后）被调度器调用，
// 处理所有 Status=PENDING 且 TradeDate=tradeDate 的订单：
//
//	流程：结转 T+1 可卖数量 -> 逐单加载行情 -> 风控校验 -> 撮合成交/拒单 -> 落库
//
// 撮合顺序：先 SELL 后 BUY（组内均按创建时间正序）。理由：卖出释放的现金
// 可以支持同批次买单，更接近真实账户操作习惯，也降低无谓的“资金不足”拒单。
//
// T+1 结转时机：仅当该交易日首次执行撮合时才结转（以 orders.trade_date 中
// 早于本交易日的记录判断“引擎已在往日运行过”）。这样既满足“次日开盘解锁
// 可卖”，又保证同日重复调用/补跑不会把当日买入错误地解锁成可卖。
//
// 返回的 error 仅在系统性故障（参数非法/查询账户失败等）时非空；
// 单笔订单被拒不算 error，而是 Results[i].Status=REJECTED。
func (b *MockBroker) SimulateNextDayOpen(tradeDate string) (*SimulationSummary, error) {
	// 1. 入参校验
	tradeDate = strings.TrimSpace(tradeDate)
	if _, err := time.Parse(dateLayout, tradeDate); err != nil {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTradeDate, tradeDate)
	}

	// 2. 进程内串行，防止补跑与定时任务并发写坏账本
	b.mu.Lock()
	defer b.mu.Unlock()

	start := time.Now()
	summary := &SimulationSummary{TradeDate: tradeDate}

	// 3. T+1 结转：昨日及更早买入的持仓今日解锁为可卖（AvailableQuantity = Quantity）。
	//    本轮成交的买单只加 Quantity 不加 Available（见 fillBuy），因此同日重跑安全。
	firstRun, err := b.isFirstRunOfDay(tradeDate)
	if err != nil {
		return nil, err
	}
	if firstRun {
		if err := b.carryOverTPlus1(); err != nil {
			return nil, fmt.Errorf("broker: carry over T+1 before simulation: %w", err)
		}
	} else {
		log.Printf("broker: %s re-run detected, skip T+1 carry-over", tradeDate)
	}

	// 4. 加载待撮合订单（SELL 优先，其次 BUY；组内按创建时间正序，保证可回放）
	pending, err := b.loadPendingOrders(tradeDate)
	if err != nil {
		return nil, err
	}
	summary.Total = len(pending)
	if summary.Total == 0 {
		summary.Elapsed = time.Since(start)
		log.Printf("broker: %s no pending orders to simulate", tradeDate)
		return summary, nil
	}

	// 5. 加载默认账户（单账户系统，取最早一行）
	account, err := b.loadDefaultAccount()
	if err != nil {
		return nil, err
	}

	// 6. 逐单撮合。cash 为本轮撮合过程中的内存态“可动用资金”，口径为：
	//    可用现金 + 本批 PENDING BUY 单中被真实冻结的资金（见 effectiveBuyableCash）。
	//    fillBuy/fillSell/reject 事务内已各自完成 DB 层的即时扣减/入账/解冻，
	//    因此第 7 步不再整体覆盖写回，只做一致性校验。
	cash, err := b.effectiveBuyableCash(account, pending)
	if err != nil {
		return nil, err
	}
	klineCache := make(map[string]*model.DailyKline) // 同批次内同股票行情只查一次

	for i := range pending {
		ord := pending[i]
		res := b.processOrder(ord, cash, klineCache)
		switch res.Status {
		case model.OrderStatusFilled:
			cash += res.CashFlow
			summary.Filled++
		case model.OrderStatusRejected:
			summary.Rejected++
		default: // PENDING：事务系统性失败，留待下次补跑
			summary.Retried++
		}
		summary.Results = append(summary.Results, res)
	}

	// 7. 安全断言：可动用资金不允许为负——每笔 BUY 都做了资金校验且事务内
	//    即时结算，若仍出现负数说明存在逻辑漏洞，宁可报错也不静默写坏账本。
	if cash < -priceEpsilon {
		return nil, fmt.Errorf("broker: invariant violated: buyable cash became %.2f < 0 after simulation", cash)
	}
	// 复核账户终态（DB 是唯一事实来源；此处仅告警，不覆盖事务已写入的值）
	if finalAcc, ferr := b.loadDefaultAccount(); ferr == nil && finalAcc.AvailableCash < -priceEpsilon {
		return nil, fmt.Errorf("broker: invariant violated: account available_cash=%.2f < 0", finalAcc.AvailableCash)
	}

	summary.Elapsed = time.Since(start)
	log.Printf("broker: %s", summary.String())
	for _, r := range summary.Results {
		switch r.Status {
		case model.OrderStatusFilled:
			log.Printf("broker:   %-8s %s %-4s %s fillPrice=%.2f fee=%.2f cashflow=%+.2f",
				r.Status, r.OrderID, r.Direction, r.Code, r.AvgPrice, r.Fee, r.CashFlow)
		default:
			log.Printf("broker:   %-8s %s %-4s %s reason=%s", r.Status, r.OrderID, r.Direction, r.Code, r.Reason)
		}
	}
	return summary, nil
}

// ---------------------------------------------------------------------------
// 单笔订单处理：校验 -> 事务落库
// ---------------------------------------------------------------------------

// processOrder 对单笔 PENDING 订单执行全部风控校验并撮合。
// cash 是当前内存态可用现金（不含本单自身冻结额）。
// 校验失败 => 置 REJECTED；成交事务系统性失败 => 保持 PENDING（不误杀单）。
func (b *MockBroker) processOrder(ord model.Order, cash float64, klineCache map[string]*model.DailyKline) FillResult {
	base := FillResult{
		OrderID:   ord.OrderID,
		Code:      ord.Code,
		Direction: strings.ToUpper(strings.TrimSpace(ord.Direction)),
	}

	// --- 静态合法性校验（不依赖行情）---
	if ord.Quantity <= 0 || ord.Quantity%LotSize != 0 {
		return b.reject(ord, base, fmt.Sprintf("委托数量 %d 非法：须为正整数且为 %d 股的整数倍", ord.Quantity, LotSize))
	}
	if ord.Price <= 0 || math.IsNaN(ord.Price) || math.IsInf(ord.Price, 0) {
		return b.reject(ord, base, fmt.Sprintf("委托价 %.4f 非法", ord.Price))
	}
	dir := base.Direction
	if dir != model.DirectionBuy && dir != model.DirectionSell {
		return b.reject(ord, base, fmt.Sprintf("订单方向 %q 非法", ord.Direction))
	}

	// --- 行情校验：当日 K 线必须存在（否则视为停牌/数据缺失，无法撮合）---
	kline, err := b.getKline(ord.Code, ord.TradeDate, klineCache)
	if err != nil {
		// 查询本身出错属系统性故障，保守起见保持 PENDING 待重试
		return FillResult{OrderID: base.OrderID, Code: base.Code, Direction: base.Direction,
			Status: model.OrderStatusPending, Reason: fmt.Sprintf("查询当日行情失败，保持 PENDING 待重试: %v", err)}
	}
	if kline == nil {
		return b.reject(ord, base, "当日无行情数据（停牌或数据未同步），无法撮合")
	}

	// --- 涨跌停 / 一字板校验（题设核心规则，优先级最高）---
	// 一字板判定：开盘价 == 涨停价（卖压锁死，买单排不到）；开盘价 == 跌停价（无买盘，卖单排不出）。
	// BUY  且 委托价 >= 涨停价 且 开盘 == 涨停价 => 拒单（买不到）
	// SELL 且 委托价 <= 跌停价 且 开盘 == 跌停价 => 拒单（卖不出）
	if dir == model.DirectionBuy &&
		ord.Price >= kline.LimitUpPrice-priceEpsilon && nearlyEqual(kline.Open, kline.LimitUpPrice) {
		return b.reject(ord, base, fmt.Sprintf("涨停一字板无法买入（开盘价=涨停价=%.2f）", kline.LimitUpPrice))
	}
	if dir == model.DirectionSell &&
		ord.Price <= kline.LimitDownPrice+priceEpsilon && nearlyEqual(kline.Open, kline.LimitDownPrice) {
		return b.reject(ord, base, fmt.Sprintf("跌停一字板无法卖出（开盘价=跌停价=%.2f）", kline.LimitDownPrice))
	}

	// --- 限价单价格穿越校验（模拟交易所限价保护）---
	// BUY ：成交价不可能优于委托价上限 => 开盘含滑点价 > 委托价，无法成交；
	// SELL：成交价不可能高于委托价下限 => 开盘含滑点价 < 委托价，无法成交。
	fillPrice := calcFillPrice(kline.Open, dir)
	if dir == model.DirectionBuy && fillPrice > ord.Price+priceEpsilon {
		return b.reject(ord, base, fmt.Sprintf("限价单无法成交：开盘含滑点价 %.2f 高于委托价 %.2f", fillPrice, ord.Price))
	}
	if dir == model.DirectionSell && fillPrice < ord.Price-priceEpsilon {
		return b.reject(ord, base, fmt.Sprintf("限价单无法成交：开盘含滑点价 %.2f 低于委托价 %.2f", fillPrice, ord.Price))
	}

	// --- 方向性风控 + 成交 ---
	switch dir {
	case model.DirectionBuy:
		amount := fillPrice * float64(ord.Quantity)
		totalCost := roundToCent(amount) + calcBuyFee(amount)
		if err := b.validateBuyFunds(ord, cash, totalCost); err != nil {
			return b.reject(ord, base, err.Error())
		}
		res, terr := b.fillBuy(ord, fillPrice)
		if terr != nil {
			log.Printf("broker: fill BUY %s failed, order stays PENDING for retry: %v", ord.OrderID, terr)
			res.Status = model.OrderStatusPending
			res.Reason = fmt.Sprintf("成交事务失败，保持 PENDING 待重试: %v", terr)
			return res
		}
		return res

	case model.DirectionSell:
		if err := b.validateSellPosition(ord); err != nil {
			return b.reject(ord, base, err.Error())
		}
		res, terr := b.fillSell(ord, fillPrice)
		if terr != nil {
			log.Printf("broker: fill SELL %s failed, order stays PENDING for retry: %v", ord.OrderID, terr)
			res.Status = model.OrderStatusPending
			res.Reason = fmt.Sprintf("成交事务失败，保持 PENDING 待重试: %v", terr)
			return res
		}
		return res
	}

	// 理论上不可达（方向已在前面校验）
	return b.reject(ord, base, "unknown direction")
}

// reject 将订单标记为 REJECTED 并回填原因；同时若该单存在下单冻结，解冻回可用现金。
// 注意：reject 属于“终态写入”，即便 DB 出错也只能记日志——此时订单仍是 PENDING，
// 下一轮会重新走一遍校验并再次尝试拒单，天然幂等。
func (b *MockBroker) reject(ord model.Order, base FillResult, reason string) FillResult {
	base.Status = model.OrderStatusRejected
	base.Reason = reason

	err := b.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.Order{}).
			Where("id = ? AND status = ?", ord.ID, model.OrderStatusPending).
			Updates(map[string]interface{}{
				"status":        model.OrderStatusRejected,
				"reject_reason": truncate(reason, 128),
			}).Error; err != nil {
			return fmt.Errorf("update order status: %w", err)
		}
		// 废单解冻：把本单历史冻结的资金退回可用现金（若从未冻结则为 no-op）
		return unfreezeOrderCash(tx, ord)
	})
	if err != nil {
		log.Printf("broker: CRITICAL persist rejection of %s failed: %v", ord.OrderID, err)
	}
	return base
}

// ---------------------------------------------------------------------------
// 风控校验
// ---------------------------------------------------------------------------

// validateBuyFunds 买入资金校验。
//
// cash 为 effectiveBuyableCash 得出的内存态“可动用资金”，其口径已经包含
// 全部 PENDING BUY 单（含本单）被真实冻结的资金，因此这里直接比较：
//
//	cash >= totalCost（成交额 + 佣金）
//
// 无论系统是否启用下单冻结（PlaceOrder 属后续 Phase），该不变式都成立，
// 且同批次多张买单会逐单扣减共享额度，不会凭空创造购买力。
func (b *MockBroker) validateBuyFunds(ord model.Order, cash, totalCost float64) error {
	if cash+priceEpsilon < totalCost {
		return fmt.Errorf("资金不足：需 %.2f（含费用），当前可动用 %.2f", totalCost, cash)
	}
	return nil
}

// validateSellPosition T+1 卖出校验：可卖数量必须覆盖委托量。
func (b *MockBroker) validateSellPosition(ord model.Order) error {
	var pos model.Position
	err := b.db.Where("code = ?", ord.Code).First(&pos).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.New("无持仓，无法卖出")
	}
	if err != nil {
		return fmt.Errorf("查询持仓失败: %w", err)
	}
	if pos.AvailableQuantity < ord.Quantity {
		return fmt.Errorf("T+1 校验失败：可卖 %d 股 < 委托 %d 股（当日买入部分不可卖）",
			pos.AvailableQuantity, ord.Quantity)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 成交事务
// ---------------------------------------------------------------------------

// fillBuy 买入成交事务（全或无）：
//   - Order    -> FILLED，回填 AvgPrice / Fee
//   - Position -> Quantity += qty，AvailableQuantity 不变（当日买入不可卖 => T+1）；
//     成本价按摊薄加权平均更新；无持仓行则新建
//   - Account  -> 总支出 = 成交额 + 佣金：优先消耗本单冻结额，不足部分走可用现金
func (b *MockBroker) fillBuy(ord model.Order, fillPrice float64) (FillResult, error) {
	amount := roundToCent(fillPrice * float64(ord.Quantity))
	fee := calcBuyFee(amount)
	totalCost := amount + fee

	res := FillResult{
		OrderID: ord.OrderID, Code: ord.Code, Direction: model.DirectionBuy,
		Status: model.OrderStatusFilled, AvgPrice: fillPrice, Fee: fee,
		CashFlow: -totalCost,
	}

	err := b.db.Transaction(func(tx *gorm.DB) error {
		// 1. 订单状态（带 status=PENDING 条件，防并发重复成交）
		r := tx.Model(&model.Order{}).
			Where("id = ? AND status = ?", ord.ID, model.OrderStatusPending).
			Updates(map[string]interface{}{
				"status":    model.OrderStatusFilled,
				"avg_price": fillPrice,
				"fee":       fee,
			})
		if r.Error != nil {
			return fmt.Errorf("update order: %w", r.Error)
		}
		if r.RowsAffected == 0 {
			return fmt.Errorf("order %s already settled by another run, skip", ord.OrderID)
		}

		// 2. 持仓 upsert
		if err := applyBuyToPosition(tx, ord, fillPrice); err != nil {
			return err
		}

		// 3. 账户结算：冻结冲抵 + 现金扣减（事务内即时完成，DB 为唯一事实来源）
		return settleBuyCash(tx, ord, totalCost)
	})
	if err != nil {
		return res, err
	}
	return res, nil
}

// fillSell 卖出成交事务（全或无）：
//   - Order    -> FILLED，回填 AvgPrice / Fee（佣金+印花税）
//   - Position -> Quantity 与 AvailableQuantity 同减（卖的必是已解锁额度）；
//     清仓保留零持仓行并将成本归零，便于审计与复盘统计
//   - Account  -> AvailableCash += （成交额 - 佣金 - 印花税）
func (b *MockBroker) fillSell(ord model.Order, fillPrice float64) (FillResult, error) {
	amount := roundToCent(fillPrice * float64(ord.Quantity))
	fee := calcSellFee(amount)
	proceeds := roundToCent(amount - fee)

	res := FillResult{
		OrderID: ord.OrderID, Code: ord.Code, Direction: model.DirectionSell,
		Status: model.OrderStatusFilled, AvgPrice: fillPrice, Fee: fee,
		CashFlow: proceeds,
	}

	err := b.db.Transaction(func(tx *gorm.DB) error {
		r := tx.Model(&model.Order{}).
			Where("id = ? AND status = ?", ord.ID, model.OrderStatusPending).
			Updates(map[string]interface{}{
				"status":    model.OrderStatusFilled,
				"avg_price": fillPrice,
				"fee":       fee,
			})
		if r.Error != nil {
			return fmt.Errorf("update order: %w", r.Error)
		}
		if r.RowsAffected == 0 {
			return fmt.Errorf("order %s already settled by another run, skip", ord.OrderID)
		}

		var pos model.Position
		if err := tx.Where("code = ?", ord.Code).First(&pos).Error; err != nil {
			return fmt.Errorf("load position for sell: %w", err)
		}
		newQty := pos.Quantity - ord.Quantity
		newAvail := pos.AvailableQuantity - ord.Quantity
		if newQty < 0 || newAvail < 0 {
			return fmt.Errorf("position underflow on sell %s: qty=%d avail=%d sell=%d",
				ord.OrderID, pos.Quantity, pos.AvailableQuantity, ord.Quantity)
		}
		updates := map[string]interface{}{
			"quantity":           newQty,
			"available_quantity": newAvail,
		}
		if newQty == 0 {
			updates["cost_price"] = 0.0 // 清仓后成本归零
		}
		if err := tx.Model(&model.Position{}).Where("id = ?", pos.ID).Updates(updates).Error; err != nil {
			return fmt.Errorf("update position: %w", err)
		}

		acc, err := loadDefaultAccountTx(tx)
		if err != nil {
			return err
		}
		if err := tx.Model(&model.Account{}).Where("id = ?", acc.ID).
			Update("available_cash", gorm.Expr("available_cash + ?", proceeds)).Error; err != nil {
			return fmt.Errorf("credit sell proceeds: %w", err)
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	return res, nil
}

// applyBuyToPosition 买入成交后的持仓变更（事务内调用）。
// 摊薄成本价：newCost = (oldQty*oldCost + qty*fillPrice) / (oldQty+qty)
// —— 采用纯成交价口径（不把手续费计入成本），与常见复盘盈亏算法一致。
func applyBuyToPosition(tx *gorm.DB, ord model.Order, fillPrice float64) error {
	var pos model.Position
	err := tx.Where("code = ?", ord.Code).First(&pos).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		pos = model.Position{
			Code:              ord.Code,
			Quantity:          ord.Quantity,
			AvailableQuantity: 0, // T+1：当日买入不可卖
			CostPrice:         fillPrice,
			BuyDate:           ord.TradeDate,
		}
		if err := tx.Create(&pos).Error; err != nil {
			return fmt.Errorf("create position for %s: %w", ord.Code, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("load position for buy: %w", err)
	}
	newQty := pos.Quantity + ord.Quantity
	newCost := fillPrice
	if pos.Quantity > 0 {
		newCost = roundToCent((float64(pos.Quantity)*pos.CostPrice + float64(ord.Quantity)*fillPrice) / float64(newQty))
	}
	// AvailableQuantity 有意不动：新买入部分今日锁定，明日结转解锁
	updates := map[string]interface{}{
		"quantity":   newQty,
		"cost_price": newCost,
		"buy_date":   ord.TradeDate,
	}
	if err := tx.Model(&model.Position{}).Where("id = ?", pos.ID).Updates(updates).Error; err != nil {
		return fmt.Errorf("update position on buy: %w", err)
	}
	return nil
}

// settleBuyCash 买入成交后的资金结算（事务内调用）。
//
// 冻结口径说明：下单冻结（PlaceOrder，属后续 Phase）会把“委托价×数量+预估佣金”
// 从 AvailableCash 移入 FrozenCash。撮合成功时本单冻结额中：
//   - min(frozen, totalCost) 部分用于冲抵实际支出（FrozenCash -= 该值）；
//   - 超出实际支出的差额退回 AvailableCash；
//   - 实际支出超出冻结的差额（如滑点导致成本略高）继续扣 AvailableCash。
//
// 若系统从未做过冻结（MVP：策略直写 PENDING 单，FrozenCash=0），公式自然
// 退化为 available_cash -= totalCost，两种模式共用一条不变式，绝不重复扣款。
func settleBuyCash(tx *gorm.DB, ord model.Order, totalCost float64) error {
	acc, err := loadDefaultAccountTx(tx)
	if err != nil {
		return err
	}
	frozenOfOrder := math.Min(orderFrozenAmount(ord), acc.FrozenCash) // 防御：不得超过账户总冻结
	deductFromFrozen := math.Min(frozenOfOrder, totalCost)
	fromAvailable := roundToCent(totalCost - deductFromFrozen)
	refundToAvailable := roundToCent(frozenOfOrder - deductFromFrozen)

	newAvailable := roundToCent(acc.AvailableCash - fromAvailable + refundToAvailable)
	newFrozen := roundToCent(acc.FrozenCash - deductFromFrozen)
	if newAvailable < -priceEpsilon {
		return fmt.Errorf("account underflow settling buy %s: available %.2f", ord.OrderID, newAvailable)
	}
	if newFrozen < -priceEpsilon {
		newFrozen = 0 // 浮点残差清理
	}
	if err := tx.Model(&model.Account{}).Where("id = ?", acc.ID).
		Updates(map[string]interface{}{"available_cash": newAvailable, "frozen_cash": newFrozen}).Error; err != nil {
		return fmt.Errorf("settle buy cash: %w", err)
	}
	return nil
}

// unfreezeOrderCash 废单解冻：若该 BUY 单曾冻结资金，将不超账户总冻结的部分退回可用现金。
func unfreezeOrderCash(tx *gorm.DB, ord model.Order) error {
	if !strings.EqualFold(ord.Direction, model.DirectionBuy) {
		return nil // SELL 单不占用资金，无需解冻
	}
	frozen := orderFrozenAmount(ord)
	if frozen <= 0 {
		return nil
	}
	acc, err := loadDefaultAccountTx(tx)
	if err != nil {
		return err
	}
	deduct := math.Min(frozen, acc.FrozenCash)
	if deduct <= 0 {
		return nil // 账户从未冻结过（MVP 直写 PENDING 的模式），no-op
	}
	return tx.Model(&model.Account{}).Where("id = ?", acc.ID).
		Updates(map[string]interface{}{
			"available_cash": roundToCent(acc.AvailableCash + deduct),
			"frozen_cash":    roundToCent(acc.FrozenCash - deduct),
		}).Error
}

// orderFrozenAmount 重算某 BUY 单的下单冻结额。
//
// MVP 约定：冻结额 = 委托价 × 数量 + 按委托价预估的佣金（最低 5 元）。
// 当前 Phase 尚无 PlaceOrder（下单冻结由后续 Phase 落地），PENDING 单的
// FrozenCash 通常为 0，此函数返回按委托价口径的“理论冻结额”，用于：
//  1. 资金校验时把本单冻结计回可动用额度（effectiveBuyableCash）；
//  2. 成交/废单时决定从 FrozenCash 冲抵/退回多少（均受账户总冻结上限约束）。
//
// 当 PlaceOrder 落地后，应改为持久化 ord.FrozenAmount 字段并在此直接读取，
// 本函数签名保持不变即可平滑切换。
func orderFrozenAmount(ord model.Order) float64 {
	if !strings.EqualFold(ord.Direction, model.DirectionBuy) {
		return 0
	}
	amount := roundToCent(ord.Price * float64(ord.Quantity))
	return roundToCent(amount + calcBuyFee(amount))
}

// ---------------------------------------------------------------------------
// 数据访问辅助
// ---------------------------------------------------------------------------

// isFirstRunOfDay 判断 tradeDate 是否为引擎当日“首次撮合运行”。
// 依据：orders 表中不存在早于本交易日的记录 => 往日未运行过 => 今日首跑，应结转 T+1；
// 存在更早记录 => 往日已运行（本日此前的调用也已处理过该日订单），补跑不再结转，
// 避免把当日刚买入的持仓错误解锁为可卖。
func (b *MockBroker) isFirstRunOfDay(tradeDate string) (bool, error) {
	var count int64
	err := b.db.Model(&model.Order{}).
		Where("trade_date < ?", tradeDate).
		Count(&count).Error
	if err != nil {
		return false, fmt.Errorf("broker: check prior orders before %s: %w", tradeDate, err)
	}
	return count == 0, nil
}

// effectiveBuyableCash 计算本轮撮合可用的“可动用资金”内存态初值：
//
//	buyable = AvailableCash + Σ(本批 PENDING BUY 单中被真实冻结的资金)
//
// 语义解释：
//   - 若下单时已冻结（PlaceOrder 模式）：冻结额虽不在 AvailableCash，但属于
//     本批买单的合法预算，必须计回，否则同批次多张买单会互相挤占而误拒；
//     每单成交/废单时会在事务里真实冲抵/退回。
//   - 若从未冻结（MVP：策略直写 PENDING，FrozenCash=0）：ΣretroFrozen 封顶后
//     为 0，buyable 退化为 AvailableCash——即“整批共享同一池现金，逐单扣减”，正确。
//
// 封顶规则：真实计入的冻结额不超过账户 FrozenCash，防止凭空创造购买力。
func (b *MockBroker) effectiveBuyableCash(account *model.Account, pending []model.Order) (float64, error) {
	var retroFrozen float64 // 按“如果启用下单冻结，本批 BUY 应冻结多少”重算
	for i := range pending {
		retroFrozen += orderFrozenAmount(pending[i])
	}
	realFrozenCredit := math.Min(retroFrozen, account.FrozenCash)
	return roundToCent(account.AvailableCash + realFrozenCredit), nil
}

// carryOverTPlus1 每日撮合前的 T+1 解锁结转：AvailableQuantity = Quantity。
// 只在两者不等时更新，避免无意义写。
func (b *MockBroker) carryOverTPlus1() error {
	r := b.db.Model(&model.Position{}).
		Where("available_quantity <> quantity").
		UpdateColumn("available_quantity", gorm.Expr("quantity"))
	if r.Error != nil {
		return fmt.Errorf("carry over positions: %w", r.Error)
	}
	if r.RowsAffected > 0 {
		log.Printf("broker: T+1 carry-over unlocked %d position row(s)", r.RowsAffected)
	}
	return nil
}

// loadPendingOrders 读取指定交易日全部 PENDING 订单：SELL 优先，组内按创建时间正序。
func (b *MockBroker) loadPendingOrders(tradeDate string) ([]model.Order, error) {
	var orders []model.Order
	err := b.db.Where("status = ? AND trade_date = ?", model.OrderStatusPending, tradeDate).
		Order("CASE WHEN direction = 'SELL' THEN 0 ELSE 1 END ASC, created_at ASC, id ASC").
		Find(&orders).Error
	if err != nil {
		return nil, fmt.Errorf("broker: load pending orders for %s: %w", tradeDate, err)
	}
	return orders, nil
}

// getKline 带批内缓存地读取当日 K 线。返回 (nil, nil) 表示当日无数据。
func (b *MockBroker) getKline(code, tradeDate string, cache map[string]*model.DailyKline) (*model.DailyKline, error) {
	key := code + "@" + tradeDate
	if k, ok := cache[key]; ok {
		return k, nil
	}
	var kline model.DailyKline
	err := b.db.Where("code = ? AND date = ?", code, tradeDate).First(&kline).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		cache[key] = nil
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query kline %s %s: %w", code, tradeDate, err)
	}
	cache[key] = &kline
	return &kline, nil
}

// loadDefaultAccount 读取默认账户（单账户系统：取 ID 最小的一行）。
func (b *MockBroker) loadDefaultAccount() (*model.Account, error) {
	return loadDefaultAccountTx(b.db)
}

// loadDefaultAccountTx 事务无关版本，供 tx 与普通 *gorm.DB 复用。
func loadDefaultAccountTx(tx *gorm.DB) (*model.Account, error) {
	var acc model.Account
	err := tx.Order("id ASC").First(&acc).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrAccountNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("broker: load default account: %w", err)
	}
	return &acc, nil
}

// ---------------------------------------------------------------------------
// 纯计算函数（无 DB 依赖，独立可测）
// ---------------------------------------------------------------------------

// calcFillPrice 依据开盘价与方向计算含滑点的成交均价（四舍五入到分）。
//
//	BUY : open × (1 + 0.002)   —— 向上吃滑点
//	SELL: open × (1 - 0.002)   —— 向下吃滑点
func calcFillPrice(open float64, direction string) float64 {
	switch strings.ToUpper(direction) {
	case model.DirectionBuy:
		return roundToCent(open * (1 + SlippageRate))
	case model.DirectionSell:
		return roundToCent(open * (1 - SlippageRate))
	default:
		return roundToCent(open)
	}
}

// calcBuyFee 买入费用 = 佣金（万2.5，最低 5 元）。买入不收印花税。
func calcBuyFee(amount float64) float64 {
	return roundToCent(math.Max(amount*CommissionRate, MinCommission))
}

// calcSellFee 卖出费用 = 佣金（万2.5，最低 5 元） + 印花税（千0.5）。
func calcSellFee(amount float64) float64 {
	commission := math.Max(amount*CommissionRate, MinCommission)
	stampTax := amount * StampTaxRate
	return roundToCent(commission + stampTax)
}

// roundToCent 金额/价格四舍五入到分。
func roundToCent(v float64) float64 {
	return math.Round(v*100) / 100
}

// nearlyEqual 以“分”为粒度的价格相等判定（用于一字板识别）。
func nearlyEqual(a, b float64) bool {
	return math.Abs(a-b) < priceEpsilon
}

// truncate 按 rune 截断，防止中文原因超长导致 UPDATE 失败。
func truncate(s string, max int) string {
	if len([]rune(s)) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max])
}
