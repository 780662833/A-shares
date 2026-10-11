// Package graph —— 漏斗 Layer3/4 的订单落库原语（统一执行计划 Step 5）。
//
// 背景（关键问题发现 🔴 漏斗图断链）：SaveDecision 只写 AIDecisionLog 表，
// 从不产生订单；MockBroker 仅撮合 orders 表的 PENDING 单，导致 Layer3/4 形同虚设。
// 本文件把原 review_graph.go 中 ExecutePlan 的下单逻辑迁移为可复用内核
// （决策 #「孤儿旧图」处理方案），供 funnel_graph.go 的 PlaceOrders 节点调用。
package graph

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"

	"gorm.io/gorm"

	dbmodel "ashare-quant/internal/model"
)

// orderSaver 抽象订单落库动作，便于单测注入 fake（生产实现 gormOrderSaver）。
type orderSaver interface {
	SaveOrder(ord *dbmodel.Order) error
}

// gormOrderSaver 生产实现：直接 INSERT orders 表（PENDING 状态由调用方设置）。
type gormOrderSaver struct{ gdb *gorm.DB }

func (sv gormOrderSaver) SaveOrder(ord *dbmodel.Order) error {
	return sv.gdb.Create(ord).Error
}

// OrderIntent 一笔待写入的订单意图（Layer3 风险预算节点的产出契约）。
//
// 字段口径与 dbmodel.Order 一致；TradeDate 必须已由调用方换算为
// 「下一交易日」（复盘在 T 日 15:30 生成，订单于 T+1 日 09:25 撮合）。
type OrderIntent struct {
	Code      string  // 6 位数字股票代码
	Direction string  // BUY / SELL
	Price     float64 // 委托价（元）
	Quantity  int64   // 股数（BUY 须为 100 的整数倍；SELL 允许零股清仓）
	Reason    string  // 决策理由（审计日志用）
	TradeDate string  // 归属交易日 YYYY-MM-DD（次日）
	StopPrice float64 // BUY 建仓时写入持仓的实际风控线（Step 6 止损监控契约；0=未设置）
}

var orderCodeRe = regexp.MustCompile(`^\d{6}$`)

// writeOrders 遍历订单意图，逐笔校验后写入 PENDING 订单，返回执行报告。
//
// 校验原则（继承 review_graph 的设计）：不信任上游任何数字——格式非法、
// 数量为负、整手违规一律拒写并计入 rejected（带原因），绝不中断整批。
func writeOrders(orders []OrderIntent, saver orderSaver, seqFn func() uint64) (*ExecutionReport, error) {
	if saver == nil {
		return nil, errors.New("write_orders: nil order saver")
	}
	if seqFn == nil {
		return nil, errors.New("write_orders: nil sequence function")
	}
	report := &ExecutionReport{}
	rejected := make([]string, 0)

	for _, od := range orders {
		code := strings.TrimSpace(od.Code)
		dir := strings.ToUpper(strings.TrimSpace(od.Direction))
		switch {
		case !orderCodeRe.MatchString(code):
			rejected = append(rejected, fmt.Sprintf("%s: 非法代码 %q", code, od.Code))
			continue
		case dir != dbmodel.DirectionBuy && dir != dbmodel.DirectionSell:
			rejected = append(rejected, fmt.Sprintf("%s: 非法方向 %q", code, od.Direction))
			continue
		case od.Price <= 0:
			rejected = append(rejected, fmt.Sprintf("%s: 非法委托价 %.4f", code, od.Price))
			continue
		case od.Quantity <= 0:
			rejected = append(rejected, fmt.Sprintf("%s: 非法数量 %d", code, od.Quantity))
			continue
		case dir == dbmodel.DirectionBuy && od.Quantity%lotSize != 0:
			rejected = append(rejected, fmt.Sprintf("%s: 买入数量 %d 非 %d 整数倍", code, od.Quantity, lotSize))
			continue
		case strings.TrimSpace(od.TradeDate) == "":
			rejected = append(rejected, fmt.Sprintf("%s: 缺少归属交易日", code))
			continue
		}

		ord := dbmodel.Order{
			OrderID:   dbmodel.NewOrderID(seqFn()),
			Code:      code,
			Direction: dir,
			Price:     dbmodel.RoundToCent(od.Price),
			Quantity:  od.Quantity,
			Status:    dbmodel.OrderStatusPending,
			TradeDate: strings.TrimSpace(od.TradeDate),
			StopPrice: dbmodel.RoundToCent(od.StopPrice), // Step 6：BUY 成交建仓时透传给持仓风控线
		}
		if err := saver.SaveOrder(&ord); err != nil {
			report.Failed++
			report.Errors = append(report.Errors, fmt.Sprintf("%s %s: persist failed: %v", dir, code, err))
			log.Printf("write_orders: ERROR persist %s %s qty=%d err=%v", dir, code, od.Quantity, err)
			continue
		}
		report.Submitted++
		report.OrderIDs = append(report.OrderIDs, ord.OrderID)
		log.Printf("write_orders: PENDING %s %s x%d @%.2f trade_date=%s order_id=%s reason=%q",
			dir, code, ord.Quantity, ord.Price, ord.TradeDate, ord.OrderID, od.Reason)
	}

	if len(rejected) > 0 {
		report.Failed += len(rejected)
		report.Errors = append(report.Errors, rejected...)
		for _, r := range rejected {
			log.Printf("write_orders: REJECT %s", r)
		}
	}
	return report, nil
}
