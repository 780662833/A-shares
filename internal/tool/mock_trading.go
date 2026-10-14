package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"gorm.io/gorm"

	"ashare-quant/internal/model"
)

// 编译期接口断言。
var (
	_ tool.BaseTool      = (*MockTradingTool)(nil)
	_ tool.InvokableTool = (*MockTradingTool)(nil)
)

// ---------------------------------------------------------------------------
// MockTradingTool：LLM 交易指令 -> PENDING 订单落库
// ---------------------------------------------------------------------------

// TradingInput 是 LLM 传入的 JSON 参数反序列化目标。
type TradingInput struct {
	Symbol    string  `json:"symbol"`    // 6 位数字股票代码
	Direction string  `json:"direction"` // buy / sell（大小写不敏感）
	Price     float64 `json:"price"`     // 委托价（元）
	Quantity  int64   `json:"quantity"`  // 委托数量（股，100 整数倍）
	Reason    string  `json:"reason"`    // 决策理由（可选，写入 RejectReason 之外的备注？——见下方说明）
}

// MockTradingConfig 构造参数。
type MockTradingConfig struct {
	// DB 已完成 AutoMigrate 的 GORM 句柄。必填。
	DB *gorm.DB
	// DefaultTradeDateFunc 计算「归属交易日」：复盘在 T 日 15:30 生成计划，
	// 订单实际在 T+1 日 09:25 撮合，因此 TradeDate 记为下一个工作日。
	// 返回 "YYYY-MM-DD"；为空则使用内置 nextWeekday。
	DefaultTradeDateFunc func(now time.Time) string
}

// MockTradingTool 将 LLM 的交易决策以 PENDING 状态写入 orders 表。
//
// 重要设计约束（业务规则决定）：
//   - 本工具【不做任何撮合】。撮合由 Phase 3 MockBroker 于次日 09:25 统一执行；
//   - Invoke 只做「参数校验 + 落库 + 回执」，回执文案固定提示等待次日撮合；
//   - Reason 字段随订单一起持久化到 RejectReason 列吗？——否。为避免污染废单语义，
//     MVP 将 reason 仅记录在服务端日志中（结构化留痕），DB schema 不变。
type MockTradingTool struct {
	db       *gorm.DB
	tradeFn  func(now time.Time) string
	orderSeq uint64
	mu       sync.Mutex // 保护 orderSeq 自增与订单写入的原子性（cron/HTTP 并发触发场景）
}

// NewMockTradingTool 构造交易工具。
func NewMockTradingTool(cfg MockTradingConfig) (*MockTradingTool, error) {
	if cfg.DB == nil {
		return nil, errors.New("mock_trading: DB is required")
	}
	fn := cfg.DefaultTradeDateFunc
	if fn == nil {
		fn = nextWeekday
	}
	return &MockTradingTool{db: cfg.DB, tradeFn: fn}, nil
}

// nextWeekday 返回 from 之后的第一个工作日（跳过周六日）。
// MVP 未接入交易日历（节假日需后续从数据网关补充接口获取）。
func nextWeekday(from time.Time) string {
	t := from.AddDate(0, 0, 1)
	for t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
		t = t.AddDate(0, 0, 1)
	}
	return t.Format("2006-01-02")
}

// Info 返回工具 schema。Desc 明确告知 LLM：这是「提交明日计划」而非即时成交，
// 防止模型误以为下单后立刻产生持仓变化而重复下单。
func (t *MockTradingTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "submit_mock_order",
		Desc: "向A股模拟账户提交一笔次日交易的委托计划（不会立即成交）。" +
			"参数：symbol 为6位数字股票代码；direction 只能是 buy 或 sell；" +
			"price 为委托价（元，建议参考最新收盘价）；quantity 为股数，必须是100的整数倍。" +
			"订单将以 PENDING 状态入库，于次一交易日 09:25 由撮合引擎按开盘价（含滑点）统一模拟成交。" +
			"卖出受 T+1 限制：当日买入的股份不可卖。",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"symbol":    {Type: schema.String, Desc: "6 位数字股票代码，如 000001", Required: true},
			"direction": {Type: schema.String, Desc: "buy 或 sell", Enum: []string{"buy", "sell"}, Required: true},
			"price":     {Type: schema.Number, Desc: "委托价格（元/股）", Required: true},
			"quantity":  {Type: schema.Integer, Desc: "委托数量（股），100 的整数倍", Required: true},
			"reason":    {Type: schema.String, Desc: "本次决策的一句话理由（仅用于审计日志）"},
		}),
	}, nil
}

// InvokableRun 解析 JSON 参数 -> 业务级校验 -> 写入 PENDING 订单 -> 返回回执。
func (t *MockTradingTool) InvokableRun(_ context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	// 1. 解析参数
	var in TradingInput
	if err := json.Unmarshal([]byte(strings.TrimSpace(argumentsInJSON)), &in); err != nil {
		return errorResult(fmt.Sprintf("arguments are not valid JSON: %v", err)), nil
	}

	// 2. 业务级校验（撮合级校验留给 Broker，这里只拦截明显非法输入，避免脏数据入库）
	in.Symbol = strings.TrimSpace(in.Symbol)
	if !symbolPattern.MatchString(in.Symbol) {
		return errorResult(fmt.Sprintf("symbol %q invalid: must be exactly 6 digits", in.Symbol)), nil
	}
	dir := strings.ToUpper(strings.TrimSpace(in.Direction))
	if !model.IsValidDirection(dir) {
		return errorResult(fmt.Sprintf("direction %q invalid: must be buy or sell", in.Direction)), nil
	}
	if in.Price <= 0 || math.IsNaN(in.Price) || math.IsInf(in.Price, 0) {
		return errorResult(fmt.Sprintf("price %.6f invalid: must be > 0", in.Price)), nil
	}
	if in.Quantity <= 0 || in.Quantity%modelLotSize != 0 {
		return errorResult(fmt.Sprintf("quantity %d invalid: must be a positive multiple of %d", in.Quantity, modelLotSize)), nil
	}

	// 3. 组装订单（TradeDate = 下一工作日，对应「次日 09:25 撮合」的业务时点）
	tradeDate := t.tradeFn(time.Now())
	ord := model.Order{
		OrderID:   model.NewOrderID(atomicNext(&t.mu, &t.orderSeq)),
		Code:      in.Symbol,
		Direction: dir,
		Price:     model.RoundToCent(in.Price),
		Quantity:  in.Quantity,
		Status:    model.OrderStatusPending,
		TradeDate: tradeDate,
	}

	// 4. 落库。唯一索引冲突等 DB 错误如实返回，让 LLM/调用方感知失败。
	if err := t.db.Create(&ord).Error; err != nil {
		log.Printf("mock_trading: persist order failed symbol=%s dir=%s qty=%d err=%v", in.Symbol, dir, in.Quantity, err)
		return errorResult(fmt.Sprintf("persist pending order failed: %v", err)), nil
	}

	log.Printf("mock_trading: PENDING order created id=%s symbol=%s dir=%s price=%.2f qty=%d trade_date=%s reason=%q",
		ord.OrderID, ord.Code, ord.Direction, ord.Price, ord.Quantity, ord.TradeDate, in.Reason)

	// 5. 固定文案回执（题设要求）+ 结构化明细
	b, _ := json.Marshal(map[string]any{
		"ok":         true,
		"message":    "订单已提交，等待次日撮合",
		"order_id":   ord.OrderID,
		"symbol":     ord.Code,
		"direction":  ord.Direction,
		"price":      ord.Price,
		"quantity":   ord.Quantity,
		"trade_date": ord.TradeDate,
	})
	return string(b), nil
}

// modelLotSize 一手股数，与 broker.LotSize 口径一致（此处复制常量避免包循环依赖）。
const modelLotSize = 100

// atomicNext 在互斥锁保护下返回自增序号。
func atomicNext(mu *sync.Mutex, seq *uint64) uint64 {
	mu.Lock()
	defer mu.Unlock()
	*seq++
	return *seq
}
