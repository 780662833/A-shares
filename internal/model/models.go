// Package model 定义 A 股日线级别 AI 模拟交易系统的核心数据模型。
//
// 设计约定：
//  1. 金额/价格统一使用 float64（元 / 元每股），MVP 阶段以可读性优先；
//     后续如需精确到分，可平滑迁移为「以厘为单位的 int64」。
//  2. 日期统一使用 "YYYY-MM-DD" 字符串存储，与 Python 数据网关返回格式对齐，
//     且天然支持字典序比较（BETWEEN / ORDER BY 均正确）。
//  3. 所有 Model 显式声明 TableName、字段类型与索引，避免 GORM 默认行为漂移。
package model

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// 枚举常量：订单方向 / 订单状态
// ---------------------------------------------------------------------------

const (
	// 订单方向
	DirectionBuy  = "BUY"
	DirectionSell = "SELL"

	// 订单状态机：PENDING ->(撮合成功) FILLED / REJECTED
	OrderStatusPending  = "PENDING"  // 待撮合（09:25 集合竞价撮合前挂起）
	OrderStatusFilled   = "FILLED"   // 已成交
	OrderStatusRejected = "REJECTED" // 已废单（资金不足 / T+1 校验失败 / 一字板无法成交等）
)

// IsValidDirection 校验订单方向合法性（大小写不敏感，入库前归一化为大写）。
func IsValidDirection(d string) bool {
	switch strings.ToUpper(strings.TrimSpace(d)) {
	case DirectionBuy, DirectionSell:
		return true
	default:
		return false
	}
}

// IsValidOrderStatus 校验订单状态合法性。
func IsValidOrderStatus(s string) bool {
	switch s {
	case OrderStatusPending, OrderStatusFilled, OrderStatusRejected:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Stock 股票基础信息表
// ---------------------------------------------------------------------------

// Stock A 股股票静态信息，由 /api/stock_list 同步写入。
type Stock struct {
	ID       uint64 `gorm:"primaryKey;autoIncrement" json:"id"`
	Code     string `gorm:"type:varchar(10);not null;uniqueIndex:uk_stock_code" json:"code"` // 如 "000001"
	Name     string `gorm:"type:varchar(64);not null" json:"name"`                           // 如 "平安银行"
	Industry string `gorm:"type:varchar(64);index:idx_stock_industry" json:"industry"`       // 行业，可为空（网关接口暂不含行业，留待扩展）

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName 固定表名，防止 GORM 复数化策略变更影响下游 SQL。
func (Stock) TableName() string { return "stocks" }

// ---------------------------------------------------------------------------
// DailyKline 日线行情表
// ---------------------------------------------------------------------------

// DailyKline 日线 K 线（前复权）。
//
// LimitUpPrice / LimitDownPrice 为模拟撮合的关键字段：
//   - 由「昨收 × (1±涨跌幅限制) 后四舍五入到分」计算得到（见 CalcLimitPrices）；
//   - 撮合时若当日最高价 >= 涨停价（一字板卖压锁死）则买单无法成交，
//     最低价 <= 跌停价则卖单无法成交。
//
// 注意：因采用前复权(qfq)数据，除权除息日后历史价格会整体平移，
// 因此「昨收」必须取本表中该股票上一交易日的 Close 列（同为前复权口径），
// 不可混用不复权价格，否则涨跌停价会算错。
type DailyKline struct {
	ID   uint64 `gorm:"primaryKey;autoIncrement" json:"id"`
	Code string `gorm:"type:varchar(10);not null;uniqueIndex:uk_kline_code_date" json:"code"`
	Date string `gorm:"type:varchar(10);not null;uniqueIndex:uk_kline_code_date" json:"date"` // "YYYY-MM-DD"

	Open   float64 `gorm:"type:real;not null" json:"open"`
	High   float64 `gorm:"type:real;not null" json:"high"`
	Low    float64 `gorm:"type:real;not null" json:"low"`
	Close  float64 `gorm:"type:real;not null" json:"close"`
	Volume int64   `gorm:"type:integer;not null" json:"volume"` // 成交量（股）
	Amount float64 `gorm:"type:real;not null" json:"amount"`    // 成交额（元）

	LimitUpPrice   float64 `gorm:"type:real;not null" json:"limit_up_price"`   // 当日涨停价
	LimitDownPrice float64 `gorm:"type:real;not null" json:"limit_down_price"` // 当日跌停价

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName 固定表名。
func (DailyKline) TableName() string { return "daily_klines" }

// ---------------------------------------------------------------------------
// CandidatePool 候选股票池表（漏斗模型 Layer1 -> Layer2 的交接数据）
// ---------------------------------------------------------------------------

// 候选池信号类型常量（与 Python 网关 /api/czsc/screen_pool 的 signal_type 对齐）。
const (
	SignalObservation = "Observation" // 观察池: >=2 中枢且最后一笔向上
	SignalBuy1        = "1B"          // 第一类买点(疑似底背驰)
	SignalBuy2        = "2B"          // 第二类买点
	SignalBuy3        = "3B"          // 第三类买点
	SignalSell1       = "1S"          // 第一类卖点(疑似顶背驰)
	SignalSell2       = "2S"          // 第二类卖点
	SignalSell3       = "3S"          // 第三类卖点
)

// IsValidSignalType 校验 czsc 信号类型合法性。
func IsValidSignalType(s string) bool {
	switch s {
	case SignalObservation, SignalBuy1, SignalBuy2, SignalBuy3,
		SignalSell1, SignalSell2, SignalSell3:
		return true
	default:
		return false
	}
}

// CandidatePool 每日粗筛产出的候选股票池条目。
//
// 由 15:30 流水线调用 Python 网关 POST /api/czsc/screen_pool 后写入；
// Layer2 的 Eino Master Agent 读取当日记录进行新闻排雷与精选。
// (TradeDate, Code) 唯一：同日重跑流水线时覆盖旧记录而非重复插入。
//
// Phase A（Step 1）结构化增强字段：买点附带的双止损线，供 Layer3 风险预算仓位计算。
// 均可为 0，表示上游未给出该线（如 Observation 信号无结构止损）。
type CandidatePool struct {
	ID        uint64 `gorm:"primaryKey;autoIncrement" json:"id"`
	TradeDate string `gorm:"type:varchar(10);not null;uniqueIndex:uk_pool_date_code" json:"trade_date"` // "YYYY-MM-DD", 哪一天筛选出的
	Code      string `gorm:"type:varchar(10);not null;uniqueIndex:uk_pool_date_code" json:"code"`       // 股票代码

	SignalType     string `gorm:"type:varchar(16);not null;index:idx_pool_signal" json:"signal_type"` // 3B/1B/Observation 等
	CzscSummary    string `gorm:"type:text" json:"czsc_summary"`                                      // czsc 自然语言摘要, 直接可喂 LLM
	ZsCount        int    `gorm:"type:integer;not null;default:0" json:"zs_count"`                    // 中枢数量
	TrendDirection string `gorm:"type:varchar(8);not null" json:"trend_direction"`                    // up/down/unknown

	// ---- Phase A: 买点结构化增强（来自 screen_pool 命中的 buy_points[0]）----
	RefPrice         float64   `gorm:"type:real;not null;default:0" json:"ref_price"`           // 信号参考价（买点笔端点价，作预估买价）
	InvalidationLine float64   `gorm:"type:real;not null;default:0" json:"invalidation_line"`   // 结构失效线（三买=中枢 ZG）
	PriceStopLine    float64   `gorm:"type:real;not null;default:0" json:"price_stop_line"`     // 价格止损线（回抽低点×0.99）
	ConfirmedAt      time.Time `gorm:"type:datetime" json:"confirmed_at"`                       // 信号确认时间（笔末端+1根K线，消前视）
	HasConfirmedAt   bool      `gorm:"-" json:"has_confirmed_at,omitempty"`                      // 非持久化：标记上游是否给出合法 confirmed_at

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName 固定表名。
func (CandidatePool) TableName() string { return "candidate_pools" }

// ---------------------------------------------------------------------------
// AIDecisionLog AI 决策日志表（漏斗模型 Layer2 产出）
// ---------------------------------------------------------------------------

// AI 决策动作常量。
const (
	ActionBuy  = "BUY"  // 技术买点 + 新闻面共振
	ActionSell = "SELL" // 技术卖点 + 新闻面平庸/偏空
	ActionDrop = "DROP" // 排雷放弃: 业绩预亏/减持/立案调查/行业利空等
)

// IsValidAction 校验 AI 决策动作合法性（大小写不敏感，入库前归一化为大写）。
func IsValidAction(a string) bool {
	switch strings.ToUpper(strings.TrimSpace(a)) {
	case ActionBuy, ActionSell, ActionDrop:
		return true
	default:
		return false
	}
}

// NormalizeAction 归一化 AI 动作字符串; 非法返回 ("", error)。
func NormalizeAction(a string) (string, error) {
	a = strings.ToUpper(strings.TrimSpace(a))
	if !IsValidAction(a) {
		return "", fmt.Errorf("model: invalid ai action %q", a)
	}
	return a, nil
}

// AIDecisionLog Eino Master Agent 对每只候选股的最终决策留痕。
//
// ConfidenceScore 约定 0-100：<60 的 BUY 建议会被风控层降级为观察。
//
// Phase B（Step 2）Layer3 回写：RiskCheckAndSize/PlaceOrders 节点把仓位计算结果
// 与订单号回填到同一行，使「AI 决策 → 风控股数 → 实际订单」三段链路一表可查。
type AIDecisionLog struct {
	ID        uint64 `gorm:"primaryKey;autoIncrement" json:"id"`
	TradeDate string `gorm:"type:varchar(10);not null;index:idx_decision_date_code" json:"trade_date"`
	Code      string `gorm:"type:varchar(10);not null;index:idx_decision_date_code" json:"code"`

	Action          string `gorm:"type:varchar(8);not null;index:idx_decision_action" json:"action"` // BUY/SELL/DROP
	Reason          string `gorm:"type:text" json:"reason"`                                          // AI 给出的排雷/买入理由
	ConfidenceScore int    `gorm:"type:integer;not null;default:0" json:"confidence_score"`          // 0-100 信心分数

	// ---- Phase B: Layer3 风险预算仓位回写（saveDecision 时为 0, RiskCheckAndSize 更新）----
	Quantity       int64   `gorm:"type:integer;not null;default:0" json:"quantity"`           // 风控核定的委托股数（BUY 才有）
	StopLossPrice  float64 `gorm:"type:real;not null;default:0" json:"stop_loss_price"`       // 实际采用的止损线
	StopSource     string  `gorm:"type:varchar(16);not null;default:''" json:"stop_source"`   // invalidation / price_stop / ''
	OrderID        string  `gorm:"type:varchar(32);not null;default:''" json:"order_id"`      // 生成的 PENDING 订单号（空=未下单）
	RiskRejectNote string  `gorm:"type:text" json:"risk_reject_note,omitempty"`               // 风控拒单原因（若有）

	CreatedAt time.Time `json:"created_at"`
}

// TableName 固定表名。
func (AIDecisionLog) TableName() string { return "ai_decision_logs" }

// ---------------------------------------------------------------------------
// WatchlistItem 动态自选股池表（漏斗四层架构的 Layer 0.5）
// ---------------------------------------------------------------------------

// 自选股来源类型常量（与 Python watchlist_generator.py 严格对齐）。
const (
	WatchSourceManual = "MANUAL"        // 人工白名单: 永不被自动清理/过期剔除
	WatchSourceSector = "SECTOR_LEADER" // 行业龙头: 资金净流入 Top 板块的大市值成分股
	WatchSourceETF    = "ETF_MOMENTUM"  // ETF 动量: 20 日涨幅 Top N 的行业/跨境 ETF
	WatchSourceFund   = "FUND_FLOW"     // 资金异动: 龙虎榜机构席位净买入
)

// 自选股状态常量。
const (
	WatchStatusActive    = "ACTIVE"    // 生效中, 参与每日 czsc 粗筛
	WatchStatusInactive  = "INACTIVE"  // 已过期/跌出动态池, 保留历史但不参与粗筛
	WatchStatusBlacklist = "BLACKLIST" // 人工屏蔽, 任何自动逻辑不得恢复
)

// WatchlistItem 动态自选股池条目。
//
// 数据来源优先级: MANUAL > ETF_MOMENTUM > SECTOR_LEADER > FUND_FLOW（同日去重时保留高优先来源）。
// 生命周期规则:
//   - SourceType == MANUAL 的记录永不自动失效（人类保留一票否决/偏好权）；
//   - 非 MANUAL 记录 ExpireDate 到期且未再被生成器命中 -> Status 置 INACTIVE；
//   - BLACKLIST 为人工终态, UpdateWatchlist 节点只读不写。
type WatchlistItem struct {
	ID   uint64 `gorm:"primaryKey;autoIncrement" json:"id"`
	Code string `gorm:"type:varchar(10);not null;uniqueIndex:uk_watchlist_code" json:"code"` // 股票/ETF 代码
	Name string `gorm:"type:varchar(64)" json:"name"`                                        // 名称, 可为空(网关补齐)

	SourceType string `gorm:"type:varchar(16);not null;index:idx_watch_source;default:MANUAL" json:"source_type"` // MANUAL/SECTOR_LEADER/ETF_MOMENTUM/FUND_FLOW
	SectorName string `gorm:"type:varchar(64)" json:"sector_name"`                                                // 所属行业板块或 ETF 名称
	Status     string `gorm:"type:varchar(16);not null;index:idx_watch_status;default:ACTIVE" json:"status"`      // ACTIVE/INACTIVE/BLACKLIST

	AddedDate  time.Time  `gorm:"not null" json:"added_date"`                              // 加入池子的日期
	ExpireDate *time.Time `gorm:"type:datetime;index:idx_watch_expire" json:"expire_date"` // 过期时间; MANUAL 为 NULL(永不过期)
	LastSeenAt *time.Time `json:"last_seen_at"`                                            // 最近一次被动态生成器命中的时间

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName 固定表名。
func (WatchlistItem) TableName() string { return "watchlist_items" }

// ---------------------------------------------------------------------------
// Position 持仓表
// ---------------------------------------------------------------------------

// Position 当前持仓。每只股票至多一行（Code 唯一）。
//
// T+1 规则落点：
//   - Quantity 总持仓；AvailableQuantity 当日可卖数量。
//   - BUY 成交时：Quantity 增加，AvailableQuantity 不变（当日买入不可卖）。
//   - 每日开盘前（或收盘后）执行统一结转：AvailableQuantity = Quantity。
//   - SELL 委托校验：AvailableQuantity >= 委托数量，否则拒单。
type Position struct {
	ID                uint64  `gorm:"primaryKey;autoIncrement" json:"id"`
	Code              string  `gorm:"type:varchar(10);not null;uniqueIndex:uk_position_code" json:"code"`
	Quantity          int64   `gorm:"type:integer;not null;default:0" json:"quantity"`           // 总持仓（股）
	AvailableQuantity int64   `gorm:"type:integer;not null;default:0" json:"available_quantity"` // 可卖数量（T+1 校验依据）
	CostPrice         float64 `gorm:"type:real;not null;default:0" json:"cost_price"`            // 摊薄成本价（纯成交均价口径）
	BuyDate           string  `gorm:"type:varchar(10);not null" json:"buy_date"`                 // 最近一次建仓/加仓日期 "YYYY-MM-DD"

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName 固定表名。
func (Position) TableName() string { return "positions" }

// ---------------------------------------------------------------------------
// Account 账户资金表
// ---------------------------------------------------------------------------

// Account 模拟账户。系统单账户运行，但保留 ID 主键以便未来多账户扩展。
//
// 恒量约束（业务层事务内必须保证）：
//
//	TotalAssets ≈ AvailableCash + FrozenCash + Σ(持仓市值)  —— 市值随行情浮动，仅在复盘快照时点严格相等
//	AvailableCash >= 0 且 FrozenCash >= 0
//
// 资金流转：
//   - 下单 BUY：AvailableCash -= 冻结额（本金+预扣费用），FrozenCash += 冻结额
//   - 成交 BUY：FrozenCash 结算实际花费与佣金印花税，多退少补回 AvailableCash
//   - 撤单/废单：FrozenCash 全额退回 AvailableCash
type Account struct {
	ID            uint64  `gorm:"primaryKey;autoIncrement" json:"id"`
	TotalAssets   float64 `gorm:"type:real;not null;default:0" json:"total_assets"`   // 总资产（现金+冻结+持仓市值，收盘复盘时刷新）
	AvailableCash float64 `gorm:"type:real;not null;default:0" json:"available_cash"` // 可用现金
	FrozenCash    float64 `gorm:"type:real;not null;default:0" json:"frozen_cash"`    // 冻结资金（未成交 BUY 委托占用）

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName 固定表名。
func (Account) TableName() string { return "accounts" }

// DefaultInitialCapital 默认初始资金：1,000,000 元。
const DefaultInitialCapital = 1_000_000.0

// ---------------------------------------------------------------------------
// Order 委托订单表
// ---------------------------------------------------------------------------

// Order 委托单。09:25 的模拟撮合任务以 PENDING 订单为输入。
//
// 状态机：
//
//	PENDING ──撮合成功──> FILLED                        （AvgPrice/Fee 成交时回填）
//	        └─撮合失败/风控拒绝──> REJECTED             （RejectReason 记录原因）
type Order struct {
	ID        uint64  `gorm:"primaryKey;autoIncrement" json:"id"`
	OrderID   string  `gorm:"type:varchar(32);not null;uniqueIndex:uk_order_order_id" json:"order_id"` // 业务订单号（全局唯一）
	Code      string  `gorm:"type:varchar(10);not null;index:idx_order_code_date" json:"code"`
	Direction string  `gorm:"type:varchar(4);not null" json:"direction"` // BUY / SELL
	Price     float64 `gorm:"type:real;not null" json:"price"`           // 委托价
	Quantity  int64   `gorm:"type:integer;not null" json:"quantity"`     // 委托数量（股，须为 100 的整数倍——业务层校验）

	Status       string  `gorm:"type:varchar(16);not null;default:'PENDING';index:idx_order_status" json:"status"` // PENDING/FILLED/REJECTED
	AvgPrice     float64 `gorm:"type:real;not null;default:0" json:"avg_price"`                                    // 成交均价（含滑点后）
	Fee          float64 `gorm:"type:real;not null;default:0" json:"fee"`                                          // 总费用（佣金万2.5+印花税千0.5，印花税仅卖出）
	RejectReason string  `gorm:"type:varchar(128)" json:"reject_reason,omitempty"`                                 // 废单原因

	TradeDate string    `gorm:"type:varchar(10);not null;index:idx_order_code_date" json:"trade_date"` // 归属交易日
	CreatedAt time.Time `gorm:"type:datetime;not null;autoCreateTime;index:idx_order_created" json:"created_at"`
	UpdatedAt time.Time `gorm:"type:datetime" json:"updated_at"`
}

// TableName 固定表名。
func (Order) TableName() string { return "orders" }

// NewOrderID 生成业务订单号：ORD + UTC 时间戳(yyyyMMddHHmmss) + 4 位序列。
// 说明：MVP 单机场景下秒级时间戳+序列足够；生产环境建议替换雪花 ID。
func NewOrderID(seq uint64) string {
	return fmt.Sprintf("ORD%s%04d", time.Now().UTC().Format("20060102150405"), seq%10000)
}

// ---------------------------------------------------------------------------
// 涨跌停价计算工具（撮合前置依赖，供 db 初始化回填与撮合引擎共用）
// ---------------------------------------------------------------------------

// RoundToCent 将价格四舍五入到「分」，符合交易所涨跌停价取整规则。
func RoundToCent(v float64) float64 {
	return math.Round(v*100) / 100
}

// PriceLimitRatio 根据股票代码判定涨跌幅限制比例。
//   - 主板（60xxxx 沪 / 00xxxx 深）：10%
//   - 创业板（30xxxx）、科创板（68xxxx）：20%
//   - ST/*ST 理论上 5%，需结合证券名称判断；本函数按非 ST 处理，
//     撮合引擎调用方如有 ST 标记可自行覆盖比例。
func PriceLimitRatio(code string) float64 {
	code = strings.TrimSpace(code)
	switch {
	case strings.HasPrefix(code, "30"), strings.HasPrefix(code, "68"):
		return 0.20
	default: // 60 / 00 / 其他，按 10% 处理
		return 0.10
	}
}

// CalcLimitPrices 由「前一交易日收盘价」（须与前复权 K 线同口径）计算当日涨跌停价。
// 交易所规则：涨停价 = round(昨收 × (1+limit), 2)，跌停价 = round(昨收 × (1-limit), 2)。
func CalcLimitPrices(prevClose float64, code string) (limitUp, limitDown float64, err error) {
	if prevClose <= 0 || math.IsNaN(prevClose) || math.IsInf(prevClose, 0) {
		return 0, 0, fmt.Errorf("invalid prev close %v for code %s", prevClose, code)
	}
	ratio := PriceLimitRatio(code)
	limitUp = RoundToCent(prevClose * (1 + ratio))
	limitDown = RoundToCent(prevClose * (1 - ratio))
	if limitDown < 0 {
		limitDown = 0
	}
	return limitUp, limitDown, nil
}
