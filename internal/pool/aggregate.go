// aggregate.go —— 阶段 2：聚合、去重与来源生命周期。
//
// 核心原则（与实施计划 4.1~4.3 对齐）：
//   - 去重是"合并证券身份"，不是"删除来源证据"：同一 symbol 的多来源命中
//     合并为一条记录，sources[] 保留全部来源及各自的 Reason / AsOf / Status；
//   - primary_source 按固定优先级 MANUAL > ETF_MOMENTUM > SECTOR_LEADER > FUND_FLOW
//     选取；主来源优先级不得覆盖黑名单与各状态字段（它们独立保存）；
//   - 人工白名单(manual_watchlist)与黑名单(blacklist)物理分离：白名单条目
//     永不被自动过期逻辑清除；黑名单在最终交易资格(tradability)检查中优先生效
//     ——即使股票仍持有 MANUAL 来源记录，其 tradability 也是 BLOCKED；
//   - 生命周期以"来源刷新状态"为唯一依据（见 SourceStatus 契约）：
//     OK 且继续命中      -> 更新 last_seen_at 并按 AutoExpireDays 续期
//     OK 且不再命中      -> 超过 AutoExpireDays(自然日) 转 EXPIRED
//     SUCCESS_EMPTY      -> 确认成功取到空结果后, 才允许按业务规则走"不再命中"分支
//     PARTIAL            -> 只更新能明确验证的记录, 其余保留原状(不批量失效)
//     FAILED             -> 保留上次有效结果, 标记 STALE, 不清除、不过期
//     STALE              -> 保留记录, 但禁止把过期数据当成当前有效信号
//   - AutoExpireDays=7 明确为 **7 个自然日**（含周末节假日, 近似 5 个交易日）;
//     接口持续失败时绝不做"简单将旧记录改成不再入选"的清理——FAILED 分支
//     根本不进入过期判定。
//   - 输出层区分三个维度："来源仍然有效"(SourceLifecycle)、
//     "来源已过期"(EXPIRED)、"当前交易资格未知"(Eligibility=UNKNOWN)。
//     来源未过期 ≠ 今天具备正常交易资格, 两者独立呈现。
package pool

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// sourcePriority 主来源优先级（数值越小优先级越高）。
var sourcePriority = map[string]int{
	sourceManual: 0,
	sourceETF:    1,
	sourceSector: 2,
	sourceFund:   3,
}

// SourceHit 单个来源对某只证券的一次命中证据（Reason 与数据日期随记录保存, 不因去重丢失）。
type SourceHit struct {
	SourceType string       `json:"source_type"`
	Reason     string       `json:"reason"`      // 本来源的入选理由（如 "近5日主力净流入Top1行业[半导体]龙头"）
	SectorName string       `json:"sector_name"` // 本来源语境下的板块/ETF 名
	AsOf       string       `json:"as_of"`       // 本来源数据的业务截止日期（Entry.AsOf/GeneratedAt; 空=上游未提供, 诚实未知——不再伪装成聚合级 asOf）
	RunAsOf    string       `json:"run_as_of"`   // 本次候选池任务的目标时点（新鲜度审计三维: AsOf=数据截止 / RunAsOf=任务时点 / LastSeenAt=确认命中时刻）
	Status     SourceStatus `json:"status"`      // 本来源本次刷新状态（契约枚举）
}

// AggEntry 聚合后的池内条目：一条证券身份 + 全部来源证据 + 独立状态字段。
type AggEntry struct {
	Symbol      string `json:"symbol"`
	Name        string `json:"name"`
	AssetType   string `json:"asset_type"` // STOCK / ETF（由代码前缀推断, 主数据 Provider 接入后覆盖）
	GeneratedAt string `json:"generated_at"`

	SectorName    string      `json:"sector_name"`    // 跟随主来源证据的板块/ETF 名
	Sources       []SourceHit `json:"sources"`        // 全部命中来源（证据, 永不因去重删除）
	PrimarySource string      `json:"primary_source"` // 优先级最高者

	// SourceType 为 PrimarySource 的别名（同一 JSON 键, 序列化时合并）。
	// 目的：1) 兼容 tool.WatchEntry.source_type 落库契约（ToGenData round-trip）;
	// 2) 兼容既有测试断言。新代码请读 PrimarySource / Sources[]。
	SourceType string `json:"source_type"`

	Momentum20DPct *float64 `json:"momentum_20d_pct,omitempty"` // 来自任一来源的最新值
	OrgNetBuy      *float64 `json:"org_net_buy,omitempty"`
	AvgAmount20D   *float64 `json:"avg_amount_20d,omitempty"`

	// ---- 精筛诊断（WatchlistGenData round-trip 自然丢弃）----
	RankScore float64      `json:"rank_score,omitempty"`
	RankPos   int          `json:"rank_pos,omitempty"`
	Strength  StrengthPath `json:"strength_path,omitempty"`

	// RSScore: 阶段5 RS 双窗口加权总分快照（超额收益百分点口径）。RankScore 会被
	// 强度管线重写为四因子百分位口径(0-100), 本字段恒保留粗排原始结论供审计区分。
	RSScore float64 `json:"rs_score,omitempty"`

	// ---- 阶段 5: 双窗口相对强度明细（R20/R60 超额收益, 单位 %）----
	RetShortExcess *float64 `json:"ret_short_excess,omitempty"` // R20 - 基准同期
	RetLongExcess  *float64 `json:"ret_long_excess,omitempty"`  // R60 - 基准同期

	// ---- 阶段 7: 个股原始收益率与行业维度超额（strength.go 路径判定/评分输入）----
	RetShortRaw      *float64 `json:"ret_short_raw,omitempty"`      // 个股 R20 %
	RetLongRaw       *float64 `json:"ret_long_raw,omitempty"`       // 个股 R60 %
	RS20IndexExcess  *float64 `json:"rs20_index_excess,omitempty"`  // R20 - 基准指数（评分因子1）
	RS20SectorExcess *float64 `json:"rs20_sector_excess,omitempty"` // R20 - 行业指数（评分因子3; nil=行业不可确认）
	StrengthReason   string   `json:"strength_reason,omitempty"`    // A/B/NONE 判定依据（审计）

	// ---- 阶段 8: 日度指标快照补充字段（daily_stock_metrics 落库输入; refine 透传）----
	Close *float64 `json:"close,omitempty"` // 最新有效收盘价（元）
	MA20  *float64 `json:"ma20,omitempty"`  // 20 日均价（元; nil=样本不足）
	MA60  *float64 `json:"ma60,omitempty"`  // 60 日均价（元; nil=样本不足）

	// ---- 独立状态字段（不受主来源优先级影响）----
	Eligibility EligibilityStatus `json:"eligibility_status"` // PASS/BLOCKED/UNKNOWN
	Liquidity   LiquidityStatus   `json:"liquidity_status"`
	Financial   FinancialStatus   `json:"financial_status"`
	Blacklisted bool              `json:"blacklisted"` // 黑名单事实位（DB status=BLACKLIST）
	BlockedBy   string            `json:"blocked_by,omitempty"`

	// ---- 生命周期（per-symbol 视角; 逐来源精确迁移见 Lifecycle.Transition）----
	SourceLifecycle string     `json:"source_lifecycle"` // ACTIVE / STALE / EXPIRED
	FirstSeenAt     time.Time  `json:"first_seen_at"`
	LastSeenAt      time.Time  `json:"last_seen_at"`
	ExpireDate      *time.Time `json:"expire_date,omitempty"` // MANUAL-only 条目为 nil（永不过期）
	HitThisRun      bool       `json:"hit_this_run"`

	Tradability string `json:"tradability_status"` // ELIGIBLE / BLOCKED / UNKNOWN（派生, 见 ComputeTradability）

	// ---- 阶段 3: 证券资格硬条件明细（eligibility 与 tradability 分离建模）----
	// EligibilityStatus 表达"研究资格"（是否符合股票池准入），Tradability 表达
	// "交易时点可交易性"（是否允许生成新订单）。一只股票可以 eligibility=PASS
	// 但因停牌 tradability=BLOCKED——历史观察记录因此不会被不必要地删除。
	TradingHalted bool   `json:"trading_halted,omitempty"`  // 停牌中 -> 禁止新订单（不删记录）
	LimitState    string `json:"limit_state,omitempty"`     // NORMAL/UP/DOWN/UNKNOWN; UNKNOWN 禁止可执行订单
	OhlcInvalid   bool   `json:"ohlc_invalid,omitempty"`    // OHLC 完整性异常 -> 不参与计算, tradability=UNKNOWN
	StaleQuote    bool   `json:"stale_quote,omitempty"`     // 行情截止日 != as_of -> STALE, tradability=UNKNOWN
	PriceBelowMin bool   `json:"price_below_min,omitempty"` // 最新有效价 < StockMinPrice -> 不入普通候选排名

	// ---- 阶段 4: 流动性过滤与交易容量（liquidity.go）----
	// Liquidity 已在上方独立状态区; 此处补充容量维度。
	// CapacityStatus: OK / OVER / UNCHECKED —— 候选池阶段无最终仓位时恒为 UNCHECKED,
	// "流动性通过"不等于"执行可行", 下单前必须按最终计划数量重算。
	CapacityStatus string `json:"capacity_status,omitempty"`
	ListingDays    int    `json:"listing_days,omitempty"` // 上市自然日数（诊断用; MinListedDays=120 近似 120 交易日口径）

	// ---- L3/L4 缠论结构识别与买点分层（复用 python czsc, 见 chan.go）----
	// CandidateTier: P0~P4 分层号; ChanStatus: 细分业务状态枚举;
	// BuyPointConfirmedAt: 结构确认日期(信号 dt, 不是最新K线日);
	// StopLoss/EffectiveStop: czsc 止损契约回填（回抽低点×0.99 / max(·,中枢ZG)）。
	// 未接入缠论层时全部为零值——落库诚实为 NULL, 绝不伪造。
	ChanStatus          ChanStatus `json:"chan_status,omitempty"`
	CandidateTier       string     `json:"candidate_tier,omitempty"`
	ChanSummary         string     `json:"chan_summary,omitempty"`
	ChanReason          string     `json:"chan_reason,omitempty"`
	BuyPointConfirmedAt string     `json:"buy_point_confirmed_at,omitempty"`
	BuyPointReason      string     `json:"buy_point_reason,omitempty"`
	StopLoss            float64    `json:"stop_loss,omitempty"`
	EffectiveStop       float64    `json:"effective_stop,omitempty"`

	// Scored 标记该条目是否实际参与 RS 打分（refinePool 设置）。未打分的股票类
	// 条目 Strength 保持 Unset、RankPos=0 —— 防止"未打分"被误判为"打分后落选"。
	Scored bool `json:"scored,omitempty"`

	// ---- 阶段 6: 财务快照溯源四字段（契约硬要求, 由 FinancialProvider 回填）----
	// report_period / announcement_date 决定回测可见性（只能使用公告日 <= as_of
	// 的快照, 防未来数据污染）; data_as_of + provider 保证可审计与新鲜度校验。
	FinReportPeriod   string `json:"fin_report_period,omitempty"`
	FinAnnounceDate   string `json:"fin_announcement_date,omitempty"`
	FinDataAsOf       string `json:"fin_data_as_of,omitempty"`
	FinProviderSource string `json:"fin_provider,omitempty"`
	FinReasons        string `json:"fin_reasons,omitempty"` // WARN/BLOCKED/DATA_INSUFFICIENT 判定理由（审计用）
}

// Tradability 状态常量（最终交易资格, 进入审批流程的唯一闸门）。
const (
	TradEligible = "ELIGIBLE" // 允许进入交易审批流程（前提：全部闸门**明确通过**, 见闸门状态常量表）
	TradBlocked  = "BLOCKED"  // 禁止（黑名单优先, 其次资格/财务明确否决）
	TradUnknown  = "UNKNOWN"  // 主数据缺失 -> 默认不放行, 需人工确认
)

// ELIGIBLE 闸门最小合法集（fail-closed 契约的白名单形式）：只有拿到**明确结论**的
// 维度才允许放行。空字符串 ""（未运行）、非法枚举、DATA_INSUFFICIENT / UNKNOWN /
// UNCHECKED（诚实未知）一律不在集合内——绝不以"排除已知坏值"的方式兜底放行,
// 因为那种写法一旦将来新增状态值就会被 default 逻辑静默放过（评审信 P0-4）。
//
// 容量维度的特判说明：CapacityStatus 是**下单前闸门**, 候选池阶段无最终仓位时恒为
// UNCHECKED（liquidity.go 契约："流动性通过"不等于"执行可行"）。因此本闸门把
// UNCHECKED 视为"容量维度已定界"（诚实未校验 -> CAPACITY_UNCHECKED 软阻塞）,
// 而 ""/非法值仍归 INVALID_STATE；OVER 在硬性否决区处理。
var (
	eligibleLiquidity  = map[LiquidityStatus]bool{LiqPass: true, LiqWarn: true}
	eligibleElig       = map[EligibilityStatus]bool{EligPass: true}
	eligibleFinancial  = map[FinancialStatus]bool{FinPass: true, FinWarn: true}
	// knownLimitStates 涨跌停状态"已定界"集合：NORMAL / UNKNOWN / UP / DOWN 均为检查
	// 实际运行后给出的诚实结论（UNKNOWN = SDK 无权威来源的锁定语义, refine.go 不伪造
	// NORMAL；UP/DOWN 属当日成交约束, 下单控制由执行层承担）。""（检查根本没运行）与
	// 非法枚举不在集合内 -> LIMIT_STATE 软阻塞（空值不得当作通过）。
	knownLimitStates = map[string]bool{LimitNormal: true, LimitUnknown: true, "UP": true, "DOWN": true}
	// 容量合法枚举全集（OK 放行; UNCHECKED 诚实未校验; OVER 硬阻断; ""/非法 INVALID）。
	validCapacityStates = map[string]bool{CapacityOK: true, CapacityOver: true, CapacityUnchecked: true}
)

// ComputeTradability 按固定顺序推导交易资格：黑名单 > 证券资格 > 财务 > 强度路径。
// 注意与来源有效性无关——一只来源 ACTIVE 的股票也可能 tradability=BLOCKED。
// strengthPipelineExecuted 报告强度管线是否实际执行过 A/B/NONE 判定。
//   - 显式传参（Generator 调用点传 g.StrengthKnown()）时以参数为准；
//   - 无参（独立 DTO / 旧调用点）时按字段推断：股票类条目只要 Strength 被显式赋值
//     （A/B/NONE）即可证明管线实际执行过；Unset 则视为"未执行"（诚实降级, 不猜测）。
func (e *AggEntry) strengthPipelineExecuted(strengthKnown []bool) bool {
	if len(strengthKnown) > 0 {
		return strengthKnown[0]
	}
	// 无参推断的适用范围必须与 STRENGTH 分支闸门（isStockLike）一致：ETF / MANUAL-only
	// 条目从不参与 A/B/NONE 判定, 若其 Strength 携带陈旧值（上游透传、历史快照或测试
	// 夹具残留）会被误判为"管线执行过且落选", 把不适用强度闸门的条目错误硬阻塞。
	return e.stockGatesApplicable() && e.Strength != StrengthUnset
}

// isStockLike 是否为股票类条目（主来源为 SECTOR_LEADER / FUND_FLOW, 与 refinePool
// 的股票类判定口径一致）。MANUAL / ETF 主来源不适用股票强度/资格/财务闸门。
func (e *AggEntry) isStockLike() bool {
	return e.PrimarySource == sourceSector || e.PrimarySource == sourceFund
}

// tradReasons 收集本条目当前全部不放行原因（黑名单 / 硬性否决在前, 诚实数据缺失在后）。
type tradReasons struct {
	hard []string // BLOCKED 级：明确证据的否决（黑名单/资格BLOCKED/财务BLOCKED/停牌/低价/强度落选/流动性FAIL/容量OVER）
	soft []string // UNKNOWN 级：数据缺失/未知状态（*_DATA / QUOTE_STATE / INVALID_STATE）
}

func (r *tradReasons) addHard(veto string) { r.hard = append(r.hard, veto) }
func (r *tradReasons) addSoft(why string)  { r.soft = append(r.soft, why) }

// deriveTradability 由原因集合推导最终资格与 BlockedBy：
//   - 任一硬性否决 -> BLOCKED, BlockedBy = 首个硬性原因（固定优先级）；
//   - 否则任一软性原因 -> UNKNOWN, BlockedBy = 首个软性原因；
//   - 全部检查明确通过 -> ELIGIBLE, BlockedBy = ""。
//
// 多因并存时的展示优先级（P0-② 定案, 2026-10-14）：
//
//	BLOCKED 链: BLACKLIST > ELIGIBILITY > FINANCIAL > HALTED > PRICE_BELOW_MIN
//	            > STRENGTH > LIQUIDITY > CAPACITY
//	UNKNOWN 链: LIQUIDITY_DATA > ELIGIBILITY_DATA > FINANCIAL_DATA > QUOTE_STATE
//	            > INVALID_STATE
//	STRENGTH 位于 FINANCIAL(BLOCKED) 之后、LIQUIDITY(LiqFail) 之前：
//	"打分后落选"是软阻塞（可人工放行）, 但不得掩盖黑名单与确定性资格/财务否决。
//
// 关键修复：**多因全量记录**。旧实现在 FINANCIAL_DATA 处提前 return, 把同一股票的
// STRENGTH（打分后落选）整体吞掉——financial_test 集成用例暴露了该遮蔽：财务供应商
// 未接入时, 已打分落选的股票只剩 FINANCIAL_DATA 一个可见原因, 审计侧误以为强度结论
// 不存在。现所有命中原因全部写入 BlockedBy（"; " 连接, 首位为主因）, 不再丢失任何
// 一条否决依据。
func deriveTradability(r tradReasons) (string, string) {
	if len(r.hard) > 0 {
		return TradBlocked, strings.Join(r.hard, "; ")
	}
	if len(r.soft) > 0 {
		return TradUnknown, strings.Join(r.soft, "; ")
	}
	return TradEligible, ""
}

// ComputeTradability 推导交易资格（最终交易闸门, fail-closed）。
//
// strengthKnown 表示强度管线是否**实际执行过** A/B/NONE 判定：
//   - 传 true（Generator 调用点传 g.StrengthKnown()）：股票类条目 Strength==NONE 是
//     "打分后落选"的确定结论 -> 软阻塞 STRENGTH；
//   - 传 false：管线未执行（精筛关闭 / 基准不可得 / 流动性闸门复算路径）——绝不允许
//     把"未重新判定"猜测成"打分后落选"；
//   - 无参（独立 DTO / 旧调用点）：按字段推断——股票类条目 Strength 已被显式赋值
//     （A/B/NONE）即证明管线实际执行过, NONE 保守阻塞（与 aggregate_test 锁定语义
//     一致）；Unset 视为未执行, 不触发 STRENGTH 分支。
//
// 修复说明（相对上一版）：
//  1. 旧实现存在**空 case 分支**（Go 的 switch 命中空 case 后直接退出, Tradability
//     保持零值 ""——既不是 ELIGIBLE 也不是诚实的 UNKNOWN），且两个 STRENGTH 分支的
//     条件互相写反（传 true 反而永不命中, 传 false 反而阻塞）。现改为确定性顺序判定。
//  2. default 放行前先做枚举合法性校验：任何空值/非法枚举（Eligibility/Liquidity/
//     Financial/LimitState/CapacityStatus/Strength）一律 UNKNOWN + INVALID_STATE,
//     绝不把"空数据"当作通过（contract.go 头注释契约）。
//  3. 行情完整性（OHLC 异常 / 行情过期 / 涨跌停不可确认）不再依赖 Scored——即使
//     未参加 RS 打分, 实际交易前仍必须验证行情与交易状态（fail-closed）。Scored
//     只表示是否参与评分, 不决定数据是否可信。
//  4. 入口清空 BlockedBy：股票从阻断恢复为 ELIGIBLE 后, 旧阻塞原因不得残留
//     （评审信 TestTradabilityClearsOldBlockedBy 契约）。
//  5. 多因并存不再短路遮蔽（见 deriveTradability 注释）。
func (e *AggEntry) ComputeTradability(strengthKnown ...bool) {
	// 每次重算都是全新推导：先清空派生字段, 防止旧 BlockedBy 在恢复 ELIGIBLE 后残留。
	e.BlockedBy = ""
	var r tradReasons
	pipelineRan := e.strengthPipelineExecuted(strengthKnown)

	// ---- 硬性否决（BLOCKED 级, 按固定优先级收集）----
	if e.Blacklisted {
		r.addHard("BLACKLIST")
	}
	if e.Eligibility == EligBlocked {
		r.addHard("ELIGIBILITY")
	}
	if e.Financial == FinBlocked {
		r.addHard("FINANCIAL")
	}
	if e.TradingHalted {
		// 停牌：研究资格(eligibility)不受影响, 仅禁止新订单; 记录保留。
		r.addHard("HALTED")
	}
	if e.PriceBelowMin {
		// 优先级修复（生产 Bug）：最新有效价 <1 元是**明确证据**的确定性否决，必须先于
		// QUOTE_STATE 判定——精筛恒置 LimitState=LimitUnknown（SDK 无权威涨跌停来源），
		// 旧顺序下已打分条目永远先命中 QUOTE_STATE/UNKNOWN，把"明确不可入池"稀释成
		// "待人工确认"。无论是否参与 RS 打分，低价格式硬条件一律 BLOCKED。
		e.Tradability = TradBlocked
		e.BlockedBy = "PRICE_BELOW_MIN"
	case e.ChanStatus.BlocksTradability():
		// L4 缠论分层：P4 结构失效（czsc 卖点确认/买点被破坏）-> 禁止新订单。
		// 与 STRENGTH 软阻塞同级（可人工复核放行）；P0~P3 不影响此判定——
		// "没有买点"只是观察状态, 不是否决证据。未接入缠论层时 ChanStatus
		// 为空值, BlocksTradability()=false, 行为与旧版完全一致。
		e.Tradability = TradBlocked
		e.BlockedBy = "CHAN_STRUCTURE"
	case e.Strength == StrengthNone && e.hasAutoSourceOnly() && len(strengthKnown) != 0 && !strengthKnown[0]:
		// 阶段 7 冲突修复：管线**已执行**且股票类条目两条路径均未入选 -> 软阻塞 STRENGTH；
		// 管线未执行(如流动性闸门复算路径)时不得把"未重新判定"猜测成"打分后落选",
		// 否则会吞掉 FINANCIAL_DATA/LIQUIDITY_DATA 等更具体的诚实结论。
	case e.Strength == StrengthNone && e.hasAutoSourceOnly() && len(strengthKnown) == 0:
		// RS 管线**实际执行过**（Strength 被显式赋值为 A/B/NONE）且股票类条目两条
		// 路径均未入选 -> 高优先级截断（软阻塞, 可人工放行）。
		// StrengthUnset（精筛关闭 / RS 失败退化 / ETF / MANUAL-only）不参与此判定,
		// 否则会把"未打分"吞成"打分后落选", 违反阈值语义诚实原则。
		r.addHard("STRENGTH")
	}
	liqFail := e.Liquidity == LiqFail
	if e.CapacityStatus == CapacityOver {
		// 计划参与率超限 -> 需降低数量或分析执行成本后方可交易。
		r.addHard("CAPACITY")
	}
	// ---- 诚实数据缺失（UNKNOWN 级, 仅当无任何硬性否决时才生效）----
	//
	// ①定案（评审信 6.1 TestManualDoesNotBypassFinancialState / P0-4）：**数据不足类
	// 软阻塞优先于强度落选**。旧实现中 STRENGTH 位于硬链首位——财务/资格供应商未接入
	// （DATA_INSUFFICIENT/UNKNOWN）时, 已打分落选的股票被显示为 BLOCKED(STRENGTH),
	// 审计侧看不到"闸门根本没运行"这一真实状态, 且 BLOCKED（确定性剔除）比 UNKNOWN
	// （待补数据）更强地误导人工处置。现规则：
	//   - 存在 *_DATA / INVALID_STATE 类软阻塞时 -> UNKNOWN, 原因链 = 全部数据不足项
	//     + STRENGTH（多因全量记录, 一条都不丢）；BLOCKED 集合中的 STRENGTH 只是
	//     "尚未复核"的占位结论, 不得掩盖闸门未运行的事实；
	//   - 闸门全部有确定结论（PASS/WARN/诚实 N/A）且无其他硬性否决时 -> STRENGTH
	//     维持 BLOCKED（打分后确定性落选, 可人工放行）。
	softData := tradReasons{} // 仅数据不足类（*_DATA / QUOTE_STATE / LIMIT_STATE / CAPACITY_UNCHECKED / INVALID_STATE）
	if e.Liquidity == LiqDataInsufficient {
		softData.addSoft("LIQUIDITY_DATA")
	} else if !eligibleLiquidity[e.Liquidity] && !liqFail {
		// 空值/非法枚举: 流动性闸门未运行不得当作通过（fail-closed 白名单）。
		// LiqFail 属明确证据, 在下方统一以 LIQUIDITY 主因处理, 不进软区重复。
		softData.addSoft("INVALID_STATE")
	}
	if e.Eligibility == EligUnknown {
		softData.addSoft("ELIGIBILITY_DATA")
	} else if !eligibleElig[e.Eligibility] {
		softData.addSoft("INVALID_STATE")
	}
	if e.Financial == FinDataInsufficient && e.stockGatesApplicable() {
		// 阶段 6: 财务快照缺失/过期 -> UNKNOWN（不放行也不剔除, 显示缺失原因）。
		// 适用范围限定：股票 EPS/ROE 画像**不适用于** ETF / MANUAL-only 条目——
		// 这些条目的 DATA_INSUFFICIENT 只是聚合层"未经确认"的占位值, 把它伪装成
		// "财务数据缺失"会污染归因（评审信 P0-2：不适用 ≠ 未通过, 更 ≠ 待补数据）。
		// 其容量维度仍由下方 CAPACITY_UNCHECKED 诚实拦截, 不存在放行缺口。
		softData.addSoft("FINANCIAL_DATA")
	} else if !eligibleFinancial[e.Financial] {
		softData.addSoft("INVALID_STATE")
	}
	if e.OhlcInvalid || e.StaleQuote {
		// 行情完整性异常（OHLC / 过期）-> 禁止"可执行"订单。数据有效性不依赖
		// Scored（fail-closed）：即使未参加 RS 打分也必须验证行情。TradUnknown
		// 本身就不进入审批流程; 若未来需要区分"可提交人工审查的候选"与"可执行
		// 订单资格", 应另行建模执行资格, 而不是放宽本分支。
		softData.addSoft("QUOTE_STATE")
	}
	if !knownLimitStates[e.LimitState] {
		// 涨跌停状态**未定界**才拦截：""（检查根本没运行）或非法枚举 -> LIMIT_STATE
		// （评审信 P0-4：default 放行必须要求全部必需检查有确定结论, 空值不得当作通过）。
		// NORMAL / UNKNOWN / UP / DOWN 是"检查已运行并给出诚实结论"：UNKNOWN 为 SDK 无
		// 权威涨跌停来源下的锁定语义（refine.go 不伪造 NORMAL）, UP/DOWN 属当日成交约束
		// ——三者的下单控制由执行层承担（涨跌停价上不得追单、委托待状态确认后重试）,
		// 不在研究池层面把已定界的诚实结论吞成泛化阻塞。ETF / MANUAL-only 同一口径。
		softData.addSoft("LIMIT_STATE")
	}
	if !validCapacityStates[e.CapacityStatus] {
		// ""/非法容量枚举 -> INVALID_STATE（合法 UNCHECKED 落入下方诚实分支）。
		softData.addSoft("INVALID_STATE")
	}
	if e.CapacityStatus == CapacityUnchecked {
		// 容量重算尚未执行 -> 诚实阻塞（下单前闸门未运行, 不放行）。该维度独立于
		// 证券闸门适用性：旧实现把 UNCHECKED 拦截限定为"非股票类条目", 依赖 *_DATA
		// 链先行拦截股票——但 *_DATA 只是数据缺失时的兜底, 当股票的资格/财务/流动性
		// 全部 PASS（Provider 已接入）时容量维度会被静默跳过并放行, 违反"全部必需
		// 检查明确通过"契约（评审信 P0-4）。凡未校验一律显式记录 CAPACITY_UNCHECKED。
		softData.addSoft("CAPACITY_UNCHECKED")
	}
	switch e.Strength {
	case StrengthUnset, StrengthA, StrengthB, StrengthNone:
	default:
		softData.addSoft("INVALID_STATE") // 非法强度枚举
	}

	hasSoftData := len(softData.soft) > 0
	if hasSoftData {
		// 软阻塞优先：数据不足类在前, STRENGTH 作为伴生原因附后。硬性否决（黑名单/
		// 资格/财务 BLOCKED/停牌/低价/CAPACITY）仍保持最高优先级——deriveTradability
		// 中 hard 非空即 BLOCKED, 本分支只是把"闸门未运行"的真实状态留在链尾供审计。
		r.soft = append(r.soft, softData.soft...)
		if e.Strength == StrengthNone && e.isStockLike() {
			r.soft = append(r.soft, "STRENGTH")
		}
		if liqFail {
			// 流动性 FAIL 是明确证据 -> BLOCKED, 主因固定 LIQUIDITY（评审信阶段 4
			// 契约: FAIL 禁止新订单）。其余未运行维度（*_DATA / INVALID_STATE）作为
			// 伴生原因保留在链尾, 一条都不丢。
			r.hard = append([]string{"LIQUIDITY"}, r.hard...)
		}
	} else if liqFail {
		// 全部必需检查都有确定结论且无其他硬性否决 -> 纯流动性剔除。
		r.addHard("LIQUIDITY")
	}
	e.Tradability, e.BlockedBy = deriveTradability(r)
}

// ---------------------------------------------------------------------------
// 来源谓词（三个互相独立的维度, 取代旧的 hasAutoSourceOnly）
// ---------------------------------------------------------------------------
//
// 旧实现 hasAutoSourceOnly() 的真实语义是"不含 MANUAL 即 true"——只有 ETF 来源、
// 甚至没有任何来源证据的条目也会返回 true, 于是聚合初始化把 ETF 当成普通股票标为
// UNKNOWN/DATA_INSUFFICIENT, 又把"MANUAL+自动来源"整体默认成 PASS/PASS（人工输入
// 间接绕过证券资格与财务检查）。现拆分为显式谓词, 聚合初始化改按 AssetType 与
// PrimarySource 判定"适用性", 而不是按"是否含 MANUAL"猜测结论。

// HasManualSource 证据中是否包含人工白名单来源（人工记录永不被自动清理, 但不代表
// 可以绕过黑名单 / 行情完整性 / 容量等硬性检查）。
func (e *AggEntry) HasManualSource() bool {
	for _, s := range e.Sources {
		if s.SourceType == sourceManual {
			return true
		}
	}
	return false
}

// HasAutomaticSource 证据中是否包含任一自动来源（SECTOR_LEADER / ETF_MOMENTUM / FUND_FLOW）。
func (e *AggEntry) HasAutomaticSource() bool {
	for _, s := range e.Sources {
		if s.SourceType != sourceManual {
			return true
		}
	}
	return false
}

// IsManualOnly 是否纯人工条目（无任何自动来源证据）。
func (e *AggEntry) IsManualOnly() bool { return len(e.Sources) > 0 && !e.HasAutomaticSource() }

// isETFLike ETF 身份判定：优先看权威 AssetType, 代码前缀启发式兜底（主数据 Provider
// 接入后由其覆盖 AssetType, 本判定自动升级为权威口径）。
func (e *AggEntry) isETFLike() bool {
	if e.AssetType != "" {
		return e.AssetType == "ETF"
	}
	return isETFCode(e.Symbol)
}

// stockGatesApplicable 股票资格 / 财务闸门是否适用于本条目：
//   - ETF 不适用（非个股证券, 股票 EPS/ROE 画像对其无意义）;
//   - MANUAL-only 股票不适用（人工自选未经自动画像采集, 不得伪造结论）;
//   - 其余（含 MANUAL+自动来源并存的股票）一律适用——人工来源只能保留记录,
//     不能把未知的证券资格变成 PASS。
func (e *AggEntry) stockGatesApplicable() bool {
	return !e.isETFLike() && !e.IsManualOnly()
}

// priority 返回该来源的优先级序号（未知来源排最后）。
func srcPriority(t string) int {
	if p, ok := sourcePriority[t]; ok {
		return p
	}
	return 99
}

// aggregateEntries 按统一证券身份合并重复记录：标准化代码 -> 合并 sources[] ->
// 保留全部理由 -> 按优先级选 primary_source。name/指标取首个非空值。
// orDefault 零值 SourceStatus（未注入状态的旧载荷, 含 Python 链路）按 OK 处理。
func (s SourceStatus) orDefault(def SourceStatus) SourceStatus {
	if s == "" {
		return def
	}
	return s
}

// entryAsOf 命中证据的业务截止日期：使用上游 Entry 自带的 GeneratedAt
// （前 10 字符 YYYY-MM-DD；Entry 契约与 Python make_entry 载荷均无独立 AsOf 字段，
// GeneratedAt 即该条命中数据的生成/截止时刻），缺失时诚实返回 ""——绝不把
// "来源日期未知"伪装成聚合运行日期（asOf）。落库层（pool_store）对空值自行兜底
// 展示日期并保留 NULL 语义。
func entryAsOf(e Entry) string {
	if len(e.GeneratedAt) >= 10 {
		return e.GeneratedAt[:10]
	}
	return ""
}

// entryHitVerified 本条上游命中是否属于"已明确验证的有效命中"（P1-4）。
// 只有 OK / SUCCESS_EMPTY(确认成功取到空结果的占位行不会走到这里) 状态才是经过
// 对应来源数据校验的结论；STALE / PARTIAL / FAILED / UNKNOWN 只是"收到了记录",
// 不构成新鲜度证明——不得据此刷新 LastSeenAt / HitThisRun（否则接口故障期间
// 旧证据会被伪装成今天仍然有效）。零值状态按 orDefault 契约视为 OK（兼容无此
// 字段的旧载荷）。
func entryHitVerified(e Entry) bool {
	s := e.HitStatus.orDefault(StatusOK)
	return s == StatusOK || s == StatusSuccessEmpty
}

// statusOf(code) 返回该股票在持久层的黑名单事实位与既有生命周期锚点（可为 nil）。
func aggregateEntries(entries []Entry, asOf string, now time.Time, statusOf func(string) *AggEntry) []AggEntry {
	type slot struct {
		agg AggEntry
		idx int
	}
	order := []string{}
	bySym := map[string]*slot{}

	// P1-2: 先按规范化 symbol + 来源优先级 + 证据键整体排序，再进入合并循环。
	// 修复说明（相对上一版）：Name/指标/最终评分的覆盖顺序此前完全取决于
	// entries 的输入顺序（Provider 返回顺序变化即改变聚合结果, 不可复现）。
	// 规范化排序后, "最后一条非 nil 值获胜"变成确定性的、与输入排列无关的规则。
	sorted := make([]Entry, len(entries))
	copy(sorted, entries)
	sort.SliceStable(sorted, func(i, j int) bool {
		si, sj := normalizeCode(sorted[i].Symbol), normalizeCode(sorted[j].Symbol)
		if si != sj {
			return si < sj
		}
		if pi, pj := srcPriority(sorted[i].SourceType), srcPriority(sorted[j].SourceType); pi != pj {
			return pi < pj // 低优先级来源在前 -> 高优先级(权威)来源的证据/指标最后写入并生效
		}
		return sourceEvidenceKey(sorted[i]) < sourceEvidenceKey(sorted[j])
	})

	for _, e := range sorted {
		sym := normalizeCode(e.Symbol)
		if sym == "" {
			continue
		}
		s, exists := bySym[sym]
		if !exists {
			s = &slot{idx: len(order)}
			s.agg = AggEntry{
				Symbol:      sym,
				Name:        e.Name,
				AssetType:   assetTypeOf(sym),
				GeneratedAt: e.GeneratedAt,
				// P1-4 修复（相对上一版）：新槽位必须显式初始化 SourceLifecycle——旧实现
				// 留空字符串, 若持久化发生在 Transition() 之前, 会产生不属于任何合法
				// 状态(ACTIVE/STALE/EXPIRED)的记录。
				SourceLifecycle: "ACTIVE",
				FirstSeenAt:     now,
				HitThisRun:      entryHitVerified(e), // "确认有效命中"而非"收到记录"
			}
			// LastSeenAt 只在本次命中经过来源数据校验时才刷新为 now；STALE/FAILED/
			// PARTIAL 证据只是"收到了记录", 不构成新鲜度证明, 不得伪装成今天仍有效。
			if entryHitVerified(e) {
				s.agg.LastSeenAt = now
			}
			// 持久层锚点：黑名单事实位优先保留；生命周期沿用既有 first/last seen
			// 与 lifecycle（白名单永不过期语义由 Transition 保证）。
			if statusOf != nil {
				if prev := statusOf(sym); prev != nil {
					s.agg.Blacklisted = prev.Blacklisted
					if !prev.FirstSeenAt.IsZero() {
						s.agg.FirstSeenAt = prev.FirstSeenAt
					}
					if !prev.LastSeenAt.IsZero() && prev.LastSeenAt.After(s.agg.LastSeenAt) {
						s.agg.LastSeenAt = prev.LastSeenAt
					}
					if prev.SourceLifecycle != "" {
						s.agg.SourceLifecycle = prev.SourceLifecycle
					}
				}
			}
			if s.agg.GeneratedAt == "" {
				s.agg.GeneratedAt = now.Format("2006-01-02 15:04:05")
			}
			bySym[sym] = s
			order = append(order, sym)
		}
		a := &s.agg
		// P1-4：后续同源条目若带来**已验证**的命中, 同样刷新 HitThisRun / LastSeenAt
		// （新槽位初始化时只反映了第一条排序后的 Entry）。
		if entryHitVerified(e) {
			a.HitThisRun = true
			if a.LastSeenAt.Before(now) {
				a.LastSeenAt = now
			}
		}
		if a.Name == "" && e.Name != "" {
			a.Name = e.Name
		}
		// 证据追加。P1-1 修复（相对上一版）：旧逻辑只要 SourceType 相同就整条丢弃,
		// 同一来源的多条**合法**命中（如一只股票同时是多个行业板块的龙头、或理由不同
		// 的多条筛选命中）会被误删——去重不等于删除有效证据。现改为按完整证据键
		// (source|sector|reason|status|as_of) 幂等去重：只有逐字段相同的重复行才跳过。
		newHit := SourceHit{
			SourceType: e.SourceType,
			Reason:     sourceReason(e),
			SectorName: e.SectorName,
			// P1-4 修复：AsOf 保存该来源业务数据的真实截止日期（Entry.AsOf / GeneratedAt）,
			// 不再无条件改写为聚合级 asOf；上游确实无日期时诚实留空（不伪装成同日更新）。
			// RunAsOf 单独记录本次候选池任务的目标时点（新鲜度审计三维之一）。
			AsOf:    entryAsOf(e),
			RunAsOf: asOf,
			Status:  e.HitStatus.orDefault(StatusOK),
		}
		dup := false
		for _, old := range a.Sources {
			if evidenceKey(old) == evidenceKey(newHit) {
				dup = true
				break
			}
		}
		if !dup {
			a.Sources = append(a.Sources, newHit)
		}
		// 最新指标透传（指针非 nil 即覆盖）。经上方规范化排序后, 覆盖顺序与
		// Provider 返回顺序无关; 高优先级来源的同名指标确定性生效。
		if e.Momentum20DPct != nil {
			v := *e.Momentum20DPct
			a.Momentum20DPct = &v
		}
		if e.OrgNetBuy != nil {
			v := *e.OrgNetBuy
			a.OrgNetBuy = &v
		}
		if e.AvgAmount20D != nil {
			v := *e.AvgAmount20D
			a.AvgAmount20D = &v
		}
		// P0-5 修复（相对上一版）：聚合阶段**不得**写入最终 RankScore / RankPos。
		// 唯一最终评分写入者是 strength.go 管线（applyStrengthPathsAndScores）;
		// 来源阶段的分数只是诊断信息, 保存在 SourceHit 级, 若透传到条目上会：
		//   - 用来源分覆盖最终强度分（双评分口径冲突）;
		//   - 使最终得分取决于 entries 遍历顺序;
		//   - 无法区分"合法的 0 分"与"未打分"。
		// refinePool 消费的是本地 Entry 切片上的分数, 不依赖此处透传, 删除安全。
		// 阶段 8: MA/收盘快照透传（refinePool 已写入 Entry; 指针非 nil 即覆盖）
		if e.Close != nil {
			v := *e.Close
			a.Close = &v
		}
		if e.MA20 != nil {
			v := *e.MA20
			a.MA20 = &v
		}
		if e.MA60 != nil {
			v := *e.MA60
			a.MA60 = &v
		}
	}

	out := make([]AggEntry, 0, len(order))
	for _, sym := range order {
		a := &bySym[sym].agg
		sort.SliceStable(a.Sources, func(i, j int) bool {
			if p := srcPriority(a.Sources[i].SourceType) - srcPriority(a.Sources[j].SourceType); p != 0 {
				return p < 0
			}
			return evidenceKey(a.Sources[i]) < evidenceKey(a.Sources[j])
		})
		if len(a.Sources) > 0 {
			a.PrimarySource = a.Sources[0].SourceType
			a.SourceType = a.PrimarySource // 兼容别名: 落库契约 source_type
			// sector_name 跟随主来源证据（多来源时保留最高优先级语境的板块名）
			a.SectorName = a.Sources[0].SectorName
		}
		// ---- 独立状态字段初始化（P0-2/P0-3 修复, 取代旧的 hasAutoSourceOnly 猜测）----
		// 规则：状态只表达"实际验证结论", 不适用维度显式记 PASS(N/A), 未经确认一律诚实 UNKNOWN。
		//   - ETF：股票证券资格画像不适用 -> PASS(N/A)（ETF 仍受黑名单/停牌/价格/行情完整性/容量约束,
		//     见 ComputeTradability 硬性分支, 这些检查与来源类型无关）;
		//   - MANUAL-only 股票：人工自选未经自动画像采集 -> UNKNOWN（精筛开启时由闸门回填实际结论;
		//     人工保留 ≠ 自动绕过证券资格/财务检查）;
		//   - 其余股票类（含 MANUAL+自动来源并存）：未经主数据/财务 Provider 确认 -> UNKNOWN /
		//     DATA_INSUFFICIENT。**MANUAL 的存在不再改变股票的资格与财务状态**。
		switch {
		case a.isETFLike():
			a.Eligibility = EligPass // N/A: 个股资格画像不适用于 ETF（显式标注, 见 FinNoteNotApplicable）
			a.Financial = FinPass    // N/A: 股票 EPS/ROE 闸门不适用于 ETF
		case a.IsManualOnly():
			a.Eligibility = EligUnknown
			a.Financial = FinDataInsufficient
		default:
			a.Eligibility = EligUnknown // 自动来源股票: 资格未经主数据 Provider 确认 -> UNKNOWN
			a.Financial = FinDataInsufficient
		}
		a.Strength = StrengthUnset // refinePool 实际打分后才覆盖为 A/B/NONE
		// P0-3 修复（fail-open -> fail-closed）：聚合阶段不得默认 Liquidity=PASS。
		// 全市场初筛（liquidityScreen）只是资金流来源的前置门槛, 其退化路径
		// （初筛失败不设门槛）下"进入池子"不代表任何流动性验证; 逐票 ADV20 由
		// 阶段 4 流动性闸门（liquidityGatePool）实测后覆盖本字段。闸门未运行/取数
		// 失败时诚实保持 DATA_INSUFFICIENT（tradability=UNKNOWN/LIQUIDITY_DATA）,
		// 绝不把"没算过"当成"已通过"。
		a.Liquidity = LiqDataInsufficient
		a.CapacityStatus = CapacityUnchecked // 候选池阶段无最终仓位 -> 未校验（下单前必须重算）
		if a.Blacklisted {
			a.Eligibility = EligBlocked
		}
		a.ComputeTradability()
		out = append(out, *a)
	}
	return out
}

// sourceEvidenceKey 单条上游命中的证据键（幂等去重 + 规范化排序共用）。
func sourceEvidenceKey(e Entry) string {
	return strings.Join([]string{e.SourceType, e.SectorName, sourceReason(e), string(e.HitStatus), entryAsOf(e)}, "\x00")
}

// evidenceKey 聚合后 SourceHit 的证据键（与 sourceEvidenceKey 同构）。
func evidenceKey(h SourceHit) string {
	return strings.Join([]string{h.SourceType, h.SectorName, h.Reason, string(h.Status), h.AsOf}, "\x00")
}

// sourceReason 从 Entry 已有信息构造本来源的命中理由（人类可读, 落库供审计）。
func sourceReason(e Entry) string {
	switch e.SourceType {
	case sourceManual:
		return "人工自选白名单"
	case sourceETF:
		if e.Momentum20DPct != nil {
			return fmt.Sprintf("ETF动量 R%d=%.2f%%", etfReasonWindow, *e.Momentum20DPct)
		}
		return "ETF 动量入选"
	case sourceSector:
		return fmt.Sprintf("行业龙头[%s]", e.SectorName)
	case sourceFund:
		if e.OrgNetBuy != nil {
			return fmt.Sprintf("龙虎榜机构净买入%s", formatYi(*e.OrgNetBuy))
		}
		return "龙虎榜机构净买入"
	default:
		return e.SectorName
	}
}

// etfReasonWindow ETF 理由文案中的动量窗口（与 Config.ETFMomWindow 默认值一致）。
const etfReasonWindow = 20

// assetTypeOf 由代码前缀推断资产类型（主数据 Provider 接入后由其权威覆盖）。
func assetTypeOf(code string) string {
	if isETFCode(code) {
		return "ETF"
	}
	return "STOCK"
}

// ---------------------------------------------------------------------------
// 生命周期状态机（阶段 2.3 过期规则的纯函数实现）
// ---------------------------------------------------------------------------

// LifeAction 描述一次刷新对某条持久记录的处理结果。
type LifeAction string

const (
	LifeTouch     LifeAction = "TOUCH"      // 继续命中: 更新 last_seen_at + 续期
	LifeKeep      LifeAction = "KEEP"       // 明确验证的记录才更新, 其余不动
	LifeExpire    LifeAction = "EXPIRE"     // OK/SUCCESS_EMPTY 且不再命中且已过截止日
	LifeMarkStale LifeAction = "MARK_STALE" // 来源 FAILED: 保留记录, 标记旧数据状态
	LifeProtect   LifeAction = "PROTECT"    // MANUAL 白名单 / BLACKLIST: 自动逻辑不得改写
)

// Transition 对单条持久记录应用一次来源刷新（纯函数, 不改入参）。
// persistedHit=本次是否命中; refreshStatus=对应来源本次刷新状态; autoExpireDays<=0 视为非法配置。
//
// 修复说明（相对上一版）：
//  1. （第 13 条）旧实现只在 ExpireDate != nil 时才判过期——aggregateEntries 新建
//     条目从不设置 ExpireDate, 若持久化/迁移没有补齐它, 自动来源记录将**永不过期**。
//     现改为：不再命中且 ExpireDate 缺失时, 用 LastSeenAt + autoExpireDays 计算兼容
//     截止时间；连 LastSeenAt 也无效则诚实标 STALE（数据不足需修复）, 绝不默认永久保留。
//  2. （第 12 条）PARTIAL 不再与 FAILED 一刀切：persistedHit=true 表示该证券属于
//     本次已成功、已验证的分片 -> 正常 TOUCH 续期；只有未确认(未知)的记录才保持
//     原状并标 STALE, 不因"整批部分成功"被批量失效。FAILED/PARTIAL 均不进入过期
//     判定（接口故障绝不清除旧证据）。
//  3. 配置防御：autoExpireDays<=0 时不得把到期时间算成 now（当天即过期）或负数
//     偏移, 一律降级为 STALE 待修复。
func Transition(rec AggEntry, persistedHit bool, refreshStatus SourceStatus, autoExpireDays int, now time.Time) (AggEntry, LifeAction) {
	out := rec
	// 人工白名单与黑名单永不被自动逻辑改写（4.2: 白名单独立于自动过期; 黑名单终态）
	if rec.PrimarySource == sourceManual || rec.Blacklisted {
		return out, LifeProtect
	}
	switch refreshStatus {
	case StatusFailed:
		// FAILED: 保留上次有效结果并标 STALE; 不进入过期判定, 也不刷新任何时间锚点。
		out.SourceLifecycle = "STALE"
		return out, LifeMarkStale
	case StatusPartial:
		// PARTIAL: 只更新能明确验证的记录, 其余保留原状（不批量失效）。
		if persistedHit {
			break // 属于已成功验证分片 -> 落入下方 OK 分支的续期逻辑
		}
		out.SourceLifecycle = "STALE" // 未知记录: 保留原数据, 禁止当作当前有效信号
		return out, LifeMarkStale
	case StatusOK, StatusSuccessEmpty:
		if persistedHit {
			out.LastSeenAt = now
			if autoExpireDays <= 0 {
				// 非法配置: 无法确定截止日, 诚实标 STALE, 不得伪造 ExpireDate=now。
				out.SourceLifecycle = "STALE"
				return out, LifeMarkStale
			}
			exp := now.AddDate(0, 0, autoExpireDays)
			out.ExpireDate = &exp
			out.SourceLifecycle = "ACTIVE"
			return out, LifeTouch
		}
		// 不再命中: 仅当已超过自然日截止才过期; 节假日/短窗口内的旧记录原样保留。
		// （第 13 条修复）ExpireDate 缺失时用 LastSeenAt + autoExpireDays 兜底计算,
		// 自动来源记录不得因缺字段而永不过期。
		exp := rec.ExpireDate
		if exp == nil {
			if autoExpireDays <= 0 || rec.LastSeenAt.IsZero() {
				out.SourceLifecycle = "STALE" // 无截止日且无新鲜度锚点 -> 数据不足, 需修复
				return out, LifeMarkStale
			}
			e2 := rec.LastSeenAt.AddDate(0, 0, autoExpireDays)
			exp = &e2
			out.ExpireDate = exp
		}
		if now.After(*exp) {
			out.SourceLifecycle = "EXPIRED"
			return out, LifeExpire
		}
		return out, LifeKeep
	case StatusStale:
		out.SourceLifecycle = "STALE" // 保留记录, 禁止当作当前有效信号
		return out, LifeMarkStale
	default:
		out.SourceLifecycle = "STALE"
		return out, LifeMarkStale
	}
	// 仅 PARTIAL && persistedHit 会到达这里（break 出 switch 后按已验证命中续期）。
	out.LastSeenAt = now
	if autoExpireDays <= 0 {
		out.SourceLifecycle = "STALE"
		return out, LifeMarkStale
	}
	exp := now.AddDate(0, 0, autoExpireDays)
	out.ExpireDate = &exp
	out.SourceLifecycle = "ACTIVE"
	return out, LifeTouch
}
