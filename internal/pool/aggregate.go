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
	AsOf       string       `json:"as_of"`       // 本来源数据的业务截止日期
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

	// ---- 阶段 5: 双窗口相对强度明细（R20/R60 超额收益, 单位 %）----
	RetShortExcess *float64 `json:"ret_short_excess,omitempty"` // R20 - 基准同期
	RetLongExcess  *float64 `json:"ret_long_excess,omitempty"`  // R60 - 基准同期

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
	TradEligible = "ELIGIBLE" // 允许进入交易审批流程
	TradBlocked  = "BLOCKED"  // 禁止（黑名单优先, 其次资格/财务明确否决）
	TradUnknown  = "UNKNOWN"  // 主数据缺失 -> 默认不放行, 需人工确认
)

// ComputeTradability 按固定顺序推导交易资格：黑名单 > 证券资格 > 财务 > 强度路径。
// 注意与来源有效性无关——一只来源 ACTIVE 的股票也可能 tradability=BLOCKED。
func (e *AggEntry) ComputeTradability() {
	switch {
	case e.Blacklisted:
		e.Tradability = TradBlocked
		e.BlockedBy = "BLACKLIST"
	case e.Eligibility == EligBlocked:
		e.Tradability = TradBlocked
		e.BlockedBy = "ELIGIBILITY"
	case e.Financial == FinBlocked:
		e.Tradability = TradBlocked
		e.BlockedBy = "FINANCIAL"
	case e.TradingHalted:
		// 停牌：研究资格(eligibility)不受影响, 仅禁止新订单; 记录保留。
		e.Tradability = TradBlocked
		e.BlockedBy = "HALTED"
	case e.PriceBelowMin:
		e.Tradability = TradBlocked
		e.BlockedBy = "PRICE_BELOW_MIN"
	case e.OhlcInvalid || e.StaleQuote || e.LimitState == LimitUnknown:
		// 数据异常/行情过期/涨跌停状态无法确认 -> 禁止生成"可执行"订单（UNKNOWN 不放行）。
		e.Tradability = TradUnknown
		e.BlockedBy = "QUOTE_STATE"
	case e.Strength == StrengthNone && e.hasAutoSourceOnly():
		// RS 管线**实际执行过**（Strength 被显式赋值为 A/B/NONE）且股票类条目两条
		// 路径均未入选 -> 高优先级截断（软阻塞, 可人工放行）。
		// StrengthUnset（精筛关闭 / RS 失败退化 / ETF / MANUAL-only）不参与此判定,
		// 否则会把"未打分"吞成"打分后落选", 违反阈值语义诚实原则。
		e.Tradability = TradBlocked
		e.BlockedBy = "STRENGTH"
	case e.Liquidity == LiqFail:
		// 阶段 4：ADV20 明确不达标 -> 禁止新订单（研究记录保留, 不删除历史观察）。
		e.Tradability = TradBlocked
		e.BlockedBy = "LIQUIDITY"
	case e.CapacityStatus == CapacityOver:
		// 计划参与率超限 -> 需降低数量或分析执行成本后方可交易。
		e.Tradability = TradBlocked
		e.BlockedBy = "CAPACITY"
	case e.Liquidity == LiqDataInsufficient:
		// 样本不足无法计算 ADV -> 诚实 UNKNOWN（不放行也不剔除）。
		e.Tradability = TradUnknown
		e.BlockedBy = "LIQUIDITY_DATA"
	case e.Eligibility == EligUnknown:
		e.Tradability = TradUnknown
		e.BlockedBy = "ELIGIBILITY_DATA"
	case e.Financial == FinDataInsufficient:
		// 阶段 6: 财务快照缺失/过期 -> UNKNOWN（不放行也不剔除, 显示缺失原因）
		e.Tradability = TradUnknown
		e.BlockedBy = "FINANCIAL_DATA"
	default:
		e.Tradability = TradEligible
	}
}

func (e *AggEntry) hasAutoSourceOnly() bool {
	for _, s := range e.Sources {
		if s.SourceType == sourceManual {
			return false
		}
	}
	return true
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
// statusOf(code) 返回该股票在持久层的黑名单事实位与既有生命周期锚点（可为 nil）。
func aggregateEntries(entries []Entry, asOf string, now time.Time, statusOf func(string) *AggEntry) []AggEntry {
	type slot struct {
		agg AggEntry
		idx int
	}
	order := []string{}
	bySym := map[string]*slot{}

	for _, e := range entries {
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
				FirstSeenAt: now,
				LastSeenAt:  now,
				HitThisRun:  true,
			}
			// 持久层锚点：黑名单事实位优先保留；生命周期沿用既有 first/last seen（白名单永不过期语义由 Transition 保证）
			if statusOf != nil {
				if prev := statusOf(sym); prev != nil {
					s.agg.Blacklisted = prev.Blacklisted
					if !prev.FirstSeenAt.IsZero() {
						s.agg.FirstSeenAt = prev.FirstSeenAt
					}
					if !prev.LastSeenAt.IsZero() && prev.LastSeenAt.After(s.agg.LastSeenAt) {
						s.agg.LastSeenAt = prev.LastSeenAt
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
		if a.Name == "" && e.Name != "" {
			a.Name = e.Name
		}
		// 证据追加（同来源重复命中视为上游 bug, 幂等跳过）
		dup := false
		for _, old := range a.Sources {
			if old.SourceType == e.SourceType {
				dup = true
				break
			}
		}
		if !dup {
			a.Sources = append(a.Sources, SourceHit{
				SourceType: e.SourceType,
				Reason:     sourceReason(e),
				SectorName: e.SectorName,
				AsOf:       asOf,
				Status:     StatusOK,
			})
		}
		// 最新指标透传（指针非 nil 即覆盖, 语义与原单来源一致）
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
		if e.RankScore != 0 {
			a.RankScore = e.RankScore
		}
		if e.RankPos != 0 {
			a.RankPos = e.RankPos
		}
	}

	out := make([]AggEntry, 0, len(order))
	for _, sym := range order {
		a := &bySym[sym].agg
		sort.SliceStable(a.Sources, func(i, j int) bool {
			return srcPriority(a.Sources[i].SourceType) < srcPriority(a.Sources[j].SourceType)
		})
		if len(a.Sources) > 0 {
			a.PrimarySource = a.Sources[0].SourceType
			a.SourceType = a.PrimarySource // 兼容别名: 落库契约 source_type
			// sector_name 跟随主来源证据（多来源时保留最高优先级语境的板块名）
			a.SectorName = a.Sources[0].SectorName
		}
		// 独立状态字段初始化（refinePool 开启时被闸门结果覆盖; 关闭时保持 UNKNOWN 语义诚实）
		if a.hasAutoSourceOnly() {
			a.Eligibility = EligUnknown // 自动来源股票: 资格未经主数据 Provider 确认 -> UNKNOWN
			a.Financial = FinDataInsufficient
		} else {
			// ETF / MANUAL-only: 不适用股票资格与财务闸门（ETF/人工条目按契约放行）。
			// 证券资格应通过权威证券主数据 Provider 判定, 名称含 "ST" 仅是启发式代理;
			// 非适用维度显式记 PASS(N/A), 避免把"不适用"误标成"未知"。
			a.Eligibility = EligPass
			a.Financial = FinPass
		}
		a.Strength = StrengthUnset // refinePool 实际打分后才覆盖为 A/B/NONE
		a.Liquidity = LiqPass      // 能进入池子的股票均已通过全市场流动性初筛（初筛失败退化时为名义值）
		if a.Blacklisted {
			a.Eligibility = EligBlocked
		}
		a.ComputeTradability()
		out = append(out, *a)
	}
	return out
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
// persistedHit=本次是否命中; refreshStatus=对应来源本次刷新状态; blacklistFact=黑名单事实位。
func Transition(rec AggEntry, persistedHit bool, refreshStatus SourceStatus, autoExpireDays int, now time.Time) (AggEntry, LifeAction) {
	out := rec
	// 人工白名单与黑名单永不被自动逻辑改写（4.2: 白名单独立于自动过期; 黑名单终态）
	if rec.PrimarySource == sourceManual || rec.Blacklisted {
		return out, LifeProtect
	}
	switch refreshStatus {
	case StatusFailed, StatusPartial:
		// FAILED: 保留上次有效结果并标 STALE; PARTIAL: 只更新可明确验证记录, 这里保守全保留
		out.SourceLifecycle = "STALE"
		return out, LifeMarkStale
	case StatusOK, StatusSuccessEmpty:
		if persistedHit {
			out.LastSeenAt = now
			exp := now.AddDate(0, 0, autoExpireDays)
			out.ExpireDate = &exp
			out.SourceLifecycle = "ACTIVE"
			return out, LifeTouch
		}
		// 不再命中: 仅当已超过自然日截止才过期; 节假日/短窗口内的旧记录原样保留
		if rec.ExpireDate != nil && now.After(*rec.ExpireDate) {
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
}
