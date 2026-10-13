// generator.go —— Go 原生动态候选池生成器（Layer 0.5）。
//
// 与原 Python watchlist_generator.py 相同的四来源语义，但取数全部改走
// stock-go SDK（见 provider.go），输出结构与 tool.WatchlistGenData 完全对齐，
// 因此 Go 端生命周期落库逻辑 ApplyWatchlistPool 无需任何改动。
//
// 迁移后新增能力：全市场流动性初筛（原 Layer 0 base_filter 的 Go 化）——
//
//	非 ST、收盘价 >= MinPrice、近 20 日均成交额(代理: 当日成交额) > MinAvgAmount、
//	总市值 >= MinMarketCap；初筛结果作为 SECTOR/FUND 两个资金流来源的前置门槛。
//
// 弱依赖原则保持不变：任一来源失败仅记入 warnings，不影响其余来源；
// 初筛失败则退化为"不做流动性门槛"，绝不因数据问题清空池子。
package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"

	"ashare-quant/internal/tool"
	"crypto/sha256"
	"encoding/hex"
)

// Config 生成器可调参数（默认值对齐原 Python 实现）。
type Config struct {
	SectorTopN     int     // 来源1: 资金净流入 Top N 行业板块（默认 3）
	SectorLeadersK int     // 来源1: 每板块市值/价格最大的 K 只成分股（默认 3）
	ETFTopM        int     // 来源2: 动量最强 Top M 只 ETF（默认 8）
	ETFLookback    int     // 来源2: 精算拉取的交易日数（默认 60）
	ETFMomWindow   int     // 来源2: 涨幅计算窗口（默认 20）
	ETFPreFilterN  int     // 来源2: 快照粗筛进入精算的最大 ETF 数（默认 60）
	LHBMinNetBuy   float64 // 来源3: 机构净买入下限（元, 默认 5e7）
	LHBLookbackD   int     // 来源3: 龙虎榜回看天数（默认 7）

	// ---- 全市场流动性初筛（Go 层新增闸门）----
	MinPrice      float64 // 最低收盘价（元, 默认 1.0; 面值退市风险）
	MinAvgAmount  float64 // 最低日均成交额（元, 默认 5e7 = 5 千万, 对齐契约 StockMinADV20; 旧 Python 口径 1e8 偏严）
	MinMarketCap  float64 // 最低总市值（元, 默认 5e8 = 5 亿; 0 表示缺失时放行）
	ExcludeST     bool    // 剔除 ST/*ST（默认 true）
	LiquidityTopN int     // 通过初筛的股票按成交额保留前 N 只做后续叠加（默认 400, 控制规模）

	// ---- 精筛管线：证券资格 / 财务状态 / 相对强度排序（Layer 0.75, Go 层新增）----
	QualifyEnabled     bool    // 总开关（默认 true）; false 时行为与迁移版完全一致
	MinListedDays      int     // 证券资格: 上市满 N 自然日方可入池（默认 120; 次新波动大且无足够 RS 样本）
	RequirePositiveEPS bool    // 财务状态: 最近一期基本 EPS < 0 剔除（默认 true; 数据缺失放行）
	MinROE             float64 // 财务状态: 加权 ROE 下限 %（默认 0 = 仅剔除明确负 ROE; -1e9 可关闭）
	RSWindow           int     // 相对强度主窗口 R60: 回看交易日数（默认 60; 兼容旧配置键）
	RSShortWindow      int     // 阶段5: 短期动量窗口 R20（默认 20）
	RSLongWindow       int     // 阶段5: 中期动量窗口 R60（默认 60; 为 0 时取 RSWindow）
	RSShortWeightPct   float64 // 阶段5: R20 超额收益在总分中的权重百分比（默认 60）
	RSBenchmark        string  // 相对强度基准指数代码（默认 "000300" 沪深300）
	RSEnabled          bool    // 是否执行 RS 打分与排序（默认 true; 失败自动退化保持原顺序）
	RSTopK             int     // RS 排序后仅保留前 K 名股票类条目（默认 0 = 全部保留但仍排序）
	PathATopN          int     // 阶段7: 路径A(趋势强势股)独立TopN上限（默认 0 = 不截断）
	PathBTopN          int     // 阶段7: 路径B(趋势修复候选)独立TopN上限（默认 0 = 不截断; B 整体排在 A 之后）
	RSConcurrency      int     // RS/资格计算并发度（默认 6）

	// ---- 阶段 4：流动性过滤与交易容量（liquidity.go）----
	LiquidityEnabled    bool    // 总开关（默认 true）; false 时跳过 ADV20 闸门, 行为与阶段 3 版一致
	MinADV20Stock       float64 // 普通 A 股最低 20 日均成交额（元; <=0 时用契约值 5e7）
	MinADV20ETF         float64 // ETF 最低 20 日均成交额（元; <=0 时用契约值 2e7; 不套用股票规则）
	PlanNotional        float64 // 计划下单金额（元; 默认 0 = 候选池阶段未知 -> capacity=UNCHECKED）
	MaxParticipationPct float64 // 参与率上限 %（默认 1.0; 真正下单前必须按最终数量重算）
}

// DefaultConfig 返回与 Python 版对齐的默认参数。
func DefaultConfig() Config {
	return Config{
		SectorTopN:     3,
		SectorLeadersK: 3,
		ETFTopM:        8,
		ETFLookback:    60,
		ETFMomWindow:   20,
		ETFPreFilterN:  60,
		LHBMinNetBuy:   5e7,
		LHBLookbackD:   7,
		MinPrice:       1.0,
		MinAvgAmount:   ContractStockMinADV20, // 5e7 = 5 千万（契约口径）
		MinMarketCap:   5e8,                   // 5 亿
		ExcludeST:      true,
		LiquidityTopN:  400,

		QualifyEnabled:     true,
		MinListedDays:      120,
		RequirePositiveEPS: true,
		MinROE:             0,
		RSWindow:           60,
		RSShortWindow:      20,
		RSLongWindow:       60,
		RSShortWeightPct:   60,
		RSBenchmark:        "000300",
		RSEnabled:          true,
		RSTopK:             0,
		PathATopN:          0,
		PathBTopN:          0,
		RSConcurrency:      6,

		LiquidityEnabled:    true,
		MinADV20Stock:       ContractStockMinADV20,
		MinADV20ETF:         ContractETFMinADV20,
		PlanNotional:        0, // 候选池阶段不知道最终仓位 -> UNCHECKED
		MaxParticipationPct: 1.0,
	}
}

// Entry 池中的一条记录（JSON 字段与 tool.WatchEntry / Python make_entry 严格对齐）。
type Entry struct {
	Symbol         string   `json:"symbol"`
	Name           string   `json:"name"`
	SourceType     string   `json:"source_type"`
	SectorName     string   `json:"sector_name"`
	GeneratedAt    string   `json:"generated_at"`
	Momentum20DPct *float64 `json:"momentum_20d_pct,omitempty"`
	OrgNetBuy      *float64 `json:"org_net_buy,omitempty"`
	AvgAmount20D   *float64 `json:"avg_amount_20d,omitempty"`

	// ---- Go 精筛管线新增诊断字段（WatchlistGenData 无对应列, round-trip 自然丢弃）----
	RankScore float64 `json:"rank_score,omitempty"` // RS 相对强度得分（RSWindow 日超额收益 %）
	RankPos   int     `json:"rank_pos,omitempty"`   // 排序后名次（1 起）; 0 表示未参与 RS 排序

	// ---- 阶段 2 契约状态（聚合层回填 AggEntry 用; 同样被 round-trip 丢弃）----
	EligibilityStatus EligibilityStatus `json:"eligibility_status,omitempty"`
	FinancialStatusV  FinancialStatus   `json:"financial_status,omitempty"`
	StrengthPathV     StrengthPath      `json:"strength_path,omitempty"`

	// HitStatus 本条命中证据的来源刷新状态（阶段 8: source_run_status / SourceHit.Status）。
	// 零值在聚合层按 StatusOK 处理（兼容 Python 链路无此字段的旧载荷）。
	HitStatus SourceStatus `json:"hit_status,omitempty"`

	// ---- 阶段 8: MA20/MA60 趋势快照（refine.go 在同一条收盘序列上计算后透传给
	// 聚合层, 供 daily_stock_metrics 落库; 样本不足时保持 nil, 绝不伪造）----
	Close *float64 `json:"close,omitempty"` // 截止 as_of 的最新有效收盘价（元）
	MA20  *float64 `json:"ma20,omitempty"`  // 20 日均价（收盘价口径, 单位元）
	MA60  *float64 `json:"ma60,omitempty"`  // 60 日均价（收盘价口径, 单位元）
}

// Result 一次生成的完整输出（tool.WatchlistGenData 的同构体 + Go 层新增诊断字段）。
type Result struct {
	TradeDate string         `json:"trade_date"`
	Total     int            `json:"total"`
	BySource  map[string]int `json:"by_source"`
	Pool      []AggEntry     `json:"pool"`
	Warnings  []string       `json:"warnings"`

	// ---- 阶段 8: 落库审计字段（screening_runs / source_run_status 数据源）----
	// RunID 由调用方注入（cmd cron 用时间戳生成）; 为空时持久化层自行生成。
	RunID string `json:"run_id,omitempty"`
	// AsOf 本次计算的业务截止日期（YYYY-MM-DD, 与 TradeDate 同值但语义独立:
	// TradeDate 面向展示, AsOf 是 daily_stock_metrics UNIQUE(symbol, as_of, version) 的键）。
	AsOf string `json:"as_of,omitempty"`
	// SourceStatuses 四个自动来源 + 全市场初筛的本轮执行状态。FAILED 只更新该
	// 来源的错误状态并保留最后一次有效结果——绝不把请求失败解释成"今天没有命中"。
	SourceStatuses map[string]SourceStatus `json:"source_statuses,omitempty"`
	// SourceErrors 记录 FAILED/PARTIAL 来源的具体错误原因（source_run_status.error_reason）。
	SourceErrors map[string]string `json:"source_errors,omitempty"`
	// ConfigHash 参数配置指纹（sha256 前 12 位）, 每次成功运行随 screening_runs 保存,
	// 供日后比较不同规则版本对候选池的影响。
	ConfigHash string `json:"config_hash,omitempty"`
	// StrategyVersion 策略版本号（daily_stock_metrics/screening_runs 的口径标识）。
	StrategyVersion string `json:"strategy_version,omitempty"`

	// LiquidityPassed 通过全市场流动性初筛的股票数（诊断字段, Go 层迁移新增;
	// 不在 WatchlistGenData 中, ToGenData round-trip 时自然丢弃）。
	LiquidityPassed int `json:"liquidity_passed"`
}

const (
	sourceManual = "MANUAL"
	sourceSector = "SECTOR_LEADER"
	sourceETF    = "ETF_MOMENTUM"
	sourceFund   = "FUND_FLOW"
)

var (
	stRe      = regexp.MustCompile(`(?i)\*?ST`)
	acodeRe   = regexp.MustCompile(`^\d{6}$`)
	etfCodeRe = regexp.MustCompile(`^(sh)?(51|56|58|15|16)\d{4}$|^(\d{6})$`)
)

// isETFCode 场内 ETF 代码启发式：沪 51/56/58 开头、深 15/16 开头的 6 位代码。
func isETFCode(code string) bool {
	c := strings.TrimPrefix(code, "sh")
	c = strings.TrimPrefix(c, "sz")
	if !acodeRe.MatchString(c) {
		return false
	}
	switch c[:2] {
	case "51", "56", "58", "15", "16":
		return true
	}
	return false
}

// Generator Go 原生动态池生成器。零值不可用，请用 NewGenerator。
type Generator struct {
	prov  Provider
	cfg   Config
	runID string
	// PersistentStatus 可选钩子：从持久层（watchlist_items）回填每条记录的
	// 黑名单事实位与 first/last_seen 生命周期锚点, 供聚合阶段消费。
	// nil-safe: 单测可不接 DB。由 cmd/main.go 接线。
	PersistentStatus func(ctx context.Context) map[string]AggEntry
	// finProv 阶段 6: 独立财务数据供应商（stock-go 无财报 API）。nil 时精筛闸门
	// 诚实输出 FinDataInsufficient —— 绝不把"财务未知"默认成"财务已通过"。
	// 由 SetFinancialProvider 注入; 单测注入 fake 实现。
	finProv FinancialProvider
	// strengthWiring 阶段 7: refinePool 产出的强度管线输入包（趋势/基准/行业因子）。
	// liquidityGatePool 计算 ADV20 后由 Generate 回填 Adv 并调用 RefineScores 二次精化。
	strengthWiring *strengthWiring
}

// SetFinancialProvider 注入阶段 6 财务数据供应商（外部财报服务 / 测试 fake）。
func (g *Generator) SetFinancialProvider(p FinancialProvider) { g.finProv = p }

// NewGenerator 创建生成器。cfg 为零值时取 DefaultConfig；对显式传入但缺省的
// 精筛字段做兜底（避免调用方只覆盖部分来源参数时 RS/资格闸门静默失效）。
func NewGenerator(prov Provider, cfg Config) *Generator {
	if cfg.SectorTopN == 0 {
		cfg = DefaultConfig()
	} else {
		d := DefaultConfig()
		if cfg.MinListedDays == 0 && cfg.QualifyEnabled {
			cfg.MinListedDays = d.MinListedDays
		}
		if cfg.RSWindow == 0 && cfg.RSEnabled {
			cfg.RSWindow = d.RSWindow
		}
		if cfg.RSBenchmark == "" {
			cfg.RSBenchmark = d.RSBenchmark
		}
		if cfg.RSConcurrency <= 0 {
			cfg.RSConcurrency = d.RSConcurrency
		}
	}
	return &Generator{prov: prov, cfg: cfg}
}

// Generate 生成今日动态池。manualSymbols 为人工白名单（透传标记, 优先级最高）。
// now 注入以便测试时间边界。
// SetRunID 注入本次筛选运行 ID（阶段 8: screening_runs.run_id; cmd cron 用时间戳生成,
// 单测可不注入——持久化层会自行兜底生成）。
func (g *Generator) SetRunID(id string) { g.runID = id }

func (g *Generator) Generate(ctx context.Context, manualSymbols []string, now time.Time) (*Result, error) {
	warnings := []string{}
	raw := []Entry{}
	tradeDate := now.Format("2006-01-02")

	// ---- 来源 4: 人工白名单（无网络, 永不失败）----
	for _, s := range manualSymbols {
		code := normalizeCode(s)
		if code == "" {
			continue
		}
		raw = append(raw, g.entry(code, "", sourceManual, "人工自选"))
	}

	// ---- 全市场流动性初筛（Go 层新增, 供 SECTOR/FUND 两来源做前置门槛）----
	// 弱依赖: 初筛失败仅记入 warnings 并退化为"不设门槛", 绝不阻断池子生成。
	// 快照集合 liquidIdx 同时为 SECTOR 来源提供市值/20日均成交额字段,
	// 使行业龙头排序可用真实市值口径（对齐 TS 端 filterBasicQuotes 的
	// circulatingMarketCap 过滤 + 多条件本地筛选设计）。
	liquidSet, liquidIdx, liquidityCount, err := g.liquidityScreen(ctx)
	srcStatus := map[string]SourceStatus{"LIQUIDITY_SCREEN": StatusOK}
	srcErrs := map[string]string{}
	markSrc := func(name string, e error) {
		if e != nil {
			srcStatus[name] = StatusFailed // FAILED: 保留上次有效结果, 不伪装成 SUCCESS_EMPTY
			srcErrs[name] = e.Error()
		} else {
			srcStatus[name] = StatusOK
		}
	}
	if err != nil {
		warnings = append(warnings, "LIQUIDITY_SCREEN 失败(已跳过, 资金流来源退化为不设门槛): "+err.Error())
		log.Printf("[pool] WARN liquidity screen failed: %v", err)
		markSrc("LIQUIDITY_SCREEN", err)
	}

	// ---- 来源 2: ETF 动量 ----
	etfEntries, err := g.genETFMomentum(ctx, now)
	if err != nil {
		warnings = append(warnings, "ETF_MOMENTUM 失败(已跳过): "+err.Error())
	} else {
		raw = append(raw, etfEntries...)
	}
	markSrc("ETF_MOMENTUM", err)
	if err == nil && len(etfEntries) == 0 {
		srcStatus["ETF_MOMENTUM"] = StatusSuccessEmpty // 成功取到空结果(与 FAILED 严格区分)
	}

	// ---- 来源 1: 行业龙头（行业资金流 Top N -> 板块成分股 Top K, 流动性门槛叠加）----
	sectorEntries, err := g.genSectorLeaders(ctx, liquidSet, liquidIdx)
	if err != nil {
		warnings = append(warnings, "SECTOR_LEADER 失败(已跳过): "+err.Error())
	} else {
		raw = append(raw, sectorEntries...)
	}
	markSrc("SECTOR_LEADER", err)
	if err == nil && len(sectorEntries) == 0 {
		srcStatus["SECTOR_LEADER"] = StatusSuccessEmpty
	}

	// ---- 来源 3: 龙虎榜机构净买入（叠加流动性门槛）----
	fundEntries, err := g.genDragonTiger(ctx, now, liquidSet)
	if err != nil {
		warnings = append(warnings, "FUND_FLOW 失败(已跳过): "+err.Error())
	} else {
		raw = append(raw, fundEntries...)
	}
	markSrc("FUND_FLOW", err)
	if err == nil && len(fundEntries) == 0 {
		srcStatus["FUND_FLOW"] = StatusSuccessEmpty
	}
	markSrc("MANUAL", nil) // 人工来源无外部取数; 已保存的人工记录不因自动刷新清除(聚合层保证)

	// ---- 持久层状态注入（黑名单事实位 + 既有生命周期锚点; nil-safe, 单测可不接 DB）----
	var statusOf func(string) *AggEntry
	if g.PersistentStatus != nil {
		ps := g.PersistentStatus(ctx)
		statusOf = func(code string) *AggEntry {
			v, ok := ps[code]
			if !ok {
				return nil
			}
			return &v
		}
	}
	// 阶段 8: 给每条命中证据打上其来源本轮刷新状态（FAILED 不伪装成正常空结果）;
	// MANUAL 恒为 OK（人工记录不因自动刷新而失效, 由聚合层保护逻辑保证）。
	for i := range raw {
		if st, ok := srcStatus[raw[i].SourceType]; ok && raw[i].HitStatus == "" {
			raw[i].HitStatus = st
		} else if raw[i].SourceType == sourceManual {
			raw[i].HitStatus = StatusOK
		}
	}
	pool := aggregateEntries(raw, tradeDate, now, statusOf)

	// ---- Layer 0.75 精筛管线：证券资格 + 财务状态闸门 -> RS 相对强度排序（Go 层新增）----
	if g.cfg.QualifyEnabled {
		pool = g.refinePool(ctx, pool, &warnings, now)
	}
	// 阶段 4：流动性闸门（ADV20）与交易容量标注，在精筛之后、落库之前。
	pool = g.liquidityGatePool(ctx, pool, &warnings, now)
	// 阶段 7: ADV20 已由流动性闸门写入条目快照(AvgAmount20D), 用它对四因子评分做
	// 最终精化(补 P(ADV20) 权重)并按 A→B→其余重排。弱依赖：无强度包时原样返回。
	pool = g.RefineScores(pool, &warnings)

	bySource := map[string]int{}
	for _, e := range pool {
		bySource[e.PrimarySource]++
	}

	return &Result{
		TradeDate:       tradeDate,
		Total:           len(pool),
		BySource:        bySource,
		Pool:            pool,
		Warnings:        warnings,
		LiquidityPassed: liquidityCount,
		AsOf:            tradeDate,
		RunID:           g.runID,
		SourceStatuses:  srcStatus,
		SourceErrors:    srcErrs,
		ConfigHash:      g.cfg.Hash(),
		StrategyVersion: g.cfg.StrategyVersion(),
	}, nil
}

// ---------------------------------------------------------------------------
// 全市场流动性初筛
// ---------------------------------------------------------------------------

// liquidityScreen 拉全市场快照并按 Config 阈值过滤, 返回通过集合(code->true)、
// 快照索引(code->StockQuote, 供来源侧做市值/流动性排序与补字段)、通过数量与错误。
// 语义约定: err != nil 时 set/index 为 nil, 调用方据此"不设门槛"退化;
// err == nil 且 set 非空时门槛生效。
func (g *Generator) liquidityScreen(ctx context.Context) (map[string]bool, map[string]StockQuote, int, error) {
	quotes, err := g.prov.AllStocks(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	passed := make([]StockQuote, 0, len(quotes)/4)
	for _, q := range quotes {
		if !acodeRe.MatchString(q.Code) {
			continue
		}
		if g.cfg.ExcludeST && stRe.MatchString(q.Name) {
			continue
		}
		if q.Price < g.cfg.MinPrice {
			continue
		}
		if q.AvgAmount20D <= g.cfg.MinAvgAmount {
			continue
		}
		if g.cfg.MinMarketCap > 0 && q.TotalMarketCap > 0 && q.TotalMarketCap < g.cfg.MinMarketCap {
			continue
		}
		passed = append(passed, q)
	}
	// 按成交额降序截断, 防止上游异常导致集合爆炸
	sort.Slice(passed, func(i, j int) bool { return passed[i].AvgAmount20D > passed[j].AvgAmount20D })
	if g.cfg.LiquidityTopN > 0 && len(passed) > g.cfg.LiquidityTopN {
		passed = passed[:g.cfg.LiquidityTopN]
	}
	set := make(map[string]bool, len(passed))
	idx := make(map[string]StockQuote, len(passed))
	for _, q := range passed {
		set[q.Code] = true
		idx[q.Code] = q
	}
	if len(set) == 0 {
		// 上游快照异常导致全军覆没: 视为初筛失败, 调用方退化为不设门槛,
		// 绝不因数据问题把资金流来源整体清空。
		return nil, nil, 0, context.DeadlineExceeded // 语义: 初筛通过数为 0
	}
	return set, idx, len(set), nil
}

// ---------------------------------------------------------------------------
// 来源 1: 行业龙头
// ---------------------------------------------------------------------------

func (g *Generator) genSectorLeaders(ctx context.Context, liquidSet map[string]bool, liquidIdx map[string]StockQuote) ([]Entry, error) {
	rank, err := g.prov.SectorFundFlowRank(ctx)
	if err != nil {
		return nil, err
	}
	if len(rank) == 0 {
		return nil, context.DeadlineExceeded // 语义: 上游返回空
	}
	// 按主力净流入降序取 Top N（防御性排序, 不信任上游顺序）
	sorted := make([]SectorFlow, len(rank))
	copy(sorted, rank)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].MainNetInflow > sorted[j].MainNetInflow })
	top := sorted
	if len(top) > g.cfg.SectorTopN {
		top = top[:g.cfg.SectorTopN]
	}

	var out []Entry
	for _, sec := range top {
		boardKey := sec.Code
		if boardKey == "" {
			boardKey = sec.Name // IndustryConstituents 支持名称查询
		}
		cons, err := g.prov.IndustryConstituents(ctx, boardKey)
		if err != nil || len(cons) == 0 {
			continue // 单板块失败跳过, 不影响其他板块
		}
		// 流动性门槛叠加（set==nil 表示初筛失败, 不设门槛）
		filtered := cons[:0:0]
		for _, c := range cons {
			if liquidSet != nil && !liquidSet[c.Code] {
				continue
			}
			filtered = append(filtered, c)
		}
		if len(filtered) == 0 {
			continue // 该板块成分股全部未过门槛, 跳过
		}
		// 龙头口径（对齐 TS 端"多因子本地筛选 + 排序"设计, 分三级键降级）:
		//   1) 总市值降序 —— 初筛快照带市值时优先, 真实龙头代理;
		//   2) 最新价降序 —— 无市值时的可得上游字段;
		//   3) 代码升序 —— 全缺失时保证确定性。
		// stock-go BoardConstituent 本身无市值字段, 市值经 liquidIdx 关联补齐。
		val := func(c ConstituentStock) (cap, price float64) {
			if q, ok := liquidIdx[c.Code]; ok {
				return q.TotalMarketCap, q.Price
			}
			return 0, c.Price
		}
		sort.SliceStable(filtered, func(i, j int) bool {
			ci, pi := val(filtered[i])
			cj, pj := val(filtered[j])
			if ci != cj {
				return ci > cj
			}
			if pi != pj {
				return pi > pj
			}
			return filtered[i].Code < filtered[j].Code
		})
		if len(filtered) > g.cfg.SectorLeadersK {
			filtered = filtered[:g.cfg.SectorLeadersK]
		}
		for _, c := range filtered {
			out = append(out, g.entry(c.Code, c.Name, sourceSector, sec.Name))
		}
	}
	if len(out) == 0 {
		return nil, context.Canceled // 所有板块均失败
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 来源 2: ETF 动量
// ---------------------------------------------------------------------------

func (g *Generator) genETFMomentum(ctx context.Context, now time.Time) ([]Entry, error) {
	codes, err := g.prov.FundList(ctx)
	if err != nil {
		return nil, err
	}
	etfCodes := make([]string, 0, len(codes)/2)
	for _, c := range codes {
		code := normalizeCode(c)
		if code != "" && isETFCode(code) {
			etfCodes = append(etfCodes, code)
		}
	}
	if len(etfCodes) == 0 {
		return nil, context.DeadlineExceeded
	}

	snaps, err := g.prov.FundBatchQuote(ctx, etfCodes)
	if err != nil {
		return nil, err
	}
	// 剔除货币/债券/理财类（名称启发式, 与 Python 版一致）
	badKW := []string{"货币", "现金", "债", "理财"}
	candidates := snaps[:0:0]
	for _, s := range snaps {
		bad := false
		for _, kw := range badKW {
			if strings.Contains(s.Name, kw) {
				bad = true
				break
			}
		}
		if !bad {
			candidates = append(candidates, s)
		}
	}
	// 有成交额数据时先粗筛 Top N；无成交额(源缺列)则按代码序截断控制精算规模
	if len(candidates) > 0 && candidates[0].Amount > 0 {
		sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Amount > candidates[j].Amount })
	}
	if len(candidates) == 0 {
		return nil, context.DeadlineExceeded // 全部为货币/债券类, 无候选
	}
	if len(candidates) > g.cfg.ETFPreFilterN {
		candidates = candidates[:g.cfg.ETFPreFilterN]
	}

	type scored struct {
		e   Entry
		mom float64
	}
	var ranked []scored
	for _, s := range candidates {
		dc, err := g.prov.ETFCloseSeries(ctx, s.Code, g.cfg.ETFLookback, now)
		if err != nil || dc == nil {
			continue // 单只 ETF 失败跳过
		}
		mom := momentumPct(dc.Closes, g.cfg.ETFMomWindow)
		if mom == nil {
			continue
		}
		e := g.entry(s.Code, s.Name, sourceETF, s.Name)
		e.Momentum20DPct = mom
		ranked = append(ranked, scored{e: e, mom: *mom})
	}
	if len(ranked) == 0 {
		return nil, context.DeadlineExceeded
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].mom > ranked[j].mom })
	out := make([]Entry, 0, g.cfg.ETFTopM)
	for i, r := range ranked {
		if i >= g.cfg.ETFTopM {
			break
		}
		out = append(out, r.e)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 来源 3: 龙虎榜机构净买入
// ---------------------------------------------------------------------------

func (g *Generator) genDragonTiger(ctx context.Context, now time.Time, liquidSet map[string]bool) ([]Entry, error) {
	start := now.AddDate(0, 0, -g.cfg.LHBLookbackD)
	rows, err := g.prov.DragonTigerInstitution(ctx, start, now)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, r := range rows {
		code := normalizeCode(r.Code)
		if code == "" || g.cfg.ExcludeST && stRe.MatchString(r.Name) {
			continue
		}
		if r.OrgNetAmount < g.cfg.LHBMinNetBuy {
			continue
		}
		if liquidSet != nil && !liquidSet[code] {
			continue // 流动性初筛叠加
		}
		e := g.entry(code, r.Name, sourceFund, "龙虎榜机构")
		v := r.OrgNetAmount
		e.OrgNetBuy = &v
		out = append(out, e)
	}
	return out, nil // 空结果合法（当日无满足阈值的机构买入）
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (g *Generator) entry(code, name, source, sector string) Entry {
	return Entry{
		Symbol:      code,
		Name:        name,
		SourceType:  source,
		SectorName:  sector,
		GeneratedAt: time.Now().Format("2006-01-02 15:04:05"),
	}
}

// ToGenData 将生成结果转换为 tool.WatchlistGenData 同构载荷（JSON round-trip），
// 供 cmd 层在 Go 原生模式下直接复用既有 tool.ApplyWatchlistPool 落库逻辑，
// 无需改动生命周期规则与 DB schema。
func (r *Result) ToGenData() (*tool.WatchlistGenData, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal pool result: %w", err)
	}
	var data tool.WatchlistGenData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("unmarshal to WatchlistGenData: %w", err)
	}
	return &data, nil
}

// Hash 返回当前参数配置的指纹（sha256 前 12 位十六进制）。
// 通过 JSON 序列化实现——新增字段自动纳入指纹, 无需手工维护字段清单;
// screening_runs.config_hash 用它比较不同规则版本对候选池的影响。
func (c Config) Hash() string {
	raw, err := json.Marshal(c)
	if err != nil {
		return "ERR"
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:12]
}

func normalizeCode(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "sh")
	s = strings.TrimPrefix(s, "sz")
	s = strings.TrimPrefix(s, "bj")
	if acodeRe.MatchString(s) {
		return s
	}
	return ""
}

// momentumPct 最近 window 个交易日涨幅百分比; 样本不足返回 nil。
func momentumPct(closes []float64, window int) *float64 {
	valid := make([]float64, 0, len(closes))
	for _, c := range closes {
		if c > 0 {
			valid = append(valid, c)
		}
	}
	if len(valid) <= window {
		return nil
	}
	base, last := valid[len(valid)-1-window], valid[len(valid)-1]
	if base <= 0 {
		return nil
	}
	v := (last/base - 1.0) * 100.0
	return &v
}

func warningsPtr(w *[]string) *[]string { return w }
