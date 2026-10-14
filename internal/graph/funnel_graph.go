// 本文件实现漏斗模型的 Eino Graph（Layer1~Layer4 全链路）：
//
//	START -> FetchPool -> FetchNews -> ManagerDecision -> SaveDecision
//	      -> RiskBudget -> PlaceOrders -> END
//
// 节点职责：
//   - FetchPool       : 非 LLM。调用 ScreenPoolTool（POST /api/czsc/screen_pool）
//     对自选股批量跑 czsc 粗筛，产出候选股票池；
//   - FetchNews       : 非 LLM。遍历候选池，并发（限流）调用 NewsSearchTool
//     拉取新闻/公告，拼接成 LLM 上下文；
//   - ManagerDecision : LLM 节点。基金经理排雷 Prompt（agent.ManagerSystemPrompt），
//     输出决策 JSON 数组（含脏输出清洗与错误回灌重试）。LLM 只输出
//     action/confidence，禁止输出 quantity（决策 #2，数量由 Layer3 硬编码）；
//   - SaveDecision    : 非 LLM。决策逐条写入 AIDecisionLog 表留痕，并回填
//     Step 1 止损契约字段（pullback_low/stop_loss/invalidation/effective_stop）；
//   - RiskBudget      : 非 LLM（Layer3）。读取大盘环境闸门（GET /api/market/environment，
//     决策 #3 硬校验），用 risk.CalculatePosition 风险预算公式计算股数；
//   - PlaceOrders     : 非 LLM（Layer4）。把通过风控的决策写成 orders 表 PENDING
//     单，修复「漏斗图断链」——MockBroker 次日 09:25 撮合的就是这些订单。
//
// 状态传递沿用「信封模式」（map[string]any）。
package graph

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

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"gorm.io/gorm"

	"ashare-quant/internal/agent"
	dbmodel "ashare-quant/internal/model"
	"ashare-quant/internal/risk"
	qtool "ashare-quant/internal/tool"
)

// ---------------------------------------------------------------------------
// 节点键名与 state 信封字段约定
// ---------------------------------------------------------------------------

const (
	NodeFetchPool       = "FetchPool"
	NodeFetchNews       = "FetchNews"
	NodeManagerDecision = "ManagerDecision"
	NodeSaveDecision    = "SaveDecision"
	NodeRiskBudget      = "RiskBudget"  // Layer3: 风险预算仓位 + 大盘闸门硬校验
	NodePlaceOrders     = "PlaceOrders" // Layer4: PENDING 订单落库（修复漏斗断链）
)

const (
	keyTradeDate   = "trade_date"          // string: 复盘交易日 YYYY-MM-DD
	keyPool        = "candidate_pool"      // []qtool.ScreenPoolHit
	keyPoolJSON    = "candidate_pool_json" // string: 候选池序列化 JSON（喂 LLM）
	keyNewsCtx     = "news_context"        // map[string]string: symbol -> 新闻 JSON
	keyNewsCtxJSON = "news_context_json"   // string: 新闻上下文序列化 JSON（喂 LLM）
	keyDecisions   = "ai_decisions"        // []agent.DecisionItem: LLM 最终决策
	keyDecisionIDs = "ai_decision_log_ids" // []uint64: 落库主键（审计用）

	// --- Layer3/4 新增信封字段（统一执行计划 Step 5）---
	keyRiskPlans   = "risk_plans"              // []enrichedDecision: 风控回填止损+数量后的决策
	keyGlobalPct   = "global_max_position_pct" // float64: 大盘总仓位闸门（0=获取失败，保守处理）
	keyRegime      = "market_regime"           // string: BULL/SIDEWAYS/BEAR（unknown=降级）
	keyOrderReport = "order_report"            // *ExecutionReport: PlaceOrders 结果汇总
)

// enrichedDecision 是 SaveDecision 之后在 state 中流转的「决策 + 风控字段」记录。
// StopXxx 四字段来自候选池 czsc 信号（Step 1 契约），Quantity 由 Layer3 硬编码计算
// （决策 #2：LLM 无权输出数量）。
type enrichedDecision struct {
	Symbol          string
	Action          string // BUY / SELL / DROP（dbmodel.Action* 常量）
	Reason          string
	ConfidenceScore int
	PullbackLow     float64
	StopLoss        float64
	Invalidation    float64
	EffectiveStop   float64
	EntryPrice      float64 // 最新收盘价（czsc signals[].price 尾部值）
	Quantity        int64   // Layer3 风险预算股数；0=未通过风控（拒单/降级观察）
	RejectNote      string  // Layer3 拒绝原因（留痕）
}

// StopOf 返回建仓时应写入持仓的实际风控线（决策 #1：max(stop_loss, invalidation)，
// Step 1 已保证 EffectiveStop 为该值的引擎口径 + Go 侧兜底）。0 表示无有效止损线。
func (d enrichedDecision) StopOf() float64 { return d.EffectiveStop }

// stopForIntent 订单意图的风控线透传规则（Step 6）：仅 BUY 单携带 StopPrice，
// SELL 清仓单不设置（其成交会把持仓清零并复位止损字段）。
func stopForIntent(d enrichedDecision, direction string) float64 {
	if direction != dbmodel.DirectionBuy {
		return 0
	}
	return d.StopOf()
}

// ---------------------------------------------------------------------------
// 对外类型
// ---------------------------------------------------------------------------

// FunnelInput 是漏斗 Graph 的入口参数（由调度器/main 构造）。
type FunnelInput struct {
	TradeDate string   // 复盘交易日 YYYY-MM-DD
	Symbols   []string // 预设自选股列表（Layer1 粗筛的输入全集）
}

// FunnelConfig 构建漏斗 Graph 所需的全部依赖。
type FunnelConfig struct {
	ChatModel       model.BaseChatModel // LLM（eino-ext openai 兼容），必填
	DB              *gorm.DB            // 已 AutoMigrate 的数据库句柄，必填
	GatewayURL      string              // Python 数据网关地址，必填
	MaxLLMRetry     int                 // 决策 JSON 解析失败时的重试次数，默认 1
	NewsConcurrency int                 // 新闻抓取并发上限，默认 5（防网关/数据源压力）
}

// BuildFunnelGraph 按 START -> FetchPool -> FetchNews -> ManagerDecision -> SaveDecision -> END
// 拓扑构建并编译漏斗复盘 Graph，返回可直接 Invoke 的 Runnable。
func BuildFunnelGraph(ctx context.Context, cfg FunnelConfig) (compose.Runnable[FunnelInput, map[string]any], error) {
	if cfg.ChatModel == nil {
		return nil, errors.New("funnel_graph: ChatModel is required")
	}
	if cfg.DB == nil {
		return nil, errors.New("funnel_graph: DB is required")
	}
	if strings.TrimSpace(cfg.GatewayURL) == "" {
		return nil, errors.New("funnel_graph: GatewayURL is required")
	}
	if cfg.MaxLLMRetry <= 0 {
		cfg.MaxLLMRetry = 1
	}
	if cfg.NewsConcurrency <= 0 {
		cfg.NewsConcurrency = 5
	}

	screenTool, err := qtool.NewScreenPoolTool(qtool.ScreenPoolConfig{GatewayBaseURL: cfg.GatewayURL})
	if err != nil {
		return nil, fmt.Errorf("funnel_graph: build screen pool tool: %w", err)
	}
	newsTool, err := qtool.NewNewsSearchTool(qtool.NewsSearchConfig{GatewayBaseURL: cfg.GatewayURL})
	if err != nil {
		return nil, fmt.Errorf("funnel_graph: build news tool: %w", err)
	}
	envTool, err := qtool.NewMarketEnvironmentTool(qtool.MarketEnvironmentConfig{GatewayBaseURL: cfg.GatewayURL})
	if err != nil {
		return nil, fmt.Errorf("funnel_graph: build market environment tool: %w", err)
	}

	g := compose.NewGraph[FunnelInput, map[string]any]()

	// ---- Node 1: FetchPool（非 LLM）----
	if err := g.AddLambdaNode(NodeFetchPool, compose.InvokableLambda(
		func(ctx context.Context, in FunnelInput) (map[string]any, error) {
			return fetchPool(ctx, screenTool, cfg.DB, in)
		})); err != nil {
		return nil, fmt.Errorf("funnel_graph: add FetchPool: %w", err)
	}

	// ---- Node 2: FetchNews（非 LLM，并发抓取）----
	if err := g.AddLambdaNode(NodeFetchNews, compose.InvokableLambda(
		func(ctx context.Context, state map[string]any) (map[string]any, error) {
			return fetchNews(ctx, state, newsTool, cfg.NewsConcurrency)
		})); err != nil {
		return nil, fmt.Errorf("funnel_graph: add FetchNews: %w", err)
	}

	// ---- Node 3: ManagerDecision（LLM 排雷精选）----
	if err := g.AddLambdaNode(NodeManagerDecision, compose.InvokableLambda(
		func(ctx context.Context, state map[string]any) (map[string]any, error) {
			return managerDecision(ctx, state, cfg.ChatModel, cfg.MaxLLMRetry)
		})); err != nil {
		return nil, fmt.Errorf("funnel_graph: add ManagerDecision: %w", err)
	}

	// ---- Node 4: SaveDecision（非 LLM，落库留痕 + 回填止损契约字段）----
	if err := g.AddLambdaNode(NodeSaveDecision, compose.InvokableLambda(
		func(_ context.Context, state map[string]any) (map[string]any, error) {
			return saveDecisionWith(state, gormDecisionSaver{cfg.DB})
		})); err != nil {
		return nil, fmt.Errorf("funnel_graph: add SaveDecision: %w", err)
	}

	// ---- Node 5: RiskBudget（Layer3：大盘闸门 + 风险预算仓位，非 LLM）----
	if err := g.AddLambdaNode(NodeRiskBudget, compose.InvokableLambda(
		func(ctx context.Context, state map[string]any) (map[string]any, error) {
			return riskBudgetWith(ctx, state, envTool, gormAccountReader{cfg.DB}, risk.DefaultConfig())
		})); err != nil {
		return nil, fmt.Errorf("funnel_graph: add RiskBudget: %w", err)
	}

	// ---- Node 6: PlaceOrders（Layer4：PENDING 订单落库，修复漏斗断链）----
	if err := g.AddLambdaNode(NodePlaceOrders, compose.InvokableLambda(
		func(_ context.Context, state map[string]any) (map[string]any, error) {
			return placeOrdersWith(state, gormOrderSaver{cfg.DB}, newOrderSeq())
		})); err != nil {
		return nil, fmt.Errorf("funnel_graph: add PlaceOrders: %w", err)
	}

	// ---- Edges ----
	for _, e := range [][2]string{
		{compose.START, NodeFetchPool},
		{NodeFetchPool, NodeFetchNews},
		{NodeFetchNews, NodeManagerDecision},
		{NodeManagerDecision, NodeSaveDecision},
		{NodeSaveDecision, NodeRiskBudget},
		{NodeRiskBudget, NodePlaceOrders},
		{NodePlaceOrders, compose.END},
	} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			return nil, fmt.Errorf("funnel_graph: add edge %s->%s: %w", e[0], e[1], err)
		}
	}

	runnable, err := g.Compile(ctx, compose.WithGraphName("ashare_funnel_review"))
	if err != nil {
		return nil, fmt.Errorf("funnel_graph: compile: %w", err)
	}
	return runnable, nil
}

// ---------------------------------------------------------------------------
// Node 1: FetchPool —— 调用 ScreenPoolTool 粗筛 + 候选池落库 CandidatePool
// ---------------------------------------------------------------------------

// screenPoolResult 是 ScreenPoolTool 返回值中本节点关心的部分。
type screenPoolResult struct {
	OK        bool                  `json:"ok"`
	Error     string                `json:"error"`
	Message   string                `json:"status"` // 上游 code!=0 时的状态说明
	TradeDate string                `json:"trade_date"`
	PoolSize  int                   `json:"pool_size"`
	Pool      []qtool.ScreenPoolHit `json:"pool"`
}

// poolSaver 抽象候选池落库动作，便于单元测试注入 fake（生产实现 gormCandidateSaver）。
type poolSaver interface {
	SaveCandidate(date string, hit qtool.ScreenPoolHit) error
}

// gormCandidateSaver 生产实现：upsert by (trade_date, code)，同日重跑覆盖旧记录。
type gormCandidateSaver struct{ gdb *gorm.DB }

func (sv gormCandidateSaver) SaveCandidate(date string, hit qtool.ScreenPoolHit) error {
	row := dbmodel.CandidatePool{
		TradeDate:      date,
		Code:           hit.Symbol,
		SignalType:     hit.SignalType,
		CzscSummary:    hit.Summary,
		ZsCount:        hit.ZsCount,
		TrendDirection: hit.TrendDirection,
	}
	return sv.gdb.Where("trade_date = ? AND code = ?", date, hit.Symbol).
		Assign(row).FirstOrCreate(&row).Error
}

func fetchPool(ctx context.Context, screenTool *qtool.ScreenPoolTool, gdb *gorm.DB, in FunnelInput) (map[string]any, error) {
	return fetchPoolWith(ctx, screenTool, gormCandidateSaver{gdb}, in)
}

// fetchPoolWith 是 FetchPool 节点的可测试内核：外部依赖仅有 ScreenPoolTool(HTTP)
// 与 poolSaver(落库)，均可在单测中以 httptest / fake saver 替换。
func fetchPoolWith(ctx context.Context, screenTool *qtool.ScreenPoolTool, saver poolSaver, in FunnelInput) (map[string]any, error) {
	in.TradeDate = strings.TrimSpace(in.TradeDate)
	if _, err := time.Parse("2006-01-02", in.TradeDate); err != nil {
		return nil, fmt.Errorf("fetch_pool: invalid trade_date %q, expect YYYY-MM-DD", in.TradeDate)
	}
	symbols := normalizeSymbols(in.Symbols)
	if len(symbols) == 0 {
		return nil, errors.New("fetch_pool: empty symbols, nothing to screen")
	}

	args, _ := json.Marshal(map[string]any{"symbols": symbols, "period": "daily"})
	raw, err := screenTool.InvokableRun(ctx, string(args))
	if err != nil {
		return nil, fmt.Errorf("fetch_pool: screen tool error: %w", err)
	}
	var res screenPoolResult
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		return nil, fmt.Errorf("fetch_pool: decode screen result: %w (raw=%.200s)", err, raw)
	}
	if !res.OK {
		return nil, fmt.Errorf("fetch_pool: screening failed: %s", res.Error)
	}
	// 粗筛零入选视为硬失败: 空池继续跑只会让下游 Manager 空转 LLM,
	// 直接中断流水线并把原因返回给调用方(API/调度器可感知降级)。
	if len(res.Pool) == 0 {
		return nil, fmt.Errorf("fetch_pool: empty pool after screening (date=%s scanned=%d status=%s)",
			in.TradeDate, len(symbols), res.Message)
	}

	// 候选池落库（同日重跑覆盖旧记录：upsert by (trade_date, code)）
	for _, hit := range res.Pool {
		if err := saver.SaveCandidate(in.TradeDate, hit); err != nil {
			log.Printf("fetch_pool: WARN upsert candidate %s failed: %v", hit.Symbol, err)
		}
	}

	poolJSON, err := json.Marshal(res.Pool)
	if err != nil {
		return nil, fmt.Errorf("fetch_pool: marshal pool: %w", err)
	}

	state := map[string]any{
		keyTradeDate: in.TradeDate,
		keyPool:      res.Pool,
		keyPoolJSON:  string(poolJSON),
	}
	log.Printf("fetch_pool: date=%s requested=%d pool=%d", in.TradeDate, len(symbols), len(res.Pool))
	return state, nil
}

// ---------------------------------------------------------------------------
// Node 2: FetchNews —— 并发遍历候选池抓取新闻
// ---------------------------------------------------------------------------

type newsToolResultLite struct {
	OK      bool   `json:"ok"`
	Summary string `json:"summary"`
	Error   string `json:"error"`
}

func fetchNews(ctx context.Context, state map[string]any, newsTool *qtool.NewsSearchTool, concurrency int) (map[string]any, error) {
	pool, _ := state[keyPool].([]qtool.ScreenPoolHit)
	if len(pool) == 0 {
		// 粗筛后无候选：合法路径，直接给 LLM 空池上下文（其应输出 []）
		state[keyNewsCtx] = map[string]string{}
		state[keyNewsCtxJSON] = "[]"
		log.Printf("fetch_news: empty pool, skip news fetching")
		return state, nil
	}

	newsCtx := make(map[string]string, len(pool))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	for _, hit := range pool {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			args, _ := json.Marshal(map[string]any{"symbol": sym, "limit": 5})
			raw, err := newsTool.InvokableRun(ctx, string(args))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				newsCtx[sym] = fmt.Sprintf(`{"ok":false,"error":"news tool error: %v"}`, err)
				return
			}
			var lite newsToolResultLite
			if uerr := json.Unmarshal([]byte(raw), &lite); uerr == nil && !lite.OK {
				log.Printf("fetch_news: symbol=%s degraded: %.120s", sym, raw)
			}
			newsCtx[sym] = raw
		}(hit.Symbol)
	}
	wg.Wait()

	ctxJSON, err := json.Marshal(newsCtx)
	if err != nil {
		return nil, fmt.Errorf("fetch_news: marshal news context: %w", err)
	}
	state[keyNewsCtx] = newsCtx
	state[keyNewsCtxJSON] = string(ctxJSON)
	log.Printf("fetch_news: fetched news for %d candidates (concurrency=%d)", len(newsCtx), concurrency)
	return state, nil
}

// ---------------------------------------------------------------------------
// Node 3: ManagerDecision —— LLM 排雷精选（脏输出清洗 + 错误回灌重试）
// ---------------------------------------------------------------------------

func managerDecision(ctx context.Context, state map[string]any, cm model.BaseChatModel, maxRetry int) (map[string]any, error) {
	tradeDate, _ := state[keyTradeDate].(string)
	poolJSON, _ := state[keyPoolJSON].(string)
	newsJSON, _ := state[keyNewsCtxJSON].(string)

	system := schema.SystemMessage(agent.ManagerSystemPrompt)
	user := schema.UserMessage(agent.BuildManagerPrompt(tradeDate, poolJSON, newsJSON))
	history := []*schema.Message{system, user}

	var lastErr error
	for attempt := 0; attempt <= maxRetry; attempt++ {
		if attempt > 0 {
			// 错误回灌：把上一轮的脏输出与解析错误告知模型，要求其修正格式
			history = append(history,
				schema.AssistantMessage(lastAssistantContent(history), nil),
				schema.UserMessage(fmt.Sprintf(
					"你上一次的输出无法解析为规定的 JSON 数组（错误：%v）。请只重新输出合法的 JSON 数组，不要任何其他文字。", lastErr)))
		}

		resp, err := cm.Generate(ctx, history)
		if err != nil {
			return nil, fmt.Errorf("manager_decision: chatmodel generate: %w", err)
		}
		decisions, perr := agent.ParseDecisions(extractJSONCandidate(resp.Content))
		if perr == nil {
			// 校验 symbol 必须来自候选池（防止 LLM 幻觉出池外股票）
			pool, _ := state[keyPool].([]qtool.ScreenPoolHit)
			valid := filterKnownSymbols(decisions, pool)
			state[keyDecisions] = valid
			log.Printf("manager_decision: attempt=%d decisions=%d (dropped hallucinated=%d)",
				attempt+1, len(valid), len(decisions)-len(valid))
			return state, nil
		}
		lastErr = perr
		log.Printf("manager_decision: attempt=%d parse failed: %v", attempt+1, perr)
	}
	return nil, fmt.Errorf("manager_decision: llm output unparseable after %d retries: %w", maxRetry, lastErr)
}

// filterKnownSymbols 剔除候选池之外的幻觉 symbol。
func filterKnownSymbols(items []agent.DecisionItem, pool []qtool.ScreenPoolHit) []agent.DecisionItem {
	known := make(map[string]bool, len(pool))
	for _, h := range pool {
		known[h.Symbol] = true
	}
	out := make([]agent.DecisionItem, 0, len(items))
	for _, it := range items {
		if known[it.Symbol] {
			out = append(out, it)
		} else {
			log.Printf("manager_decision: WARN drop hallucinated symbol %q not in candidate pool", it.Symbol)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Node 4: SaveDecision —— 决策落库 AIDecisionLog + 回填 Step 1 止损契约字段
// ---------------------------------------------------------------------------

// decisionSaver 抽象决策落库动作，便于单测注入 fake（生产实现 gormDecisionSaver）。
type decisionSaver interface {
	SaveDecision(row *dbmodel.AIDecisionLog) error
}

type gormDecisionSaver struct{ gdb *gorm.DB }

func (sv gormDecisionSaver) SaveDecision(row *dbmodel.AIDecisionLog) error {
	return sv.gdb.Create(row).Error
}

// stopFields 候选池 czsc 信号中提取出的止损契约四字段 + 入场价（Step 1 契约）。
type stopFields struct {
	PullbackLow   float64
	StopLoss      float64
	Invalidation  float64
	EffectiveStop float64
	EntryPrice    float64
}

// extractStops 从候选池一只股票的 buy_points[0] 提取止损字段。
// BUY 决策必须能取到有效 effective_stop；SELL/DROP 缺失不阻断（Layer3 会跳过）。
func extractStops(hit qtool.ScreenPoolHit) stopFields {
	var out stopFields
	if len(hit.BuyPoints) == 0 {
		return out
	}
	bp := hit.BuyPoints[0] // czsc_engine 按时间序输出，最新确认买点在前
	out.PullbackLow = bp.PullbackLow
	out.StopLoss = bp.StopLoss
	out.Invalidation = bp.Invalidation
	out.EffectiveStop = bp.EffectiveStop
	out.EntryPrice = bp.Price
	// 兜底：引擎未显式给 effective_stop 时，Go 侧按 max(stop_loss, invalidation) 重算
	if out.EffectiveStop <= 0 {
		if out.StopLoss > out.Invalidation {
			out.EffectiveStop = out.StopLoss
		} else {
			out.EffectiveStop = out.Invalidation
		}
	}
	return out
}

func saveDecision(state map[string]any, gdb *gorm.DB) (map[string]any, error) {
	return saveDecisionWith(state, gormDecisionSaver{gdb})
}

// saveDecisionWith 是 SaveDecision 节点的可测试内核：落库依赖仅 decisionSaver。
// 产出 []enrichedDecision（含止损字段）写入 state[keyRiskPlans]，供 Layer3 消费。
func saveDecisionWith(state map[string]any, saver decisionSaver) (map[string]any, error) {
	tradeDate, _ := state[keyTradeDate].(string)
	items, ok := state[keyDecisions].([]agent.DecisionItem)
	if !ok {
		return nil, errors.New("save_decision: missing ai_decisions in state")
	}
	pool, _ := state[keyPool].([]qtool.ScreenPoolHit)
	stopByCode := make(map[string]stopFields, len(pool))
	for _, h := range pool {
		stopByCode[h.Symbol] = extractStops(h)
	}

	ids := make([]uint64, 0, len(items))
	plans := make([]enrichedDecision, 0, len(items))
	for _, it := range items {
		action, err := dbmodel.NormalizeAction(it.Action)
		if err != nil { // ParseDecisions 已归一化，理论不可达；防御性跳过
			log.Printf("save_decision: WARN skip invalid action %q for %s: %v", it.Action, it.Symbol, err)
			continue
		}
		st := stopByCode[it.Symbol]
		row := dbmodel.AIDecisionLog{
			TradeDate:       tradeDate,
			Code:            it.Symbol,
			Action:          action,
			Reason:          it.Reason,
			ConfidenceScore: it.ConfidenceScore,
			PullbackLow:     st.PullbackLow,
			StopLoss:        st.StopLoss,
			Invalidation:    st.Invalidation,
			EffectiveStop:   st.EffectiveStop,
		}
		if err := saver.SaveDecision(&row); err != nil {
			log.Printf("save_decision: WARN insert decision log %s failed: %v", it.Symbol, err)
			continue
		}
		ids = append(ids, row.ID)
		plans = append(plans, enrichedDecision{
			Symbol:          it.Symbol,
			Action:          action,
			Reason:          it.Reason,
			ConfidenceScore: it.ConfidenceScore,
			PullbackLow:     st.PullbackLow,
			StopLoss:        st.StopLoss,
			Invalidation:    st.Invalidation,
			EffectiveStop:   st.EffectiveStop,
			EntryPrice:      st.EntryPrice,
		})
	}

	buys := 0
	for _, p := range plans {
		if p.Action == dbmodel.ActionBuy && p.ConfidenceScore >= 60 {
			buys++
		}
	}
	state[keyDecisionIDs] = ids
	state[keyRiskPlans] = plans
	log.Printf("save_decision: date=%s saved=%d strong_buys=%d stops_filled=%d",
		tradeDate, len(ids), buys, len(plans))
	return state, nil
}

// ---------------------------------------------------------------------------
// Node 5: RiskBudget —— Layer3 大盘闸门硬校验 + 风险预算仓位（决策 #2/#3）
// ---------------------------------------------------------------------------

// envFetcher 抽象大盘环境查询（生产实现 *MarketEnvironmentTool），便于单测替换。
type envFetcher interface {
	GlobalMaxPositionPct(ctx context.Context) (float64, bool)
}

// accountReader 抽象账户/持仓读取（生产实现 gormAccountReader）。
type accountReader interface {
	ReadSnapshot() (accountSnapshot, error)
}

type gormAccountReader struct{ gdb *gorm.DB }

// ReadSnapshot 组装账户快照：现金查 Account 表，持仓市值用 Position 表成本价口径
// （复盘时点无实时行情工具输入，保守用 CostPrice 估计市值——与撮合日 09:25 前值接近）。
func (r gormAccountReader) ReadSnapshot() (accountSnapshot, error) {
	var acc dbmodel.Account
	if err := r.gdb.Order("id asc").First(&acc).Error; err != nil {
		return accountSnapshot{}, fmt.Errorf("risk_budget: load account: %w", err)
	}
	var posRows []dbmodel.Position
	if err := r.gdb.Where("quantity > 0").Find(&posRows).Error; err != nil {
		return accountSnapshot{}, fmt.Errorf("risk_budget: query positions: %w", err)
	}
	snap := accountSnapshot{AvailableCash: acc.AvailableCash, FrozenCash: acc.FrozenCash}
	for _, p := range posRows {
		mv := p.CostPrice * float64(p.Quantity)
		snap.Positions = append(snap.Positions, PositionView{
			Code: p.Code, Quantity: p.Quantity, AvailableQuantity: p.AvailableQuantity,
			CostPrice: p.CostPrice, MarketValue: round2(mv),
		})
		snap.HoldValue += mv
	}
	snap.HoldValue = round2(snap.HoldValue)
	snap.TotalAssets = round2(snap.AvailableCash + snap.FrozenCash + snap.HoldValue)
	return snap, nil
}

// riskBudget 节点入口（生产签名）。
func riskBudget(ctx context.Context, state map[string]any, envTool *qtool.MarketEnvironmentTool, gdb *gorm.DB) (map[string]any, error) {
	return riskBudgetWith(ctx, state, envTool, gormAccountReader{gdb}, risk.DefaultConfig())
}

// riskBudgetWith 是 RiskBudget 节点的可测试内核。
//
// 流程：
//  1. 取大盘闸门（决策 #3：BEAR 市以 Go 侧硬校验为主）。工具失败 → regime=unknown、
//     globalPct=0，并按最保守口径 0.30 处理（宁可少买，不可裸奔）。
//  2. 读账户快照得到 TotalAssets / AvailableCash / 现有持仓。
//  3. 逐条决策：BUY 走 risk.CalculatePosition 硬编码算数量（LLM 无权输出 quantity，
//     决策 #2）；SELL 数量取可用持仓（T+1）；DROP 直接透传 Quantity=0。
//  4. 计算失败的 BUY 降级为 OBSERVE 留痕（RejectNote），绝不中断整批。
func riskBudgetWith(ctx context.Context, state map[string]any, env envFetcher, reader accountReader, rcfg risk.Config) (map[string]any, error) {
	plans, ok := state[keyRiskPlans].([]enrichedDecision)
	if !ok {
		return nil, errors.New("risk_budget: missing risk_plans in state (SaveDecision not run?)")
	}

	globalPct, regime := 0.0, "unknown"
	if p, usable := env.GlobalMaxPositionPct(ctx); usable {
		globalPct = p
		regime = regimeFromPct(p)
	} else {
		globalPct = conservativeBearPct
		log.Printf("risk_budget: WARN market environment unavailable, fallback to conservative gate %.2f", globalPct)
	}
	state[keyGlobalPct] = globalPct
	state[keyRegime] = regime

	snap, err := reader.ReadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("risk_budget: %w", err)
	}

	heldValueByCode := make(map[string]float64, len(snap.Positions))
	availQtyByCode := make(map[string]int64, len(snap.Positions))
	for _, pv := range snap.Positions {
		heldValueByCode[pv.Code] = pv.MarketValue
		availQtyByCode[pv.Code] = pv.AvailableQuantity
	}

	committedValue := 0.0 // 本批 BUY 已占用金额，用于全局闸门逐笔累加校验
	out := make([]enrichedDecision, 0, len(plans))
	for _, p := range plans {
		switch p.Action {
		case dbmodel.ActionBuy:
			remainingGate := snap.TotalAssets*globalPct - snap.HoldValue - committedValue
			effectiveGlobal := globalPct
			if globalPct > 0 && snap.TotalAssets > 0 {
				if gatePct := remainingGate / snap.TotalAssets; gatePct < effectiveGlobal {
					effectiveGlobal = math.Max(gatePct, 0)
				}
			}
			res, cerr := risk.CalculatePosition(risk.PositionRequest{
				TotalAssets:   snap.TotalAssets,
				AvailableCash: snap.AvailableCash,
				HeldValue:     heldValueByCode[p.Symbol],
				EntryPrice:    p.EntryPrice,
				EffectiveStop: p.EffectiveStop,
				GlobalMaxPct:  effectiveGlobal,
				Confidence:    p.ConfidenceScore,
			}, rcfg)
			if cerr != nil {
				p.Quantity = 0
				p.RejectNote = cerr.Error()
				log.Printf("risk_budget: BUY %s downgraded to observe: %v", p.Symbol, cerr)
			} else {
				p.Quantity = res.Quantity
				committedValue += res.PositionValue
				log.Printf("risk_budget: BUY %s qty=%d value=%.2f capped_by=%s (gate=%.0f%% regime=%s)",
					p.Symbol, res.Quantity, res.PositionValue, res.CappedBy, globalPct*100, regime)
			}
		case dbmodel.ActionSell:
			q := availQtyByCode[p.Symbol]
			if q <= 0 {
				p.Quantity = 0
				p.RejectNote = "no available position to sell (T+1 or not held)"
				log.Printf("risk_budget: SELL %s skipped: no available holding", p.Symbol)
			} else {
				p.Quantity = q
				log.Printf("risk_budget: SELL %s qty=%d (full exit of available holding)", p.Symbol, q)
			}
		default: // DROP：不产生订单
			p.Quantity = 0
		}
		out = append(out, p)
	}

	state[keyRiskPlans] = out
	log.Printf("risk_budget: date=%s regime=%s gate=%.0f%% assets=%.2f cash=%.2f plans=%d",
		strings.TrimSpace(fmt.Sprint(state[keyTradeDate])), regime, globalPct*100,
		snap.TotalAssets, snap.AvailableCash, len(out))
	return state, nil
}

// conservativeBearPct 大盘环境获取失败时的最保守总仓位闸门（30%）。
const conservativeBearPct = 0.30

// regimeFromPct 由闸门值反推 regime 标签（仅日志/审计用途：0.8+≈BULL，0.5+≈SIDEWAYS）。
func regimeFromPct(pct float64) string {
	switch {
	case pct >= 0.8:
		return "BULL"
	case pct >= 0.5:
		return "SIDEWAYS"
	default:
		return "BEAR"
	}
}

// ---------------------------------------------------------------------------
// Node 6: PlaceOrders —— Layer4 PENDING 订单落库（修复漏斗断链 🔴）
// ---------------------------------------------------------------------------

// newOrderSeq 返回进程内自增序列函数（订单号后缀，防同秒碰撞）。
func newOrderSeq() func() uint64 {
	var mu sync.Mutex
	var n uint64
	return func() uint64 {
		mu.Lock()
		defer mu.Unlock()
		n++
		return n
	}
}

// placeOrders 节点入口（生产签名）。
func placeOrders(state map[string]any, gdb *gorm.DB) (map[string]any, error) {
	return placeOrdersWith(state, gormOrderSaver{gdb}, newOrderSeq())
}

// placeOrdersWith 是 PlaceOrders 节点的可测试内核：
// 只把 BUY/SELL 且 Quantity>0 的决策转成订单意图；TradeDate 换算为 T+1 工作日
// （复盘 T 日 15:30 生成，MockBroker T+1 日 09:25 集合竞价撮合）。
// DROP / 风控拒绝（Quantity=0）静默跳过——它们只留 AIDecisionLog 痕迹。
func placeOrdersWith(state map[string]any, saver orderSaver, seqFn func() uint64) (map[string]any, error) {
	tradeDate, _ := state[keyTradeDate].(string)
	plans, ok := state[keyRiskPlans].([]enrichedDecision)
	if !ok {
		return nil, errors.New("place_orders: missing risk_plans in state (RiskBudget not run?)")
	}

	orderDate, err := nextWeekdayDate(tradeDate)
	if err != nil {
		return nil, fmt.Errorf("place_orders: %w", err)
	}

	intents := make([]OrderIntent, 0, len(plans))
	skipped := 0
	for _, p := range plans {
		if p.Quantity <= 0 || (p.Action != dbmodel.ActionBuy && p.Action != dbmodel.ActionSell) {
			skipped++
			continue
		}
		dir := dbmodel.DirectionBuy
		if p.Action == dbmodel.ActionSell {
			dir = dbmodel.DirectionSell
		}
		price := p.EntryPrice
		if price <= 0 {
			// 无有效入场价参考：拒写而非乱挂市价单（writeOrders 同样会拦截，双保险）
			log.Printf("place_orders: SKIP %s %s: entry_price<=0", dir, p.Symbol)
			skipped++
			continue
		}
		intents = append(intents, OrderIntent{
			Code:      p.Symbol,
			Direction: dir,
			Price:     price,
			Quantity:  p.Quantity,
			Reason:    p.Reason,
			TradeDate: orderDate,
			// Step 6：BUY 建仓单透传实际风控线（决策 #1 effective_stop），
			// MockBroker fillBuy 成交时据此落 Position.StopPrice；SELL 无意义留 0。
			StopPrice: stopForIntent(p, dir),
		})
	}

	report, err := writeOrders(intents, saver, seqFn)
	if err != nil {
		return nil, fmt.Errorf("place_orders: %w", err)
	}
	state[keyOrderReport] = report
	log.Printf("place_orders: date=%s order_date=%s intents=%d skipped=%d %s",
		tradeDate, orderDate, len(intents), skipped, report.Summary())
	return state, nil
}
