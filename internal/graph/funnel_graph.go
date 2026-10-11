// 本文件实现漏斗模型 Layer2 的 Eino Graph：
//
//	START -> FetchPool -> FetchNews -> ManagerDecision -> SaveDecision -> END
//
// 节点职责（对应架构师 Step 4）：
//   - FetchPool       : 非 LLM。调用 ScreenPoolTool（POST /api/czsc/screen_pool）
//     对自选股批量跑 czsc 粗筛，产出候选股票池；
//   - FetchNews       : 非 LLM。遍历候选池，并发（限流）调用 NewsSearchTool
//     拉取新闻/公告，拼接成 LLM 上下文；
//   - ManagerDecision : LLM 节点。基金经理排雷 Prompt（agent.ManagerSystemPrompt），
//     输出决策 JSON 数组（含脏输出清洗与错误回灌重试）；
//   - SaveDecision    : 非 LLM。决策逐条写入 AIDecisionLog 表留痕。
//
// 状态传递沿用「信封模式」（map[string]any），与 review_graph.go 同一风格。
package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"gorm.io/gorm"

	"ashare-quant/internal/agent"
	dbmodel "ashare-quant/internal/model"
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
)

const (
	keyPool               = "candidate_pool"      // []qtool.ScreenPoolHit
	keyPoolJSON           = "candidate_pool_json" // string: 候选池序列化 JSON（喂 LLM）
	keyNewsCtx            = "news_context"        // map[string]string: symbol -> 新闻 JSON
	keyNewsCtxJSON        = "news_context_json"   // string: 新闻上下文序列化 JSON（喂 LLM）
	keyDecisions          = "ai_decisions"        // []agent.DecisionItem: LLM 最终决策
	keyDecisionIDs        = "ai_decision_log_ids" // []uint64: 落库主键（审计用）
	keyDecisionViolations = "decision_violations" // []string: Manager 决策服务端校验违规说明
)

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
			return managerDecisionWithPositions(ctx, state, cfg.ChatModel, cfg.MaxLLMRetry, cfg.DB)
		})); err != nil {
		return nil, fmt.Errorf("funnel_graph: add ManagerDecision: %w", err)
	}

	// ---- Node 4: SaveDecision（非 LLM，落库留痕）----
	if err := g.AddLambdaNode(NodeSaveDecision, compose.InvokableLambda(
		func(_ context.Context, state map[string]any) (map[string]any, error) {
			return saveDecision(state, cfg.DB)
		})); err != nil {
		return nil, fmt.Errorf("funnel_graph: add SaveDecision: %w", err)
	}

	// ---- Edges ----
	for _, e := range [][2]string{
		{compose.START, NodeFetchPool},
		{NodeFetchPool, NodeFetchNews},
		{NodeFetchNews, NodeManagerDecision},
		{NodeManagerDecision, NodeSaveDecision},
		{NodeSaveDecision, compose.END},
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
		// Phase A: 结构化风控字段落库（0/零值时间 = 上游未给出该线）
		RefPrice:         hit.RefPrice,
		InvalidationLine: hit.InvalidationLine,
		PriceStopLine:    hit.StopLoss,
	}
	if ct, err := time.Parse("2006-01-02", hit.ConfirmedAt); err == nil {
		row.ConfirmedAt = ct
		row.HasConfirmedAt = true
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
	return managerDecisionWithPositions(ctx, state, cm, maxRetry, nil)
}

// loadPositionSymbols 读取当前持仓（quantity>0）代码集合，供 Manager 的 SELL
// 校验与 Prompt 上下文使用。DB 为 nil（单测注入场景）时返回空集合+nil error，
// 语义等价"无持仓"——保守方向：SELL 会被降级为 DROP。
func loadPositionSymbols(gdb *gorm.DB) (map[string]bool, []dbmodel.Position, error) {
	if gdb == nil {
		return map[string]bool{}, nil, nil
	}
	var rows []dbmodel.Position
	if err := gdb.Where("quantity > 0").Find(&rows).Error; err != nil {
		return nil, nil, fmt.Errorf("manager_decision: load positions: %w", err)
	}
	set := make(map[string]bool, len(rows))
	for _, p := range rows {
		set[p.Code] = true
	}
	return set, rows, nil
}

// managerDecisionWithPositions 是 ManagerDecision 的可测实现：
//   - 持仓清单进入 User Prompt（杜绝 LLM 对无持仓股票生成 SELL 的幻觉前提）；
//   - 解析后先 filterKnownSymbols 剔池外幻觉，再 agent.ValidateDecisions 做
//     服务端完整性校验（重复/缺失留痕、无持仓 SELL 硬降级为 DROP）——
//     这些检查由代码兜底，不依赖 LLM 遵守提示词。
func managerDecisionWithPositions(ctx context.Context, state map[string]any, cm model.BaseChatModel, maxRetry int, gdb *gorm.DB) (map[string]any, error) {
	tradeDate, _ := state[keyTradeDate].(string)
	poolJSON, _ := state[keyPoolJSON].(string)
	newsJSON, _ := state[keyNewsCtxJSON].(string)

	heldSet, heldRows, err := loadPositionSymbols(gdb)
	if err != nil {
		return nil, err
	}
	positionsJSON := "[]"
	if b, jerr := json.Marshal(heldRows); jerr == nil {
		positionsJSON = string(b)
	} else {
		log.Printf("manager_decision: WARN marshal positions failed: %v", jerr)
	}

	system := schema.SystemMessage(agent.ManagerSystemPrompt)
	user := schema.UserMessage(agent.BuildManagerPrompt(tradeDate, poolJSON, positionsJSON, newsJSON))
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
			// 第一道闸：symbol 必须来自候选池（防止 LLM 幻觉出池外股票）
			pool, _ := state[keyPool].([]qtool.ScreenPoolHit)
			valid := filterKnownSymbols(decisions, pool)

			// 第二道闸：服务端完整性校验（重复去重 / 无持仓 SELL 降级 DROP / 缺失留痕）。
			// expected 传 nil：候选池允许被排雷层部分丢弃，"每只都必答"仅作软告警来源，
			// 缺失项在此记录但不阻断（下游按已答项继续，未答=无决策=不下单，安全方向）。
			expected := make(map[string]bool, len(pool))
			for _, h := range pool {
				expected[h.Symbol] = true
			}
			valid, violations := agent.ValidateDecisions(valid, nil, heldSet)
			for _, miss := range missingCandidates(valid, expected) {
				violations = append(violations, "missing decision for candidate "+miss)
			}
			state[keyDecisions] = valid
			state[keyDecisionViolations] = violations
			log.Printf("manager_decision: attempt=%d decisions=%d violations=%d held_positions=%d",
				attempt+1, len(valid), len(violations), len(heldSet))
			for _, v := range violations {
				log.Printf("manager_decision:   violation: %s", v)
			}
			return state, nil
		}
		lastErr = perr
		log.Printf("manager_decision: attempt=%d parse failed: %v", attempt+1, perr)
	}
	return nil, fmt.Errorf("manager_decision: llm output unparseable after %d retries: %w", maxRetry, lastErr)
}

// missingCandidates 返回候选池中没有任何决策项的代码（排序保证可复现日志）。
func missingCandidates(valid []agent.DecisionItem, expected map[string]bool) []string {
	answered := make(map[string]bool, len(valid))
	for _, it := range valid {
		answered[it.Symbol] = true
	}
	var miss []string
	for sym := range expected {
		if !answered[sym] {
			miss = append(miss, sym)
		}
	}
	sort.Strings(miss)
	return miss
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
// Node 4: SaveDecision —— 决策落库 AIDecisionLog
// ---------------------------------------------------------------------------

// saveDecision 落库留痕。V2.0：BUY 决策入库前做服务端资格复核——候选池中
// Observation（无明确买卖点）的股票不允许以 BUY 身份进入开仓链路（Prompt 只是
// 软约束，这里代码兜底），命中则降级 DROP 并 WARN。
func saveDecision(state map[string]any, gdb *gorm.DB) (map[string]any, error) {
	tradeDate, _ := state[keyTradeDate].(string)
	items, ok := state[keyDecisions].([]agent.DecisionItem)
	if !ok {
		return nil, errors.New("save_decision: missing ai_decisions in state")
	}
	pool, _ := state[keyPool].([]qtool.ScreenPoolHit)
	observation := make(map[string]bool, len(pool))
	for _, h := range pool {
		if !isEntrySignal(h.SignalType) {
			observation[h.Symbol] = true
		}
	}

	ids := make([]uint64, 0, len(items))
	for _, it := range items {
		if it.Action == "BUY" && observation[it.Symbol] {
			log.Printf("save_decision: WARN %s BUY downgraded to DROP (no confirmed entry signal in czsc pool)", it.Symbol)
			it.Action = "DROP"
		}
		action, err := dbmodel.NormalizeAction(it.Action)
		if err != nil { // ParseDecisions 已归一化，理论不可达；防御性跳过
			log.Printf("save_decision: WARN skip invalid action %q for %s: %v", it.Action, it.Symbol, err)
			continue
		}
		row := dbmodel.AIDecisionLog{
			TradeDate:       tradeDate,
			Code:            it.Symbol,
			Action:          action,
			Reason:          it.Reason,
			ConfidenceScore: it.ConfidenceScore,
		}
		if err := gdb.Create(&row).Error; err != nil {
			log.Printf("save_decision: WARN insert decision log %s failed: %v", it.Symbol, err)
			continue
		}
		ids = append(ids, row.ID)
	}

	buys := 0
	for _, it := range items {
		if it.Action == dbmodel.ActionBuy && it.ConfidenceScore >= 60 {
			buys++
		}
	}
	state[keyDecisionIDs] = ids
	log.Printf("save_decision: date=%s saved=%d strong_buys=%d", tradeDate, len(ids), buys)
	return state, nil
}
