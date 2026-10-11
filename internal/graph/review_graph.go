// Package graph 使用 Eino compose.Graph 编排「日终复盘 -> AI 决策 -> 硬风控 -> 下单」工作流。
//
// 拓扑（题设要求）：
//
//	START -> FetchData -> AnalyzeAndPlan -> RiskCheck -> ExecutePlan -> END
//
// 节点类型：
//   - FetchData      : 非 LLM，Lambda 节点，调用 MarketDataTool（HTTP -> Python 网关）拉行情；
//   - AnalyzeAndPlan : LLM 节点，ChatModel + Master Prompt，输出交易计划 JSON；
//   - RiskCheck      : 非 LLM，Lambda 节点，服务端硬编码风控（不信任 LLM 输出）；
//   - ExecutePlan    : 非 LLM，Lambda 节点，遍历通过风控的计划项调用 MockTradingTool 写入 PENDING 订单。
//
// 状态传递采用「信封模式」（map[string]any）：Eino Graph 要求相邻节点 IO 类型匹配，
// 统一信封让每个节点透传上游字段并追加自身产出，兼顾单一入口/出口类型与扩展性。
package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"regexp"
	"strings"
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
// 节点键名与 state 信封字段约定（集中定义，防止跨节点拼写漂移）
// ---------------------------------------------------------------------------

const (
	NodeFetchData      = "FetchData"
	NodeAnalyzeAndPlan = "AnalyzeAndPlan"
	NodeRiskCheck      = "RiskCheck"
	NodeExecutePlan    = "ExecutePlan"
)

const (
	keyTradeDate   = "trade_date"   // string: 复盘交易日 YYYY-MM-DD
	keyMarketData  = "market_data"  // map[string]string: symbol -> K线工具返回 JSON
	keyAccountSnap = "account_snap" // accountSnapshot: 账户+市值快照（含最新价，供风控复用）
	keyLLMPlans    = "llm_plans"    // []agent.TradePlanItem: LLM 原始计划
	keyApproved    = "approved"     // []agent.TradePlanItem: 通过风控的计划
	keyRejected    = "rejected"     // []Rejection: 风控拒绝明细（审计留痕）
	keyExecReport  = "exec_report"  // *ExecutionReport: 下单结果汇总
)

// ---------------------------------------------------------------------------
// 对外类型
// ---------------------------------------------------------------------------

// ReviewInput 是 Graph 的入口参数（由 Phase 6 调度器构造）。
type ReviewInput struct {
	TradeDate string   // 复盘交易日 YYYY-MM-DD
	Watchlist []string // 候选股票池（应包含全部当前持仓股代码）
}

// Rejection 一条被风控拒绝的计划及原因。
type Rejection struct {
	Plan   agent.TradePlanItem `json:"plan"`
	Reason string              `json:"reason"`
}

// ExecutionReport ExecutePlan 节点的产出汇总。
type ExecutionReport struct {
	Submitted int      `json:"submitted"`
	Failed    int      `json:"failed"`
	OrderIDs  []string `json:"order_ids,omitempty"`
	Errors    []string `json:"errors,omitempty"`
}

// Summary 生成人类可读的单段摘要（供 main 打印 / HTTP 接口返回）。
func (r *ExecutionReport) Summary() string {
	if r == nil {
		return "<nil>"
	}
	return fmt.Sprintf("submitted=%d failed=%d orders=%v", r.Submitted, r.Failed, r.OrderIDs)
}

// PositionView 持仓视图（喂给 LLM 与风控共用的口径）。
type PositionView struct {
	Code              string  `json:"code"`
	Quantity          int64   `json:"quantity"`
	AvailableQuantity int64   `json:"available_quantity"`
	CostPrice         float64 `json:"cost_price"`
	LastClose         float64 `json:"last_close"` // 最新收盘价（来自行情信封）
	MarketValue       float64 `json:"market_value"`
	FloatProfitPct    float64 `json:"float_profit_pct"`
}

// accountSnapshot FetchData 节点产出的账户事实，后续节点只读复用。
type accountSnapshot struct {
	AvailableCash float64        `json:"available_cash"`
	FrozenCash    float64        `json:"frozen_cash"`
	Positions     []PositionView `json:"positions"`
	HoldValue     float64        `json:"hold_value"`   // Σ 持仓市值
	TotalAssets   float64        `json:"total_assets"` // available + frozen + hold_value（复盘时点恒等）
}

// ---------------------------------------------------------------------------
// Config 与构建入口
// ---------------------------------------------------------------------------

// Config 构建复盘 Graph 所需的全部依赖。
type Config struct {
	ChatModel    model.BaseChatModel // Eino ChatModel（eino-ext openai/deepseek 等），必填
	DB           *gorm.DB            // 已完成 AutoMigrate 的数据库句柄，必填
	GatewayURL   string              // Python 数据网关地址，必填
	MaxLLMRetry  int                 // LLM 输出解析失败时的重试次数，默认 1
	LookbackDays int                 // K 线自然日回溯窗口，默认 90
}

// BuildReviewGraph 按题设拓扑构建并编译复盘 Graph，返回可直接 Invoke 的 Runnable。
func BuildReviewGraph(ctx context.Context, cfg Config) (compose.Runnable[ReviewInput, map[string]any], error) {
	if cfg.ChatModel == nil {
		return nil, errors.New("graph: ChatModel is required")
	}
	if cfg.DB == nil {
		return nil, errors.New("graph: DB is required")
	}
	if strings.TrimSpace(cfg.GatewayURL) == "" {
		return nil, errors.New("graph: GatewayURL is required")
	}
	if cfg.MaxLLMRetry <= 0 {
		cfg.MaxLLMRetry = 1
	}
	if cfg.LookbackDays <= 0 {
		cfg.LookbackDays = 90
	}

	marketTool, err := qtool.NewMarketDataTool(qtool.MarketDataConfig{GatewayBaseURL: cfg.GatewayURL})
	if err != nil {
		return nil, fmt.Errorf("graph: build market data tool: %w", err)
	}
	tradingTool, err := qtool.NewMockTradingTool(qtool.MockTradingConfig{DB: cfg.DB})
	if err != nil {
		return nil, fmt.Errorf("graph: build mock trading tool: %w", err)
	}

	g := compose.NewGraph[ReviewInput, map[string]any]()

	// ---- Node 1: FetchData（非 LLM）----
	if err := g.AddLambdaNode(NodeFetchData, compose.InvokableLambda(
		func(ctx context.Context, in ReviewInput) (map[string]any, error) {
			return fetchData(ctx, cfg.DB, marketTool, in, cfg.LookbackDays)
		})); err != nil {
		return nil, fmt.Errorf("graph: add FetchData: %w", err)
	}

	// ---- Node 2: AnalyzeAndPlan（LLM）----
	if err := g.AddLambdaNode(NodeAnalyzeAndPlan, compose.InvokableLambda(
		func(ctx context.Context, state map[string]any) (map[string]any, error) {
			return analyzeAndPlan(ctx, state, cfg.ChatModel, cfg.MaxLLMRetry)
		})); err != nil {
		return nil, fmt.Errorf("graph: add AnalyzeAndPlan: %w", err)
	}

	// ---- Node 3: RiskCheck（非 LLM，硬编码风控）----
	if err := g.AddLambdaNode(NodeRiskCheck, compose.InvokableLambda(
		func(_ context.Context, state map[string]any) (map[string]any, error) {
			return riskCheck(state)
		})); err != nil {
		return nil, fmt.Errorf("graph: add RiskCheck: %w", err)
	}

	// ---- Node 4: ExecutePlan（非 LLM）----
	if err := g.AddLambdaNode(NodeExecutePlan, compose.InvokableLambda(
		func(ctx context.Context, state map[string]any) (map[string]any, error) {
			return executePlan(ctx, state, tradingTool)
		})); err != nil {
		return nil, fmt.Errorf("graph: add ExecutePlan: %w", err)
	}

	// ---- Edges: START -> FetchData -> AnalyzeAndPlan -> RiskCheck -> ExecutePlan -> END ----
	for _, e := range [][2]string{
		{compose.START, NodeFetchData},
		{NodeFetchData, NodeAnalyzeAndPlan},
		{NodeAnalyzeAndPlan, NodeRiskCheck},
		{NodeRiskCheck, NodeExecutePlan},
		{NodeExecutePlan, compose.END},
	} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			return nil, fmt.Errorf("graph: add edge %s->%s: %w", e[0], e[1], err)
		}
	}

	runnable, err := g.Compile(ctx, compose.WithGraphName("ashare_daily_review"))
	if err != nil {
		return nil, fmt.Errorf("graph: compile: %w", err)
	}
	return runnable, nil
}

// ---------------------------------------------------------------------------
// Node 1: FetchData —— 调用 MarketDataTool 获取行情 + 读取账户/持仓事实
// ---------------------------------------------------------------------------

func fetchData(ctx context.Context, gdb *gorm.DB, marketTool *qtool.MarketDataTool,
	in ReviewInput, lookbackDays int) (map[string]any, error) {

	in.TradeDate = strings.TrimSpace(in.TradeDate)
	if _, err := time.Parse("2006-01-02", in.TradeDate); err != nil {
		return nil, fmt.Errorf("fetch_data: invalid trade_date %q, expect YYYY-MM-DD", in.TradeDate)
	}

	// 候选池去空白去重；同时并入当前持仓股（复盘必须覆盖持仓，哪怕不在 watchlist）
	watch := normalizeSymbols(in.Watchlist)
	var posRows []dbmodel.Position
	if err := gdb.Where("quantity > 0").Find(&posRows).Error; err != nil {
		return nil, fmt.Errorf("fetch_data: query positions: %w", err)
	}
	for _, p := range posRows {
		watch = appendSymbol(watch, p.Code)
	}
	if len(watch) == 0 {
		return nil, errors.New("fetch_data: empty watchlist and no positions, nothing to review")
	}

	end := time.Now().UTC()
	startArg := end.AddDate(0, 0, -lookbackDays).Format("20060102")
	endArg := end.Format("20060102")

	market := make(map[string]string, len(watch))
	var degraded []string
	for _, sym := range watch {
		args, _ := json.Marshal(map[string]string{"symbol": sym, "start_date": startArg, "end_date": endArg})
		result, err := marketTool.InvokableRun(ctx, string(args))
		if err != nil {
			// 工具抛错 = 系统性故障，但该票降级不阻断整批复盘（LLM 将看到 ok=false 提示）
			log.Printf("fetch_data: market tool error symbol=%s err=%v", sym, err)
			degraded = append(degraded, sym)
			continue
		}
		if !strings.Contains(result, `"ok":true`) {
			log.Printf("fetch_data: symbol=%s data unavailable, degraded: %.200s", sym, result)
			degraded = append(degraded, sym)
			continue
		}
		market[sym] = result
	}
	if len(market) == 0 {
		return nil, fmt.Errorf("fetch_data: all symbols failed to fetch market data: %v", degraded)
	}

	// 组装账户快照：现金查库，持仓市值用刚拉到的最新收盘价
	var acc dbmodel.Account
	if err := gdb.Order("id asc").First(&acc).Error; err != nil {
		return nil, fmt.Errorf("fetch_data: load account: %w", err)
	}
	snap := accountSnapshot{AvailableCash: acc.AvailableCash, FrozenCash: acc.FrozenCash}
	posByCode := make(map[string]dbmodel.Position, len(posRows))
	for _, p := range posRows {
		posByCode[p.Code] = p
	}
	for _, sym := range watch {
		p, held := posByCode[sym]
		lastClose, ok := lastCloseOf(market[sym])
		if !ok {
			continue
		}
		if held {
			mv := lastClose * float64(p.Quantity)
			pv := PositionView{
				Code: p.Code, Quantity: p.Quantity, AvailableQuantity: p.AvailableQuantity,
				CostPrice: p.CostPrice, LastClose: lastClose, MarketValue: round2(mv),
			}
			if p.CostPrice > 0 {
				pv.FloatProfitPct = round2((lastClose/p.CostPrice - 1) * 100)
			}
			snap.Positions = append(snap.Positions, pv)
			snap.HoldValue += mv
		}
	}
	snap.HoldValue = round2(snap.HoldValue)
	snap.TotalAssets = round2(snap.AvailableCash + snap.FrozenCash + snap.HoldValue)

	state := map[string]any{
		keyTradeDate:   in.TradeDate,
		keyMarketData:  market,
		keyAccountSnap: snap,
	}
	log.Printf("fetch_data: date=%s symbols=%d degraded=%d total_assets=%.2f cash=%.2f holds=%d",
		in.TradeDate, len(watch), len(degraded), snap.TotalAssets, snap.AvailableCash, len(snap.Positions))
	return state, nil
}

// klineEnvelopeLite 只需 bars 尾部即可取最新收盘价，容错两种响应形态由工具层已归一为 {"klines":[...]}。
type klineEnvelopeLite struct {
	Klines []struct {
		Date  string  `json:"date"`
		Close float64 `json:"close"`
	} `json:"klines"`
}

// lastCloseOf 从行情 JSON 中取最后一根 K 线的收盘价。
func lastCloseOf(rawJSON string) (float64, bool) {
	if rawJSON == "" {
		return 0, false
	}
	var env klineEnvelopeLite
	if err := json.Unmarshal([]byte(rawJSON), &env); err != nil || len(env.Klines) == 0 {
		return 0, false
	}
	c := env.Klines[len(env.Klines)-1].Close
	if c <= 0 || math.IsNaN(c) || math.IsInf(c, 0) {
		return 0, false
	}
	return c, true
}

// ---------------------------------------------------------------------------
// Node 2: AnalyzeAndPlan —— LLM 节点（Master Prompt + JSON 容错解析 + 错误回灌重试）
// ---------------------------------------------------------------------------

var codeBlockRe = regexp.MustCompile("(?s)```(?:json|JSON)?\\s*(.*?)\\s*```")

func analyzeAndPlan(ctx context.Context, state map[string]any, cm model.BaseChatModel, maxRetry int) (map[string]any, error) {
	snap, ok := state[keyAccountSnap].(accountSnapshot)
	if !ok {
		return nil, errors.New("analyze_and_plan: missing account snapshot from FetchData node")
	}
	market, ok := state[keyMarketData].(map[string]string)
	if !ok {
		return nil, errors.New("analyze_and_plan: missing market data from FetchData node")
	}
	tradeDate, _ := state[keyTradeDate].(string)

	acctJSON, _ := json.MarshalIndent(map[string]any{
		"available_cash": snap.AvailableCash,
		"frozen_cash":    snap.FrozenCash,
		"hold_value":     snap.HoldValue,
		"total_assets":   snap.TotalAssets,
		"position_ratio": fmt.Sprintf("%.1f%%", ratioPct(snap.HoldValue, snap.TotalAssets)),
	}, "", "  ")
	posJSON, _ := json.MarshalIndent(snap.Positions, "", "  ")

	// 行情信封瘦身：只保留最近 60 根，控制 token；序列化失败则退回原始 JSON
	marketCompact := make(map[string]string, len(market))
	for sym, raw := range market {
		marketCompact[sym] = compactKlines(raw, 60)
	}
	mktJSON, err := json.Marshal(marketCompact)
	if err != nil {
		return nil, fmt.Errorf("analyze_and_plan: marshal market data: %w", err)
	}

	userMsg := agent.BuildUserPrompt(tradeDate, string(acctJSON), string(posJSON), string(mktJSON))
	system := schema.SystemMessage(agent.MasterSystemPrompt)
	history := []*schema.Message{system, schema.UserMessage(userMsg)}

	var lastErr error
	for attempt := 0; attempt <= maxRetry; attempt++ {
		if attempt > 0 {
			// 把上一次的解析错误回灌给 LLM，要求其仅输出合法 JSON —— 生产环境显著提升成功率
			history = append(history,
				schema.AssistantMessage(lastAssistantContent(history), nil),
				schema.UserMessage(fmt.Sprintf("你上一次的输出无法解析为 JSON 数组（错误：%v）。请重新输出，只输出合法的 JSON 数组，不要任何其他文字。", lastErr)))
			log.Printf("analyze_and_plan: retry %d/%d after parse error", attempt, maxRetry)
		}

		resp, err := cm.Generate(ctx, history)
		if err != nil {
			return nil, fmt.Errorf("analyze_and_plan: chat model generate failed: %w", err)
		}
		plans, perr := agent.ParseTradePlans(extractJSONCandidate(resp.Content))
		if perr == nil {
			state[keyLLMPlans] = plans
			log.Printf("analyze_and_plan: got %d plan items (attempt=%d)", len(plans), attempt+1)
			return state, nil
		}
		lastErr = perr
	}
	return nil, fmt.Errorf("analyze_and_plan: llm output unparseable after %d retries: %w", maxRetry, lastErr)
}

// extractJSONCandidate 预清洗：若模型套了 markdown 围栏，先把围栏内容提出来。
// （agent.ParseTradePlans 内部还有四级兜底，这里是第一道快速路径。）
func extractJSONCandidate(content string) string {
	if m := codeBlockRe.FindStringSubmatch(content); len(m) == 2 {
		return m[1]
	}
	return content
}

// compactKlines 将行情 JSON 截断为最近 n 根；任何失败原样返回（宁可多花 token 也不丢数据）。
func compactKlines(raw string, n int) string {
	var env struct {
		OK        bool            `json:"ok"`
		Symbol    string          `json:"symbol"`
		Count     int             `json:"count"`
		Truncated bool            `json:"truncated"`
		Klines    json.RawMessage `json:"klines"`
	}
	if err := json.Unmarshal([]byte(raw), &env); err != nil || len(env.Klines) == 0 {
		return raw
	}
	var bars []qtool.KlineBar
	if err := json.Unmarshal(env.Klines, &bars); err != nil || len(bars) == 0 {
		return raw
	}
	if len(bars) > n {
		bars = bars[len(bars)-n:]
	}
	env.Count = len(bars)
	env.Truncated = true
	out, err := json.Marshal(env)
	if err != nil {
		return raw
	}
	return string(out)
}

func lastAssistantContent(history []*schema.Message) string {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == schema.Assistant {
			return history[i].Content
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Node 3: RiskCheck —— 服务端硬编码风控（不信任 LLM 的任何数字）
// ---------------------------------------------------------------------------

const lotSize = 100

// riskCheck 对每笔计划做全量校验；通过的进 approved，失败的进 rejected（带原因），
// 绝不因单笔非法而中断整个流程。校验顺序即优先级：格式 -> 数量规则 -> T+1 -> 仓位 -> 资金。
func riskCheck(state map[string]any) (map[string]any, error) {
	plans, ok := state[keyLLMPlans].([]agent.TradePlanItem)
	if !ok {
		return nil, errors.New("risk_check: missing llm plans from AnalyzeAndPlan node")
	}
	snap, ok := state[keyAccountSnap].(accountSnapshot)
	if !ok {
		return nil, errors.New("risk_check: missing account snapshot")
	}
	market, _ := state[keyMarketData].(map[string]string)

	posByCode := make(map[string]*PositionView, len(snap.Positions))
	for i := range snap.Positions {
		p := &snap.Positions[i]
		posByCode[p.Code] = p
	}

	// 模拟运行态副本：同一批多条同票计划要累计占用额度（先校验先扣减）
	cashRemaining := snap.AvailableCash
	holdValue := snap.HoldValue
	valueByCode := make(map[string]float64)
	for code, p := range posByCode {
		valueByCode[code] = p.MarketValue
	}

	approved := make([]agent.TradePlanItem, 0, len(plans))
	rejected := make([]Rejection, 0)

	for _, pl := range plans {
		if reason := validateShape(pl); reason != "" {
			rejected = append(rejected, Rejection{pl, reason})
			continue
		}

		switch pl.Action {
		case dbmodel.DirectionBuy:
			newValue := pl.Price * float64(pl.Quantity)

			// 规则 2：单票仓位 ≤ 20%（以现有市值 + 本次新增计算）
			projected := valueByCode[pl.Symbol] + newValue
			if projected > snap.TotalAssets*agent.MaxPositionRatioPerStock+1e-6 {
				rejected = append(rejected, Rejection{pl, fmt.Sprintf(
					"违反单票20%%上限：该票预计市值 %.2f > 总资产 %.2f × 20%%", projected, snap.TotalAssets)})
				continue
			}
			// 规则 3：总仓位 ≤ 80%
			if holdValue+newValue > snap.TotalAssets*agent.MaxTotalPositionRatio+1e-6 {
				rejected = append(rejected, Rejection{pl, fmt.Sprintf(
					"违反总仓位80%%上限：加仓后总市值 %.2f > 总资产 %.2f × 80%%", holdValue+newValue, snap.TotalAssets)})
				continue
			}
			// 资金充足性（含预估佣金，保守口径）
			fee := buyFee(newValue)
			if newValue+fee > cashRemaining+1e-6 {
				rejected = append(rejected, Rejection{pl, fmt.Sprintf(
					"可用资金不足：需 %.2f（含佣金），仅剩 %.2f", newValue+fee, cashRemaining)})
				continue
			}
			// 涨停价 sanity：委托价高于当日涨停价的买单必死，直接拒（省一次无效挂单）
			if lu, okP := limitUpOf(market[pl.Symbol]); okP && pl.Price > lu+0.005 {
				rejected = append(rejected, Rejection{pl, fmt.Sprintf("委托价 %.2f 高于涨停价 %.2f，必然废单", pl.Price, lu)})
				continue
			}

			cashRemaining -= newValue + fee
			holdValue += newValue
			valueByCode[pl.Symbol] = projected
			approved = append(approved, pl)

		case dbmodel.DirectionSell:
			p, held := posByCode[pl.Symbol]
			// 规则 1（T+1 硬校验）：卖出数量 ≤ 可用数量
			if !held || p.AvailableQuantity < pl.Quantity {
				avail := int64(0)
				if held {
					avail = p.AvailableQuantity
				}
				rejected = append(rejected, Rejection{pl, fmt.Sprintf(
					"T+1/持仓校验失败：拟卖 %d 股 > 可用 %d 股（当日买入不可卖）", pl.Quantity, avail)})
				continue
			}
			proceeds := pl.Price*float64(pl.Quantity) - sellFee(pl.Price*float64(pl.Quantity))
			cashRemaining += proceeds
			delta := pl.Price * float64(pl.Quantity)
			if nv := valueByCode[pl.Symbol] - delta; nv > 0 {
				valueByCode[pl.Symbol] = nv
			} else {
				valueByCode[pl.Symbol] = 0
			}
			holdValue -= delta
			if holdValue < 0 {
				holdValue = 0
			}
			approved = append(approved, pl)
		}
	}

	state[keyApproved] = approved
	state[keyRejected] = rejected
	log.Printf("risk_check: total=%d approved=%d rejected=%d", len(plans), len(approved), len(rejected))
	for _, rj := range rejected {
		log.Printf("risk_check: REJECT %s %s x%d @%.2f -> %s", rj.Plan.Action, rj.Plan.Symbol, rj.Plan.Quantity, rj.Plan.Price, rj.Reason)
	}
	return state, nil
}

// validateShape 纯格式校验：方向、代码、价格、整手。返回空串表示通过。
func validateShape(pl agent.TradePlanItem) string {
	if pl.Action != dbmodel.DirectionBuy && pl.Action != dbmodel.DirectionSell {
		return fmt.Sprintf("action %q 非法（仅支持 BUY/SELL）", pl.Action)
	}
	if !regexp.MustCompile(`^\d{6}$`).MatchString(pl.Symbol) {
		return fmt.Sprintf("symbol %q 非法（须为6位数字代码）", pl.Symbol)
	}
	if pl.Price <= 0 || math.IsNaN(pl.Price) || math.IsInf(pl.Price, 0) {
		return fmt.Sprintf("price %.6f 非法", pl.Price)
	}
	if pl.Quantity <= 0 {
		return fmt.Sprintf("quantity %d 非法（须为正数）", pl.Quantity)
	}
	if pl.Action == dbmodel.DirectionBuy && pl.Quantity%lotSize != 0 {
		return fmt.Sprintf("买入数量 %d 必须是 %d 的整数倍", pl.Quantity, lotSize)
	}
	return ""
}

// limitUpOf 从行情 JSON 估算次日涨停价：昨收 × (1 + 板块比例)。
// 注意：这是复盘时点的预估口径；真实一字板判定在次日撮合时由 Broker 用当日 K 线执行。
func limitUpOf(klineJSON string) (float64, bool) {
	close, ok := lastCloseOf(klineJSON)
	if !ok {
		return 0, false
	}
	// 从信封里拿 symbol 判定板块比例
	var env struct {
		Symbol string `json:"symbol"`
	}
	_ = json.Unmarshal([]byte(klineJSON), &env)
	ratio := dbmodel.PriceLimitRatio(env.Symbol)
	return dbmodel.RoundToCent(close * (1 + ratio)), true
}

func buyFee(amount float64) float64 {
	f := amount * 0.00025
	if f < 5 {
		f = 5
	}
	return round2(f)
}

func sellFee(amount float64) float64 {
	return round2(buyFee(amount) + amount*0.0005)
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func ratioPct(part, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return part / total * 100
}

// ---------------------------------------------------------------------------
// Node 4: ExecutePlan —— 遍历通过风控的计划，调用 MockTradingTool 写入 PENDING 订单
// ---------------------------------------------------------------------------

func executePlan(ctx context.Context, state map[string]any, tradingTool *qtool.MockTradingTool) (map[string]any, error) {
	approved, ok := state[keyApproved].([]agent.TradePlanItem)
	if !ok {
		return nil, errors.New("execute_plan: missing approved plans from RiskCheck node")
	}
	report := &ExecutionReport{}

	for _, pl := range approved {
		args, _ := json.Marshal(map[string]any{
			"symbol": pl.Symbol, "direction": strings.ToLower(pl.Action),
			"price": pl.Price, "quantity": pl.Quantity, "reason": pl.Reason,
		})
		result, err := tradingTool.InvokableRun(ctx, string(args))
		if err != nil {
			report.Failed++
			report.Errors = append(report.Errors, fmt.Sprintf("%s %s: %v", pl.Action, pl.Symbol, err))
			log.Printf("execute_plan: tool error symbol=%s err=%v", pl.Symbol, err)
			continue
		}
		// 工具约定：业务失败返回 ok=false 的 JSON（而非 Go error）
		var out struct {
			OK      bool   `json:"ok"`
			OrderID string `json:"order_id"`
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal([]byte(result), &out); err != nil || !out.OK {
			report.Failed++
			msg := out.Error
			if msg == "" {
				msg = "unrecognized tool result: " + truncate(result, 120)
			}
			report.Errors = append(report.Errors, fmt.Sprintf("%s %s: %s", pl.Action, pl.Symbol, msg))
			continue
		}
		report.Submitted++
		report.OrderIDs = append(report.OrderIDs, out.OrderID)
	}

	state[keyExecReport] = report
	log.Printf("execute_plan: %s", report.Summary())
	return state, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func normalizeSymbols(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		out = appendSymbol(out, s)
	}
	return out
}

func appendSymbol(list []string, s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return list
	}
	for _, exist := range list {
		if exist == s {
			return list
		}
	}
	return append(list, s)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
