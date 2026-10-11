// 本文件实现漏斗模型 Layer1 的「规则可信度」Eino 工具：Walk-Forward 滚动回测查询。
//
// 封装 Python 数据网关两个接口（统一执行计划 Step 6 的 Go 端消费层）：
//   - GET  /api/czsc/walk_forward/{symbol}  单票样本外稳健性检验
//   - POST /api/czsc/walk_forward/rules    批量检验并聚合规则级结论
//
// 回答的问题是：「czsc 三类买卖点这套规则，在它没见过的行情里还赚钱吗？」
// verdict 语义（供 LLM / Graph 决策降权用）：
//   - 单票: oos_positive(样本外正期望) / overfit_suspect(训练正、样本外负→疑似
//     过拟合应降权) / oos_negative / insufficient_data
//   - 规则级: robust_positive / fragile_positive(典型过拟合, AI 决策应降权) /
//     negative / mixed / insufficient_data
//
// 设计规范与 screen_pool.go / market_environment.go 保持一致：
//   - Info(ctx) 返回 *schema.ToolInfo；InvokableRun 接收 JSON、返回 JSON 字符串；
//   - 失败以 {"ok":false,"error":...} 返回而非抛 Go error，便于流水线降级。
package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

var (
	_ tool.BaseTool      = (*WalkForwardTool)(nil)
	_ tool.InvokableTool = (*WalkForwardTool)(nil)
)

// ---------------------------------------------------------------------------
// 入参 / 出参结构（字段与网关 /api/czsc/walk_forward/* 对齐）
// ---------------------------------------------------------------------------

// WalkForwardInput 是 LLM / Graph 传入的 JSON 参数反序列化目标。
// Symbols 长度为 1 时走单票 GET 接口，多只时走批量 POST 接口。
type WalkForwardInput struct {
	Symbols   []string `json:"symbols"`              // 6位代码列表, 1~50只
	Period    string   `json:"period,omitempty"`     // daily(默认)/weekly/monthly
	StartDate string   `json:"start_date,omitempty"` // YYYYMMDD, 可空(网关默认近4年)
	EndDate   string   `json:"end_date,omitempty"`   // 同上
	// TrainSize 训练窗口K线数，默认 250(~1年)，合法范围 60~1000。
	TrainSize int `json:"train_size,omitempty"`
	// TestSize 测试窗口K线数，默认 63(~1季度)，合法范围 10~250。
	TestSize int   `json:"test_size,omitempty"`
	Anchored *bool `json:"anchored,omitempty"`  // true=锚定式扩张训练段, 默认滚动式
	Stride   int   `json:"stride,omitempty"`    // 信号重放步长 1~20, 默认5
	UseCache *bool `json:"use_cache,omitempty"` // false=强制重算, 默认true(读7天磁盘缓存)
}

// WFMetrics 单个窗口段的绩效指标（网关契约键名）。
type WFMetrics struct {
	TradeCount        int      `json:"trade_count"`
	WinRatePct        *float64 `json:"win_rate_pct"`
	AvgTradeReturnPct float64  `json:"avg_trade_return_pct"`
	TotalReturnPct    *float64 `json:"total_return_pct"`
	MaxDrawdownPct    *float64 `json:"max_drawdown_pct"`
	AlphaVsHoldPct    float64  `json:"alpha_vs_hold_pct"`
}

// WFWindow 一个滚动窗口的训练/测试段结果。
type WFWindow struct {
	TrainRange   string    `json:"train_range"`
	TestRange    string    `json:"test_range"`
	TrainMetrics WFMetrics `json:"train_metrics"`
	TestMetrics  WFMetrics `json:"test_metrics"`
}

// WFSummary 单票样本外汇总。
type WFSummary struct {
	NWindows              int      `json:"n_windows"`
	TestTradeCount        int      `json:"test_trade_count"`
	AvgTrainExpectancyPct *float64 `json:"avg_train_expectancy_pct"`
	AvgTestExpectancyPct  *float64 `json:"avg_test_expectancy_pct"`
	DegradationPP         *float64 `json:"degradation_pp"` // 训练→测试期望衰减(百分点)
	Verdict               string   `json:"verdict"`
}

// WFSymbolResult 单票 walk-forward 完整报告（GET 接口 data 字段）。
type WFSymbolResult struct {
	Symbol   string     `json:"symbol"`
	Period   string     `json:"period"`
	Mode     string     `json:"mode"`
	Bars     int        `json:"bars"`
	Windows  []WFWindow `json:"windows"`
	Summary  WFSummary  `json:"summary"`
	Source   string     `json:"source"` // live / cache
	Error    string     `json:"error,omitempty"`
	Required int        `json:"required_min,omitempty"`
}

// WFRuleSummary 跨股票聚合的规则级结论（POST 接口 rule_summary 字段）。
type WFRuleSummary struct {
	Rule                 string   `json:"rule"`
	SymbolsTested        int      `json:"symbols_tested"`
	SymbolsWithOOSTrades int      `json:"symbols_with_oos_trades"`
	AvgOOSEspectancyPct  *float64 `json:"avg_oos_expectancy_pct"`
	AvgDegradationPP     *float64 `json:"avg_degradation_pp"`
	OOSPositiveRatioPct  *float64 `json:"oos_positive_ratio_pct"`
	OverfitSuspectCount  int      `json:"overfit_suspect_count"`
	Verdict              string   `json:"verdict"`
}

type wfGetEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    *WFSymbolResult `json:"data"`
}

type wfRulesData struct {
	RuleSummary *WFRuleSummary   `json:"rule_summary"`
	Window      string           `json:"window"`
	Mode        string           `json:"mode"`
	Results     []WFSymbolResult `json:"results"`
	Errors      []struct {
		Symbol string `json:"symbol"`
		Error  string `json:"error"`
	} `json:"errors"`
}

type wfRulesEnvelope struct {
	Code    int          `json:"code"`
	Message string       `json:"message"`
	Data    *wfRulesData `json:"data"`
}

// ---------------------------------------------------------------------------
// WalkForwardTool 实现
// ---------------------------------------------------------------------------

// WalkForwardConfig 构造参数。
type WalkForwardConfig struct {
	// GatewayBaseURL Python 数据网关地址，如 "http://127.0.0.1:8000"。必填。
	GatewayBaseURL string
	// HTTPTimeout 单次请求超时。walk-forward 计算重（每窗口截断重放），
	// 批量 50 只可能数分钟，默认给 10 分钟。
	HTTPTimeout time.Duration
}

// WalkForwardTool Walk-Forward 滚动回测查询的 Eino 工具。零值不可用。
type WalkForwardTool struct {
	cfg    WalkForwardConfig
	client *http.Client
}

// NewWalkForwardTool 构造工具，校验配置合法性。
func NewWalkForwardTool(cfg WalkForwardConfig) (*WalkForwardTool, error) {
	cfg.GatewayBaseURL = strings.TrimRight(strings.TrimSpace(cfg.GatewayBaseURL), "/")
	if cfg.GatewayBaseURL == "" {
		return nil, errors.New("walk_forward: GatewayBaseURL is required")
	}
	if !strings.HasPrefix(cfg.GatewayBaseURL, "http://") && !strings.HasPrefix(cfg.GatewayBaseURL, "https://") {
		return nil, fmt.Errorf("walk_forward: invalid GatewayBaseURL %q, expect http(s)://...", cfg.GatewayBaseURL)
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 10 * time.Minute
	}
	return &WalkForwardTool{cfg: cfg, client: &http.Client{Timeout: cfg.HTTPTimeout}}, nil
}

// Info 返回工具 schema。Desc 明确告知 LLM verdict 的决策含义（降权规则）。
func (t *WalkForwardTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "query_walk_forward",
		Desc: "查询缠论买卖点规则的 Walk-Forward 滚动样本外回测结论（防过拟合检验）：" +
			"对传入股票（1~50只）在训练窗口内选规则、测试窗口外验证，" +
			"返回各窗口 train/test 期望收益与 verdict。" +
			"verdict 含义：oos_positive=样本外正期望可信；overfit_suspect/fragile_positive=疑似过拟合，" +
			"该股票的 czsc 信号应在交易计划中降权或回避；negative/mixed=规则失效需人工复核。" +
			"参数 symbols 为6位数字代码数组；train_size 默认250、test_size 默认63；period 默认 daily。",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"symbols": {
				Type:     schema.Array,
				Desc:     "股票代码列表，元素为6位数字字符串，如 [\"300750\"]，单次最多50只",
				Required: true,
				ElemInfo: &schema.ParameterInfo{Type: schema.String},
			},
			"period":     {Type: schema.String, Desc: "K线周期：daily(默认)/weekly/monthly", Required: false},
			"start_date": {Type: schema.String, Desc: "起始日期 YYYYMMDD，可省略（默认近4年）", Required: false},
			"end_date":   {Type: schema.String, Desc: "结束日期 YYYYMMDD，可省略（默认今天）", Required: false},
			"train_size": {Type: schema.Integer, Desc: "训练窗口K线数，默认250，范围60~1000", Required: false},
			"test_size":  {Type: schema.Integer, Desc: "测试窗口K线数，默认63，范围10~250", Required: false},
			"anchored":   {Type: schema.Boolean, Desc: "true=锚定式(训练段从起点扩张)，默认false滚动式", Required: false},
			"use_cache":  {Type: schema.Boolean, Desc: "false=无视7天磁盘缓存强制重算，默认true", Required: false},
		}),
	}, nil
}

// InvokableRun 解析 JSON 参数 -> 单票走 GET / 批量走 POST -> 透传结论 JSON。
func (t *WalkForwardTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	var in WalkForwardInput
	if err := json.Unmarshal([]byte(strings.TrimSpace(argumentsInJSON)), &in); err != nil {
		return errorResult(fmt.Sprintf("arguments are not valid JSON: %v", err)), nil
	}
	if len(in.Symbols) == 0 {
		return errorResult("symbols is empty: at least one 6-digit stock code required"), nil
	}
	if len(in.Symbols) > 50 {
		return errorResult(fmt.Sprintf("too many symbols: %d, max 50 per call (walk-forward is compute-heavy)", len(in.Symbols))), nil
	}
	cleaned := make([]string, 0, len(in.Symbols))
	for _, s := range in.Symbols {
		s = strings.TrimSpace(s)
		if !symbolPattern.MatchString(s) {
			return errorResult(fmt.Sprintf("symbol %q invalid: must be exactly 6 digits, e.g. 300750", s)), nil
		}
		dup := false
		for _, c := range cleaned {
			if c == s {
				dup = true
			}
		}
		if !dup {
			cleaned = append(cleaned, s)
		}
	}
	in.Period = strings.ToLower(strings.TrimSpace(in.Period))
	if in.Period == "" {
		in.Period = "daily"
	}
	switch in.Period {
	case "daily", "weekly", "monthly":
	default:
		return errorResult(fmt.Sprintf("period %q invalid: must be daily/weekly/monthly", in.Period)), nil
	}
	if in.TrainSize != 0 && (in.TrainSize < 60 || in.TrainSize > 1000) {
		return errorResult(fmt.Sprintf("train_size %d out of range 60~1000", in.TrainSize)), nil
	}
	if in.TestSize != 0 && (in.TestSize < 10 || in.TestSize > 250) {
		return errorResult(fmt.Sprintf("test_size %d out of range 10~250", in.TestSize)), nil
	}
	startDate, endDate := "", ""
	for _, d := range []struct{ key, raw string }{{"start_date", in.StartDate}, {"end_date", in.EndDate}} {
		if strings.TrimSpace(d.raw) == "" {
			continue
		}
		norm, err := normalizeDate(d.raw)
		if err != nil {
			return errorResult(d.key + ": " + err.Error()), nil
		}
		compact := strings.ReplaceAll(norm, "-", "") // 网关要求 YYYYMMDD
		if d.key == "start_date" {
			startDate = compact
		} else {
			endDate = compact
		}
	}

	if len(cleaned) == 1 {
		return t.runSingle(ctx, cleaned[0], &in, startDate, endDate)
	}
	return t.runRules(ctx, cleaned, &in, startDate, endDate)
}

// runSingle 单票 GET /api/czsc/walk_forward/{symbol}。
func (t *WalkForwardTool) runSingle(ctx context.Context, symbol string, in *WalkForwardInput,
	startDate, endDate string) (string, error) {
	q := []string{"period=" + in.Period}
	if startDate != "" {
		q = append(q, "start_date="+startDate)
	}
	if endDate != "" {
		q = append(q, "end_date="+endDate)
	}
	if in.TrainSize != 0 {
		q = append(q, fmt.Sprintf("train_size=%d", in.TrainSize))
	}
	if in.TestSize != 0 {
		q = append(q, fmt.Sprintf("test_size=%d", in.TestSize))
	}
	if in.Anchored != nil {
		q = append(q, fmt.Sprintf("anchored=%v", *in.Anchored))
	}
	if in.UseCache != nil && !*in.UseCache {
		q = append(q, "use_cache=false")
	}
	url := fmt.Sprintf("%s/api/czsc/walk_forward/%s?%s", t.cfg.GatewayBaseURL, symbol, strings.Join(q, "&"))

	reqCtx, cancel := context.WithTimeout(ctx, t.cfg.HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return errorResult(fmt.Sprintf("build request failed: %v", err)), nil
	}
	req.Header.Set("Accept", "application/json")

	raw, status, err := t.do(req)
	if err != nil {
		return errorResult(err.Error()), nil
	}
	if status != http.StatusOK {
		log.Printf("walk_forward: gateway status=%d body=%.512s", status, raw)
		return errorResult(fmt.Sprintf("gateway returned HTTP %d: %.256s", status, string(raw))), nil
	}
	var env wfGetEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return errorResult(fmt.Sprintf("decode walk_forward response failed: %v", err)), nil
	}
	d := env.Data
	if d == nil {
		return errorResult("gateway returned no data: " + env.Message), nil
	}
	if d.Error != "" {
		// 业务级失败（insufficient_bars/no_data 等）不是传输错误，原样结构化返回
		return marshalOK("walk_forward", map[string]any{
			"status":  d.Error,
			"symbol":  symbol,
			"summary": fmt.Sprintf("%s 历史K线不足（需要至少 %d 根），无法完成样本外检验，按保守处理。", symbol, d.Required),
		}), nil
	}
	log.Printf("walk_forward: symbol=%s windows=%d test_trades=%d verdict=%s source=%s",
		symbol, d.Summary.NWindows, d.Summary.TestTradeCount, d.Summary.Verdict, d.Source)
	return marshalOK("walk_forward", map[string]any{
		"symbol":       symbol,
		"mode":         d.Mode,
		"bars":         d.Bars,
		"windows":      d.Windows,
		"summary":      d.Summary,
		"source":       d.Source,
		"verdict_hint": verdictHint(d.Summary.Verdict),
		"summary_text": fmt.Sprintf("%s(%s模式): %d个滚动窗口, 样本外成交%d笔, verdict=%s。%s",
			symbol, d.Mode, d.Summary.NWindows, d.Summary.TestTradeCount,
			d.Summary.Verdict, verdictHint(d.Summary.Verdict)),
	}), nil
}

// runRules 批量 POST /api/czsc/walk_forward/rules。
func (t *WalkForwardTool) runRules(ctx context.Context, symbols []string, in *WalkForwardInput,
	startDate, endDate string) (string, error) {
	body := map[string]any{"symbols": symbols, "period": in.Period}
	if startDate != "" {
		body["start_date"] = startDate
	}
	if endDate != "" {
		body["end_date"] = endDate
	}
	if in.TrainSize != 0 {
		body["train_size"] = in.TrainSize
	}
	if in.TestSize != 0 {
		body["test_size"] = in.TestSize
	}
	if in.Anchored != nil {
		body["anchored"] = *in.Anchored
	}
	if in.UseCache != nil {
		body["use_cache"] = *in.UseCache
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return errorResult(fmt.Sprintf("marshal request failed: %v", err)), nil
	}
	url := t.cfg.GatewayBaseURL + "/api/czsc/walk_forward/rules"

	reqCtx, cancel := context.WithTimeout(ctx, t.cfg.HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, strings.NewReader(string(payload)))
	if err != nil {
		return errorResult(fmt.Sprintf("build request failed: %v", err)), nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	raw, status, err := t.do(req)
	if err != nil {
		return errorResult(err.Error()), nil
	}
	if status != http.StatusOK {
		log.Printf("walk_forward(rules): gateway status=%d body=%.512s", status, raw)
		return errorResult(fmt.Sprintf("gateway returned HTTP %d: %.256s", status, string(raw))), nil
	}
	var env wfRulesEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return errorResult(fmt.Sprintf("decode walk_forward rules response failed: %v", err)), nil
	}
	d := env.Data
	if d == nil || d.RuleSummary == nil {
		return errorResult("gateway returned no rule summary: " + env.Message), nil
	}
	rs := d.RuleSummary
	log.Printf("walk_forward(rules): symbols=%d tested=%d verdict=%s avg_oos=%v",
		len(symbols), rs.SymbolsWithOOSTrades, rs.Verdict, rs.AvgOOSEspectancyPct)

	// 每票只透出 verdict + 关键数字，避免整包 windows 撑爆 LLM 上下文
	perSymbol := make([]map[string]any, 0, len(d.Results))
	for _, r := range d.Results {
		perSymbol = append(perSymbol, map[string]any{
			"symbol":                  r.Symbol,
			"verdict":                 r.Summary.Verdict,
			"test_trade_count":        r.Summary.TestTradeCount,
			"avg_test_expectancy_pct": r.Summary.AvgTestExpectancyPct,
			"degradation_pp":          r.Summary.DegradationPP,
		})
	}
	return marshalOK("walk_forward_rules", map[string]any{
		"rule_summary": rs,
		"window":       d.Window,
		"mode":         d.Mode,
		"results":      perSymbol,
		"errors":       d.Errors,
		"verdict_hint": verdictHint(rs.Verdict),
		"summary_text": fmt.Sprintf("规则「%s」跨 %d 只股票样本外检验: verdict=%s, 样本外平均期望=%v%%, 过拟合嫌疑%d只。%s",
			rs.Rule, rs.SymbolsTested, rs.Verdict, rs.AvgOOSEspectancyPct,
			rs.OverfitSuspectCount, verdictHint(rs.Verdict)),
	}), nil
}

// do 发送请求并读取响应体（4MB 防御性上限）。
func (t *WalkForwardTool) do(req *http.Request) ([]byte, int, error) {
	resp, err := t.client.Do(req)
	if err != nil {
		log.Printf("walk_forward: gateway request failed url=%s err=%v", req.URL, err)
		return nil, 0, fmt.Errorf("data gateway unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read gateway response failed: %w", err)
	}
	return raw, resp.StatusCode, nil
}

// verdictHint 把 verdict 翻译成给 LLM 的一句话行动指令（决策降权口径）。
func verdictHint(v string) string {
	switch v {
	case "oos_positive", "robust_positive":
		return "规则在样本外保持正期望，技术面信号可按正常权重采信。"
	case "overfit_suspect", "fragile_positive":
		return "训练段盈利但样本外转负，疑似过拟合：该股/该规则的 czsc 信号必须降权，交易计划应降低置信度或回避。"
	case "oos_negative", "negative":
		return "规则样本内外皆负期望，应废弃该信号来源，不得据此建仓。"
	case "mixed":
		return "样本外结论分歧（均值为负但过半数为正等），需人工复核后再决定。"
	case "insufficient_data":
		return "有效样本不足，无法给出统计结论，按最保守口径处理（不新增仓位）。"
	default:
		return "未知 verdict，按最保守口径处理。"
	}
}
