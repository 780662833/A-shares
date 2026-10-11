// Package tool 将数据网关与撮合引擎封装为 Eino (cloudwego/eino) 的 tool.BaseTool，
// 供 LLM Agent / Graph 节点调用。
//
// Phase 4 提供两个工具：
//   - MarketDataTool : HTTP 调用 Python 数据网关（Phase 1）获取日 K 线；
//   - MockTradingTool: 将 LLM 交易决策以 PENDING 订单写入数据库（不直接撮合）。
package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

// ---------------------------------------------------------------------------
// 编译期接口断言：确保 *MarketDataTool 同时满足 BaseTool 与 InvokableTool。
// Eino 官方规范：Info() 提供 schema 元数据，InvokableRun() 接收 JSON 参数并返回字符串结果。
// ---------------------------------------------------------------------------

var (
	_ tool.BaseTool      = (*MarketDataTool)(nil)
	_ tool.InvokableTool = (*MarketDataTool)(nil)
)

// ---------------------------------------------------------------------------
// MarketDataTool 入参 / 出参结构
// ---------------------------------------------------------------------------

// MarketDataInput 是 LLM 传入的 JSON 参数反序列化目标。
// 日期既接受网关原生的 YYYY-MM-DD，也接受 Prompt 中约定的紧凑格式 YYYYMMDD，
// 由 normalizeDate 统一归一化，最大化对 LLM 输出格式的容错。
type MarketDataInput struct {
	Symbol    string `json:"symbol"`     // 6 位数字股票代码，如 "000001"
	StartDate string `json:"start_date"` // YYYYMMDD 或 YYYY-MM-DD
	EndDate   string `json:"end_date"`   // YYYYMMDD 或 YYYY-MM-DD
}

// KlineBar 单根日 K 线（与 Phase 1 网关 /api/daily_kline 返回字段对齐）。
type KlineBar struct {
	Date   string  `json:"date"`
	Open   float64 `json:"open"`
	Close  float64 `json:"close"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Volume int64   `json:"volume"`
	Amount float64 `json:"amount"`
}

// klineResponseEnvelope 兼容网关两种可能的响应形态：
//  1. 裸数组            [ {...}, {...} ]
//  2. data 包裹对象     { "data": [...] }
type klineResponseEnvelope struct {
	bars []KlineBar
}

func (e *klineResponseEnvelope) UnmarshalJSON(b []byte) error {
	// 优先尝试裸数组
	var arr []KlineBar
	if err := json.Unmarshal(b, &arr); err == nil {
		e.bars = arr
		return nil
	}
	// 再尝试 {"data": [...]}
	var obj struct {
		Data []KlineBar `json:"data"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("market_data: unrecognized response shape: %w", err)
	}
	e.bars = obj.Data
	return nil
}

// ---------------------------------------------------------------------------
// MarketDataTool 实现
// ---------------------------------------------------------------------------

// MarketDataConfig 构造参数。
type MarketDataConfig struct {
	// GatewayBaseURL Python 数据网关地址，如 "http://127.0.0.1:8000"。必填。
	GatewayBaseURL string
	// HTTPTimeout 单次请求超时，默认 15s。
	HTTPTimeout time.Duration
	// MaxBarsPerCall 单次返回给 LLM 的最大 K 线条数（尾部截断，控制 token 消耗），默认 120。
	MaxBarsPerCall int
}

// MarketDataTool 获取 A 股日 K 线数据的 Eino 工具。零值不可用，请用 NewMarketDataTool 构造。
type MarketDataTool struct {
	cfg    MarketDataConfig
	client *http.Client
}

// NewMarketDataTool 构造行情工具，校验配置合法性。
func NewMarketDataTool(cfg MarketDataConfig) (*MarketDataTool, error) {
	cfg.GatewayBaseURL = strings.TrimRight(strings.TrimSpace(cfg.GatewayBaseURL), "/")
	if cfg.GatewayBaseURL == "" {
		return nil, errors.New("market_data: GatewayBaseURL is required")
	}
	if !strings.HasPrefix(cfg.GatewayBaseURL, "http://") && !strings.HasPrefix(cfg.GatewayBaseURL, "https://") {
		return nil, fmt.Errorf("market_data: invalid GatewayBaseURL %q, expect http(s)://...", cfg.GatewayBaseURL)
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 15 * time.Second
	}
	if cfg.MaxBarsPerCall <= 0 {
		cfg.MaxBarsPerCall = 120
	}
	return &MarketDataTool{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HTTPTimeout},
	}, nil
}

// symbolPattern 6 位数字股票代码。
var symbolPattern = regexp.MustCompile(`^\d{6}$`)

// normalizeDate 把 YYYYMMDD / YYYY-MM-DD 统一为网关要求的 YYYY-MM-DD。
func normalizeDate(s string) (string, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{"2006-01-02", "20060102"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format("2006-01-02"), nil
		}
	}
	return "", fmt.Errorf("invalid date %q, expect YYYYMMDD or YYYY-MM-DD", s)
}

// Info 返回工具的 schema 元数据。Desc 严格按照产品要求撰写，
// 显式告知 LLM 用途、代码位数与日期格式，降低幻觉参数概率。
func (t *MarketDataTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "get_daily_kline",
		Desc: "获取A股指定股票的日K线数据（前复权），用于分析趋势。" +
			"参数 symbol 为6位数字股票代码（如 000001），" +
			"start_date 与 end_date 格式为 YYYYMMDD（如 20260101）。" +
			"返回按日期升序排列的 JSON 数组，每条含 date/open/close/high/low/volume/amount。",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"symbol": {
				Type:     schema.String,
				Desc:     "6 位数字股票代码，如 000001、600519",
				Required: true,
			},
			"start_date": {
				Type:     schema.String,
				Desc:     "起始日期，格式 YYYYMMDD",
				Required: true,
			},
			"end_date": {
				Type:     schema.String,
				Desc:     "结束日期，格式 YYYYMMDD",
				Required: true,
			},
		}),
	}, nil
}

// InvokableRun 执行工具调用：解析 LLM 传入的 JSON 参数 -> HTTP GET 网关 ->
// 归一化后返回 JSON 字符串。任何失败都以「LLM 可读的错误 JSON」返回，
// 而不是抛 Go error —— 让 Agent 有机会修正参数重试，避免整条 Graph 中断。
func (t *MarketDataTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	// 1. 解析参数
	var in MarketDataInput
	if err := json.Unmarshal([]byte(strings.TrimSpace(argumentsInJSON)), &in); err != nil {
		msg := fmt.Sprintf("arguments are not valid JSON: %v", err)
		log.Printf("market_data: parse args failed: %v", err)
		return errorResult(msg), nil
	}

	// 2. 参数校验
	in.Symbol = strings.TrimSpace(in.Symbol)
	if !symbolPattern.MatchString(in.Symbol) {
		return errorResult(fmt.Sprintf("symbol %q invalid: must be exactly 6 digits, e.g. 000001", in.Symbol)), nil
	}
	startDate, err := normalizeDate(in.StartDate)
	if err != nil {
		return errorResult("start_date: " + err.Error()), nil
	}
	endDate, err := normalizeDate(in.EndDate)
	if err != nil {
		return errorResult("end_date: " + err.Error()), nil
	}
	if startDate > endDate {
		return errorResult(fmt.Sprintf("start_date %s later than end_date %s", startDate, endDate)), nil
	}

	// 3. 调用网关
	url := fmt.Sprintf("%s/api/daily_kline?symbol=%s&start_date=%s&end_date=%s",
		t.cfg.GatewayBaseURL, in.Symbol, startDate, endDate)

	reqCtx, cancel := context.WithTimeout(ctx, t.cfg.HTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return errorResult(fmt.Sprintf("build request failed: %v", err)), nil
	}
	req.Header.Set("Accept", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		log.Printf("market_data: gateway request failed url=%s err=%v", url, err)
		return errorResult(fmt.Sprintf("data gateway unreachable: %v", err)), nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20)) // 8MB 防御性上限
	if err != nil {
		return errorResult(fmt.Sprintf("read gateway response failed: %v", err)), nil
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("market_data: gateway status=%d body=%.512s", resp.StatusCode, body)
		return errorResult(fmt.Sprintf("gateway returned HTTP %d: %.256s", resp.StatusCode, string(body))), nil
	}

	// 4. 解析响应（兼容裸数组与 {"data":[...]} 两种形态）
	var env klineResponseEnvelope
	if err := env.UnmarshalJSON(body); err != nil {
		log.Printf("market_data: decode kline failed: %v, raw=%.512s", err, body)
		return errorResult(fmt.Sprintf("decode kline response failed: %v", err)), nil
	}
	bars := env.bars

	// 5. 尾部截断，控制回灌给 LLM 的 token 规模
	truncated := false
	if len(bars) > t.cfg.MaxBarsPerCall {
		bars = bars[len(bars)-t.cfg.MaxBarsPerCall:]
		truncated = true
	}

	out := map[string]any{
		"ok":        true,
		"symbol":    in.Symbol,
		"count":     len(bars),
		"truncated": truncated,
		"klines":    bars,
	}
	result, err := json.Marshal(out)
	if err != nil {
		return errorResult(fmt.Sprintf("marshal result failed: %v", err)), nil
	}
	log.Printf("market_data: symbol=%s range=[%s,%s] bars=%d", in.Symbol, startDate, endDate, len(bars))
	return string(result), nil
}

// errorResult 统一生成「工具执行失败」的 JSON 返回值。
// ok=false + error 字段，便于 LLM 识别并按 Prompt 约定放弃/修正该数据源。
func errorResult(msg string) string {
	b, _ := json.Marshal(map[string]any{"ok": false, "error": msg})
	return string(b)
}
