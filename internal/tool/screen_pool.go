// 本文件实现漏斗模型 Layer1 的 Eino 工具：批量粗筛候选股票池。
//
// 封装 Python 数据网关 POST /api/czsc/screen_pool 接口，把自选股列表交给
// czsc 引擎批量计算缠论结构，仅保留出现买卖点或趋势向上的候选股，
// 输出 JSON 直接透传给 LLM / Graph（Layer2 排雷精选的输入）。
//
// 设计规范与 czsc_analysis.go / market_data.go 保持一致：
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
	_ tool.BaseTool      = (*ScreenPoolTool)(nil)
	_ tool.InvokableTool = (*ScreenPoolTool)(nil)
)

// ---------------------------------------------------------------------------
// ScreenPoolTool 入参 / 出参结构（字段与网关 /api/czsc/screen_pool 对齐）
// ---------------------------------------------------------------------------

// ScreenPoolInput 是 LLM / Graph 传入的 JSON 参数反序列化目标。
type ScreenPoolInput struct {
	Symbols   []string `json:"symbols"`    // 股票代码列表, 如 ["603236","000001"]
	StartDate string   `json:"start_date"` // YYYYMMDD 或 YYYY-MM-DD, 可空
	EndDate   string   `json:"end_date"`   // 同上, 可空
	Period    string   `json:"period"`     // daily(默认)/weekly/monthly
}

// ScreenPoolHit 一只通过粗筛的候选股摘要。
type ScreenPoolHit struct {
	Symbol         string       `json:"symbol"`
	SignalType     string       `json:"signal_type"` // 3B/1B/3S/Observation 等
	BuyPoints      []czscSignal `json:"buy_points"`
	SellPoints     []czscSignal `json:"sell_points"`
	ZsCount        int          `json:"zs_count"`
	BiCount        int          `json:"bi_count"`
	TrendDirection string       `json:"trend_direction"`
	Summary        string       `json:"summary"`
}

type screenPoolData struct {
	TradeDate      string          `json:"trade_date"`
	Period         string          `json:"period"`
	Window         string          `json:"window"`
	TotalRequested int             `json:"total_requested"`
	TotalScanned   int             `json:"total_scanned"`
	PoolSize       int             `json:"pool_size"`
	Pool           []ScreenPoolHit `json:"pool"`
	Errors         []struct {
		Symbol string `json:"symbol"`
		Error  string `json:"error"`
	} `json:"errors"`
}

type screenPoolEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    *screenPoolData `json:"data"`
}

// ---------------------------------------------------------------------------
// ScreenPoolTool 实现
// ---------------------------------------------------------------------------

// ScreenPoolConfig 构造参数。
type ScreenPoolConfig struct {
	// GatewayBaseURL Python 数据网关地址，如 "http://127.0.0.1:8000"。必填。
	GatewayBaseURL string
	// HTTPTimeout 单次批量扫描超时。网关每只股票间隔 0.5s 限速，
	// 50 只股票约需 1~2 分钟，默认给 5 分钟。
	HTTPTimeout time.Duration
}

// ScreenPoolTool 批量粗筛候选股票池的 Eino 工具。零值不可用。
type ScreenPoolTool struct {
	cfg    ScreenPoolConfig
	client *http.Client
}

// NewScreenPoolTool 构造批量粗筛工具，校验配置合法性。
func NewScreenPoolTool(cfg ScreenPoolConfig) (*ScreenPoolTool, error) {
	cfg.GatewayBaseURL = strings.TrimRight(strings.TrimSpace(cfg.GatewayBaseURL), "/")
	if cfg.GatewayBaseURL == "" {
		return nil, errors.New("screen_pool: GatewayBaseURL is required")
	}
	if !strings.HasPrefix(cfg.GatewayBaseURL, "http://") && !strings.HasPrefix(cfg.GatewayBaseURL, "https://") {
		return nil, fmt.Errorf("screen_pool: invalid GatewayBaseURL %q, expect http(s)://...", cfg.GatewayBaseURL)
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 5 * time.Minute
	}
	return &ScreenPoolTool{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HTTPTimeout},
	}, nil
}

// Info 返回工具 schema。Desc 明确告知 LLM 这是「漏斗第一层」：
// 输出的候选池仍需结合新闻做基本面排雷后才能形成交易计划。
func (t *ScreenPoolTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "screen_czsc_pool",
		Desc: "批量粗筛候选股票池（缠论漏斗第一层）：对传入的一批A股代码逐一执行czsc缠论分析，" +
			"仅保留出现三类买卖点信号(1B/2B/3B/1S/2S/3S)或「至少2个中枢且最后一笔向上」的股票，" +
			"返回候选池JSON（含 signal_type 与中文 summary）。" +
			"参数 symbols 为6位数字代码字符串数组，start_date/end_date 为 YYYYMMDD(可省略)，period 默认 daily。" +
			"注意：候选池只是技术面初筛结果，必须再结合新闻/公告排雷后才能形成最终交易计划。",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"symbols": {
				Type:     schema.Array,
				Desc:     "股票代码列表，元素为6位数字字符串，如 [\"603236\",\"000001\"]，单次最多100只",
				Required: true,
				ElemInfo: &schema.ParameterInfo{Type: schema.String},
			},
			"start_date": {
				Type:     schema.String,
				Desc:     "起始日期 YYYYMMDD，可省略（默认近一年半）",
				Required: false,
			},
			"end_date": {
				Type:     schema.String,
				Desc:     "结束日期 YYYYMMDD，可省略（默认今天）",
				Required: false,
			},
			"period": {
				Type:     schema.String,
				Desc:     "K线周期：daily(默认)/weekly/monthly",
				Required: false,
			},
		}),
	}, nil
}

// InvokableRun 解析 JSON 参数 -> POST /api/czsc/screen_pool -> 透传候选池 JSON。
func (t *ScreenPoolTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	var in ScreenPoolInput
	if err := json.Unmarshal([]byte(strings.TrimSpace(argumentsInJSON)), &in); err != nil {
		return errorResult(fmt.Sprintf("arguments are not valid JSON: %v", err)), nil
	}

	// 参数清洗与校验
	if len(in.Symbols) == 0 {
		return errorResult("symbols is empty: at least one 6-digit stock code required"), nil
	}
	if len(in.Symbols) > 100 {
		return errorResult(fmt.Sprintf("too many symbols: %d, max 100 per call (AKShare rate limit)", len(in.Symbols))), nil
	}
	cleaned := make([]string, 0, len(in.Symbols))
	for _, s := range in.Symbols {
		s = strings.TrimSpace(s)
		if !symbolPattern.MatchString(s) {
			return errorResult(fmt.Sprintf("symbol %q invalid: must be exactly 6 digits, e.g. 000001", s)), nil
		}
		cleaned = append(cleaned, s)
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

	body := map[string]any{
		"symbols": cleaned,
		"period":  in.Period,
	}
	for _, d := range []struct {
		key, raw string
	}{{"start_date", in.StartDate}, {"end_date", in.EndDate}} {
		if strings.TrimSpace(d.raw) == "" {
			continue
		}
		norm, err := normalizeDate(d.raw)
		if err != nil {
			return errorResult(d.key + ": " + err.Error()), nil
		}
		body[d.key] = strings.ReplaceAll(norm, "-", "") // 网关要求 YYYYMMDD
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return errorResult(fmt.Sprintf("marshal request failed: %v", err)), nil
	}

	url := t.cfg.GatewayBaseURL + "/api/czsc/screen_pool"
	reqCtx, cancel := context.WithTimeout(ctx, t.cfg.HTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, strings.NewReader(string(payload)))
	if err != nil {
		return errorResult(fmt.Sprintf("build request failed: %v", err)), nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		log.Printf("screen_pool: gateway request failed url=%s err=%v", url, err)
		return errorResult(fmt.Sprintf("data gateway unreachable: %v", err)), nil
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20)) // 4MB 防御性上限
	if err != nil {
		return errorResult(fmt.Sprintf("read gateway response failed: %v", err)), nil
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("screen_pool: gateway status=%d body=%.512s", resp.StatusCode, raw)
		return errorResult(fmt.Sprintf("gateway returned HTTP %d: %.256s", resp.StatusCode, string(raw))), nil
	}

	var env screenPoolEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		log.Printf("screen_pool: decode failed: %v, raw=%.512s", err, raw)
		return errorResult(fmt.Sprintf("decode screen_pool response failed: %v", err)), nil
	}
	if env.Data == nil {
		return marshalOK("pool", map[string]any{
			"status":  env.Message,
			"summary": "网关未返回候选池数据",
		}), nil
	}

	d := env.Data
	log.Printf("screen_pool: scanned=%d pool=%d errors=%d window=%s",
		d.TotalScanned, d.PoolSize, len(d.Errors), d.Window)

	// 透传候选池 + 一句人类可读总结，方便 LLM 直接进入排雷环节
	out := map[string]any{
		"trade_date":    d.TradeDate,
		"window":        d.Window,
		"total_scanned": d.TotalScanned,
		"pool_size":     d.PoolSize,
		"pool":          d.Pool,
		"scan_errors":   d.Errors,
		"summary": fmt.Sprintf("共扫描 %d 只股票，%d 只通过缠论粗筛进入候选池（交易日 %s）。",
			d.TotalScanned, d.PoolSize, d.TradeDate),
	}
	return marshalOK("pool", out), nil
}
