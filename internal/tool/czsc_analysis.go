// 本文件将 Python 数据网关的 czsc 缠论分析接口 (/api/czsc/analyze)
// 封装为 Eino (cloudwego/eino) 的 InvokableTool，供复盘 Graph 的 FetchData 节点调用。
//
// 设计原则（与 market_data.go 保持同一规范）：
//   - Info(ctx) 返回 schema.ToolInfo，InvokableRun 接收 JSON 参数、返回 JSON 字符串；
//   - 任何失败都以 {"ok":false,"error":...} 形式返回而非抛 Go error，
//     让 LLM/Graph 有机会降级，而不是中断整条复盘流水线；
//   - 遵循专家建议第 3 条：优先把网关返回的自然语言 summary 回灌给 LLM，
//     并附带精简后的买卖点信号，避免 LLM 直接啃深层嵌套 JSON。
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
	_ tool.BaseTool      = (*CzscAnalysisTool)(nil)
	_ tool.InvokableTool = (*CzscAnalysisTool)(nil)
)

// ---------------------------------------------------------------------------
// CzscAnalysisTool 入参 / 出参结构（字段与 data_gateway /api/czsc/analyze 对齐）
// ---------------------------------------------------------------------------

// CzscInput 是 LLM / Graph 传入的 JSON 参数反序列化目标。
type CzscInput struct {
	Symbol    string `json:"symbol"`     // 6 位数字股票代码，如 "000001"
	Period    string `json:"period"`     // daily / weekly / monthly，默认 daily
	StartDate string `json:"start_date"` // YYYYMMDD 或 YYYY-MM-DD，可空（网关默认近一年半）
	EndDate   string `json:"end_date"`   // 同上，可空
}

// czscBi 单笔结构（只保留决策相关字段）。
type czscBi struct {
	Direction   string  `json:"direction"` // up / down
	StartDt     string  `json:"start_dt"`
	EndDt       string  `json:"end_dt"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Length      int     `json:"length"`
	PowerPrice  float64 `json:"power_price"`
}

// czscZS 最近中枢。
type czscZS struct {
	Zg       float64 `json:"zg"`
	Zd       float64 `json:"zd"`
	StartDt  string  `json:"start_dt"`
	EndDt    string  `json:"end_dt"`
	BiCount  int     `json:"bi_count"`
}

// czscSignal 一个买/卖点信号。
//
// Phase A（Step1 买点结构化增强）：每个买卖点附带双止损线与消前视确认时间。
// 三个新字段均为指针语义的零值兼容设计——旧版网关不返回时保持 0/空串，
// Go 端 risk.StopPlan 会把 0 视为"该线缺失"并自动降级到另一条线。
type czscSignal struct {
	Type    string  `json:"type"`     // 1B/2B/3B 或 1S/2S/3S
	NameCn  string  `json:"name_cn"`  // 中文名，如 "第三类买点"
	Price   float64 `json:"price"`
	Dt      string  `json:"dt"`
	Reason  string  `json:"reason"`

	// ---- Phase A 增强字段（来自 Python czsc_engine 的结构化输出）----
	StopLoss         float64 `json:"stop_loss"`          // 价格止损线（回抽低点×0.99；卖点时为反弹高点×1.01）
	InvalidationLine float64 `json:"invalidation_line"`  // 结构失效线（三买=中枢ZG / 三卖=中枢ZD）
	ConfirmedAt      string  `json:"confirmed_at"`       // 信号确认时间（笔末端+1根K线, ISO 格式），消除前视偏差
}

// czscGatewayData 网关 data 字段。
type czscGatewayData struct {
	Symbol    string `json:"symbol"`
	Freq      string `json:"freq"`
	Structures struct {
		BiCount int     `json:"bi_count"`
		ZsCount int     `json:"zs_count"`
		LastBi  *czscBi `json:"last_bi"`
		LastZs  *czscZS `json:"last_zs"`
	} `json:"structures"`
	Signals struct {
		BuyPoints  []czscSignal `json:"buy_points"`
		SellPoints []czscSignal `json:"sell_points"`
	} `json:"signals"`
	Summary string `json:"summary"`
}

// czscGatewayEnvelope 网关统一响应包裹 {code, message, symbol, data}。
type czscGatewayEnvelope struct {
	Code    int              `json:"code"`
	Message string           `json:"message"`
	Symbol  string           `json:"symbol"`
	Data    *czscGatewayData `json:"data"`
}

// ---------------------------------------------------------------------------
// CzscAnalysisTool 实现
// ---------------------------------------------------------------------------

// CzscAnalysisConfig 构造参数。
type CzscAnalysisConfig struct {
	// GatewayBaseURL Python 数据网关地址，如 "http://127.0.0.1:8000"。必填。
	GatewayBaseURL string
	// HTTPTimeout 单次请求超时。czsc 计算 + AKShare 拉取较慢，默认 30s。
	HTTPTimeout time.Duration
}

// CzscAnalysisTool 缠论结构 + 三类买卖点信号的 Eino 工具。零值不可用。
type CzscAnalysisTool struct {
	cfg    CzscAnalysisConfig
	client *http.Client
}

// NewCzscAnalysisTool 构造缠论分析工具，校验配置合法性。
func NewCzscAnalysisTool(cfg CzscAnalysisConfig) (*CzscAnalysisTool, error) {
	cfg.GatewayBaseURL = strings.TrimRight(strings.TrimSpace(cfg.GatewayBaseURL), "/")
	if cfg.GatewayBaseURL == "" {
		return nil, errors.New("czsc_analysis: GatewayBaseURL is required")
	}
	if !strings.HasPrefix(cfg.GatewayBaseURL, "http://") && !strings.HasPrefix(cfg.GatewayBaseURL, "https://") {
		return nil, fmt.Errorf("czsc_analysis: invalid GatewayBaseURL %q, expect http(s)://...", cfg.GatewayBaseURL)
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 30 * time.Second
	}
	return &CzscAnalysisTool{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HTTPTimeout},
	}, nil
}

// Info 返回工具 schema 元数据。Desc 明确告知 LLM：这是「找位置」的结构化证据，
// 最终买卖决策仍由 LLM 结合基本面/仓位规则综合判断。
func (t *CzscAnalysisTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "analyze_czsc_structure",
		Desc: "对指定A股股票执行缠论(czsc)结构分析：识别分型/笔/中枢，并输出标准三类买卖点信号" +
			"(一买/二买/三买及对应卖点)与自然语言摘要。用于判断当前技术形态是否处于买点或卖点。" +
			"参数 symbol 为6位数字股票代码(如 000001)，period 可选 daily/weekly/monthly(默认 daily)，" +
			"start_date/end_date 为 YYYYMMDD(可省略，默认最近约500个交易日)。" +
			"返回 ok=true 时含 summary(中文摘要)、buy_points/sell_points(带 reason 的买卖点列表)、last_zs(最近中枢区间)。",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"symbol": {
				Type:     schema.String,
				Desc:     "6 位数字股票代码，如 000001、600519",
				Required: true,
			},
			"period": {
				Type:     schema.String,
				Desc:     "K线周期：daily(默认)/weekly/monthly",
				Required: false,
			},
			"start_date": {
				Type:     schema.String,
				Desc:     "起始日期 YYYYMMDD，可省略",
				Required: false,
			},
			"end_date": {
				Type:     schema.String,
				Desc:     "结束日期 YYYYMMDD，可省略",
				Required: false,
			},
		}),
	}, nil
}

// InvokableRun 解析 JSON 参数 -> GET /api/czsc/analyze -> 精简归一化后返回 JSON 字符串。
func (t *CzscAnalysisTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	// 1. 解析参数
	var in CzscInput
	if err := json.Unmarshal([]byte(strings.TrimSpace(argumentsInJSON)), &in); err != nil {
		return errorResult(fmt.Sprintf("arguments are not valid JSON: %v", err)), nil
	}

	// 2. 参数校验（复用 market_data.go 的 symbolPattern / normalizeDate）
	in.Symbol = strings.TrimSpace(in.Symbol)
	if !symbolPattern.MatchString(in.Symbol) {
		return errorResult(fmt.Sprintf("symbol %q invalid: must be exactly 6 digits, e.g. 000001", in.Symbol)), nil
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

	q := fmt.Sprintf("symbol=%s&period=%s", in.Symbol, in.Period)
	for _, d := range []struct {
		name, raw string
	}{{"start_date", in.StartDate}, {"end_date", in.EndDate}} {
		if strings.TrimSpace(d.raw) == "" {
			continue // 留空由网关取默认区间
		}
		norm, err := normalizeDate(d.raw)
		if err != nil {
			return errorResult(d.name + ": " + err.Error()), nil
		}
		q += "&" + d.name + "=" + strings.ReplaceAll(norm, "-", "") // 网关要求 YYYYMMDD
	}

	// 3. 调用网关
	url := t.cfg.GatewayBaseURL + "/api/czsc/analyze?" + q
	reqCtx, cancel := context.WithTimeout(ctx, t.cfg.HTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return errorResult(fmt.Sprintf("build request failed: %v", err)), nil
	}
	req.Header.Set("Accept", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		log.Printf("czsc_analysis: gateway request failed url=%s err=%v", url, err)
		return errorResult(fmt.Sprintf("data gateway unreachable: %v", err)), nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20)) // 4MB 防御性上限
	if err != nil {
		return errorResult(fmt.Sprintf("read gateway response failed: %v", err)), nil
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("czsc_analysis: gateway status=%d body=%.512s", resp.StatusCode, body)
		return errorResult(fmt.Sprintf("gateway returned HTTP %d: %.256s", resp.StatusCode, string(body))), nil
	}

	// 4. 解析网关响应
	var env czscGatewayEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		log.Printf("czsc_analysis: decode failed: %v, raw=%.512s", err, body)
		return errorResult(fmt.Sprintf("decode czsc response failed: %v", err)), nil
	}
	if env.Data == nil {
		// no_data / insufficient_data：行情缺失不算工具故障，如实告知 LLM
		return marshalOK(in.Symbol, map[string]any{
			"status":  env.Message,
			"summary": fmt.Sprintf("%s 在指定区间内无可用行情数据或K线数量不足，无法进行缠论分析", in.Symbol),
		}), nil
	}

	// 5. 精简归一化：summary 前置 + 关键结构 + 带 reason 的买卖点（LLM 友好）
	d := env.Data
	out := map[string]any{
		"symbol":      d.Symbol,
		"freq":        d.Freq,
		"summary":     d.Summary,
		"bi_count":    d.Structures.BiCount,
		"zs_count":    d.Structures.ZsCount,
		"last_bi":     d.Structures.LastBi,
		"last_zs":     d.Structures.LastZs,
		"buy_points":  d.Signals.BuyPoints,
		"sell_points": d.Signals.SellPoints,
	}
	log.Printf("czsc_analysis: symbol=%s period=%s bi=%d zs=%d buys=%d sells=%d",
		in.Symbol, in.Period, d.Structures.BiCount, d.Structures.ZsCount,
		len(d.Signals.BuyPoints), len(d.Signals.SellPoints))
	return marshalOK(in.Symbol, out), nil
}

// marshalOK 统一生成 {"ok":true, ...payload} 的 JSON 返回值。
func marshalOK(symbol string, payload map[string]any) string {
	payload["ok"] = true
	payload["symbol"] = symbol
	b, err := json.Marshal(payload)
	if err != nil {
		b, _ = json.Marshal(map[string]any{"ok": false, "error": "marshal result failed: " + err.Error()})
	}
	return string(b)
}
