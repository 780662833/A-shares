// 本文件实现漏斗 Layer3 的 Eino 工具：大盘环境查询（统一执行计划 Step 2）。
//
// 封装 Python 数据网关 GET /api/market/environment 接口，返回 regime
// （BULL/SIDEWAYS/BEAR）与 global_max_position_pct —— 后者是 Go 侧总仓位
// 硬闸门的唯一输入（决策 #3：BEAR 市处理以硬闸门为主，Prompt 软引导为辅）。
//
// 设计规范与 screen_pool.go / news_search.go 保持一致：
//   - Info(ctx) 返回 *schema.ToolInfo；InvokableRun 接收 JSON、返回 JSON 字符串；
//   - 失败以 {"ok":false,...} 返回而非抛 Go error，便于流水线降级。
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
	"sync"
	"time"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

var (
	_ tool.BaseTool      = (*MarketEnvironmentTool)(nil)
	_ tool.InvokableTool = (*MarketEnvironmentTool)(nil)
)

// ---------------------------------------------------------------------------
// 类型定义
// ---------------------------------------------------------------------------

// MarketEnvironmentConfig 构造参数。
type MarketEnvironmentConfig struct {
	// GatewayBaseURL Python 数据网关地址，如 "http://127.0.0.1:8000"。必填。
	GatewayBaseURL string
	// HTTPTimeout 单次请求超时，默认 10s。
	HTTPTimeout time.Duration
	// CacheTTL 进程内结果缓存时长，默认 5min（网关侧本身有 300s 缓存，
	// 这里主要防止同一批复盘流程重复打网关）。
	CacheTTL time.Duration
}

// MarketEnvironmentData 网关 data 字段中与风控相关的核心部分。
type MarketEnvironmentData struct {
	Index                string  `json:"index"`
	TradeDate            string  `json:"trade_date"`
	Close                float64 `json:"close"`
	Ma20                 float64 `json:"ma20"`
	Ma60                 float64 `json:"ma60"`
	ChangePct1D          float64 `json:"change_pct_1d"`
	ChangePct20D         float64 `json:"change_pct_20d"`
	Regime               string  `json:"regime"` // BULL / SIDEWAYS / BEAR
	GlobalMaxPositionPct float64 `json:"global_max_position_pct"`
}

// marketEnvironmentResult InvokableRun 的输出信封。
type marketEnvironmentResult struct {
	OK      bool                   `json:"ok"`
	Data    *MarketEnvironmentData `json:"data,omitempty"`
	Error   string                 `json:"error,omitempty"`
	Summary string                 `json:"summary,omitempty"`
}

// MarketEnvironmentTool 查询大盘环境的 Eino 工具。零值不可用。
type MarketEnvironmentTool struct {
	cfg    MarketEnvironmentConfig
	client *http.Client

	mu       sync.Mutex
	cacheAt  time.Time
	cacheVal *marketEnvironmentResult
}

// NewMarketEnvironmentTool 构造工具实例。
func NewMarketEnvironmentTool(cfg MarketEnvironmentConfig) (*MarketEnvironmentTool, error) {
	cfg.GatewayBaseURL = strings.TrimRight(strings.TrimSpace(cfg.GatewayBaseURL), "/")
	if cfg.GatewayBaseURL == "" {
		return nil, errors.New("market_environment: GatewayBaseURL is required")
	}
	if !strings.HasPrefix(cfg.GatewayBaseURL, "http://") && !strings.HasPrefix(cfg.GatewayBaseURL, "https://") {
		return nil, fmt.Errorf("market_environment: invalid GatewayBaseURL %q, expect http(s)://...", cfg.GatewayBaseURL)
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 10 * time.Second
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 5 * time.Minute
	}
	return &MarketEnvironmentTool{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HTTPTimeout},
	}, nil
}

// Info 返回工具 schema。
func (t *MarketEnvironmentTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "query_market_environment",
		Desc: "查询大盘市场环境：返回上证指数 regime（BULL/SIDEWAYS/BEAR）与 " +
			"global_max_position_pct（总仓位硬闸门上限）。该上限由服务端风控消费，" +
			"LLM 不得自行放大仓位。无参数。",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{}),
	}, nil
}

// InvokableRun 调用网关并返回 JSON 字符串结果（业务失败不抛 Go error）。
func (t *MarketEnvironmentTool) InvokableRun(ctx context.Context, _ string, _ ...tool.Option) (string, error) {
	t.mu.Lock()
	if t.cacheVal != nil && time.Since(t.cacheAt) < t.cfg.CacheTTL {
		cached := t.cacheVal
		t.mu.Unlock()
		b, _ := json.Marshal(cached)
		return string(b), nil
	}
	t.mu.Unlock()

	res := t.fetch(ctx)
	t.mu.Lock()
	t.cacheAt, t.cacheVal = time.Now(), res
	t.mu.Unlock()

	b, err := json.Marshal(res)
	if err != nil {
		return errorResult(fmt.Sprintf("market_environment: marshal result: %v", err)), nil
	}
	return string(b), nil
}

// fetch 实际发起 HTTP 请求；任何失败都降级为 ok=false（调用方决定保守闸门）。
func (t *MarketEnvironmentTool) fetch(ctx context.Context) *marketEnvironmentResult {
	url := t.cfg.GatewayBaseURL + "/api/market/environment"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return &marketEnvironmentResult{OK: false, Error: fmt.Sprintf("build request: %v", err)}
	}
	resp, err := t.client.Do(req)
	if err != nil {
		log.Printf("market_environment: WARN gateway unreachable: %v", err)
		return &marketEnvironmentResult{OK: false, Error: fmt.Sprintf("gateway request failed: %v", err)}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return &marketEnvironmentResult{OK: false, Error: fmt.Sprintf("read body: %v", err)}
	}
	if resp.StatusCode != http.StatusOK {
		return &marketEnvironmentResult{OK: false,
			Error: fmt.Sprintf("gateway status %d: %.200s", resp.StatusCode, string(body))}
	}

	var env struct {
		Code    int                   `json:"code"`
		Message string                `json:"message"`
		Data    MarketEnvironmentData `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return &marketEnvironmentResult{OK: false, Error: fmt.Sprintf("decode envelope: %v", err)}
	}
	if env.Code != 0 || env.Data.Regime == "" {
		return &marketEnvironmentResult{OK: false,
			Error: fmt.Sprintf("gateway business error: code=%d message=%.120s", env.Code, env.Message)}
	}
	data := env.Data
	return &marketEnvironmentResult{
		OK:   true,
		Data: &data,
		Summary: fmt.Sprintf("regime=%s global_max_position_pct=%.2f (close=%.2f ma20=%.2f ma60=%.2f)",
			data.Regime, data.GlobalMaxPositionPct, data.Close, data.Ma20, data.Ma60),
	}
}

// GlobalMaxPositionPct 便捷读取：供 Graph 节点直接取闸门值。
// 工具不可用/网关失败时返回 (0,false)，调用方必须按最保守口径处理。
func (t *MarketEnvironmentTool) GlobalMaxPositionPct(ctx context.Context) (float64, bool) {
	raw, err := t.InvokableRun(ctx, "{}")
	if err != nil {
		return 0, false
	}
	var res marketEnvironmentResult
	if uerr := json.Unmarshal([]byte(raw), &res); uerr != nil || !res.OK || res.Data == nil {
		return 0, false
	}
	p := res.Data.GlobalMaxPositionPct
	if p <= 0 || p > 1 {
		return 0, false
	}
	return p, true
}
