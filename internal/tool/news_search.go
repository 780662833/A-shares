// 本文件实现漏斗模型 Layer2 的新闻/公告获取工具。
//
// MVP 阶段策略（与架构师约定）：
//   - 优先尝试 AKShare 经分接口 stock_news_economy? 不——Go 端无法直接调 AKShare，
//     新闻源同样走 Python 数据网关；网关暂无 /api/news 接口时优雅降级为 Mock 数据，
//     保证 Eino Graph 在离线/开发环境可端到端跑通排雷流程。
//   - Mock 数据覆盖典型「地雷」场景（业绩预亏/高管减持/立案调查/行业利空）
//     与利好场景（业绩大增/回购/中标），供 LLM 练习排雷判断。
//
// 设计规范与 czsc_analysis.go 一致：失败以 {"ok":false,...} 返回而非抛 error。
package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	_ tool.BaseTool      = (*NewsSearchTool)(nil)
	_ tool.InvokableTool = (*NewsSearchTool)(nil)
)

// ---------------------------------------------------------------------------
// NewsSearchTool 入参 / 出参结构
// ---------------------------------------------------------------------------
// NewsInput LLM / Graph 传入的 JSON 参数。
type NewsInput struct {
	Symbol string `json:"symbol"` // 6 位数字股票代码
	Limit  int    `json:"limit"`  // 期望新闻条数, 默认 5, 上限 10
}

// NewsItem 一条新闻/公告摘要。Sentiment 为粗标注情绪标签, 便于 LLM 快速排雷。
type NewsItem struct {
	Title     string `json:"title"`
	Summary   string `json:"summary"`
	Published string `json:"published"` // "YYYY-MM-DD"
	Source    string `json:"source"`    // 东方财富 / 巨潮公告 / Mock
	Sentiment string `json:"sentiment"` // positive / negative / neutral
	RedFlag   bool   `json:"red_flag"`  // 是否命中硬雷关键词(减持/立案/预亏等)
}

// redFlagKeywords 排雷关键词表（负向硬信号）。
var redFlagKeywords = []string{
	"预亏", "亏损", "减持", "立案", "调查", "处罚", "警示函", "退市",
	"商誉减值", "质押爆仓", "违规", "诉讼", "暂停上市", "实施ST",
}

// positiveKeywords 利好关键词表。
var positiveKeywords = []string{
	"增长", "大增", "扭亏", "回购", "中标", "涨停", "获批", "分红", "订单", "突破",
}

func classifySentiment(title, summary string) (sentiment string, redFlag bool) {
	text := title + summary
	for _, k := range redFlagKeywords {
		if strings.Contains(text, k) {
			return "negative", true
		}
	}
	for _, k := range positiveKeywords {
		if strings.Contains(text, k) {
			return "positive", false
		}
	}
	return "neutral", false
}

// mockNewsFor 基于代码尾号确定性生成 Mock 新闻（同代码每次结果一致, 便于测试断言）。
func mockNewsFor(symbol string, limit int) []NewsItem {
	if limit <= 0 || limit > 10 {
		limit = 5
	}
	tail := 0
	for _, ch := range symbol {
		if ch >= '0' && ch <= '9' {
			tail = int(ch - '0')
		}
	}
	pool := []NewsItem{
		{Title: symbol + " 发布年度业绩预告：净利润同比增长 45%-60%", Summary: "公司主营景气度上行, 机构上调盈利预测。"},
		{Title: symbol + " 控股股东拟减持不超过总股本 2%", Summary: "减持原因为股东自身资金需求, 或对市场情绪形成短期压制。"},
		{Title: symbol + " 收到证监会立案告知书", Summary: "因涉嫌信息披露违规被立案调查, 存在退市风险警示可能。"},
		{Title: symbol + " 中标 8.6 亿元重大合同", Summary: "合同履行预计对明年营收产生积极影响。"},
		{Title: symbol + " 发布回购报告书：拟以 1-2 亿元自有资金回购", Summary: "回购价格上限较现价有约 30% 空间, 体现管理层信心。"},
		{Title: symbol + " 三季度报告：营收平稳, 净利润同比小幅下滑 5%", Summary: "毛利率基本稳定, 无重大经营风险提示。"},
		{Title: symbol + " 关于产品降价的行业点评", Summary: "行业价格战加剧, 分析师下调板块盈利预期。"},
	}
	items := make([]NewsItem, 0, limit)
	for i := 0; i < limit; i++ {
		it := pool[(tail+i)%len(pool)]
		it.Published = time.Now().AddDate(0, 0, -(i * 3)).Format("2006-01-02")
		it.Source = "Mock"
		it.Sentiment, it.RedFlag = classifySentiment(it.Title, it.Summary)
		items = append(items, it)
	}
	return items
}

// ---------------------------------------------------------------------------
// NewsSearchTool 实现
// ---------------------------------------------------------------------------
// NewsSearchConfig 构造参数。
type NewsSearchConfig struct {
	// GatewayBaseURL Python 数据网关地址。为空则直接使用 Mock 数据。
	GatewayBaseURL string
	// HTTPTimeout 单次新闻请求超时, 默认 10s。
	HTTPTimeout time.Duration
}

// NewsSearchTool 获取指定股票近期新闻/公告的 Eino 工具。零值不可用。
type NewsSearchTool struct {
	cfg    NewsSearchConfig
	client *http.Client
}

// NewNewsSearchTool 构造新闻检索工具。
func NewNewsSearchTool(cfg NewsSearchConfig) (*NewsSearchTool, error) {
	cfg.GatewayBaseURL = strings.TrimRight(strings.TrimSpace(cfg.GatewayBaseURL), "/")
	if cfg.GatewayBaseURL != "" &&
		!strings.HasPrefix(cfg.GatewayBaseURL, "http://") &&
		!strings.HasPrefix(cfg.GatewayBaseURL, "https://") {
		return nil, fmt.Errorf("news_search: invalid GatewayBaseURL %q, expect http(s)://...", cfg.GatewayBaseURL)
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 10 * time.Second
	}
	return &NewsSearchTool{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HTTPTimeout},
	}, nil
}

// Info 返回工具 schema。
func (t *NewsSearchTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "search_stock_news",
		Desc: "获取指定A股股票近期的新闻/公告列表（含标题、摘要、日期与情绪标签 sentiment、" +
			"red_flag 硬雷标记）。用于对候选池股票进行基本面排雷：" +
			"若出现业绩预亏、高管减持、立案调查、行业重大利空等负面新闻，应放弃该股。" +
			"参数 symbol 为6位数字股票代码，limit 为期望条数(默认5)。",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"symbol": {
				Type:     schema.String,
				Desc:     "6 位数字股票代码，如 000001",
				Required: true,
			},
			"limit": {
				Type:     schema.Integer,
				Desc:     "返回新闻条数，默认5，最大10",
				Required: false,
			},
		}),
	}, nil
}

// InvokableRun 解析参数 -> 尝试网关 /api/news -> 失败降级 Mock -> 返回 JSON。
func (t *NewsSearchTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	var in NewsInput
	if err := json.Unmarshal([]byte(strings.TrimSpace(argumentsInJSON)), &in); err != nil {
		return errorResult(fmt.Sprintf("arguments are not valid JSON: %v", err)), nil
	}
	in.Symbol = strings.TrimSpace(in.Symbol)
	if !symbolPattern.MatchString(in.Symbol) {
		return errorResult(fmt.Sprintf("symbol %q invalid: must be exactly 6 digits, e.g. 000001", in.Symbol)), nil
	}
	if in.Limit < 0 || in.Limit > 10 {
		in.Limit = 5
	}
	items, source, err := t.fetchFromGateway(ctx, in.Symbol, in.Limit)
	if err != nil {
		// 网关不可用/接口未上线 -> 优雅降级到 Mock, 保证开发链路不断
		log.Printf("news_search: gateway unavailable for %s (%v), fallback to mock data", in.Symbol, err)
		items = mockNewsFor(in.Symbol, in.Limit)
		source = "mock"
	}
	neg, flags := 0, 0
	for _, it := range items {
		if it.Sentiment == "negative" {
			neg++
		}
		if it.RedFlag {
			flags++
		}
	}
	out := map[string]any{
		"symbol":         in.Symbol,
		"source":         source,
		"news_count":     len(items),
		"negative_count": neg,
		"red_flag_count": flags,
		"news":           items,
		"summary": fmt.Sprintf("%s 近期共 %d 条新闻/公告, 其中负面 %d 条, 命中硬雷关键词 %d 条。",
			in.Symbol, len(items), neg, flags),
	}
	return marshalOK(in.Symbol, out), nil
}

// fetchFromGateway 尝试从 Python 网关 GET /api/news 拉真实新闻。
// 网关未提供该接口(404)或不可达时返回 error, 由调用方降级 Mock。
func (t *NewsSearchTool) fetchFromGateway(ctx context.Context, symbol string, limit int) ([]NewsItem, string, error) {
	if t.cfg.GatewayBaseURL == "" {
		return nil, "", errors.New("gateway base url not configured")
	}
	url := fmt.Sprintf("%s/api/news?symbol=%s&limit=%d", t.cfg.GatewayBaseURL, symbol, limit)
	reqCtx, cancel := context.WithTimeout(ctx, t.cfg.HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("gateway news returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, "", err
	}
	var env struct {
		Code int `json:"code"`
		Data struct {
			News []NewsItem `json:"news"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, "", fmt.Errorf("decode news response: %w", err)
	}
	if len(env.Data.News) == 0 {
		return nil, "", errors.New("gateway returned empty news list")
	}
	// 补齐情绪标签（网关侧若已带 sentiment 则保留）
	for i := range env.Data.News {
		it := &env.Data.News[i]
		if it.Sentiment == "" {
			it.Sentiment, it.RedFlag = classifySentiment(it.Title, it.Summary)
		} else if it.Sentiment == "negative" {
			it.RedFlag = true
		}
	}
	return env.Data.News, "gateway", nil
}
