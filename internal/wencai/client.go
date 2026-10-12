// 本文件复刻 pywencai/wencai.py + headers.py：HTTP 客户端、请求头、
// get_robot_data / get_page / loop_page / Get 主流程。
package wencai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// 三个上游端点常量仅作协议文档；实际请求 URL = c.baseURL + path，
// 以便 httptest 注入（Config.BaseURL）。
const (
	urlGetRobotData = "http://www.iwencai.com/customized/chart/get-robot-data"
	urlGetDataList  = "http://www.iwencai.com/gateway/urp/v7/landing/getDataList"
	urlFind         = "http://www.iwencai.com/unifiedwap/unified-wap/v2/stock-pick/find"
)

var _ = []string{urlGetRobotData, urlGetDataList, urlFind}

// userAgents 确定性 UA 轮转表（替代 fake_useragent，保证测试可复现）。
var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
}

// Config Client 构造参数。
type Config struct {
	// Retry while_do 语义：单次逻辑调用最多尝试次数，默认 10（同上游）。
	Retry int
	// SleepBetweenRetry 每次尝试前的固定间隔，默认 0（同上游 sleep=0）。
	SleepBetweenRetry time.Duration
	// HTTPTimeout 单次请求整体超时，默认 15s。
	HTTPTimeout time.Duration
	// Cookie 可选：登录态 cookie 字符串（问财部分高级查询需要）。
	Cookie string
	// UserAgent 固定 UA；为空时按 uaIndex 轮转。
	UserAgent string
	// Pro 问财专业版参数（iwcpro=1），默认 false。
	Pro bool
	// NodeBin node 可执行文件路径，空则 PATH 探测。
	NodeBin string
	// TokenTTL hexin-v token 缓存时长，<=0 用默认 10min。
	TokenTTL time.Duration
	// Log 打印重试/分页过程日志，默认 false（对应上游 log=False）。
	Log bool
	// BaseURL 覆盖 iwencai 域名前缀（httptest 集成测试注入用），默认 http://www.iwencai.com。
	BaseURL string
}

// Client 问财数据源客户端。零值不可用，请用 NewClient。并发安全。
type Client struct {
	cfg     Config
	http    *http.Client
	token   *TokenProvider
	uaIdx   int // 简单轮转计数；Go1.19 无 atomic.Int32，读多写少场景容忍非严格递增
	baseURL string
}

// NewClient 用零值安全的默认值构造客户端。
func NewClient(cfg Config) *Client {
	if cfg.Retry <= 0 {
		cfg.Retry = 10
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 15 * time.Second
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		base = "http://www.iwencai.com"
	}
	tp := NewTokenProvider(cfg.TokenTTL)
	tp.node = cfg.NodeBin
	return &Client{
		cfg:     cfg,
		http:    &http.Client{Timeout: cfg.HTTPTimeout},
		token:   tp,
		baseURL: base,
	}
}

// ---------------------------------------------------------------------------
// headers（复刻 headers.py）
// ---------------------------------------------------------------------------

func (c *Client) nextUA() string {
	if c.cfg.UserAgent != "" {
		return c.cfg.UserAgent
	}
	c.uaIdx++
	return userAgents[c.uaIdx%len(userAgents)]
}

// buildHeaders 生成一次请求所需 header；hexin-v token 来自 TTL 缓存。
func (c *Client) buildHeaders(ctx context.Context) (http.Header, error) {
	tk, err := c.token.Get(ctx)
	if err != nil {
		return nil, err
	}
	h := http.Header{}
	h.Set("hexin-v", tk)
	h.Set("User-Agent", c.nextUA())
	if c.cfg.Cookie != "" {
		h.Set("Cookie", c.cfg.Cookie)
	}
	return h, nil
}

// ---------------------------------------------------------------------------
// while_do 重试骨架（复刻 wencai.while_do：吞错误重试 retry 次，最终返回 nil 语义
// 在这里改为返回聚合 error，便于 Go 侧错误传播与日志定位）
// ---------------------------------------------------------------------------

func (c *Client) whileDo(ctx context.Context, desc string, do func() error) error {
	var lastErr error
	for count := 0; count < c.cfg.Retry; count++ {
		if c.cfg.SleepBetweenRetry > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.SleepBetweenRetry):
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = do()
		if lastErr == nil {
			return nil
		}
		if c.cfg.Log {
			log.Printf("[wencai] %s: 第%d次尝试失败: %v", desc, count+1, lastErr)
		}
	}
	return fmt.Errorf("%s failed after %d retries: %w", desc, c.cfg.Retry, lastErr)
}

// ---------------------------------------------------------------------------
// get_robot_data（复刻 wencai.get_robot_data）
// ---------------------------------------------------------------------------

// robotRequest 组装 get-robot-data 的 JSON body（字段顺序无关，内容 1:1 对齐上游）。
func (c *Client) robotRequest(query, queryType string) map[string]any {
	data := map[string]any{
		"add_info":         `{"urp":{"scene":1,"company":1,"business":1},"contentType":"json","searchInfo":true}`,
		"perpage":          "10",
		"page":             1,
		"source":           "Ths_iwencai_Xuangu",
		"log_info":         `{"input_type":"click"}`,
		"version":          "2.0",
		"secondary_intent": queryType,
		"question":         query,
	}
	if c.cfg.Pro {
		data["iwcpro"] = 1
	}
	return data
}

// RobotParams get_robot_data -> convert 的产物（对应 Python dict params）。
type RobotParams struct {
	Data      map[string]any    // xuangu_tableV1_handler 或 multi_show_type_handler 结果
	RowCount  int               // data.meta.extra.row_count（仅选股类有值）
	URL       string            // components[0].config.other_info.footer_info.url
	URLParams map[string]string // 从 URL query 解析出的分页参数
}

// getRobotData POST get-robot-data 并做 convert 结构化解析。
func (c *Client) getRobotData(ctx context.Context, query, queryType string) (*RobotParams, error) {
	body, err := json.Marshal(c.robotRequest(query, queryType))
	if err != nil {
		return nil, err
	}
	var params *RobotParams
	err = c.whileDo(ctx, "get_robot_data", func() error {
		hdrs, herr := c.buildHeaders(ctx)
		if herr != nil {
			return herr
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/customized/chart/get-robot-data", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		for k, vs := range hdrs {
			for _, v := range vs {
				req.Header.Set(k, v)
			}
		}
		resp, rerr := c.http.Do(req)
		if rerr != nil {
			return rerr
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		parsed, cerr := convertResponse(raw)
		if cerr != nil {
			return cerr
		}
		params = parsed
		return nil
	})
	if err != nil {
		if c.cfg.Log {
			log.Printf("[wencai] get_robot_data 失败: %v", err)
		}
		return nil, err
	}
	if c.cfg.Log {
		log.Printf("[wencai] get_robot_data 成功")
	}
	return params, nil
}

// ---------------------------------------------------------------------------
// get_page / loop_page（复刻分页取数）
// ---------------------------------------------------------------------------

// pageQuery 单页取数的全部参数。
type pageQuery struct {
	URLParams map[string]string
	Extra     map[string]string // condition/comp_id/uuid/sort_key 等透传表单字段
	Page      int
	Perpage   int
	QueryType string
	Find      []string // 非空走 find 接口（按代码列表精确查找）
}

// getPage 抓取一页数据，返回行列表 + 列顺序（等价 pd.DataFrame.from_dict）。
func (c *Client) getPage(ctx context.Context, q pageQuery) (*Table, error) {
	form := url.Values{}
	for k, v := range q.URLParams {
		form.Set(k, v)
	}
	for k, v := range q.Extra {
		form.Set(k, v)
	}
	form.Set("perpage", fmt.Sprint(q.Perpage))
	form.Set("page", fmt.Sprint(q.Page))

	target := c.baseURL + "/gateway/urp/v7/landing/getDataList"
	pathPrefix := []string{"answer", "components", "0", "data", "datas"}
	if len(q.Find) > 0 {
		target = c.baseURL + "/unifiedwap/unified-wap/v2/stock-pick/find"
		form.Set("query_type", q.QueryType)
		form.Set("question", strings.Join(q.Find, ","))
		pathPrefix = []string{"data", "data", "datas"}
	}
	if c.cfg.Pro {
		target += "?iwcpro=1"
	}

	var table *Table
	err := c.whileDo(ctx, fmt.Sprintf("第%d页", q.Page), func() error {
		hdrs, herr := c.buildHeaders(ctx)
		if herr != nil {
			return herr
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		for k, vs := range hdrs {
			for _, v := range vs {
				req.Header.Set(k, v)
			}
		}
		resp, rerr := c.http.Do(req)
		if rerr != nil {
			return rerr
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		var doc any
		if jerr := json.Unmarshal(raw, &doc); jerr != nil {
			return fmt.Errorf("page %d: invalid json: %w", q.Page, jerr)
		}
		node, ok := getPath(doc, pathPrefix...)
		if !ok {
			return fmt.Errorf("page %d: missing path %s in response", q.Page, strings.Join(pathPrefix, "."))
		}
		rows, cok := toRowList(node)
		if !cok || len(rows) == 0 {
			if c.cfg.Log {
				log.Printf("[wencai] 第%d页返回空！", q.Page)
			}
			return errors.New("data_list is empty!")
		}
		table = rowsToTable(rows)
		if c.cfg.Log {
			log.Printf("[wencai] 第%d页成功 rows=%d", q.Page, len(table.Rows))
		}
		return nil
	})
	if err != nil {
		if c.cfg.Log {
			log.Printf("[wencai] 第%d页失败: %v", q.Page, err)
		}
		return nil, err
	}
	return table, nil
}

// loopPage 按 row_count 分页拉全量并纵向拼接（复刻 loop_page + pd.concat）。
func (c *Client) loopPage(ctx context.Context, loopCount int, rowCount int, q pageQuery) (*Table, error) {
	var merged *Table
	for count := 0; count < loopCount; count++ {
		q.Page = q.startPage() + count
		page, err := c.getPage(ctx, q)
		if err != nil {
			return nil, err
		}
		if merged == nil {
			merged = page
		} else {
			merged.Append(page)
		}
	}
	if merged == nil {
		merged = &Table{}
	}
	return merged, nil
}

// startPage helper：loop_page 中 initPage 语义（getPage 已保证 Page 初值）。
func (q pageQuery) startPage() int { return q.Page }

// ---------------------------------------------------------------------------
// Get 主入口（复刻 pywencai.get）
// ---------------------------------------------------------------------------

// Options 对应 Python kwargs（question/retry/perpage/page/sort_key/find/no_detail 等）。
type Options struct {
	Query     string            // 自然语言问句，如 "近5日涨幅大于30%且市盈率小于30的股票"
	QueryType string            // secondary_intent，默认 stock
	Loop      bool              // true 时按 row_count 分页拉全量
	MaxLoop   int               // Loop=true 但想限制页数时使用（0=不限，等于 ceil(row_count/perpage)）
	Perpage   int               // 每页行数，默认 100
	Page      int               // 起始页，默认 1
	SortKey   string            // urp_sort_index
	SortOrder string            // urp_sort_way(up/down)
	Find      []string          // 指定股票代码列表 -> 走 find 接口
	NoDetail  bool              // condition 缺失时是否只返回元信息(默认返回 data)
	Extra     map[string]string // 其余透传表单字段
}

// Result Get 的统一返回值：命中选股表格时为 Table；问答/资讯类为 Structured。
type Result struct {
	Kind       string         // "table" | "structured" | "meta_only"
	Table      *Table         // Kind=table 时有值
	Structured map[string]any // Kind=structured：multi_show_type_handler 结果
	Meta       map[string]any // 始终附带：condition/comp_id/uuid/row_count/url 摘要
}

// Get 问财主查询入口。语义与 pywencai.get(loop=..., **kwargs) 对齐。
func (c *Client) Get(ctx context.Context, opt Options) (*Result, error) {
	if strings.TrimSpace(opt.Query) == "" {
		return nil, errors.New("wencai: Query is required")
	}
	if opt.QueryType == "" {
		opt.QueryType = "stock"
	}
	if opt.Perpage <= 0 {
		opt.Perpage = 100
	}
	if opt.Page <= 0 {
		opt.Page = 1
	}

	params, err := c.getRobotData(ctx, opt.Query, opt.QueryType)
	if err != nil {
		return nil, err
	}

	condition, _ := params.Data["condition"].(string)
	meta := map[string]any{
		"condition": condition,
		"row_count": params.RowCount,
		"url":       params.URL,
	}
	for _, k := range []string{"comp_id", "uuid"} {
		if v, ok := params.Data[k]; ok {
			meta[k] = v
		}
	}

	if condition == "" {
		// 非选股类回答（txt/龙虎榜/股东户数等 show_type 结构）
		if opt.NoDetail {
			return &Result{Kind: "meta_only", Meta: meta}, nil
		}
		sd, _ := params.Data["structured"].(map[string]any)
		return &Result{Kind: "structured", Structured: sd, Meta: meta}, nil
	}

	// ---- 选股链路：拼分页参数 ----
	extra := map[string]string{"condition": condition}
	if compID, ok := params.Data["comp_id"].(string); ok {
		extra["comp_id"] = compID
	}
	if uu, ok := params.Data["uuid"].(string); ok {
		extra["uuid"] = uu
	}
	if opt.SortKey != "" {
		extra["urp_sort_index"] = opt.SortKey
	}
	if opt.SortOrder != "" {
		extra["urp_sort_way"] = opt.SortOrder
	}
	for k, v := range opt.Extra {
		extra[k] = v
	}
	q := pageQuery{
		URLParams: params.URLParams,
		Extra:     extra,
		Page:      opt.Page,
		Perpage:   opt.Perpage,
		QueryType: opt.QueryType,
		Find:      opt.Find,
	}

	if len(opt.Find) > 0 || !opt.Loop {
		// find 模式或单页模式（Python: if loop and find is None ... else get_page）
		tbl, err := c.getPage(ctx, q)
		if err != nil {
			return nil, err
		}
		return &Result{Kind: "table", Table: tbl, Meta: meta}, nil
	}

	loopCount := int(math.Ceil(float64(params.RowCount) / float64(opt.Perpage)))
	if opt.MaxLoop > 0 && opt.MaxLoop < loopCount {
		loopCount = opt.MaxLoop
	}
	tbl, err := c.loopPage(ctx, loopCount, params.RowCount, q)
	if err != nil {
		return nil, err
	}
	return &Result{Kind: "table", Table: tbl, Meta: meta}, nil
}
