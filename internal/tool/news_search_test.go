package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// news_search.go 单元测试（search_stock_news 工具）
//   - classifySentiment 情绪/硬雷关键词分类
//   - mockNewsFor       Mock 数据确定性生成
//   - NewNewsSearchTool 构造参数校验
//   - InvokableRun      入参校验 / 网关成功 / 网关降级 Mock（httptest 模拟 Python 网关）
//   - TestNewsRealDataSource 真实新闻数据源集成测试（AKShare 东财 stock_news_em，
//     默认 skip，需显式开启：ASHARE_NEWS_REAL_TEST=1 go test -run TestNewsRealDataSource ./internal/tool/ -v）
// ---------------------------------------------------------------------------

func TestClassifySentiment(t *testing.T) {
	cases := []struct {
		name           string
		title, summary string
		wantSentiment  string
		wantRedFlag    bool
	}{
		{"业绩预亏命中硬雷", "某公司发布业绩预告：全年预亏", "亏损扩大", "negative", true},
		{"立案调查命中硬雷", "收到证监会立案告知书", "涉嫌信息披露违规", "negative", true},
		{"减持命中硬雷", "控股股东拟减持不超过2%", "资金需求", "negative", true},
		{"利好不含硬雷", "净利润同比增长45%", "景气度上行", "positive", false},
		{"回购为利好", "发布回购报告书", "自有资金回购", "positive", false},
		{"中性描述", "召开年度股东大会", "审议常规议案", "neutral", false},
		{"负向优先于正向", "营收增长但商誉减值风险上升", "提示关注", "negative", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sent, flag := classifySentiment(tc.title, tc.summary)
			if sent != tc.wantSentiment || flag != tc.wantRedFlag {
				t.Fatalf("classifySentiment(%q,%q) = (%q,%v), want (%q,%v)",
					tc.title, tc.summary, sent, flag, tc.wantSentiment, tc.wantRedFlag)
			}
		})
	}
}

func TestMockNewsForDeterministic(t *testing.T) {
	a := mockNewsFor("600519", 5)
	b := mockNewsFor("600519", 5)
	if len(a) != 5 {
		t.Fatalf("mockNewsFor limit=5 got %d items", len(a))
	}
	for i := range a {
		if a[i].Title != b[i].Title {
			t.Fatalf("mock not deterministic at %d: %q vs %q", i, a[i].Title, b[i].Title)
		}
		if a[i].Source != "Mock" {
			t.Fatalf("mock item source = %q, want Mock", a[i].Source)
		}
		if a[i].Sentiment == "" {
			t.Fatalf("mock item sentiment empty")
		}
	}
	// limit 越界(0/11)时回退默认 5 条
	if got := len(mockNewsFor("000001", 0)); got != 5 {
		t.Fatalf("limit=0 fallback want 5, got %d", got)
	}
	if got := len(mockNewsFor("000001", 11)); got != 5 {
		t.Fatalf("limit=11 fallback want 5, got %d", got)
	}
	// 不同代码尾号 -> 首条内容不同（覆盖地雷与利好场景池）
	if mockNewsFor("600519", 1)[0].Title == mockNewsFor("600518", 1)[0].Title {
		t.Fatalf("different symbols produced identical first item")
	}
}

func TestNewNewsSearchToolConfig(t *testing.T) {
	t.Run("invalid gateway url rejected", func(t *testing.T) {
		if _, err := NewNewsSearchTool(NewsSearchConfig{GatewayBaseURL: "127.0.0.1:8000"}); err == nil {
			t.Fatal("expect error for GatewayBaseURL without http(s) scheme")
		}
	})
	t.Run("empty gateway ok and timeout default", func(t *testing.T) {
		tool, err := NewNewsSearchTool(NewsSearchConfig{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tool.cfg.HTTPTimeout != 10*time.Second {
			t.Fatalf("default HTTPTimeout = %v, want 10s", tool.cfg.HTTPTimeout)
		}
	})
	t.Run("trailing slash trimmed", func(t *testing.T) {
		tool, err := NewNewsSearchTool(NewsSearchConfig{GatewayBaseURL: "http://127.0.0.1:8000///"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tool.cfg.GatewayBaseURL != "http://127.0.0.1:8000" {
			t.Fatalf("GatewayBaseURL = %q, want trailing slashes trimmed", tool.cfg.GatewayBaseURL)
		}
	})
}

// decodeNewsResult 解析工具返回的 {"ok":true,...} JSON。
func decodeNewsResult(t *testing.T, raw string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("result not valid JSON: %v\nraw=%s", err, raw)
	}
	if ok, _ := m["ok"].(bool); !ok {
		t.Fatalf("result ok=false: %v", m["error"])
	}
	return m
}

func assertNewsItems(t *testing.T, m map[string]any) []NewsItem {
	t.Helper()
	b, err := json.Marshal(m["news"])
	if err != nil {
		t.Fatalf("re-marshal news: %v", err)
	}
	var items []NewsItem
	if err := json.Unmarshal(b, &items); err != nil {
		t.Fatalf("decode news items: %v", err)
	}
	count, _ := m["news_count"].(float64)
	if int(count) != len(items) {
		t.Fatalf("news_count=%v but %d items decoded", m["news_count"], len(items))
	}
	return items
}

func TestNewsInvokableRunInvalidInput(t *testing.T) {
	tool, err := NewNewsSearchTool(NewsSearchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		args string
		want string // 期望 error 中包含的关键字
	}{
		{"not json", `{"symbol":`, "valid JSON"},
		{"short symbol", `{"symbol":"519","limit":5}`, "6 digits"},
		{"non numeric", `{"symbol":"abcdef"}`, "6 digits"},
		{"empty symbol", `{}`, "6 digits"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := tool.InvokableRun(context.Background(), tc.args)
			if err != nil {
				t.Fatalf("tool should return error in payload, got go error: %v", err)
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(raw), &m); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}
			if ok, _ := m["ok"].(bool); ok {
				t.Fatalf("expect ok=false for invalid input, got %s", raw)
			}
			msg, _ := m["error"].(string)
			if !strings.Contains(msg, tc.want) {
				t.Fatalf("error = %q, want contains %q", msg, tc.want)
			}
		})
	}
}

func TestNewsInvokableRunMockFallback(t *testing.T) {
	// 未配置网关 -> fetchFromGateway 直接报错 -> 优雅降级 Mock
	tool, err := NewNewsSearchTool(NewsSearchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tool.InvokableRun(context.Background(), `{"symbol":"000001","limit":3}`)
	if err != nil {
		t.Fatal(err)
	}
	m := decodeNewsResult(t, raw)
	if m["source"] != "mock" {
		t.Fatalf("source = %v, want mock", m["source"])
	}
	if m["symbol"] != "000001" {
		t.Fatalf("symbol = %v, want 000001", m["symbol"])
	}
	items := assertNewsItems(t, m)
	if len(items) != 3 {
		t.Fatalf("want 3 items, got %d", len(items))
	}
	if summary, _ := m["summary"].(string); !strings.Contains(summary, "000001") {
		t.Fatalf("summary %q missing symbol", summary)
	}
}

func TestNewsInvokableRunGatewaySuccess(t *testing.T) {
	var gotPath, gotSymbol, gotLimit string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSymbol = r.URL.Query().Get("symbol")
		gotLimit = r.URL.Query().Get("limit")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"code":0,"data":{"news":[
			{"title":"因涉嫌信披违规被立案调查","summary":"证监会已下发告知书","published":"2026-10-01","source":"东方财富"},
			{"title":"净利润同比大增60%","summary":"机构上调评级","published":"2026-09-20","source":"东方财富","sentiment":"positive"}
		]}}`)
	}))
	defer srv.Close()

	tool, err := NewNewsSearchTool(NewsSearchConfig{GatewayBaseURL: srv.URL + "/"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tool.InvokableRun(context.Background(), `{"symbol":"600519","limit":2}`)
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/news" || gotSymbol != "600519" || gotLimit != "2" {
		t.Fatalf("gateway request path=%s symbol=%s limit=%s", gotPath, gotSymbol, gotLimit)
	}
	m := decodeNewsResult(t, raw)
	if m["source"] != "gateway" {
		t.Fatalf("source = %v, want gateway", m["source"])
	}
	items := assertNewsItems(t, m)
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	// 第 1 条无 sentiment -> 本地补齐为 negative/redFlag
	if items[0].Sentiment != "negative" || !items[0].RedFlag {
		t.Fatalf("item0 classify: got (%q,%v), want (negative,true)", items[0].Sentiment, items[0].RedFlag)
	}
	// 第 2 条网关自带 positive -> 保留且不标红
	if items[1].Sentiment != "positive" || items[1].RedFlag {
		t.Fatalf("item1 classify: got (%q,%v), want (positive,false)", items[1].Sentiment, items[1].RedFlag)
	}
	neg, _ := m["negative_count"].(float64)
	flags, _ := m["red_flag_count"].(float64)
	if neg != 1 || flags != 1 {
		t.Fatalf("negative_count=%v red_flag_count=%v, want 1/1", m["negative_count"], m["red_flag_count"])
	}
}

func TestNewsInvokableRunGatewayDegrade(t *testing.T) {
	degradeCases := []struct {
		name string
		h    http.HandlerFunc
	}{
		{"404 endpoint missing", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}},
		{"500 server error", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}},
		{"broken json", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `not-a-json`)
		}},
		{"empty news list", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"code":0,"data":{"news":[]}}`)
		}},
	}
	for _, tc := range degradeCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.h)
			defer srv.Close()
			tool, err := NewNewsSearchTool(NewsSearchConfig{GatewayBaseURL: srv.URL, HTTPTimeout: 2 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := tool.InvokableRun(context.Background(), `{"symbol":"300750","limit":4}`)
			if err != nil {
				t.Fatal(err)
			}
			m := decodeNewsResult(t, raw) // 必须仍 ok=true
			if m["source"] != "mock" {
				t.Fatalf("source = %v, want mock fallback", m["source"])
			}
			assertNewsItems(t, m)
		})
	}
	// 网关地址不可达（连接被拒）同样降级 Mock
	tool, err := NewNewsSearchTool(NewsSearchConfig{GatewayBaseURL: "http://127.0.0.1:1", HTTPTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := tool.InvokableRun(context.Background(), `{"symbol":"000002"}`)
	if err != nil {
		t.Fatal(err)
	}
	m := decodeNewsResult(t, raw)
	if m["source"] != "mock" {
		t.Fatalf("unreachable gateway: source = %v, want mock", m["source"])
	}
}

func TestNewsToolInfo(t *testing.T) {
	tool, err := NewNewsSearchTool(NewsSearchConfig{})
	if err != nil {
		t.Fatal(err)
	}
	info, err := tool.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "search_stock_news" {
		t.Fatalf("tool name = %q", info.Name)
	}
	js, err := info.ParamsOneOf.ToJSONSchema()
	if err != nil {
		t.Fatalf("ToJSONSchema: %v", err)
	}
	sym, ok := js.Properties.Get("symbol")
	if !ok {
		t.Fatal("schema missing symbol property")
	}
	lim, ok := js.Properties.Get("limit")
	if !ok {
		t.Fatal("schema missing limit property")
	}
	required := map[string]bool{}
	for _, r := range js.Required {
		required[r] = true
	}
	if !required["symbol"] {
		t.Fatalf("symbol should be required, required=%v", js.Required)
	}
	if required["limit"] {
		t.Fatalf("limit should be optional, required=%v", js.Required)
	}
	if sym.Type != "string" {
		t.Fatalf("symbol type = %q, want string", sym.Type)
	}
	if lim.Type != "integer" {
		t.Fatalf("limit type = %q, want integer", lim.Type)
	}
}

// ---------------------------------------------------------------------------
// TestNewsRealDataSource 真实新闻数据源集成测试
//
// 目的：验证 NewsSearchTool 对接「真实」Python 网关 /api/news（底层为 AKShare
// 东方财富 stock_news_em 实时数据，非 httptest 假响应、非 Mock）时的端到端行为。
//
// 因依赖外网且上游接口有偶发限流，默认 skip，不影响常规 CI。开启方式：
//   ASHARE_NEWS_REAL_TEST=1 go test -run TestNewsRealDataSource ./internal/tool/ -v
//
// 可选环境变量：
//   ASHARE_NEWS_GATEWAY_URL  自定义网关地址（须已实现 GET /api/news?symbol=&limit=）；
//                            未设置时本测试会自动拉起本地 Python 真实源网关
//                            （data_gateway/news.py + uvicorn，需 python3/akshare/fastapi）。
//   ASHARE_NEWS_SYMBOL       指定测试股票代码，默认 600519（贵州茅台，新闻覆盖率高）。
// ---------------------------------------------------------------------------

func TestNewsRealDataSource(t *testing.T) {
	if os.Getenv("ASHARE_NEWS_REAL_TEST") != "1" {
		t.Skip("真实新闻数据源集成测试默认跳过（需外网+AKShare），设置 ASHARE_NEWS_REAL_TEST=1 开启")
	}

	symbol := os.Getenv("ASHARE_NEWS_SYMBOL")
	if symbol == "" {
		symbol = "600519" // 贵州茅台：东财新闻覆盖稳定
	}

	baseURL := os.Getenv("ASHARE_NEWS_GATEWAY_URL")
	if baseURL == "" {
		baseURL = startRealNewsGateway(t) // 自动拉起本地真实源网关
	}

	toolInst, err := NewNewsSearchTool(NewsSearchConfig{
		GatewayBaseURL: baseURL,
		HTTPTimeout:    60 * time.Second, // 真实源经 AKShare 回源东财，首拉可能较慢
	})
	if err != nil {
		t.Fatalf("NewNewsSearchTool: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	start := time.Now()
	raw, runErr := toolInst.InvokableRun(ctx, fmt.Sprintf(`{"symbol":%q,"limit":5}`, symbol))
	if runErr != nil {
		t.Fatalf("InvokableRun returned go error (should never happen): %v", runErr)
	}
	t.Logf("真实源耗时 %v, 原始返回前 200 字节: %.200s", time.Since(start), raw)

	var out struct {
		OK            bool       `json:"ok"`
		Symbol        string     `json:"symbol"`
		Source        string     `json:"source"`
		NewsCount     int        `json:"news_count"`
		NegativeCount int        `json:"negative_count"`
		RedFlagCount  int        `json:"red_flag_count"`
		News          []NewsItem `json:"news"`
		Summary       string     `json:"summary"`
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("decode result: %v", err)
	}

	// ---- 核心断言：必须命中真实网关而非 Mock 降级 ----
	if !out.OK {
		t.Fatalf("real gateway run should be ok=true, got: %.300s", raw)
	}
	if out.Source != "gateway" {
		t.Fatalf("source = %q, want \"gateway\"（真实源不可用时请检查网络/AKShare, 不允许静默用 mock 通过本测试）", out.Source)
	}
	if out.Symbol != symbol {
		t.Fatalf("symbol = %q, want %q", out.Symbol, symbol)
	}
	if out.NewsCount == 0 || len(out.News) != out.NewsCount {
		t.Fatalf("news_count = %d, len(news) = %d, 真实源应返回 >0 条且一致", out.NewsCount, len(out.News))
	}
	if out.NewsCount > 5 {
		t.Fatalf("news_count = %d, 请求 limit=5 不应超量", out.NewsCount)
	}

	// ---- 真实数据质量断言 ----
	for i, it := range out.News {
		if strings.TrimSpace(it.Title) == "" {
			t.Errorf("news[%d].title 为空, 不符合真实新闻结构", i)
		}
		if it.Source == "Mock" {
			t.Errorf("news[%d].source=Mock, 混入了 mock 数据", i)
		}
		switch it.Sentiment {
		case "positive", "negative", "neutral":
		default:
			t.Errorf("news[%d].sentiment=%q 非法", i, it.Sentiment)
		}
		// 情绪标签与关键词表一致性（工具契约：negative 必标红雷）
		if it.Sentiment == "negative" && !it.RedFlag {
			gotSent, gotFlag := classifySentiment(it.Title, it.Summary)
			if gotSent == "negative" && gotFlag {
				t.Errorf("news[%d] negative 但未标 red_flag", i)
			}
		}
	}
	neg, flags := 0, 0
	for _, it := range out.News {
		if it.Sentiment == "negative" {
			neg++
		}
		if it.RedFlag {
			flags++
		}
	}
	if neg != out.NegativeCount || flags != out.RedFlagCount {
		t.Errorf("统计不一致: negative %d/%d, red_flag %d/%d", neg, out.NegativeCount, flags, out.RedFlagCount)
	}
	if !strings.Contains(out.Summary, symbol) {
		t.Errorf("summary 应包含代码 %q: %q", symbol, out.Summary)
	}
	t.Logf("✅ 真实源返回 %d 条新闻（负面 %d / 硬雷 %d），首条: %s [%s]",
		out.NewsCount, neg, flags, out.News[0].Title, out.News[0].Source)
}

// startRealNewsGateway 启动本地 Python 真实新闻源网关子进程（uvicorn + data_gateway/news.py），
// 轮询 /api/news 直到就绪或超时；测试结束自动回收进程。
func startRealNewsGateway(t *testing.T) string {
	t.Helper()
	port := fmt.Sprintf("%d", 18300+os.Getpid()%400)
	base := "http://127.0.0.1:" + port

	script := `
import sys
sys.path.insert(0, "data_gateway")
from fastapi import FastAPI, Query, HTTPException
import news as news_mod
app = FastAPI()
@app.get("/api/news")
def get_news(symbol: str = Query(...), limit: int = Query(5)):
    if len(symbol) != 6 or not symbol.isdigit():
        raise HTTPException(status_code=400, detail="bad symbol")
    r = news_mod.get_stock_news(symbol, limit=limit, include_notice=False)
    if "error" in r:
        raise HTTPException(status_code=400, detail=r["error"])
    return {"code": 0, "message": "success", "symbol": symbol, "data": r}
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(sys.argv[1]), log_level="warning")
`
	cmd := exec.Command("python3", "-c", script, port)
	cmd.Dir = repoRoot(t)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动真实源网关失败（需要 python3 + akshare + fastapi + uvicorn）: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/api/news?symbol=000001&limit=1")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return base // 服务已就绪（无论业务码，能应答即可开始正式用例）
		}
		time.Sleep(time.Second)
	}
	t.Fatal("真实源网关 60s 内未就绪，请确认本机可访问东财新闻接口")
	return ""
}

// repoRoot 从当前测试工作目录向上找 go.mod 所在目录，保证子进程相对路径 data_gateway 可用。
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for d := wd; ; {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			t.Fatal("go.mod not found above test dir")
		}
		d = parent
	}
}
