package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
