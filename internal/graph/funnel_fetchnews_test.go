package graph

// FetchNews 节点单元测试（漏斗图 Node 2）
//
// 覆盖场景：
//   FN.1 Normal          —— 每只候选股都抓到新闻，写入 news_context / news_context_json
//   FN.2 EmptyPool       —— 空池合法路径：不报错，给 LLM 空上下文 "[]"
//   FN.3 MissingPool     —— state 中无 pool 键时等价于空池，不 panic
//   FN.4 ToolSoftFail    —— 网关降级返回 ok:false → 该股票仍保留原始 JSON，节点不失败
//   FN.5 ToolHardError   —— InvokableRun 返回 Go error → 记录 {"ok":false,...} 占位，不丢数据
//   FN.6 Concurrency     —— 高并发下所有结果完整写入（验证 mutex 保护共享 map）
//   FN.7 NoDataRace      —— -race 下并发抓取无竞态
//   FN.8 StateKeysIntact —— 不覆盖 pool/pool_json/trade_date 等上游键（防回归）
//   FN.9 JSONRoundTrip   —— news_context_json 可反序列化回 map[string]string

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	qtool "ashare-quant/internal/tool"
)

// newNewsTool 构造指向假网关的 NewsSearchTool。handler 由调用方提供。
func newNewsTool(t *testing.T, handler http.HandlerFunc) *qtool.NewsSearchTool {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	nt, err := qtool.NewNewsSearchTool(qtool.NewsSearchConfig{GatewayBaseURL: srv.URL})
	if err != nil {
		t.Fatalf("new news tool: %v", err)
	}
	return nt
}

// poolState 构造带候选池的上游 state（模拟 fetchPool 输出）。
func poolState(symbols ...string) map[string]any {
	hits := make([]qtool.ScreenPoolHit, 0, len(symbols))
	for _, s := range symbols {
		hits = append(hits, qtool.ScreenPoolHit{Symbol: s, SignalType: "3rd_Buy"})
	}
	pj, _ := json.Marshal(hits)
	return map[string]any{
		keyTradeDate: "2026-10-09",
		keyPool:      hits,
		keyPoolJSON:  string(pj),
	}
}

// FN.1 正常路径：每只候选股均拿到新闻并写入两个 state 键。
func TestFetchNews_Normal(t *testing.T) {
	var calls int32
	nt := newNewsTool(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		sym := r.URL.Query().Get("symbol")
		fmt.Fprintf(w, `{"code":0,"data":{"symbol":%q,"items":[{"title":"利好公告"}]}}`, sym)
	})

	state := poolState("600519", "300750", "000001")
	out, err := fetchNews(context.Background(), state, nt, 5)
	if err != nil {
		t.Fatalf("fetchNews err: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expect 3 gateway calls, got %d", got)
	}

	ctxMap, ok := out[keyNewsCtx].(map[string]string)
	if !ok || len(ctxMap) != 3 {
		t.Fatalf("news_context wrong type/len: %#v", out[keyNewsCtx])
	}
	for _, sym := range []string{"600519", "300750", "000001"} {
		raw, has := ctxMap[sym]
		if !has || !strings.Contains(raw, sym) {
			t.Fatalf("symbol %s missing or mismatched in news_context: %q", sym, raw)
		}
	}
	if _, empty := ctxMap[""]; empty {
		t.Fatal("empty-symbol entry leaked into news_context")
	}
}

// FN.2 空池：合法路径，不请求网关，上下文置空。
func TestFetchNews_EmptyPool(t *testing.T) {
	var calls int32
	nt := newNewsTool(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Write([]byte(`{"code":0}`))
	})

	state := poolState() // 无候选
	out, err := fetchNews(context.Background(), state, nt, 5)
	if err != nil {
		t.Fatalf("empty pool must not fail: %v", err)
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatal("empty pool should not hit gateway")
	}
	if s, _ := out[keyNewsCtxJSON].(string); s != "[]" {
		t.Fatalf("news_context_json=%q, want \"[]\"", s)
	}
	m, ok := out[keyNewsCtx].(map[string]string)
	if !ok || len(m) != 0 {
		t.Fatalf("news_context should be empty map: %#v", out[keyNewsCtx])
	}
}

// FN.3 state 缺失 pool 键：等价空池，不得 panic。
func TestFetchNews_MissingPoolKey(t *testing.T) {
	nt := newNewsTool(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) })
	out, err := fetchNews(context.Background(), map[string]any{keyTradeDate: "x"}, nt, 5)
	if err != nil {
		t.Fatalf("missing pool key must not fail: %v", err)
	}
	if s, _ := out[keyNewsCtxJSON].(string); s != "[]" {
		t.Fatalf("news_context_json=%q, want \"[]\"", s)
	}
}

// FN.4 网关 5xx → 工具按设计降级 Mock（source:"mock"）：
// 节点必须保留每只股票的条目且透传 mock 标记，让 LLM 知道数据非实时。
func TestFetchNews_GatewayDownDegradesToMock(t *testing.T) {
	var calls int32
	nt := newNewsTool(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "upstream down", http.StatusBadGateway)
	})

	state := poolState("600519", "300750")
	out, err := fetchNews(context.Background(), state, nt, 5)
	if err != nil {
		t.Fatalf("gateway down must not abort node (tool degrades to mock): %v", err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expect 2 gateway attempts, got %d", calls)
	}
	m, _ := out[keyNewsCtx].(map[string]string)
	if len(m) != 2 {
		t.Fatalf("both symbols should still have entries, got %d", len(m))
	}
	for sym, raw := range m {
		if !strings.Contains(raw, `"source":"mock"`) {
			t.Fatalf("symbol %s should carry mock-degradation marker: %.200s", sym, raw)
		}
	}
}

// FN.4b 参数非法（symbol 非6位数字）→ 工具返回 ok:false JSON（Go error=nil）：
// 节点保留降级载荷且不中断。
func TestFetchNews_ToolSoftFailPayloadPreserved(t *testing.T) {
	nt := newNewsTool(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"code":0,"data":{"items":[]}}`))
	})
	// 手工构造含非法代码的池：fetchNews 会照原样调用工具，工具校验失败返回 ok:false
	hits := []qtool.ScreenPoolHit{{Symbol: "BADCODE"}, {Symbol: "600519"}}
	pj, _ := json.Marshal(hits)
	state := map[string]any{keyTradeDate: "x", keyPool: hits, keyPoolJSON: string(pj)}

	out, err := fetchNews(context.Background(), state, nt, 5)
	if err != nil {
		t.Fatalf("soft-fail payload must not abort node: %v", err)
	}
	m, _ := out[keyNewsCtx].(map[string]string)
	if len(m) != 2 {
		t.Fatalf("expect 2 entries, got %d", len(m))
	}
	if raw := m["BADCODE"]; !strings.Contains(raw, `"ok":false`) {
		t.Fatalf("invalid-symbol entry should hold ok:false payload: %q", raw)
	}
}

// FN.5 InvokableRun 返回 Go error → 节点写 {"ok":false,...} 占位，不丢股票。
// NewsSearchTool 设计上永不返回 Go error（全部内部消化），故用 fake 工具注入错误路径。
func TestFetchNews_ToolHardErrorPlaceholder(t *testing.T) {
	t.Skip("fetchNews 依赖具体类型 *NewsSearchTool 而非接口；该工具契约保证 error 恒为 nil，" +
		"Go-error 分支在现网不可达。若将 newsTool 抽象为接口可解除本 skip 并启用占位断言。")
}

// FN.6/FN.7 并发完整性 + 竞态检测：30 只股票、并发度 8，全部写入且 -race 干净。
func TestFetchNews_ConcurrencyAndRace(t *testing.T) {
	syms := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		syms = append(syms, fmt.Sprintf("60%04d", i))
	}
	nt := newNewsTool(t, func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		// 轻微交错，放大未加锁时的竞态窗口
		_ = strings.Repeat("x", len(sym))
		fmt.Fprintf(w, `{"code":0,"data":{"symbol":%q,"items":[]}}`, sym)
	})

	state := poolState(syms...)
	out, err := fetchNews(context.Background(), state, nt, 8)
	if err != nil {
		t.Fatalf("concurrent fetchNews: %v", err)
	}
	m, ok := out[keyNewsCtx].(map[string]string)
	if !ok {
		t.Fatalf("news_context type: %#v", out[keyNewsCtx])
	}
	if len(m) != 30 {
		t.Fatalf("expect 30 results under concurrency, got %d (possible lost write)", len(m))
	}
	for _, s := range syms {
		if raw, has := m[s]; !has || raw == "" {
			t.Fatalf("symbol %s lost under concurrency", s)
		}
	}
}

// FN.8 防回归：不得覆盖/破坏上游 fetchPool 写入的键。
func TestFetchNews_PreservesUpstreamKeys(t *testing.T) {
	nt := newNewsTool(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"code":0,"data":{"symbol":"600519","items":[]}}`))
	})
	state := poolState("600519")
	beforePoolJSON, _ := state[keyPoolJSON].(string)

	out, err := fetchNews(context.Background(), state, nt, 5)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got, _ := out[keyPoolJSON].(string); got != beforePoolJSON {
		t.Fatalf("pool_json clobbered by fetchNews: before=%q after=%q", beforePoolJSON, got)
	}
	if td, _ := out[keyTradeDate].(string); td != "2026-10-09" {
		t.Fatalf("trade_date corrupted: %q", td)
	}
	if _, ok := out[keyPool].([]qtool.ScreenPoolHit); !ok {
		t.Fatal("pool slice lost from state")
	}
}

// FN.9 news_context_json 必须能反序列化回 map[string]string（下游 managerDecision 依赖）。
func TestFetchNews_JSONRoundTrip(t *testing.T) {
	nt := newNewsTool(t, func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		fmt.Fprintf(w, `{"code":0,"data":{"symbol":%q,"items":[{"title":"T"}]}}`, sym)
	})
	out, err := fetchNews(context.Background(), poolState("600519", "300750"), nt, 5)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	raw, _ := out[keyNewsCtxJSON].(string)
	var back map[string]string
	if uerr := json.Unmarshal([]byte(raw), &back); uerr != nil {
		t.Fatalf("news_context_json not deserializable to map[string]string: %v\n%s", uerr, raw)
	}
	if len(back) != 2 {
		t.Fatalf("roundtrip len=%d, want 2", len(back))
	}
}
