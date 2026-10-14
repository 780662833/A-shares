// WalkForwardTool 单元测试（httptest 模拟网关 /api/czsc/walk_forward/*）。
package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeWFGateway 同时模拟单票 GET 与批量 POST 两个端点，并记录请求。
func fakeWFGateway(t *testing.T, captured *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*captured = append(*captured, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/czsc/walk_forward/"):
			sym := strings.TrimPrefix(r.URL.Path, "/api/czsc/walk_forward/")
			if sym == "500000" { // 约定: 该代码返回业务级 insufficient_bars
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "failed",
					"data": map[string]any{"symbol": sym, "error": "insufficient_bars", "bars": 120, "required_min": 290}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "success",
				"data": map[string]any{
					"symbol": sym, "period": "daily", "mode": "rolling", "bars": 760,
					"source": "live",
					"windows": []any{map[string]any{
						"train_range": "2024-01-02~2025-06-30", "test_range": "2025-07-01~2025-10-31",
						"train_metrics": map[string]any{"trade_count": 5, "avg_trade_return_pct": 1.2, "alpha_vs_hold_pct": 0.8},
						"test_metrics":  map[string]any{"trade_count": 2, "avg_trade_return_pct": 0.5, "alpha_vs_hold_pct": 0.1},
					}},
					"summary": map[string]any{
						"n_windows": 4, "test_trade_count": 9,
						"avg_train_expectancy_pct": 1.2, "avg_test_expectancy_pct": 0.6,
						"degradation_pp": 0.6, "verdict": "oos_positive",
					},
				}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/czsc/walk_forward/rules":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			syms, _ := body["symbols"].([]any)
			results := make([]any, 0, len(syms))
			for _, s := range syms {
				results = append(results, map[string]any{
					"symbol": s, "bars": 760, "source": "cache",
					"summary": map[string]any{
						"n_windows": 4, "test_trade_count": 6,
						"avg_train_expectancy_pct": 1.5, "avg_test_expectancy_pct": -0.4,
						"degradation_pp": 1.9, "verdict": "overfit_suspect",
					},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "success",
				"data": map[string]any{
					"window": "20221009~20261009", "mode": "rolling",
					"rule_summary": map[string]any{
						"rule": "WF 250/63 rolling", "symbols_tested": len(syms),
						"symbols_with_oos_trades": len(syms),
						"avg_oos_expectancy_pct":  -0.4, "avg_degradation_pp": 1.9,
						"oos_positive_ratio_pct": 0.0, "overfit_suspect_count": len(syms),
						"verdict": "fragile_positive",
					},
					"results": results,
					"errors":  []any{},
				}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func newWFTool(t *testing.T, url string) *WalkForwardTool {
	t.Helper()
	tool, err := NewWalkForwardTool(WalkForwardConfig{GatewayBaseURL: url})
	if err != nil {
		t.Fatalf("NewWalkForwardTool: %v", err)
	}
	return tool
}

func TestWalkForwardTool_BadConfig(t *testing.T) {
	if _, err := NewWalkForwardTool(WalkForwardConfig{}); err == nil {
		t.Fatal("empty GatewayBaseURL must fail")
	}
	if _, err := NewWalkForwardTool(WalkForwardConfig{GatewayBaseURL: "127.0.0.1:8000"}); err == nil {
		t.Fatal("non-http GatewayBaseURL must fail")
	}
}

func TestWalkForwardTool_Info(t *testing.T) {
	var calls []string
	srv := fakeWFGateway(t, &calls)
	defer srv.Close()
	info, err := newWFTool(t, srv.URL).Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "query_walk_forward" {
		t.Fatalf("name=%s", info.Name)
	}
	if !strings.Contains(info.Desc, "overfit_suspect") || !strings.Contains(info.Desc, "降权") {
		t.Fatal("Desc must teach LLM the verdict downgrade semantics")
	}
}

func TestWalkForwardTool_SingleSymbol_GET(t *testing.T) {
	var calls []string
	srv := fakeWFGateway(t, &calls)
	defer srv.Close()

	out, err := newWFTool(t, srv.URL).InvokableRun(context.Background(),
		`{"symbols":["300750"],"train_size":350,"test_size":80,"start_date":"2024-01-01","use_cache":false}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || !strings.HasPrefix(calls[0], "GET /api/czsc/walk_forward/300750") {
		t.Fatalf("expected single GET, got %v", calls)
	}
	// 日期归一化为 YYYYMMDD、参数透传
	if !strings.Contains(calls[0], "start_date=20240101") || !strings.Contains(calls[0], "train_size=350") ||
		!strings.Contains(calls[0], "test_size=80") || !strings.Contains(calls[0], "use_cache=false") {
		t.Fatalf("query params wrong: %s", calls[0])
	}
	// marshalOK 平铺输出: {"ok":true,"symbol":"walk_forward",...payload}
	var env struct {
		OK          bool       `json:"ok"`
		Source      string     `json:"source"`
		Summary     WFSummary  `json:"summary"`
		Windows     []WFWindow `json:"windows"`
		VerdictHint string     `json:"verdict_hint"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("output not JSON: %v (%s)", err, out)
	}
	if !env.OK || env.Summary.Verdict != "oos_positive" {
		t.Fatalf("bad payload: %s", out)
	}
	if len(env.Windows) != 1 || env.Windows[0].TestMetrics.TradeCount != 2 {
		t.Fatalf("windows contract broken: %s", out)
	}
	if env.Source != "live" {
		t.Fatalf("source should pass through: %s", out)
	}
}

func TestWalkForwardTool_MultiSymbol_POST_Rules(t *testing.T) {
	var calls []string
	srv := fakeWFGateway(t, &calls)
	defer srv.Close()

	out, err := newWFTool(t, srv.URL).InvokableRun(context.Background(),
		`{"symbols":["600519","000001","600519"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || !strings.HasPrefix(calls[0], "POST /api/czsc/walk_forward/rules") {
		t.Fatalf("expected single POST, got %v", calls)
	}
	var env struct {
		OK          bool          `json:"ok"`
		RuleSummary WFRuleSummary `json:"rule_summary"`
		Results     []struct {
			Symbol  string   `json:"symbol"`
			Verdict string   `json:"verdict"`
			Degr    *float64 `json:"degradation_pp"`
		} `json:"results"`
		VerdictHint string `json:"verdict_hint"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("output not JSON: %v", err)
	}
	if env.RuleSummary.Verdict != "fragile_positive" {
		t.Fatalf("verdict=%s", env.RuleSummary.Verdict)
	}
	if len(env.Results) != 2 { // 去重后 2 只
		t.Fatalf("dedup failed: %+v", env.Results)
	}
	if !strings.Contains(env.VerdictHint, "降权") {
		t.Fatalf("fragile_positive must instruct downgrade: %s", env.VerdictHint)
	}
}

func TestWalkForwardTool_BusinessErrorStructured(t *testing.T) {
	var calls []string
	srv := fakeWFGateway(t, &calls)
	defer srv.Close()
	out, err := newWFTool(t, srv.URL).InvokableRun(context.Background(), `{"symbols":["500000"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"ok":true`) || !strings.Contains(out, "insufficient_bars") ||
		!strings.Contains(out, "按保守处理") {
		t.Fatalf("business error must be structured ok=true with conservative hint: %s", out)
	}
}

func TestWalkForwardTool_InputValidation(t *testing.T) {
	var calls []string
	srv := fakeWFGateway(t, &calls)
	defer srv.Close()
	tw := newWFTool(t, srv.URL)
	cases := []string{
		`not json`,
		`{"symbols":[]}`,
		`{"symbols":["abc"]}`, // 非6位数字
		fmt.Sprintf(`{"symbols":[%s]}`, quoteN(51)), // 超50只
		`{"symbols":["000001"],"period":"minute"}`,  // 非法周期
		`{"symbols":["000001"],"train_size":10}`,    // 越界
		`{"symbols":["000001"],"test_size":300}`,    // 越界
		`{"symbols":["000001"],"start_date":"garbage"}`,
	}
	for i, c := range cases {
		out, err := tw.InvokableRun(context.Background(), c)
		if err != nil {
			t.Fatalf("case %d: err %v", i, err)
		}
		if !strings.Contains(out, `"ok":false`) {
			t.Fatalf("case %d must soft-fail: %s", i, out)
		}
	}
	if len(calls) != 0 {
		t.Fatalf("validation failures must not hit gateway: %v", calls)
	}
}

func TestWalkForwardTool_GatewayDown(t *testing.T) {
	tw, err := NewWalkForwardTool(WalkForwardConfig{GatewayBaseURL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := tw.InvokableRun(context.Background(), `{"symbols":["000001"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "gateway unreachable") {
		t.Fatalf("down gateway must soft-fail: %s", out)
	}
}

func quoteN(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("%q", fmt.Sprintf("%06d", i+1))
	}
	return strings.Join(parts, ",")
}
