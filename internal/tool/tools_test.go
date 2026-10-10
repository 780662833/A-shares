package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"ashare-quant/internal/db"
	"ashare-quant/internal/model"
)

// ---------------------------------------------------------------------------
// T1.3.x MarketDataTool / MockTradingTool 单元测试（httptest 模拟 Python 网关）
// ---------------------------------------------------------------------------

func fakeGateway(t *testing.T, bars []KlineBar) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/daily_kline" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		q := r.URL.Query()
		if q.Get("symbol") == "" || q.Get("start_date") == "" || q.Get("end_date") == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"detail":"missing params"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "data": bars})
	}))
}

var sampleBars = []KlineBar{
	{Date: "2026-10-12", Open: 10.0, Close: 10.5, High: 10.8, Low: 9.9, Volume: 1_000_000, Amount: 1.05e7},
	{Date: "2026-10-13", Open: 10.5, Close: 11.0, High: 11.2, Low: 10.4, Volume: 1_200_000, Amount: 1.3e7},
}

func TestMarketDataTool_Info(t *testing.T) { // T1.3.1
	tool, err := NewMarketDataTool(MarketDataConfig{GatewayBaseURL: "http://127.0.0.1:8000"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	info, err := tool.Info(context.Background())
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.Name != "get_daily_kline" {
		t.Errorf("name = %q want get_daily_kline", info.Name)
	}
	if !strings.Contains(info.Desc, "日K线") || !strings.Contains(info.Desc, "YYYYMMDD") {
		t.Errorf("desc must instruct LLM about usage/date format, got %q", info.Desc)
	}
	if info.ParamsOneOf == nil {
		t.Error("params schema missing")
	}
}

func TestMarketDataTool_BadConfig(t *testing.T) {
	for _, u := range []string{"", "ftp://x", "127.0.0.1:8000"} {
		if _, err := NewMarketDataTool(MarketDataConfig{GatewayBaseURL: u}); err == nil {
			t.Errorf("expected error for base url %q", u)
		}
	}
}

func TestMarketDataTool_NormalInvoke(t *testing.T) { // T1.3.2
	srv := fakeGateway(t, sampleBars)
	defer srv.Close()
	tool, _ := NewMarketDataTool(MarketDataConfig{GatewayBaseURL: srv.URL})

	out, err := tool.InvokableRun(context.Background(),
		`{"symbol":"000001","start_date":"20260901","end_date":"20261013"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	var env struct {
		OK      bool       `json:"ok"`
		Klines  []KlineBar `json:"klines"`
		Count   int        `json:"count"`
		Symbol  string     `json:"symbol"`
		Truncat bool       `json:"truncated"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("output not json: %v\n%s", err, out)
	}
	if !env.OK || len(env.Klines) != 2 || env.Count != 2 || env.Symbol != "000001" {
		t.Fatalf("out = %s", out)
	}
	if env.Klines[1].Close != 11.0 {
		t.Errorf("bar fields lost: %+v", env.Klines[1])
	}
}

func TestMarketDataTool_DateNormalization(t *testing.T) {
	cases := map[string]string{"20260101": "2026-01-01", "2026-01-01": "2026-01-01"}
	for in, want := range cases {
		got, err := normalizeDate(in)
		if err != nil || got != want {
			t.Errorf("normalizeDate(%q)=%q,%v want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "2026/01/01", "202601", "abc"} {
		if _, err := normalizeDate(bad); err == nil {
			t.Errorf("normalizeDate(%q) should fail", bad)
		}
	}
}

func TestMarketDataTool_InvalidArgs(t *testing.T) { // T1.3.3
	srv := fakeGateway(t, sampleBars)
	defer srv.Close()
	tool, _ := NewMarketDataTool(MarketDataConfig{GatewayBaseURL: srv.URL})

	badInputs := []string{
		`not json`,
		`{"symbol":"12345","start_date":"20260101","end_date":"20260110"}`,  // 5位代码
		`{"symbol":"ABCDEF","start_date":"20260101","end_date":"20260110"}`, // 非数字
		`{"symbol":"000001","start_date":"2026-1-1","end_date":"20260110"}`, // 坏日期
	}
	for _, in := range badInputs {
		out, err := tool.InvokableRun(context.Background(), in)
		if err != nil {
			t.Fatalf("run(%q) hard err: %v", in, err)
		}
		if !strings.Contains(out, `"ok":false`) {
			t.Errorf("run(%q) should be soft-fail json, got %s", in, out)
		}
	}
}

func TestMarketDataTool_GatewayDown(t *testing.T) { // T1.3.4 宕机不 panic
	srv := fakeGateway(t, sampleBars)
	url := srv.URL
	srv.Close() // 立刻关闭，模拟宕机

	tool, _ := NewMarketDataTool(MarketDataConfig{GatewayBaseURL: url})
	out, err := tool.InvokableRun(context.Background(),
		`{"symbol":"000001","start_date":"20260101","end_date":"20260110"}`)
	if err != nil {
		t.Logf("gateway down returned err=%v (acceptable)", err)
	} else if !strings.Contains(out, `"ok":false`) {
		t.Errorf("want soft-fail, got %s", out)
	}
}

func TestMarketDataTool_GatewayHTTP500(t *testing.T) { // T2.2 契约：上游 500 → Go 软失败
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"detail":"akshare upstream failed"}`)
	}))
	defer srv.Close()
	tool, _ := NewMarketDataTool(MarketDataConfig{GatewayBaseURL: srv.URL})
	out, _ := tool.InvokableRun(context.Background(),
		`{"symbol":"000001","start_date":"20260101","end_date":"20260110"}`)
	if !strings.Contains(out, `"ok":false`) {
		t.Errorf("want ok:false on HTTP500, got %s", out)
	}
}

func TestMarketDataTool_MaxBarsTruncation(t *testing.T) {
	bars := make([]KlineBar, 300)
	for i := range bars {
		bars[i] = KlineBar{Date: fmt.Sprintf("2025-01-%02d", i%28+1), Close: float64(i)}
	}
	srv := fakeGateway(t, bars)
	defer srv.Close()
	tool, _ := NewMarketDataTool(MarketDataConfig{GatewayBaseURL: srv.URL, MaxBarsPerCall: 50})
	out, _ := tool.InvokableRun(context.Background(),
		`{"symbol":"000001","start_date":"20240101","end_date":"20260101"}`)
	var env struct {
		Klines  []KlineBar `json:"klines"`
		Truncat bool       `json:"truncated"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(env.Klines) != 50 {
		t.Fatalf("bars=%d want 50 (tail truncation): %.200s", len(env.Klines), out)
	}
	if !env.Truncat {
		t.Errorf("truncated flag should be true")
	}
	if env.Klines[49].Close != 299 { // 保留的是尾部
		t.Errorf("truncation kept wrong segment, last close=%v want 299", env.Klines[49].Close)
	}
}

// ---------------------------------------------------------------------------
// MockTradingTool
// ---------------------------------------------------------------------------

func TestMockTradingTool_BuyCreatesPendingOrder(t *testing.T) { // T1.3.5
	gdb, err := db.InitDB(t.TempDir() + "/trading.db")
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	tt, err := NewMockTradingTool(MockTradingConfig{DB: gdb})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	out, err := tt.InvokableRun(context.Background(),
		`{"symbol":"600519","direction":"buy","price":1500.5,"quantity":100,"reason":"金叉"}`)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out, "订单已提交，等待次日撮合") {
		t.Fatalf("receipt text wrong: %s", out)
	}
	var resp struct {
		OK      bool   `json:"ok"`
		OrderID string `json:"order_id"`
		TradeDt string `json:"trade_date"`
	}
	if err := json.Unmarshal([]byte(out), &resp); err != nil || !resp.OK {
		t.Fatalf("resp = %s err=%v", out, err)
	}

	var ord model.Order
	if err := gdb.Where("order_id = ?", resp.OrderID).First(&ord).Error; err != nil {
		t.Fatalf("order not persisted: %v", err)
	}
	if ord.Status != model.OrderStatusPending || ord.Direction != "BUY" ||
		ord.Code != "600519" || ord.Quantity != 100 {
		t.Fatalf("persisted order wrong: %+v", ord)
	}
	if ord.TradeDate <= "" {
		t.Errorf("trade_date empty")
	}
	// 关键约束：不得直接撮合 —— 账户现金必须纹丝不动
	var acc model.Account
	if err := gdb.Order("id ASC").First(&acc).Error; err != nil {
		t.Fatal(err)
	}
	if acc.AvailableCash != 1_000_000 {
		t.Fatalf("cash changed by tool! got %.2f want 1000000 (no immediate matching allowed)", acc.AvailableCash)
	}
}

func TestMockTradingTool_SellAlsoPending(t *testing.T) { // T1.3.6
	gdb, _ := db.InitDB(t.TempDir() + "/trading.db")
	t.Cleanup(func() { _ = db.Close() })
	tt, _ := NewMockTradingTool(MockTradingConfig{DB: gdb})

	out, _ := tt.InvokableRun(context.Background(),
		`{"symbol":"000001","direction":"SELL","price":10.0,"quantity":200}`)
	if !strings.Contains(out, "订单已提交") {
		t.Fatalf("out=%s", out)
	}
	var cnt int64
	gdb.Model(&model.Order{}).Where("direction = ? AND status = ?", "SELL", model.OrderStatusPending).Count(&cnt)
	if cnt != 1 {
		t.Fatalf("pending sell count=%d want 1", cnt)
	}
}

func TestMockTradingTool_RejectsInvalidInput(t *testing.T) { // T1.3.7
	gdb, _ := db.InitDB(t.TempDir() + "/trading.db")
	t.Cleanup(func() { _ = db.Close() })
	tt, _ := NewMockTradingTool(MockTradingConfig{DB: gdb})

	bad := []string{
		`{"symbol":"60051","direction":"buy","price":10,"quantity":100}`,   // 代码位数
		`{"symbol":"600519","direction":"hold","price":10,"quantity":100}`, // 非法方向
		`{"symbol":"600519","direction":"buy","price":-1,"quantity":100}`,  // 负价
		`{"symbol":"600519","direction":"buy","price":10,"quantity":150}`,  // 非整手
		`{"symbol":"600519","direction":"buy","price":10,"quantity":0}`,    // 零数量
		`garbage`, // 非 JSON
	}
	for _, in := range bad {
		out, err := tt.InvokableRun(context.Background(), in)
		if err != nil {
			t.Fatalf("run(%q): unexpected hard err %v", in, err)
		}
		if !strings.Contains(out, `"ok":false`) {
			t.Errorf("run(%q) should reject, got %s", in, out)
		}
	}
	var cnt int64
	gdb.Model(&model.Order{}).Count(&cnt)
	if cnt != 0 {
		t.Fatalf("invalid inputs must not persist orders, count=%d", cnt)
	}
}

func TestMockTradingTool_OrderIDUniqueUnderConcurrency(t *testing.T) {
	gdb, _ := db.InitDB(t.TempDir() + "/trading.db")
	t.Cleanup(func() { _ = db.Close() })
	tt, _ := NewMockTradingTool(MockTradingConfig{DB: gdb})

	done := make(chan string, 20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			out, _ := tt.InvokableRun(context.Background(),
				fmt.Sprintf(`{"symbol":"6005%02d","direction":"buy","price":10,"quantity":100}`, i%100))
			var r struct {
				OrderID string `json:"order_id"`
			}
			_ = json.Unmarshal([]byte(out), &r)
			done <- r.OrderID
		}(i)
	}
	seen := map[string]bool{}
	for i := 0; i < 20; i++ {
		id := <-done
		if id == "" || seen[id] {
			t.Fatalf("duplicate/empty order id: %q (all=%v)", id, seen)
		}
		seen[id] = true
	}
}
