package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/gorm"

	"ashare-quant/internal/db"
	dbmodel "ashare-quant/internal/model"
)

// ---------------------------------------------------------------------------
// 测试脚手架
// ---------------------------------------------------------------------------

func setupDB(t *testing.T) *gorm.DB {
	t.Helper()
	gdb, err := db.InitDB(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, e := gdb.DB(); e == nil {
			_ = sqlDB.Close()
		}
	})
	return gdb
}

// fakeGateway 模拟 Python 网关 /api/daily_kline：
// failSymbols 中的股票返回 500；emptySymbols 返回空 data；其余返回正常 K 线。
func fakeGateway(t *testing.T, failSymbols, emptySymbols map[string]bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/daily_kline", func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		if failSymbols[sym] {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"detail":{"source":"akshare","message":"boom for %s"}}`, sym)
			return
		}
		if emptySymbols[sym] {
			_, _ = w.Write([]byte(`{"code":0,"count":0,"data":[]}`))
			return
		}
		// 三天数据：10.00 → 11.00 → 12.10（主板 10% 涨停恰好连续打板）
		rows := []klineRow{
			{Date: "2026-10-07", Open: 9.9, Close: 10.0, High: 10.1, Low: 9.8, Volume: 50000, Amount: 5e5},
			{Date: "2026-10-08", Open: 10.5, Close: 11.0, High: 11.0, Low: 10.4, Volume: 60000, Amount: 6e5},
			{Date: "2026-10-09", Open: 12.10, Close: 12.10, High: 12.10, Low: 12.10, Volume: 70000, Amount: 7e5}, // 一字板
		}
		env := map[string]any{"code": 0, "message": "success", "symbol": sym, "count": len(rows), "data": rows}
		_ = json.NewEncoder(w).Encode(env)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newSyncer(t *testing.T, gdb *gorm.DB, base string) *DataSyncer {
	t.Helper()
	s, err := New(gdb, Config{GatewayBaseURL: base, HTTPTimeout: 3 * time.Second, LookbackDays: 15})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

func TestNewValidation(t *testing.T) {
	gdb := setupDB(t)
	if _, err := New(nil, Config{GatewayBaseURL: "http://x"}); err == nil {
		t.Fatal("nil DB should error")
	}
	if _, err := New(gdb, Config{}); err == nil {
		t.Fatal("empty gateway url should error")
	}
	if _, err := New(gdb, Config{GatewayBaseURL: "ftp://x"}); err == nil {
		t.Fatal("non-http scheme should error")
	}
	s, err := New(gdb, Config{GatewayBaseURL: "http://127.0.0.1:8000/"}) // 尾斜杠应被清理
	if err != nil || s.cfg.GatewayBaseURL != "http://127.0.0.1:8000" {
		t.Fatalf("trailing slash handling: %v %+v", err, s)
	}
}

func TestSyncDailyKlines_UpsertAndLimitPrices(t *testing.T) {
	gdb := setupDB(t)
	srv := fakeGateway(t, nil, nil)
	s := newSyncer(t, gdb, srv.URL)

	res, err := s.SyncDailyKlines(context.Background(), []string{"000001", "000001", "600519"}, "20261009")
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if res.Succeeded != 2 || res.Failed != 0 {
		t.Fatalf("want 2 ok 0 fail, got %+v", res)
	}
	// 首根无昨收被跳过：每股落 2 根
	if res.RowsUpsert != 4 {
		t.Fatalf("want 4 rows, got %d", res.RowsUpsert)
	}

	var k dbmodel.DailyKline
	if err := gdb.Where("code = ? AND date = ?", "000001", "2026-10-09").First(&k).Error; err != nil {
		t.Fatalf("target day kline missing: %v", err)
	}
	// 昨收 11.00 → 涨停 12.10 / 跌停 9.90
	if k.LimitUpPrice != 12.10 || k.LimitDownPrice != 9.90 {
		t.Fatalf("limit prices wrong: %+v", k)
	}
	if k.Volume != 7000000 {
		t.Fatalf("volume unit conversion (手→股) wrong: %d", k.Volume)
	}
}

func TestSyncDailyKlines_IdempotentUpsert(t *testing.T) {
	gdb := setupDB(t)
	srv := fakeGateway(t, nil, nil)
	s := newSyncer(t, gdb, srv.URL)

	for i := 0; i < 2; i++ {
		if _, err := s.SyncDailyKlines(context.Background(), []string{"000001"}, "2026-10-09"); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
	}
	var n int64
	gdb.Model(&dbmodel.DailyKline{}).Where("code = ?", "000001").Count(&n)
	if n != 2 {
		t.Fatalf("upsert must not duplicate rows, want 2 got %d", n)
	}
}

func TestSyncDailyKlines_PartialFailureSkipped(t *testing.T) {
	gdb := setupDB(t)
	srv := fakeGateway(t, map[string]bool{"510500": false, "300750": true}, map[string]bool{"688111": true})
	s := newSyncer(t, gdb, srv.URL)

	res, err := s.SyncDailyKlines(context.Background(),
		[]string{"000001", "300750", "688111", "bad1"}, "20261009")
	if err != nil {
		t.Fatalf("systemic error while some symbols failed: %v", err)
	}
	if res.Succeeded != 1 || res.Failed != 3 {
		t.Fatalf("want 1 ok 3 fail, got %+v", res)
	}
	if len(res.Failures) != 3 {
		t.Fatalf("failures detail missing: %+v", res.Failures)
	}
	// 好股票照常落库
	var n int64
	gdb.Model(&dbmodel.DailyKline{}).Where("code = ?", "000001").Count(&n)
	if n == 0 {
		t.Fatal("healthy symbol must still be persisted")
	}
}

func TestSyncDailyKlines_GatewayDownNoPanic(t *testing.T) {
	gdb := setupDB(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	s := newSyncer(t, gdb, srv.URL)

	res, err := s.SyncDailyKlines(context.Background(), []string{"000001"}, "20261009")
	if err != nil {
		t.Fatalf("gateway 500 must be per-symbol failure, got systemic: %v", err)
	}
	if res.Failed != 1 || res.Succeeded != 0 {
		t.Fatalf("want all failed, got %+v", res)
	}
}

func TestSyncDailyKlines_BrokerCanReadLimitPrices(t *testing.T) {
	// 端到端契约：同步落库后，撮合引擎关心的「次日一字板判定所需字段」必须可查。
	gdb := setupDB(t)
	srv := fakeGateway(t, nil, nil)
	s := newSyncer(t, gdb, srv.URL)
	if _, err := s.SyncDailyKlines(context.Background(), []string{"000001"}, "20261009"); err != nil {
		t.Fatal(err)
	}
	var k dbmodel.DailyKline
	if err := gdb.Where("code = ? AND date = ?", "000001", "2026-10-09").First(&k).Error; err != nil {
		t.Fatalf("broker query path broken: %v", err)
	}
	if k.Open != k.LimitUpPrice {
		t.Fatalf("fixture should produce one-word limit-up board: open=%.2f lu=%.2f", k.Open, k.LimitUpPrice)
	}
}

func TestNormalizeHelpers(t *testing.T) {
	if d, err := normalizeTradeDate("20261009"); err != nil || d != "2026-10-09" {
		t.Fatalf("compact: %s %v", d, err)
	}
	if d, err := normalizeTradeDate("2026-10-09"); err != nil || d != "2026-10-09" {
		t.Fatalf("dash: %s %v", d, err)
	}
	if _, err := normalizeTradeDate("2026-13-99"); err == nil {
		t.Fatal("invalid date must error")
	}
	if normalizeBarDate("20261009") != "2026-10-09" || normalizeBarDate("garbage") != "" {
		t.Fatal("bar date normalization wrong")
	}
}
