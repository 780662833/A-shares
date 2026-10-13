// liquidity_screen_test.go —— Generator.liquidityScreen（全市场流动性初筛）专项测试。
//
// 锁定语义约定（见 generator.go 注释）：
//   - err != nil 时 set 必须为 nil，调用方据此"不设门槛"退化；
//   - err == nil 且 set 非空时门槛生效；
//   - 通过数为 0 视为初筛失败（数据问题不得把资金流来源整体清空）。
package pool

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// sprintfCode 生成 6 位数字 A 股代码（i ∈ [0,99] -> "6000ii"）。
func sprintfCode(i int) string { return fmt.Sprintf("6000%02d", i) }

// TestLiquidityScreenDefaultConfigThresholds 验证默认系统配置阈值下的过滤边界：
// MinPrice=1.0 / MinAvgAmount=1e8 / MinMarketCap=5e9 / ExcludeST=true / LiquidityTopN=400。
func TestLiquidityScreenDefaultConfigThresholds(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{
		quote("600001", "边界上", 1.0, 1e8, 5e9),     // 恰好等于三个下限 -> 放行（闭区间）
		quote("600002", "价格差一分", 0.99, 5e8, 1e11), // 低于 MinPrice -> 剔除
		quote("600003", "缩量一分", 10, 9.99e7, 1e11), // 低于 MinAvgAmount -> 剔除
		quote("600004", "市值差一点", 10, 5e8, 4.99e9), // 低于 MinMarketCap -> 剔除
		quote("600005", "*ST退市警示", 10, 5e8, 1e11), // ST 剔除
	}
	g := NewGenerator(f, DefaultConfig())
	set, n, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if n != 1 || len(set) != 1 || !set["600001"] {
		t.Fatalf("want only 600001 (boundary-equal) passed, got set=%v n=%d", set, n)
	}
}

// TestLiquidityScreenZeroMarketCapPasses 锁定 MinMarketCap 的"缺失放行"约定：
// 总市值字段为 0（上游缺数据）时不得误杀，仅当 >0 且低于门槛才剔除。
func TestLiquidityScreenZeroMarketCapPasses(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{
		quote("600001", "市值缺失", 10, 5e8, 0), // TotalMarketCap=0 -> 视同未知, 放行
	}
	g := NewGenerator(f, DefaultConfig())
	set, n, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || !set["600001"] {
		t.Fatalf("want zero-cap stock passed (missing data != fail), got set=%v n=%d", set, n)
	}
}

// TestLiquidityScreenMinMarketCapDisabled 验证 MinMarketCap=0 显式关闭市值门槛。
func TestLiquidityScreenMinMarketCapDisabled(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{quote("600001", "微盘股", 10, 5e8, 1e7)}
	cfg := DefaultConfig()
	cfg.MinMarketCap = 0 // 0 表示不设市值门槛
	g := NewGenerator(f, cfg)
	set, n, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || !set["600001"] {
		t.Fatalf("want micro-cap passed with MinMarketCap=0, got set=%v n=%d", set, n)
	}
}

// TestLiquidityScreenExcludeSTDisabled 验证 ExcludeST=false 时 ST 不再被剔除。
func TestLiquidityScreenExcludeSTDisabled(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{quote("600001", "ST博通", 10, 5e8, 1e11)}
	cfg := DefaultConfig()
	cfg.ExcludeST = false
	g := NewGenerator(f, cfg)
	set, _, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !set["600001"] {
		t.Fatalf("want ST kept when ExcludeST=false, got set=%v", set)
	}
}

// TestLiquidityScreenNonAcodeFiltered 验证非 A 股代码格式（正则 acodeRe）不参与初筛。
func TestLiquidityScreenNonAcodeFiltered(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{
		{Code: "hk00700", Name: "腾讯", Price: 400, AvgAmount20D: 5e9, TotalMarketCap: 4e12},
		{Code: "", Name: "空代码", Price: 10, AvgAmount20D: 5e9},
		quote("600001", "合规A股", 10, 5e9, 1e11),
	}
	g := NewGenerator(f, DefaultConfig())
	set, n, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(set) != 1 || !set["600001"] {
		t.Fatalf("want only valid A-code passed, got set=%v n=%d", set, n)
	}
}

// TestLiquidityScreenProviderErrorNilSet 锁定核心契约：err != nil 时 set 必须为 nil。
func TestLiquidityScreenProviderErrorNilSet(t *testing.T) {
	f := newFake()
	f.errStocks = errors.New("snapshot unavailable")
	g := NewGenerator(f, DefaultConfig())
	set, n, err := g.liquidityScreen(context.Background())
	if err == nil {
		t.Fatal("want error propagated from AllStocks")
	}
	if set != nil {
		t.Fatalf("contract violation: set must be nil when err != nil, got %v", set)
	}
	if n != 0 {
		t.Fatalf("want n=0 on error, got %d", n)
	}
}

// TestLiquidityScreenZeroPassReturnsDeadlineExceeded 锁定"全军覆没"退化路径：
// 通过数为 0 时返回非 nil 错误（语义: 初筛失败）+ nil set，防止数据问题清空来源。
func TestLiquidityScreenZeroPassReturnsDeadlineExceeded(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{quote("600004", "缩量股", 10, 1e6, 1e11)}
	g := NewGenerator(f, DefaultConfig())
	set, n, err := g.liquidityScreen(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded sentinel on zero-pass, got %v", err)
	}
	if set != nil || n != 0 {
		t.Fatalf("want nil set + n=0 on zero-pass, got set=%v n=%d", set, n)
	}
}

// TestLiquidityScreenTopNDescendingTruncation 验证按 AvgAmount20D 降序截断 TopN，
// 并检查集合内容严格等于成交额最大的前 N 只。
func TestLiquidityScreenTopNDescendingTruncation(t *testing.T) {
	f := newFake()
	for i := 0; i < 20; i++ {
		f.stocks = append(f.stocks, quote(
			sprintfCode(i), "股", 10, float64(i+1)*1e8, 1e11))
	}
	cfg := DefaultConfig()
	cfg.LiquidityTopN = 5
	g := NewGenerator(f, cfg)
	set, n, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 || len(set) != 5 {
		t.Fatalf("want exactly top-5, got n=%d len=%d", n, len(set))
	}
	// 成交额最大的是 i=19..15 -> code 600019..600015
	for _, want := range []string{"600019", "600018", "600017", "600016", "600015"} {
		if !set[want] {
			t.Fatalf("top-N must contain %s by descending amount, set=%v", want, set)
		}
	}
	if set["600014"] {
		t.Fatalf("rank-6 stock leaked into top-5, set=%v", set)
	}
}

// TestLiquidityScreenTopNDisabled 验证 LiquidityTopN<=0 时不截断。
func TestLiquidityScreenTopNDisabled(t *testing.T) {
	f := newFake()
	for i := 0; i < 12; i++ {
		f.stocks = append(f.stocks, quote(sprintfCode(i), "股", 10, float64(i+1)*1e8, 1e11))
	}
	cfg := DefaultConfig()
	cfg.LiquidityTopN = 0
	g := NewGenerator(f, cfg)
	_, n, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 12 {
		t.Fatalf("want no truncation with TopN=0, got n=%d", n)
	}
}
