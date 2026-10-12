package pool

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// fakeProvider：离线全链路测试替身（不触网, 不依赖 stock-go SDK）
// ---------------------------------------------------------------------------

type fakeProvider struct {
	stocks       []StockQuote
	sectorRank   []SectorFlow
	constituents map[string][]ConstituentStock
	dragon       []InstitutionBuy
	fundCodes    []string
	fundSnaps    []FundSnapshot
	kline        func(code string) *DailyClose
	errAll       error
	errStocks    error
	calls        map[string]int
}

func newFake() *fakeProvider {
	return &fakeProvider{constituents: map[string][]ConstituentStock{}, calls: map[string]int{}}
}

func (f *fakeProvider) AllStocks(ctx context.Context) ([]StockQuote, error) {
	f.calls["AllStocks"]++
	if f.errStocks != nil {
		return nil, f.errStocks
	}
	return f.stocks, nil
}
func (f *fakeProvider) SectorFundFlowRank(ctx context.Context) ([]SectorFlow, error) {
	f.calls["SectorRank"]++
	return f.sectorRank, nil
}
func (f *fakeProvider) IndustryConstituents(ctx context.Context, board string) ([]ConstituentStock, error) {
	f.calls["Constituents:"+board]++
	return f.constituents[board], nil
}
func (f *fakeProvider) DragonTigerInstitution(ctx context.Context, start, end time.Time) ([]InstitutionBuy, error) {
	f.calls["DragonTiger"]++
	return f.dragon, nil
}
func (f *fakeProvider) FundList(ctx context.Context) ([]string, error) {
	f.calls["FundList"]++
	return f.fundCodes, nil
}
func (f *fakeProvider) FundBatchQuote(ctx context.Context, codes []string) ([]FundSnapshot, error) {
	f.calls["FundQuote"]++
	return f.fundSnaps, nil
}
func (f *fakeProvider) ETFCloseSeries(ctx context.Context, code string, lookback int, end time.Time) (*DailyClose, error) {
	f.calls["Kline:"+code]++
	if f.kline == nil {
		return nil, errors.New("no kline")
	}
	return f.kline(code), nil
}

var testNow = time.Date(2026, 10, 12, 15, 0, 0, 0, time.UTC)

func quote(code, name string, price, amt, cap float64) StockQuote {
	return StockQuote{Code: code, Name: name, Price: price, AvgAmount20D: amt, TotalMarketCap: cap}
}

// ---------------------------------------------------------------------------
// 流动性初筛
// ---------------------------------------------------------------------------

func TestLiquidityScreen(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{
		quote("600001", "贵州茅台", 1500, 5e9, 2e12), // 通过
		quote("600002", "ST杂毛", 10, 5e8, 1e11),   // ST 剔除
		quote("600003", "仙股", 0.5, 5e8, 1e11),    // 价格剔除
		quote("600004", "缩量股", 10, 1e6, 1e11),    // 成交额剔除
		quote("600005", "微盘股", 10, 5e8, 1e9),     // 市值剔除
		{Code: "abc123", Name: "非法代码", Price: 10, AvgAmount20D: 5e9},
	}
	g := NewGenerator(f, DefaultConfig())
	set, n, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if n != 1 || len(set) != 1 || !set["600001"] {
		t.Fatalf("want only 600001 passed, got set=%v n=%d", set, n)
	}
}

func TestLiquidityScreenEmptySetDegenerates(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{quote("600004", "缩量股", 10, 1e6, 1e11)} // 全军覆没
	g := NewGenerator(f, DefaultConfig())
	set, _, err := g.liquidityScreen(context.Background())
	if err == nil || set != nil {
		t.Fatalf("want err + nil set on zero-pass, got set=%v err=%v", set, err)
	}
}

func TestLiquidityTopNTruncation(t *testing.T) {
	f := newFake()
	for i := 0; i < 50; i++ {
		f.stocks = append(f.stocks, quote(fmt.Sprintf("6000%02d", i), fmt.Sprintf("股%d", i), 10, float64(i+1)*1e8, 1e11))
	}
	cfg := DefaultConfig()
	cfg.LiquidityTopN = 10
	g := NewGenerator(f, cfg)
	set, n, err := g.liquidityScreen(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 || !set["600049"] || set["600000"] {
		t.Fatalf("want top10 by amount (has 600049, no 600000), got n=%d", n)
	}
}

// ---------------------------------------------------------------------------
// Generate 全链路
// ---------------------------------------------------------------------------

func baseFake() *fakeProvider {
	f := newFake()
	f.stocks = []StockQuote{
		quote("600519", "贵州茅台", 1500, 5e9, 2e12),
		quote("300750", "宁德时代", 200, 3e9, 9e11),
		quote("601127", "赛力斯", 100, 2e9, 1e11),
	}
	f.sectorRank = []SectorFlow{{Code: "BK0438", Name: "汽车整车", MainNetInflow: 3e9},
		{Code: "BK0488", Name: "食品饮料", MainNetInflow: 5e9}}
	f.constituents["BK0488"] = []ConstituentStock{{Code: "600519", Name: "贵州茅台", Price: 1500},
		{Code: "000001", Name: "平安银行", Price: 12}} // 000001 未过初筛 -> 应被门槛剔除
	f.constituents["BK0438"] = []ConstituentStock{{Code: "601127", Name: "赛力斯", Price: 100},
		{Code: "300750", Name: "宁德时代", Price: 200}}
	f.dragon = []InstitutionBuy{
		{Code: "300750", Name: "宁德时代", OrgNetAmount: 2e8}, // 达标且在 liquidSet
		{Code: "600004", Name: "缩量股", OrgNetAmount: 9e8},  // 不在 liquidSet -> 剔除
		{Code: "600005", Name: "低买机构", OrgNetAmount: 1e7}, // 低于阈值 -> 剔除
	}
	f.fundCodes = []string{"510300", "159915", "511990"} // 最后一个是货币ETF(badKW 无"债/货币"名, 见 snaps)
	f.fundSnaps = []FundSnapshot{
		{Code: "510300", Name: "沪深300ETF", Price: 4, Amount: 8e9},
		{Code: "159915", Name: "创业板ETF", Price: 2, Amount: 6e9},
		{Code: "511990", Name: "添富快线货币", Price: 100, Amount: 1e10},
	}
	f.kline = func(code string) *DailyClose {
		base := 1.0
		if code == "510300" {
			base = 1.0
		} else {
			base = 2.0
		}
		closes := make([]float64, 0, 60)
		for i := 0; i < 60; i++ {
			closes = append(closes, base)
		}
		closes[59] = base * 1.1 // 20日窗口内涨幅体现在最后一条
		return &DailyClose{Closes: closes}
	}
	return f
}

func TestGenerateFourSourcesAndDedup(t *testing.T) {
	f := baseFake()
	g := NewGenerator(f, DefaultConfig())
	res, err := g.Generate(context.Background(), []string{"sh600000", "bad"}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if res.TradeDate != "2026-10-12" {
		t.Errorf("trade_date=%s", res.TradeDate)
	}
	bySrc := map[string]string{}
	for _, e := range res.Pool {
		bySrc[e.Symbol] = e.SourceType
	}
	if bySrc["600000"] != sourceManual {
		t.Errorf("manual missing/wrong: %+v", bySrc)
	}
	if _, ok := bySrc["bad"]; ok {
		t.Error("invalid code should be dropped")
	}
	if bySrc["510300"] != sourceETF || bySrc["159915"] != sourceETF {
		t.Errorf("etf entries wrong: %+v", bySrc)
	}
	if bySrc["511990"] != "" {
		t.Error("money-market ETF must be excluded")
	}
	if bySrc["600519"] != sourceSector || bySrc["601127"] != sourceSector {
		t.Errorf("sector entries wrong: %+v", bySrc)
	}
	if bySrc["000001"] != "" {
		t.Error("illiquid constituent must be gated out")
	}
	// 300750: SECTOR 先于 FUND 入池 -> 去重保留 SECTOR? 顺序为 MANUAL>ETF>SECTOR>FUND
	if bySrc["300750"] != sourceSector {
		t.Errorf("dedup priority wrong: %s", bySrc["300750"])
	}
	if res.BySource[sourceFund] != 0 && res.BySource[sourceFund] != 1 {
		t.Errorf("by_source fund=%d", res.BySource[sourceFund])
	}
	if res.Total != len(res.Pool) {
		t.Errorf("total mismatch")
	}
	if res.LiquidityPassed != 3 {
		t.Errorf("liquidity_passed=%d want 3", res.LiquidityPassed)
	}
}

func TestGenerateWeakDependencyWarnings(t *testing.T) {
	f := baseFake()
	f.errStocks = errors.New("boom") // 初筛失败 -> 退化不设门槛
	f.sectorRank = nil               // 行业资金流空 -> SECTOR 失败
	f.dragon = nil                   // 龙虎榜空 -> FUND 为空但合法
	f.fundCodes = nil                // ETF 列表失败
	g := NewGenerator(f, DefaultConfig())
	res, err := g.Generate(context.Background(), []string{"600000"}, testNow)
	if err != nil {
		t.Fatalf("must never fail overall: %v", err)
	}
	if len(res.Warnings) < 3 {
		t.Fatalf("want >=3 warnings, got %v", res.Warnings)
	}
	if len(res.Pool) != 1 || res.Pool[0].Symbol != "600000" {
		t.Fatalf("manual whitelist must survive all-source failure: %+v", res.Pool)
	}
	sawLiquid := false
	for _, w := range res.Warnings {
		if contains(w, "LIQUIDITY_SCREEN") {
			sawLiquid = true
		}
	}
	if !sawLiquid {
		t.Errorf("missing liquidity warning: %v", res.Warnings)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// helpers / ToGenData round-trip
// ---------------------------------------------------------------------------

func TestToGenDataRoundTrip(t *testing.T) {
	f := baseFake()
	g := NewGenerator(f, DefaultConfig())
	res, err := g.Generate(context.Background(), nil, testNow)
	if err != nil {
		t.Fatal(err)
	}
	data, err := res.ToGenData()
	if err != nil {
		t.Fatal(err)
	}
	if data.TradeDate != res.TradeDate || data.Total != res.Total || len(data.Pool) != len(res.Pool) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", data, res)
	}
	for i, e := range data.Pool {
		if e.Symbol != res.Pool[i].Symbol || e.SourceType != res.Pool[i].SourceType {
			t.Fatalf("entry %d mismatch", i)
		}
	}
}

func TestMomentumPct(t *testing.T) {
	closes := make([]float64, 30)
	for i := range closes {
		closes[i] = 10
	}
	closes[29] = 12 // 相对 closes[9] 涨 20%
	m := momentumPct(closes, 20)
	if m == nil || *m < 19.9 || *m > 20.1 {
		t.Fatalf("want ~20, got %v", m)
	}
	if momentumPct(closes[:10], 20) != nil {
		t.Error("insufficient sample must return nil")
	}
	withZero := append([]float64{0, -1}, closes...)
	if momentumPct(withZero, 20) == nil {
		t.Error("zero/negative closes must be filtered, not poison result")
	}
}

func TestIsETFCodeAndNormalize(t *testing.T) {
	for c, want := range map[string]bool{"510300": true, "159915": true, "561100": true, "588000": true,
		"164900": true, "600519": false, "300750": false, "123456": false} {
		if got := isETFCode(c); got != want {
			t.Errorf("isETFCode(%s)=%v want %v", c, got, want)
		}
	}
	if normalizeCode("sh600519") != "600519" || normalizeCode(" 600519 ") != "600519" || normalizeCode("hk00700") != "" {
		t.Error("normalizeCode wrong")
	}
}

func TestSectorLeaderOrderingTopK(t *testing.T) {
	f := newFake()
	f.stocks = []StockQuote{quote("600010", "A", 5, 5e8, 1e11), quote("600011", "B", 15, 5e8, 1e11), quote("600012", "C", 25, 5e8, 1e11)}
	f.sectorRank = []SectorFlow{{Code: "BK1", Name: "板块1", MainNetInflow: 1e9}}
	f.constituents["BK1"] = []ConstituentStock{{Code: "600010", Name: "A", Price: 5}, {Code: "600011", Name: "B", Price: 15}, {Code: "600012", Name: "C", Price: 25}}
	cfg := DefaultConfig()
	cfg.SectorLeadersK = 2
	g := NewGenerator(f, cfg)
	out, err := g.genSectorLeaders(context.Background(), map[string]bool{"600010": true, "600011": true, "600012": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].Symbol != "600012" || out[1].Symbol != "600011" {
		t.Fatalf("want price-desc top2 [600012 600011], got %+v", out)
	}
}
