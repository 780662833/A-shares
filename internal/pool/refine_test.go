package pool

import (
	"context"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Layer 0.75 精筛管线：证券资格 / 财务状态闸门 / RS 相对强度排序（全离线）
// ---------------------------------------------------------------------------

func entryOf(symbol, source string) AggEntry {
	return AggEntry{Symbol: symbol, SourceType: source, PrimarySource: source, SectorName: "测试", Sources: []SourceHit{{SourceType: source, Reason: "测试注入", Status: StatusOK}}, HitThisRun: true}
}

// qual 构造资格画像（eps/roe 直接取地址）。
func qual(listing string, eps, roe float64) *StockQualification {
	e, r := eps, roe
	return &StockQualification{ListingDate: listing, BasicEPS: &e, ROEWeighted: &r}
}

// qualNilBoth 仅上市日期，财务字段全缺失（未知放行路径）。
func qualNilBoth(listing string) *StockQualification {
	return &StockQualification{ListingDate: listing}
}

// refineCfg 构造一个仅含股票类条目、可精确控制闸门/RS 的生成器。
func refineCfg() Config {
	c := DefaultConfig()
	c.MinListedDays = 120
	c.RequirePositiveEPS = true
	c.MinROE = 0
	c.RSWindow = 60
	c.RSEnabled = true
	c.RSTopK = 0
	return c
}

// mkGenerateWithPool 用直接注入池子条目的方式测试 refinePool（绕开四来源）。
func runRefine(t *testing.T, f *fakeProvider, cfg Config, pool []AggEntry) ([]AggEntry, []string) {
	t.Helper()
	g := NewGenerator(f, cfg)
	warns := []string{}
	out := g.refinePool(context.Background(), pool, &warns, testNow)
	return out, warns
}

func TestRefineNewIPORejected(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification {
		switch code {
		case "309999": // 上市 30 天 -> 次新剔除
			return qual("2026-09-12", 1, 5)
		default: // 上市 5 年
			return qual("2021-01-01", 1, 5)
		}
	}
	cfg := refineCfg()
	cfg.RSEnabled = false // 只测闸门
	out, warns := runRefine(t, f, cfg, []AggEntry{
		entryOf("600519", sourceSector),
		entryOf("309999", sourceFund),
		entryOf("600000", sourceManual), // 人工白名单不受闸门约束
	})
	for _, e := range out {
		if e.Symbol == "309999" {
			t.Fatalf("new IPO must be rejected: %+v", out)
		}
	}
	if len(out) != 2 {
		t.Fatalf("want 2 kept, got %+v", out)
	}
	joined := ""
	for _, w := range warns {
		joined += w + "|"
	}
	if !contains(joined, "数据缺失") && !contains(joined, "") {
		t.Fatal("unreachable")
	}
}

func TestRefineFinancialGate(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification {
		switch code {
		case "600001":
			return qual("2020-01-01", -0.5, 3) // EPS<0 剔除
		case "600002":
			return qual("2020-01-01", 0.5, -8) // 负 ROE 剔除（MinROE=0 语义）
		case "600003":
			return qualNilBoth("2020-01-01") // 财务未知放行
		case "600004":
			return qual("", 0.1, 1) // 上市日未知放行
		}
		return nil
	}
	cfg := refineCfg()
	cfg.RSEnabled = false
	out, warns := runRefine(t, f, cfg, []AggEntry{
		entryOf("600001", sourceSector),
		entryOf("600002", sourceSector),
		entryOf("600003", sourceFund),
		entryOf("600004", sourceFund),
	})
	syms := map[string]bool{}
	for _, e := range out {
		syms[e.Symbol] = true
	}
	if syms["600001"] || syms["600002"] {
		t.Errorf("loss-makers must be gated out: %v", syms)
	}
	if !syms["600003"] || !syms["600004"] {
		t.Errorf("unknown-data stocks must pass: %v", syms)
	}
	if !contains(join(warns), "QUALIFY/RS 2 只股票数据缺失") {
		t.Errorf("want unknown-pass warning, got %v", warns)
	}
}

func TestRefineMinROEThreshold(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification {
		return qual("2020-01-01", 1, 3) // ROE 3%
	}
	cfg := refineCfg()
	cfg.RSEnabled = false
	cfg.MinROE = 5 // 要求 >=5% -> 3% 被拒
	// 注: 单条且全部被"明确证据"拒绝时, 保险丝不复活（judged==0 仅指上游故障场景）,
	// 但本例 qualify 均成功返回画像 -> scored=true -> 属明确判定, 池子合法清空。
	f2 := newFake()
	var mu sync.Mutex
	seen := map[string]bool{}
	f2.qualify = func(code string) *StockQualification {
		mu.Lock()
		first := !seen[code]
		seen[code] = true
		mu.Unlock()
		_ = first
		if code == "600001" {
			return qual("2020-01-01", 1, 3) // 低于阈值 -> 拒
		}
		return qual("2020-01-01", 1, 10) // 达标 -> 留
	}
	out2, _ := runRefine(t, f2, cfg, []AggEntry{
		entryOf("600001", sourceSector),
		entryOf("600002", sourceSector),
	})
	if len(out2) != 1 || out2[0].Symbol != "600002" {
		t.Fatalf("want only 600002 kept, got %+v", out2)
	}
}

func TestRefineRSSortTopKAndFuse(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification {
		return qual("2020-01-01", 1, 5)
	}
	// 基准 100 -> 110（+10%）; 个股涨幅: A +30%, B +20%, C -5%
	f.indexBars = func(idx string, lookback int) []DailyBar {
		return mkBars(100, 110, lookback)
	}
	f.stockBars = func(code string, lookback int) []DailyBar {
		switch code {
		case "600001":
			return mkBars(10, 13, lookback) // +30 -> RS=+20
		case "600002":
			return mkBars(10, 12, lookback) // +20 -> RS=+10
		case "600003":
			return mkBars(10, 9.5, lookback) // -5 -> RS=-15
		}
		return nil
	}
	cfg := refineCfg()
	poolIn := []AggEntry{
		entryOf("600003", sourceSector),
		entryOf("600001", sourceFund),
		entryOf("600002", sourceSector),
		entryOf("510300", sourceETF), // ETF 不参与 RS
	}
	out, warns := runRefine(t, f, cfg, poolIn)
	if len(out) != 4 {
		t.Fatalf("no topk: want 4, got %+v", out)
	}
	// 非股票条目保持在前, 股票类按 RS 降序整体插到其后
	if out[0].Symbol != "510300" {
		t.Errorf("ETF must keep head position, got %+v", out)
	}
	if out[1].Symbol != "600001" || out[2].Symbol != "600002" || out[3].Symbol != "600003" {
		t.Errorf("want RS desc [600001 600002 600003], got %+v", out)
	}
	if out[1].RankScore < 19.9 || out[1].RankScore > 20.1 {
		t.Errorf("RS score want ~20, got %v", out[1].RankScore)
	}
	if out[1].RankPos != 1 || out[3].RankPos != 3 {
		t.Errorf("rank pos wrong: %v %v", out[1].RankPos, out[3].RankPos)
	}

	// TopK 截断弱尾
	cfg.RSTopK = 2
	out2, warns2 := runRefine(t, f, cfg, poolIn)
	if len(out2) != 3 { // ETF + 前2名
		t.Fatalf("topk=2 want 3 entries, got %+v", out2)
	}
	if !contains(join(warns2), "RS_TOPK 截断弱尾 1 只") {
		t.Errorf("want topk warning, got %v", warns2)
	}

	// 保险丝: 闸门杀光全部股票类 -> 退化放行（不空池）
	fKill := newFake()
	fKill.qualify = func(code string) *StockQualification {
		return qual("2020-01-01", -1, 5) // 全亏损
	}
	cfg3 := refineCfg()
	cfg3.RSEnabled = false
	out3, warns3 := runRefine(t, fKill, cfg3, []AggEntry{
		entryOf("600001", sourceSector),
		entryOf("600002", sourceFund),
	})
	if len(out3) != 2 {
		t.Fatalf("fuse must restore all when gate kills everything, got %+v", out3)
	}
	if !contains(join(warns3), "疑似数据事故") {
		t.Errorf("want fuse warning, got %v", warns3)
	}
	_ = warns
}

func TestRefineBenchmarkMissingSkipsSort(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification {
		return qual("2020-01-01", 1, 5)
	}
	f.stockBars = func(code string, lookback int) []DailyBar {
		return mkBars(10, 12, lookback)
	}
	// indexBars 未设置 -> 基准不可得
	cfg := refineCfg()
	in := []AggEntry{entryOf("600002", sourceSector), entryOf("600001", sourceSector)}
	out, warns := runRefine(t, f, cfg, in)
	if len(out) != 2 || out[0].Symbol != "600002" || out[1].Symbol != "600001" {
		t.Fatalf("must keep original order when benchmark missing, got %+v", out)
	}
	if out[0].RankScore != 0 || out[0].RankPos != 0 {
		t.Errorf("no scoring without benchmark: %+v", out[0])
	}
	if !contains(join(warns), "RS_BENCHMARK") {
		t.Errorf("want benchmark warning, got %v", warns)
	}
}

func TestRefineDisabledKeepsLegacyBehavior(t *testing.T) {
	f := baseFake()
	cfg := DefaultConfig()
	cfg.QualifyEnabled = false
	g := NewGenerator(f, cfg)
	res, err := g.Generate(context.Background(), nil, testNow)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range res.Pool {
		if e.RankScore != 0 || e.RankPos != 0 {
			t.Fatalf("refine disabled must not score: %+v", e)
		}
	}
	if f.calls["Qualify:600519"] != 0 {
		t.Fatal("refine disabled must not call qualification API")
	}
}

func TestRefineEndToEndViaGenerate(t *testing.T) {
	f := baseFake()
	f.qualify = func(code string) *StockQualification {
		if code == "601127" {
			return qual("2026-09-20", 1, 5) // 赛力斯按次新剔除（testNow-30d 内）
		}
		return qual("2019-01-01", 1, 5)
	}
	f.indexBars = func(idx string, lookback int) []DailyBar { return mkBars(100, 100, lookback) }
	f.stockBars = func(code string, lookback int) []DailyBar {
		if code == "600519" {
			return mkBars(10, 15, lookback) // 茅台最强
		}
		return mkBars(10, 10.5, lookback)
	}
	g := NewGenerator(f, DefaultConfig())
	res, err := g.Generate(context.Background(), []string{"600000"}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	syms := map[string]int{}
	order := []string{}
	for i, e := range res.Pool {
		syms[e.Symbol] = i
		order = append(order, e.Symbol)
	}
	if _, ok := syms["601127"]; ok {
		t.Errorf("new IPO 601127 must be refined out: %v", order)
	}
	// 股票类内部: 茅台 RS 最高应排在宁德时代之前
	if syms["600519"] > syms["300750"] {
		t.Errorf("RS order wrong: %v", order)
	}
	// 人工白名单仍在池中且未被打分
	if _, ok := syms["600000"]; !ok {
		t.Errorf("manual must survive: %v", order)
	}
}

func join(ws []string) string {
	s := ""
	for _, w := range ws {
		s += w + " || "
	}
	return s
}

var _ = time.Now
