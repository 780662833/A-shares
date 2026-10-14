package pool

import (
	"context"
	"errors"
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
	// 显式双窗口（20/60）: 契约默认口径, 夹具不依赖 DefaultConfig 的内部默认值,
	// 避免"配置解析回落"与"窗口取值"两件事在同一用例里耦合（此前长窗未设置时
	// 解析回落为短窗同值, 触发单窗口退化路径, 使双窗口用例失真）。
	c.RSShortWindow = 20
	c.RSLongWindow = 60
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
	// 阶段 5 双窗口口径下, 本用例固定单窗口退化模式（长期窗口=短期窗口）,
	// 使中间态总分 = R20 超额收益；最终 RankScore 由阶段 7 四因子百分位口径覆盖
	// （断言已迁移为百分位口径, 见 README §12.2）。
	f := newFake()
	f.qualify = func(code string) *StockQualification {
		return qual("2020-01-01", 1, 5)
	}
	// 基准 100 -> 110（+10%）; 个股涨幅: A +30%, B +20%, C -5%
	f.indexBars = func(idx string, lookback int) []DailyBar {
		return mkBars(100, 110, lookback)
	}
	// 行业指数显式置为不可得：本用例验证"行业侧未确认 → 最高归 B"契约。
	// fake 的 IndexBars 在未注入 sectorIndexBars 时会把行业名请求复用到宽基
	// indexBars(+10%)，使 RS20_sector 被伪确认为正、A 门槛失真；真实供应商按
	// 行业名取数失败返回 error——这里用 error 注入模拟取数失败（符合 Go 错误
	// 处理契约, refinePool 行业侧对 err != nil 诚实降级为 nil"无法确认"）。
	f.sectorIndexBars = func(name string, lookback int) []DailyBar { return nil }
	f.indexBarsErr = errors.New("sector unavailable")
	f.stockBars = func(code string, lookback int) []DailyBar {
		switch code {
		case "600001":
			return mkFlat(61, 13, 10, 10) // R20 +30 / R60 +30 -> RS_index=+20
		case "600002":
			return mkFlat(61, 12, 10, 10) // R20 +20 / R60 +20 -> RS_index=+10
		case "600003":
			return mkFlat(61, 9.5, 10, 10) // R20 -5 / R60 -5 -> RS_index=-15
		}
		return nil
	}
	cfg := refineCfg()
	cfg.RSLongWindow = 20 // 单窗口退化: 中间分 = R20 超额（长窗端点样本不足）
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
	bySym := map[string]AggEntry{}
	for _, e := range out {
		bySym[e.Symbol] = e
	}
	a, b, c := bySym["600001"], bySym["600002"], bySym["600003"]
	// 路径判定（契约语义, 与旧版数值基准一致）：
	//   600001: R20>0 ∧ RS20_index>0 ∧ 行业侧未确认 → B（修复候选段首位）
	//   600002: 同上但 RS 弱一档 → B（第二段）
	//   600003: R20<0 → NONE（不满足 B 共同准入），落其余段但仍参与打分排序
	if a.Strength != StrengthB || b.Strength != StrengthB {
		t.Errorf("600001/600002 want B (short-strong vs index, sector unconfirmed), got %v/%v", a.Strength, b.Strength)
	}
	if c.Strength != StrengthNone {
		t.Errorf("600003 want NONE (R20<0 fails B admission), got %v", c.Strength)
	}
	// 分段输出顺序：非股票直通段(ETF/MANUAL)保持原相对顺序整体在前，
	// 其后为股票类分段 A → B → NONE（refinePool 池重组契约）。
	if out[0].Symbol != "510300" {
		t.Errorf("want pass-through ETF first, got %v", out[0].Symbol)
	}
	if out[1].Symbol != "600001" || out[2].Symbol != "600002" {
		t.Errorf("want B-segment order [600001 600002], got [%v %v]", out[1].Symbol, out[2].Symbol)
	}
	if out[3].Symbol != "600003" {
		t.Errorf("want tail NONE-segment [600003], got %v", out[3].Symbol)
	}
	// 百分位口径终裁（n=3, midrank, P=(rank-1)/(n-1)*100）：
	//   RS20_index 超额 20/10/-15 全互异 → P ∈ {100, 50, 0}；
	//   R60（单窗口退化复用短窗原始收益）30/20/-5 → P ∈ {100, 50, 0}；
	//   行业侧与 ADV 不可得 → 权重归一后 Score = 0.7*P(RS)+0.3*P(R60)。
	//   600001 = 0.7*100+0.3*100 = 100；600002 = 50；600003 = 0。
	if a.RankScore < 99.9 || a.RankScore > 100.1 {
		t.Errorf("percentile score want ~100 (top of both factors), got %v", a.RankScore)
	}
	if b.RankScore < 49.9 || b.RankScore > 50.1 {
		t.Errorf("percentile score want ~50 (midrank p50), got %v", b.RankScore)
	}
	if c.RankScore < -0.1 || c.RankScore > 0.1 {
		t.Errorf("percentile score want ~0 (bottom of both factors), got %v", c.RankScore)
	}
	// RankPos 只对打分股票连续编号（B 段 1/2, NONE 参与打分排 3, ETF 不编号）
	if a.RankPos != 1 || b.RankPos != 2 || c.RankPos != 3 {
		t.Errorf("rank pos wrong: %v %v %v", a.RankPos, b.RankPos, c.RankPos)
	}
	if bySym["510300"].RankPos != 0 {
		t.Errorf("ETF must stay unnumbered, got %v", bySym["510300"].RankPos)
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

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 阶段 5: 双窗口相对强度（R20 + R60）专项测试
// ---------------------------------------------------------------------------

// mkOHLCFromDaily 由收盘价序列生成合法 OHLC（High/Low 围绕收盘对称，满足
// High >= max(Open,Close)、Low <= min(Open,Close)）。
func mkOHLCFromDaily(bars []DailyBar) []barOHLC {
	out := make([]barOHLC, 0, len(bars))
	for _, b := range bars {
		out = append(out, barOHLC{Date: b.Date, Open: b.Close, High: b.Close * 1.01, Low: b.Close * 0.99, Close: b.Close})
	}
	return out
}

// mkAmounts 生成 n 行成交额序列：前段固定 base，尾部 m 行为 last（下标按尾部定位）。
// 用于构造 ADV20 因子可区分的流动性夹具（liquidityGatePool 只看最近 advWindow=20 行）。
func mkAmounts(n int, base, last float64, tail int) []DailyAmountRow {
	rows := make([]DailyAmountRow, 0, n)
	for i := 0; i < n; i++ {
		v := base
		if i >= n-tail {
			v = last
		}
		rows = append(rows, DailyAmountRow{Amount: v})
	}
	return rows
}

// runRefineDual 以显式短/长窗口执行精筛（仅供测试构造退化场景；传 0 走配置解析）。
func runRefineDual(t *testing.T, f *fakeProvider, cfg Config, pool []AggEntry, shortWin, longWin int) ([]AggEntry, []string) {
	t.Helper()
	g := NewGenerator(f, cfg)
	warns := []string{}
	out := g.refinePoolWith(context.Background(), pool, &warns, testNow, shortWin, longWin)
	return out, warns
}

func TestRefineDualWindowWeightedScore(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
	// 基准单序列: R20 +4%（bars[n-21]=100）、R60 +8%（bars[n-61]=96.296）
	f.indexBars = func(idx string, lookback int) []DailyBar {
		return mkFlat(61, 104, 100, 96.296) // 104/100-1=4%, 104/96.296-1≈8%
	}
	// 个股 X: R20 +14% (超额+10), R60 +2% (超额-6) -> 总分 = 0.6*10 + 0.4*(-6) = 3.6
	sb := mkFlat(61, 13, 11.4035, 12.7451) // 13/11.4035-1≈14%, 13/12.7451-1≈2%
	f.stockBars = func(code string, lookback int) []DailyBar { return sb }
	// 阶段 3 硬条件校验单独取 OHLC: 复用同一序列生成合法 OHLC,
	// 尾部日期与 as_of(testNow=asOfDate 当日) 对齐。
	f.barsOHLC = func(code string, lookback int) []barOHLC { return mkOHLCFromDaily(sb) }
	cfg := refineCfg() // RSShortWindow=20, RSLongWindow=60, weight=60%
	// 显式注入短/长窗口 20/60: 基准与个股都是固定 61 根序列, R20/R60 端点语义唯一,
	// 不依赖 fake 钩子对 lookback 的透传方式。
	out, _ := runRefineDual(t, f, cfg, []AggEntry{entryOf("600001", sourceSector)}, 20, 60)
	if len(out) != 1 {
		t.Fatalf("entry must survive, got %d", len(out))
	}
	e := out[0]
	// README §12.2 断言口径迁移：RankScore 已从旧线性加权总分(0.6*10+0.4*(-6)=3.6)
	// 改为四因子百分位合成（0-100）。单样本不构成分布，百分位恒取中位锚 50；
	// 加权总分的符号语义改由窗口超额中间量断言承载（见下），路径判定不受总分影响。
	if e.RankScore != 50 {
		t.Fatalf("single-sample percentile anchor want 50, got %v", e.RankScore)
	}
	if e.RetShortExcess == nil || *e.RetShortExcess < 9.9 || *e.RetShortExcess > 10.1 {
		t.Errorf("want R20 excess ~10, got %v", e.RetShortExcess)
	}
	if e.RetLongExcess == nil || *e.RetLongExcess < -6.1 || *e.RetLongExcess > -5.9 {
		t.Errorf("want R60 excess ~-6, got %v", e.RetLongExcess)
	}
	// 阶段 5 语义修正：A/B 判定看的是"窗口超额"而非加权总分符号——
	// 短期强(R20 超额为正)且排名列前 1/3（池内唯一打分者 rank=1/1）-> A 路径，
	// 即使中期大幅弱于基准拉负总分，仍应标 A（总分仅用于排序截断，不否决路径）。
	if e.Strength != StrengthA {
		t.Errorf("short-dominant stock should be path A, got %v", e.Strength)
	}
}

func TestRefineStrengthPathSemantics(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
	// 基准两窗口均为 +0%（横盘：现价=R20端点=R60端点=100）
	f.indexBars = func(idx string, lookback int) []DailyBar {
		return mkFlat(61, 100, 100, 100)
	}
	// 行业指数 K 线: "测试" 行业 R20 = +20%（bars[n-21]=82.5 -> 99/82.5-1=+20）。
	// 刻意让最强个股 600001（R20=+10）跑输行业（RS20_sector=-10），验证 A 路径的
	// RS20_sector>0 硬门槛——行业侧确认不过关时必须诚实降级 B（契约 6.2: 未知或为
	// 负都不进 A），绝不允许"排名列前即 A"。
	f.sectorIndexBars = func(name string, lookback int) []DailyBar {
		return mkFlat(61, 99, 82.5, 82.5)
	}
	f.stockBars = func(code string, lookback int) []DailyBar {
		switch code {
		case "600001": // R20 +10, R60 +10 -> 短期超额正 -> B（行业侧 -10 否决 A）
			return mkFlat(61, 110, 100, 100)
		case "600002": // R20 -5, R60 +20 -> 短期超额负 -> NONE（B 需 R20 与 RS20_index 皆正）
			return mkFlat(61, 120, 126.316, 100) // 120/126.316-1=-5%, 120/100-1=+20%
		case "600003": // R20 -10, R60 -10 -> 双窗皆弱 -> NONE
			return mkFlat(61, 90, 100, 100)
		}
		return nil
	}
	cfg := refineCfg()
	in := []AggEntry{
		entryOf("600002", sourceSector),
		entryOf("600003", sourceSector),
		entryOf("600001", sourceSector),
	}
	out, _ := runRefine(t, f, cfg, in)
	var byCode = map[string]AggEntry{}
	for _, e := range out {
		byCode[e.Symbol] = e
	}
	if byCode["600001"].Strength != StrengthB {
		t.Errorf("600001 want B (short-strong vs index but sector unconfirmed-negative), got %v (score %v)", byCode["600001"].Strength, byCode["600001"].RankScore)
	}
	if byCode["600002"].Strength != StrengthNone {
		t.Errorf("600002 want NONE (R20 excess negative fails B admission), got %v (score %v)", byCode["600002"].Strength, byCode["600002"].RankScore)
	}
	if byCode["600003"].Strength != StrengthNone {
		t.Errorf("600003 want NONE (both windows weak), got %v", byCode["600003"].Strength)
	}
	// 分段序 A→B→其余 + 段内按四因子百分位分降序连续编号：
	// 600001(B, P(RS)=100/P(R60)=100 -> 100) RankPos=1；
	// 600002(NONE, P(RS)=50/P(R60)=100 -> 75) RankPos=2；600003(全末位 -> 0) RankPos=3。
	if byCode["600001"].RankPos != 1 || byCode["600002"].RankPos != 2 || byCode["600003"].RankPos != 3 {
		t.Errorf("rank pos wrong: %+v", []int{byCode["600001"].RankPos, byCode["600002"].RankPos, byCode["600003"].RankPos})
	}
}

// mkFlat 生成恰好 n 根的升序三段式日 K，显式指定三个锚点价格消除歧义：
//   - 现价 last（bars[n-1]）
//   - R20 端点价 shortBase（bars[n-21]）→ R20 = last/shortBase - 1
//   - R60 端点价 longBase（bars[n-61]）→ R60 = last/longBase - 1
//
// n 不足对应段长度时该段不存在（调用方按样本不足处理）。索引一律按尾部定位，
// 因此同一序列在任意 lookback 请求下 R20/R60 端点语义一致。
func mkFlat(n int, last, shortBase, longBase float64) []DailyBar {
	bars := make([]DailyBar, 0, n)
	base := asOfDate.AddDate(0, 0, -(n - 1))
	for i := 0; i < n; i++ {
		var c float64
		switch {
		case i >= n-20:
			c = last
		case i >= n-60:
			c = shortBase
		default:
			c = longBase
		}
		bars = append(bars, DailyBar{Date: base.AddDate(0, 0, i).Format("2006-01-02"), Close: c})
	}
	return bars
}

// asOfDate 与 testNow 同日: 使 quoteAsOfMatches 校验通过（尾部 K 线截止日 == as_of）。
var asOfDate = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

func TestRefineSingleWindowDegeneration(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
	// 基准只有短期窗口可得：mkFlat(21,...) 无 R60 端点段（21 < needBars(60)=61），
	// 且本用例以显式长窗=60 执行（绕过配置解析），精确构造"长期不可得"。
	f.indexBars = func(idx string, lookback int) []DailyBar {
		return mkFlat(21, 105, 100, 100) // R20 = 105/100-1 = +5%; R60 不可得
	}
	// 个股 R20 +15%、R60 样本不足不可得 -> 只用短窗口打分, 超额 = 15 - 5 = 10
	f.stockBars = func(code string, lookback int) []DailyBar {
		return mkFlat(21, 11.5, 10, 10) // 11.5/10-1 = +15%
	}
	cfg := refineCfg()
	out, _ := runRefineDual(t, f, cfg, []AggEntry{entryOf("600001", sourceSector)}, 0, 60)
	if len(out) != 1 {
		t.Fatalf("entry must survive, got %d", len(out))
	}
	e := out[0]
	if !e.Scored {
		t.Fatalf("should score with available short window only")
	}
	// README §12.2 断言口径迁移：单窗口退化下 RankScore 不再是"短窗超额原值(~10)"，
	// 而是百分位口径（n==1 恒取中位锚 50）；超额数值本身由 RetShortExcess 断言承载。
	if e.RankScore != 50 {
		t.Errorf("single-window degeneration want percentile anchor 50, got %v", e.RankScore)
	}
	if e.RetShortExcess == nil || *e.RetShortExcess < 9.9 || *e.RetShortExcess > 10.1 {
		t.Errorf("want R20 excess ~10, got %v", e.RetShortExcess)
	}
	if e.RetLongExcess != nil {
		t.Errorf("long excess must stay nil when long window unavailable, got %v", *e.RetLongExcess)
	}
	// 契约注释语义：单窗口退化时唯一正超额窗口即视为短期强 -> A
	if e.Strength != StrengthA {
		t.Errorf("want A via short-only window, got %v", e.Strength)
	}
}

func TestRefineInvalidWindowsFallback(t *testing.T) {
	f := newFake()
	f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
	// 基准固定 61 根: R20 端点=现价=100 -> 基准R20=0; R60 端点=80 -> 基准R60=+25%
	f.indexBars = func(idx string, lookback int) []DailyBar { return mkFlat(61, 100, 100, 80) }
	// 个股固定 61 根: 现价 13、R20 端点 10、R60 端点 10 -> R20=R60=+30%
	// 回落正确(short=20,long=60): R20超额=30-0=30, R60超额=30-25=5
	//   总分 = 0.6*30 + 0.4*5 = 20。
	// 若非法 short=90 未回落被直接采用: needBars(90)=91 > 61 -> 短窗样本不足,
	//   单窗口退化只用长窗打分 -> 总分=5 ≠ 20，断言可区分两种口径。
	f.stockBars = func(code string, lookback int) []DailyBar { return mkFlat(61, 13, 10, 10) }
	cfg := refineCfg()
	cfg.RSShortWindow = 90 // >= longWin(60) -> 非法, 应回落 20
	cfg.RSLongWindow = 0   // 非法 -> 取 RSWindow=60
	out, _ := runRefine(t, f, cfg, []AggEntry{entryOf("600001", sourceSector)})
	if len(out) != 1 {
		t.Fatalf("entry must survive, got %d", len(out))
	}
	e := out[0]
	// README §12.2 断言口径迁移：回落正确性不再用旧线性加权总分(~20)验证，
	// 而由窗口超额中间量直接证明（short=90 未回落时短窗样本不足, R20 超额将为 nil）；
	// RankScore 为百分位口径, n==1 恒取中位锚 50。
	if e.RankScore != 50 {
		t.Errorf("want percentile anchor 50 (n==1), got %v", e.RankScore)
	}
	if e.RetShortExcess == nil || *e.RetShortExcess < 29.9 || *e.RetShortExcess > 30.1 {
		t.Errorf("want R20 excess ~30 after fallback, got %v", e.RetShortExcess)
	}
}
