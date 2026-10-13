package pool

// ============================================================================
// 阶段 6 专项测试：财务状态与重大风险过滤（纯函数 + 管线集成，全部离线）
// ============================================================================

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// finAsOf 比 testNow(当日15:00) 晚一天: classifyFinSnapshot 的公告日校验是
// d.After(asOf) 严格比较 —— 同日但更早时刻的 asOf 会把公告判成未来数据。
var finAsOf = testNow.AddDate(0, 0, 1)

func fptr(v float64) *float64 { return &v }

// cleanSnapshot 构造一份"完全健康"的非金融快照基准，测试按维度逐项劣化。
func cleanSnapshot() *FinSnapshot {
	return &FinSnapshot{
		Symbol:         "600001.SH",
		TtmNetProfit:   fptr(3e8),
		OpCFYearly:     fptr(2e8),
		OpCFPrevYearly: fptr(1.5e8),
		ROE3yMedian:    fptr(12.0),
		RevenueYoY:     fptr(8), ProfitYoY: fptr(10), DeclineYears: 0,
		Audit:          AuditStandard,
		IsFinancialSec: false,
		ReportPeriod:   "2025Q4", AnnouncementDt: "2026-03-31",
		DataAsOf: "2026-09-30", Source: "test-fin-vendor",
	}
}

func TestFinClassifyPass(t *testing.T) {
	st, reasons := classifyFinSnapshot(cleanSnapshot(), finAsOf)
	if st != FinPass {
		t.Fatalf("健康快照应 PASS, got %s (%v)", st, reasons)
	}
}

func TestFinBlockedMajorRiskRequiresEvidence(t *testing.T) {
	s := cleanSnapshot()
	s.MajorRisks = []MajorRiskFlag{{Kind: "FRAUD", Evidence: "处罚〔2026〕1号"}}
	st, _ := classifyFinSnapshot(s, finAsOf)
	if st != FinBlocked {
		t.Fatalf("已核实财务造假应 BLOCKED, got %s", st)
	}
	// 无核实依据的风险线索 -> 不得直接 BLOCKED（避免空口断言杀票）
	s2 := cleanSnapshot()
	s2.MajorRisks = []MajorRiskFlag{{Kind: "FRAUD", Evidence: ""}}
	st2, rs := classifyFinSnapshot(s2, finAsOf)
	if st2 == FinBlocked {
		t.Fatalf("缺依据的风险线索不得 BLOCKED")
	}
	if !strings.Contains(strings.Join(rs, ";"), "不计入BLOCKED") {
		t.Errorf("应记录降级原因, got %v", rs)
	}
}

func TestFinAuditOpinionGrading(t *testing.T) {
	for kind, want := range map[AuditOpinionKind]FinancialStatus{
		AuditAdverse:    FinBlocked,
		AuditDisclaimer: FinBlocked,
		AuditQualified:  FinBlocked, // 保留意见 -> 硬条件 BLOCKED（审计师存疑不可评分抵消）
		AuditEmphasis:   FinWarn,    // 带强调事项段 -> WARN 关注
	} {
		s := cleanSnapshot()
		s.Audit = kind
		st, _ := classifyFinSnapshot(s, finAsOf)
		if st != want {
			t.Errorf("审计%s 期望%s got %s", kind, want, st)
		}
	}
}

func TestFinFutureDataRejected(t *testing.T) {
	// 公告日晚于 as_of：回测/实时链路都禁止使用未来财报
	s := cleanSnapshot()
	s.AnnouncementDt = "2026-12-31"
	if st, _ := classifyFinSnapshot(s, finAsOf); st != FinDataInsufficient {
		t.Fatalf("未来数据必须 DATA_INSUFFICIENT, got %s", st)
	}
	// 公告日缺失同样按未知处理，绝不默认通过
	s2 := cleanSnapshot()
	s2.AnnouncementDt = ""
	if st, _ := classifyFinSnapshot(s2, finAsOf); st != FinDataInsufficient {
		t.Fatalf("公告时点缺失必须 DATA_INSUFFICIENT, got %s", st)
	}
}

func TestFinStaleAndNoProvider(t *testing.T) {
	s := cleanSnapshot()
	s.DataAsOf = "2024-01-01" // 远超 FinMaxAgeDays=400
	if st, rs := classifyFinSnapshot(s, finAsOf); st != FinDataInsufficient ||
		!strings.Contains(strings.Join(rs, ";"), "数据过期") {
		t.Fatalf("过期快照应 DATA_INSUFFICIENT, got %s %v", st, rs)
	}
	s2 := cleanSnapshot()
	s2.Source = ""
	if st, _ := classifyFinSnapshot(s2, finAsOf); st != FinDataInsufficient {
		t.Fatalf("缺 provider 溯源应 DATA_INSUFFICIENT, got %s", st)
	}
}

func TestFinQualityWeaknessIsWarnNotBlock(t *testing.T) {
	// 亏损成长股：TTM 净利为负 -> WARN（保留观察、降权），不是 BLOCKED
	s := cleanSnapshot()
	s.TtmNetProfit = fptr(-1e7)
	st, rs := classifyFinSnapshot(s, finAsOf)
	if st != FinWarn {
		t.Fatalf("亏损但无重大风险应 WARN, got %s", st)
	}
	if !strings.Contains(strings.Join(rs, ";"), "仓位从严") {
		t.Errorf("WARN 理由应提示仓位约束, got %v", rs)
	}
	// ROE 中位数不足 / 现金流连续两年为负 / 连续双降 -> 均 WARN
	mutate := func(fn func(*FinSnapshot)) FinancialStatus {
		x := cleanSnapshot()
		fn(x)
		st, _ := classifyFinSnapshot(x, finAsOf)
		return st
	}
	if got := mutate(func(x *FinSnapshot) { x.ROE3yMedian = fptr(5.0) }); got != FinWarn {
		t.Errorf("ROE 5%%<8%% 应 WARN, got %s", got)
	}
	if got := mutate(func(x *FinSnapshot) { x.OpCFYearly = fptr(-1e7); x.OpCFPrevYearly = fptr(-2e7) }); got != FinWarn {
		t.Errorf("现金流连续两年负应 WARN, got %s", got)
	}
	if got := mutate(func(x *FinSnapshot) { x.DeclineYears = 3 }); got != FinWarn {
		t.Errorf("收入利润连续3年双降应 WARN, got %s", got)
	}
	// 关键字段缺失 = "不知道"，不是"弱" -> DATA_INSUFFICIENT
	if got := mutate(func(x *FinSnapshot) { x.ROE3yMedian = nil }); got != FinDataInsufficient {
		t.Errorf("ROE 字段缺失应 DATA_INSUFFICIENT, got %s", got)
	}
}

func TestFinFinancialSectorSeparateRules(t *testing.T) {
	// 银行股 ROE 口径不同：不适用制造业阈值；质量指标差也不判 WARN
	s := cleanSnapshot()
	s.IsFinancialSec = true
	s.ROE3yMedian = fptr(2.0)
	s.TtmNetProfit = fptr(-1)
	st, rs := classifyFinSnapshot(s, finAsOf)
	if st != FinPass {
		t.Fatalf("金融板块第一版仅查重大风险, got %s (%v)", st, rs)
	}
	// 但重大风险对金融行业同样 BLOCKED
	s.MajorRisks = []MajorRiskFlag{{Kind: "DELIST_RISK", Evidence: "退市警示函〔2026〕3号"}}
	if st2, _ := classifyFinSnapshot(s, finAsOf); st2 != FinBlocked {
		t.Fatalf("金融板块重大风险仍应 BLOCKED, got %s", st2)
	}
}

func TestFinRankPenalty(t *testing.T) {
	if finRankPenalty(FinWarn) != 0.5 || finRankPenalty(FinPass) != 1.0 || finRankPenalty(FinDataInsufficient) != 1.0 {
		t.Fatal("WARN 降权系数应为 0.5, 其余 1.0")
	}
}

// ---- 管线集成测试 ----

type fakeFin struct {
	fn func(code string, asOf time.Time) (*FinSnapshot, error)
}

func (f *fakeFin) GetFinSnapshot(_ context.Context, code string, asOf time.Time) (*FinSnapshot, error) {
	return f.fn(code, asOf)
}

func refineCfgWithFin() Config {
	c := refineCfg()
	c.QualifyEnabled = true
	return c
}

func TestPipelineFinBlockedDroppedButRecordKept(t *testing.T) {
	f := newFake()
	f.qualify = func(string) *StockQualification { return qual("2020-01-01", 1, 5) }
	ib := mkFlat(61, 100, 100, 100)
	sb := mkFlat(61, 110, 110, 110)
	f.indexBars = func(string, int) []DailyBar { return ib }
	f.stockBars = func(string, int) []DailyBar { return sb }
	f.barsOHLC = func(string, int) []barOHLC { return mkOHLCFromDaily(sb) }
	g := NewGenerator(f, refineCfgWithFin())
	g.SetFinancialProvider(&fakeFin{fn: func(code string, asOf time.Time) (*FinSnapshot, error) {
		s := cleanSnapshot()
		s.Symbol = code
		s.MajorRisks = []MajorRiskFlag{{Kind: "FRAUD", Evidence: "处罚〔2026〕9号"}}
		return s, nil
	}})
	warns := []string{}
	poolIn := []AggEntry{entryOf("600001", sourceSector)}
	out := g.refinePoolWith(context.Background(), poolIn, &warns, finAsOf, 0, 0)
	// 被剔除出可交易队列（dropped 分支）——检查 warning 统计与 dropped 记录存在性
	var hasFin bool
	for _, w := range warns {
		if strings.Contains(w, "FINANCIAL") && strings.Contains(w, "BLOCKED 1") {
			hasFin = true
		}
	}
	if !hasFin {
		t.Fatalf("应有 FINANCIAL BLOCKED 统计 warning, got %v", warns)
	}
	for _, e := range out {
		if e.Symbol == "600001" && e.Financial != FinBlocked {
			t.Fatalf("若保留在输出则 financial_status 必须如实标 BLOCKED")
		}
	}
}

func TestPipelineProviderNotWiredHonestUnknown(t *testing.T) {
	// 未接入财务供应商：股票条目财务维度必须 DATA_INSUFFICIENT + tradability UNKNOWN，
	// 绝不能因为"没接供应商"就默认财务已通过。
	f := newFake()
	f.qualify = func(string) *StockQualification { return qual("2020-01-01", 1, 5) }
	ib := mkFlat(61, 100, 100, 100)
	sb := mkFlat(61, 110, 110, 110)
	f.indexBars = func(string, int) []DailyBar { return ib }
	f.stockBars = func(string, int) []DailyBar { return sb }
	f.barsOHLC = func(string, int) []barOHLC { return mkOHLCFromDaily(sb) }
	g := NewGenerator(f, refineCfgWithFin()) // 不调 SetFinancialProvider
	warns := []string{}
	out := g.refinePoolWith(context.Background(), []AggEntry{entryOf("600001", sourceSector)}, &warns, finAsOf, 0, 0)
	if len(out) != 1 {
		t.Fatalf("provider 未接入不应剔票, got %d", len(out))
	}
	e := out[0]
	if e.Financial != FinDataInsufficient {
		t.Fatalf("未接入应 DATA_INSUFFICIENT, got %s", e.Financial)
	}
	if e.Tradability != TradUnknown || e.BlockedBy != "FINANCIAL_DATA" {
		t.Fatalf("未接入应 UNKNOWN/FINANCIAL_DATA, got %s/%s", e.Tradability, e.BlockedBy)
	}
	if !strings.Contains(fmt.Sprint(warns), "provider 未接入") {
		t.Errorf("warning 应明示未接入, got %v", warns)
	}
}

func TestPipelineWarnDownrankAndProvenance(t *testing.T) {
	f := newFake()
	f.qualify = func(string) *StockQualification { return qual("2020-01-01", 1, 5) }
	ib := mkFlat(61, 100, 100, 100)
	sb := mkFlat(61, 110, 110, 110)
	f.indexBars = func(string, int) []DailyBar { return ib }
	f.stockBars = func(string, int) []DailyBar { return sb }
	f.barsOHLC = func(string, int) []barOHLC { return mkOHLCFromDaily(sb) }
	g := NewGenerator(f, refineCfgWithFin())
	g.SetFinancialProvider(&fakeFin{fn: func(code string, asOf time.Time) (*FinSnapshot, error) {
		s := cleanSnapshot()
		s.Symbol = code
		if code == "600001" { // 只让 600001 变 WARN
			s.ROE3yMedian = fptr(3.0)
		}
		return s, nil
	}})
	warns := []string{}
	in := []AggEntry{entryOf("600001", sourceSector), entryOf("600002", sourceSector)}
	out := g.refinePoolWith(context.Background(), in, &warns, finAsOf, 0, 0)
	bySym := map[string]AggEntry{}
	for _, e := range out {
		bySym[e.Symbol] = e
	}
	w, ok1 := bySym["600001"]
	p, ok2 := bySym["600002"]
	if !ok1 || !ok2 {
		t.Fatalf("两只都应保留, got %v", out)
	}
	if w.Financial != FinWarn || p.Financial != FinPass {
		t.Fatalf("状态不符: %s / %s", w.Financial, p.Financial)
	}
	// 同分数下 WARN 降权后名次必须低于 PASS
	if !(p.RankPos < w.RankPos) {
		t.Fatalf("WARN 应被降权排后: warn pos=%d pass pos=%d scores %v/%v",
			w.RankPos, p.RankPos, w.RankScore, p.RankScore)
	}
	// 溯源四字段必须回填
	if w.FinReportPeriod != "2025Q4" || w.FinAnnounceDate != "2026-03-31" ||
		w.FinDataAsOf != "2026-09-30" || w.FinProviderSource != "test-fin-vendor" {
		t.Fatalf("溯源字段未回填: %+v", w)
	}
	if w.FinReasons == "" || !strings.Contains(w.FinReasons, "ROE") {
		t.Fatalf("WARN 理由必须落库, got %q", w.FinReasons)
	}
	// WARN 不阻塞交易资格（能否交易由后续规则决定）
	if w.Tradability == TradBlocked && w.BlockedBy == "FINANCIAL" {
		t.Fatalf("WARN 不应被 FINANCIAL 硬阻塞")
	}
}
