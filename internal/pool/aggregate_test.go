package pool

// ---------------------------------------------------------------------------
// 阶段 2 验收测试：聚合、去重（证据保全）与来源生命周期（全离线, 不触网）
// ---------------------------------------------------------------------------

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func rawEntry(symbol, source string) Entry {
	return Entry{Symbol: symbol, SourceType: source, SectorName: source + "板块"}
}

// 4.1 合并逻辑: 同身份多来源 -> sources[] 全保留, primary_source 按优先级。
func TestAggregateMergesSourcesAndKeepsEvidence(t *testing.T) {
	raw := []Entry{
		rawEntry("600519", sourceFund),
		rawEntry("600519", sourceSector),
		rawEntry("600519", sourceETF),
	}
	out := aggregateEntries(raw, "2026-10-13", testNow, nil)
	if len(out) != 1 {
		t.Fatalf("must merge to single identity, got %d", len(out))
	}
	e := out[0]
	if len(e.Sources) != 3 {
		t.Fatalf("dedup must NOT delete evidence: sources=%d want 3", len(e.Sources))
	}
	// 优先级排序后 ETF(1) > SECTOR(2) > FUND(3)
	if e.PrimarySource != sourceETF || e.SourceType != sourceETF {
		t.Errorf("primary=%s alias=%s want %s", e.PrimarySource, e.SourceType, sourceETF)
	}
	wantOrder := []string{sourceETF, sourceSector, sourceFund}
	for i, s := range e.Sources {
		if s.SourceType != wantOrder[i] {
			t.Fatalf("sources order %v wrong at %d: %s", wantOrder, i, s.SourceType)
		}
		if s.AsOf != "2026-10-13" || s.Status != StatusOK {
			t.Errorf("source hit must carry as_of & status: %+v", s)
		}
		if s.Reason == "" {
			t.Errorf("source hit must carry reason: %+v", s)
		}
	}
}

// 代码标准化在聚合入口完成: sh600000 / 600000 视为同一身份。
func TestAggregateNormalizesCode(t *testing.T) {
	out := aggregateEntries([]Entry{rawEntry("sh600000", sourceETF), rawEntry("600000", sourceManual)}, "2026-10-13", testNow, nil)
	if len(out) != 1 || out[0].Symbol != "600000" {
		t.Fatalf("normalize+merge failed: %+v", out)
	}
	if out[0].PrimarySource != sourceManual {
		t.Errorf("MANUAL must outrank ETF, got %s", out[0].PrimarySource)
	}
}

// 4.2 MANUAL + 黑名单并存: 保留人工来源记录, 但交易资格 BLOCKED(黑名单优先)。
func TestManualPlusBlacklistStaysBlocked(t *testing.T) {
	statusOf := func(code string) *AggEntry {
		if code == "600000" {
			return &AggEntry{Blacklisted: true}
		}
		return nil
	}
	out := aggregateEntries([]Entry{rawEntry("600000", sourceManual)}, "2026-10-13", testNow, statusOf)
	e := out[0]
	if !e.Blacklisted {
		t.Fatal("blacklist fact bit must survive aggregation")
	}
	if e.PrimarySource != sourceManual {
		t.Errorf("manual source record must be preserved, got %s", e.PrimarySource)
	}
	e.ComputeTradability()
	if e.Tradability != TradBlocked || e.BlockedBy != "BLACKLIST" {
		t.Errorf("blacklist must beat manual: trad=%s by=%s", e.Tradability, e.BlockedBy)
	}
}

// 状态字段独立保存: 主来源优先级不得覆盖资格/财务判定结果。
func TestStatusFieldsIndependentOfPrimary(t *testing.T) {
	out := aggregateEntries([]Entry{rawEntry("600001", sourceManual), rawEntry("600001", sourceSector)}, "2026-10-13", testNow, nil)
	e := &out[0]
	e.Eligibility = EligBlocked // 闸门回填的独立事实
	e.Financial = FinPass
	e.ComputeTradability()
	if e.Tradability != TradBlocked || e.BlockedBy != "ELIGIBILITY" {
		t.Errorf("eligibility must not be overridden by MANUAL primary: %+v", e)
	}
	// UNKNOWN 语义诚实: 精筛关闭时资格未知 -> tradability UNKNOWN, 默认不放行
	out2 := aggregateEntries([]Entry{rawEntry("600002", sourceSector)}, "2026-10-13", testNow, nil)
	out2[0].ComputeTradability()
	if out2[0].Tradability != TradUnknown {
		t.Errorf("unjudged auto stock must be UNKNOWN, got %s", out2[0].Tradability)
	}
}

// 4.3 过期规则状态机（六状态逐条）。
func mkRec(primary string) AggEntry {
	exp := testNow.AddDate(0, 0, -1) // 昨天已到期
	return AggEntry{Symbol: "600100", PrimarySource: primary, SourceLifecycle: "ACTIVE", ExpireDate: &exp}
}

func TestTransitionSixRefreshStates(t *testing.T) {
	cases := []struct {
		name    string
		rec     AggEntry
		hit     bool
		status  SourceStatus
		wantAct LifeAction
		wantLc  string
	}{
		{"OK继续命中->续期", mkRec(sourceSector), true, StatusOK, LifeTouch, "ACTIVE"},
		{"OK不再命中未到期->保留", mkRec(sourceSector), false, StatusOK, LifeKeep, "ACTIVE"},
		{"OK不再命中已过截止->过期", mkRec(sourceSector), false, StatusOK, LifeExpire, "EXPIRED"},
		{"SUCCESS_EMPTY不再命中->允许过期", mkRec(sourceSector), false, StatusSuccessEmpty, LifeExpire, "EXPIRED"},
		{"PARTIAL->不批量失效标STALE", mkRec(sourceSector), false, StatusPartial, LifeMarkStale, "STALE"},
		{"FAILED->保留上次有效标STALE", mkRec(sourceSector), false, StatusFailed, LifeMarkStale, "STALE"},
		{"STALE->禁止当有效信号", mkRec(sourceSector), false, StatusStale, LifeMarkStale, "STALE"},
		{"MANUAL永不改写", mkRec(sourceManual), false, StatusOK, LifeProtect, "ACTIVE"},
		{"黑名单永不改写", func() AggEntry { r := mkRec(sourceSector); r.Blacklisted = true; return r }(), false, StatusOK, LifeProtect, "ACTIVE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// "已过截止"用例把 ExpireDate 提前到过去; "未到期"用例推后
			rec := c.rec
			switch c.wantAct {
			case LifeExpire:
				past := testNow.AddDate(0, 0, -1)
				rec.ExpireDate = &past
			case LifeKeep:
				future := testNow.AddDate(0, 0, 3)
				rec.ExpireDate = &future
			}
			out, act := Transition(rec, c.hit, c.status, 7, testNow)
			if act != c.wantAct {
				t.Fatalf("action=%s want %s", act, c.wantAct)
			}
			if c.wantLc != "" && out.SourceLifecycle != c.wantLc {
				t.Fatalf("lifecycle=%s want %s", out.SourceLifecycle, c.wantLc)
			}
			if act == LifeTouch {
				if !out.LastSeenAt.Equal(testNow) {
					t.Error("TOUCH must refresh last_seen_at")
				}
				if out.ExpireDate == nil || !out.ExpireDate.After(testNow) {
					t.Error("TOUCH must extend expire date")
				}
			}
			if act == LifeProtect && out.SourceLifecycle != rec.SourceLifecycle {
				t.Error("PROTECT must not mutate lifecycle")
			}
		})
	}
}

// 7 天口径明确为自然日: 周五成功刷新, 下周五(7 自然日)内即使经历周末仍有效, 第 8 天过期。
func TestAutoExpireIsCalendarDays(t *testing.T) {
	rec := mkRec(sourceSector)
	fri := time.Date(2026, 10, 9, 16, 0, 0, 0, time.UTC) // 周五
	out, act := Transition(rec, true, StatusOK, AutoExpireDays, fri)
	if act != LifeTouch || out.ExpireDate == nil {
		t.Fatal("touch expected")
	}
	nextFri := fri.AddDate(0, 0, 7) // 恰好第 7 个自然日: 未过截止
	if _, act := Transition(out, false, StatusOK, AutoExpireDays, nextFri); act != LifeKeep {
		t.Errorf("day7 must still valid (calendar-day semantics incl. weekend), got %s", act)
	}
	day8 := fri.AddDate(0, 0, 8)
	if _, act := Transition(out, false, StatusOK, AutoExpireDays, day8); act != LifeExpire {
		t.Errorf("day8 must expire, got %s", act)
	}
}

// FAILED 持续发生（如长假接口故障）: 记录绝不被清理, 只是 STALE。
func TestPersistentFailureNeverExpires(t *testing.T) {
	rec := mkRec(sourceSector)
	cur := rec
	for i := 0; i < 30; i++ {
		now := testNow.AddDate(0, 0, i)
		next, act := Transition(cur, false, StatusFailed, AutoExpireDays, now)
		if act != LifeMarkStale {
			t.Fatalf("day%d: FAILED must never expire/clear, got %s", i, act)
		}
		if next.SourceLifecycle == "EXPIRED" {
			t.Fatalf("day%d: interface outage must not mark EXPIRED", i)
		}
		cur = next
	}
}

// Generate 端到端: 同一股票命中两个来源时, Result.Pool 只有一条且带双证据。
func TestGenerateEndToEndEvidencePreserved(t *testing.T) {
	f := newFakeProvider()
	// 让 600519 同时进入行业龙头与龙虎榜机构两个来源
	f.sectorRank = []SectorFlow{indFlow("半导体", 10)}
	f.constituents = map[string][]ConstituentStock{"半导体": {boardStock("600519", "贵州茅台")}}
	f.dragon = []InstitutionBuy{lhbRow("600519", "贵州茅台", 6000)} // 机构净买入 6000 万 >= 5000 万门槛
	f.stocks = []StockQuote{liteQuote("600519", "贵州茅台", 1500, 30, 2000*YuanPerYi)}
	g := NewGenerator(f, noRefineCfg())
	res, err := g.Generate(context.Background(), nil, testNow)
	if err != nil {
		t.Fatal(err)
	}
	var hit *AggEntry
	for i := range res.Pool {
		if res.Pool[i].Symbol == "600519" {
			hit = &res.Pool[i]
		}
	}
	if hit == nil {
		t.Fatal("600519 missing from pool")
	}
	if len(hit.Sources) != 2 {
		t.Fatalf("both source evidences must survive dedup, got %+v", hit.Sources)
	}
	if hit.PrimarySource != sourceSector { // SECTOR(2) > FUND(3)
		t.Errorf("primary=%s want %s", hit.PrimarySource, sourceSector)
	}
	if res.BySource[sourceSector] != 1 || res.BySource[sourceFund] != 0 {
		t.Errorf("by_source must count primary only: %v", res.BySource)
	}
}

// ---------------------------------------------------------------------------
// 阶段 2 测试共享辅助（按 provider.go 实际类型定义装载 fakeProvider）
// ---------------------------------------------------------------------------

// AutoExpireDays 自动来源默认生命周期：7 个自然日（含周末节假日, 约等于 5 个交易日）。
// 口径声明见 aggregate.go 头注释。
const AutoExpireDays = 7

func noRefineCfg() Config {
	c := DefaultConfig()
	c.QualifyEnabled = false // 纯聚合链路：关闭精筛, 走未知放行退化路径
	return c
}

func newFakeProvider() *fakeProvider { return newFake() }

// 紧凑构造器：与真实 Provider DTO 一一对应（单位遵循适配层契约：元/亿元字段名后缀标明）。
func indFlow(name string, netYi float64) SectorFlow {
	return SectorFlow{Name: name, MainNetInflow: netYi * YuanPerYi}
}
func boardStock(code, name string) ConstituentStock {
	return ConstituentStock{Code: code, Name: name}
}
func lhbRow(code, name string, orgNetWan float64) InstitutionBuy {
	return InstitutionBuy{Code: code, Name: name, OrgNetAmount: orgNetWan * YuanPerWan, Date: "2026-10-13"}
}
func liteQuote(code, name string, price, avgAmtYi, capYuan float64) StockQuote {
	return StockQuote{Code: code, Name: name, Price: price, AvgAmount20D: avgAmtYi * YuanPerYi, TotalMarketCap: capYuan}
}

// ---------------------------------------------------------------------------
// 4.2/输出层: Tradability 三态语义（ELIGIBLE / BLOCKED / UNKNOWN）逐条锁定
// ---------------------------------------------------------------------------

func trad(e AggEntry, want, by string) error {
	if e.Tradability != want || (by != "" && e.BlockedBy != by) {
		return fmt.Errorf("tradability=%s blocked_by=%q want %s/%q", e.Tradability, e.BlockedBy, want, by)
	}
	return nil
}

func TestTradabilitySemantics(t *testing.T) {
	stock := func() AggEntry {
		e := mkRec(sourceSector)
		e.Sources = []SourceHit{{SourceType: sourceSector}}
		e.Eligibility, e.Financial, e.Strength = EligUnknown, FinDataInsufficient, StrengthUnset
		return e
	}
	t.Run("未判定自动股票->UNKNOWN默认不放行", func(t *testing.T) {
		e := stock()
		e.ComputeTradability()
		if err := trad(e, TradUnknown, ""); err != nil {
			t.Error(err)
		}
	})
	t.Run("黑名单优先于一切(含MANUAL来源)", func(t *testing.T) {
		e := stock()
		e.PrimarySource = sourceManual
		e.Blacklisted = true
		e.Eligibility, e.Financial = EligPass, FinPass
		e.ComputeTradability()
		if err := trad(e, TradBlocked, "BLACKLIST"); err != nil {
			t.Error(err)
		}
	})
	t.Run("资格BLOCKED不被PASS的财务覆盖", func(t *testing.T) {
		e := stock()
		e.Eligibility, e.Financial = EligBlocked, FinPass
		e.ComputeTradability()
		if err := trad(e, TradBlocked, "ELIGIBILITY"); err != nil {
			t.Error(err)
		}
	})
	t.Run("财务负EPS归FINANCIAL维度", func(t *testing.T) {
		e := stock()
		e.Eligibility, e.Financial = EligPass, FinBlocked
		e.ComputeTradability()
		if err := trad(e, TradBlocked, "FINANCIAL"); err != nil {
			t.Error(err)
		}
	})
	t.Run("RS打分后落选(NONE)->软阻塞STRENGTH", func(t *testing.T) {
		e := stock()
		e.Eligibility, e.Financial, e.Strength = EligPass, FinPass, StrengthNone
		e.ComputeTradability()
		if err := trad(e, TradBlocked, "STRENGTH"); err != nil {
			t.Error(err)
		}
	})
	t.Run("未打分(Unset)不得吞成STRENGTH落选", func(t *testing.T) {
		e := stock()
		e.Eligibility, e.Financial, e.Strength = EligPass, FinPass, StrengthUnset
		e.ComputeTradability()
		if err := trad(e, TradEligible, ""); err != nil {
			t.Error(err)
		}
	})
	t.Run("ETF条目不适用股票闸门->按契约ELIGIBLE", func(t *testing.T) {
		out := aggregateEntries([]Entry{rawEntry("510300", sourceETF)}, "2026-10-13", testNow, nil)
		e := out[0]                                    // hasAutoSourceOnly()==false for ETF? ETF 是自动来源 -> 走 Unset 分支但 Scored=false
		e.Eligibility, e.Financial = EligPass, FinPass // refine 对 ETF 显式 PASS(N/A)
		e.ComputeTradability()
		if err := trad(e, TradEligible, ""); err != nil {
			t.Error(err)
		}
	})
	t.Run("全维度PASS且路径A/B->ELIGIBLE", func(t *testing.T) {
		for _, p := range []StrengthPath{StrengthA, StrengthB} {
			e := stock()
			e.Eligibility, e.Financial, e.Strength = EligPass, FinPass, p
			e.ComputeTradability()
			if err := trad(e, TradEligible, ""); err != nil {
				t.Errorf("%s: %v", p, err)
			}
		}
	})
}

// Generate 端到端 + PersistentStatus 钩子: DB 中的 BLACKLIST 事实位必须进入
// 聚合结果并压制 MANUAL 主来源的交易资格（用户示例场景）。
func TestGenerateBlacklistOverridesManual(t *testing.T) {
	f := newFakeProvider()
	g := NewGenerator(f, noRefineCfg())
	g.PersistentStatus = func(ctx context.Context) map[string]AggEntry {
		r := mkRec(sourceManual)
		r.Symbol = "600600"
		r.Blacklisted = true
		m := map[string]AggEntry{"600600": r}
		return m
	}
	res, err := g.Generate(context.Background(), []string{"600600"}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	var hit *AggEntry
	for i := range res.Pool {
		if res.Pool[i].Symbol == "600600" {
			hit = &res.Pool[i]
		}
	}
	if hit == nil {
		t.Fatal("manual entry must survive even if blacklisted")
	}
	if hit.PrimarySource != sourceManual {
		t.Errorf("manual source record must be preserved, got %s", hit.PrimarySource)
	}
	if !hit.Blacklisted || hit.Tradability != TradBlocked || hit.BlockedBy != "BLACKLIST" {
		t.Errorf("blacklist must win over manual: %+v", *hit)
	}
}
