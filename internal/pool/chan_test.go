// chan_test.go —— L3/L4 缠论分层（复用 python czsc）的离线单测。
//
// 覆盖三组验收口径：
//  1. fake ChanLayerClient 注入, 验证七条映射路径:
//     1B->P1 / 2B->P2 / 3B->P3 / 纯卖点->P4 / 买卖同日冲突->P4 /
//     Observation(无信号)->P0 / 网关未返回该票->DATA_INSUFFICIENT;
//  2. 网关整体失败 -> 整池条目原样保留(不删任何记录) + markAll 告警文案断言;
//  3. P4 条目经 ApplyChanLayer 末尾 ComputeTradability 重推导后
//     Tradability=BLOCKED, BlockedBy=CHAN_STRUCTURE 的端到端断言
//     （注: aggregate.go 实际实现使用 CHAN_STRUCTURE 作为 BlockedBy 常量,
//       早期讨论稿中的 "CHAN_INVALID" 已统一为该命名, 见本文件说明）。
package pool

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// fake ChanLayerClient: 可编程返回命中表或整体错误, 并记录调用参数供断言
// ---------------------------------------------------------------------------

type fakeChanClient struct {
	hits      map[string]ChanHit
	err       error
	gotSyms   []string
	gotCacheO bool
	calls     int
}

func (f *fakeChanClient) ScreenBatch(ctx context.Context, symbols []string, cacheOnly bool) (map[string]ChanHit, error) {
	f.calls++
	f.gotSyms = append([]string(nil), symbols...)
	f.gotCacheO = cacheOnly
	if f.err != nil {
		return nil, f.err
	}
	return f.hits, nil
}

// newChanTestGenerator 构造一个只开了缠论层的最小 Generator（不接 Provider,
// ApplyChanLayer 不依赖 prov/cfg 其他字段, ChanEnabled=true 是唯一必需开关）。
func newChanTestGenerator(cli ChanLayerClient) *Generator {
	cfg := DefaultConfig()
	cfg.ChanEnabled = true
	g := NewGenerator(nil, cfg)
	g.SetChanClient(cli)
	return g
}

// chanStockEntry 构造一条参与缠论分层的股票类条目（SECTOR_LEADER/FUND_FLOW 主来源）。
func chanStockEntry(sym string) AggEntry {
	return AggEntry{
		Symbol:        sym,
		Name:          sym + "-T",
		PrimarySource: sourceSector,
		Sources:       []SourceHit{{SourceType: sourceSector}},
		Eligibility:   EligPass,
		Liquidity:     LiqPass,
		Financial:     FinPass,
	}
}

func buySig(typ, dt string) ChanSignal {
	return ChanSignal{Type: typ, Dt: dt, Price: 10.5, Reason: typ + " 结构确认",
		PullbackLow: 9.8, StopLoss: 9.7, EffectiveStop: 9.7}
}

func sellSig(dt string) ChanSignal {
	return ChanSignal{Type: "1S", Dt: dt, Price: 12.0, Reason: "顶背驰卖点"}
}

// ---------------------------------------------------------------------------
// 1. 七条映射路径（classifyChanHit 单元级 + ApplyChanLayer 集成级双重锁定）
// ---------------------------------------------------------------------------

func TestClassifyChanHit_SevenMappingPaths(t *testing.T) {
	cases := []struct {
		name string
		hit  *ChanHit
		want ChanStatus
	}{
		{"1B->P1 BUY1_WATCH", &ChanHit{BuyPoints: []ChanSignal{buySig("1B", "2026-10-09")}}, ChanBuy1Watch},
		{"2B->P2 BUY2_CONFIRMED", &ChanHit{BuyPoints: []ChanSignal{buySig("2B", "2026-10-09")}}, ChanBuy2Confirm},
		{"3B->P3 BUY3_CONFIRMED", &ChanHit{BuyPoints: []ChanSignal{buySig("3B", "2026-10-09")}}, ChanBuy3Confirm},
		{"纯卖点->P4 STRUCTURE_INVALID", &ChanHit{SellPoints: []ChanSignal{sellSig("2026-10-09")}}, ChanInvalid},
		{"买卖同日冲突->P4(保守判失效)", &ChanHit{
			BuyPoints:  []ChanSignal{buySig("2B", "2026-10-09")},
			SellPoints: []ChanSignal{sellSig("2026-10-09")},
		}, ChanInvalid},
		{"Observation/无信号->P0 STRUCTURE_WATCH", &ChanHit{SignalType: "Observation"}, ChanWatch},
		{"nil hit->DATA_INSUFFICIENT", nil, ChanNoData},
		// 补充锁定: 买点更新而卖点更旧 -> 买点队列胜出(P2), 不因历史卖点误判失效。
		{"买点晚于卖点->按最新买点P2", &ChanHit{
			BuyPoints:  []ChanSignal{buySig("2B", "2026-10-12")},
			SellPoints: []ChanSignal{sellSig("2026-09-30")},
		}, ChanBuy2Confirm},
		// 补充锁定: 未知买点类型保守归观察, 不冒充确认。
		{"未知买点类型->P0不冒充确认", &ChanHit{BuyPoints: []ChanSignal{buySig("9X", "2026-10-09")}}, ChanWatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _ := classifyChanHit(tc.hit)
			if status != tc.want {
				t.Fatalf("classifyChanHit = %s, want %s", status, tc.want)
			}
		})
	}
}

func TestApplyChanLayer_SevenPathsEndToEnd(t *testing.T) {
	syms := []string{"S1B", "S2B", "S3B", "SSELL", "SCONFLICT", "SOBS", "SMISSING"}
	fake := &fakeChanClient{hits: map[string]ChanHit{
		"S1B":  {Symbol: "S1B", BuyPoints: []ChanSignal{buySig("1B", "2026-10-08")}, Summary: "底背驰候选"},
		"S2B":  {Symbol: "S2B", BuyPoints: []ChanSignal{buySig("2B", "2026-10-09")}, Summary: "二买成立"},
		"S3B":  {Symbol: "S3B", BuyPoints: []ChanSignal{buySig("3B", "2026-10-10")}, Summary: "三买成立"},
		"SSELL": {Symbol: "SSELL", SellPoints: []ChanSignal{sellSig("2026-10-10")}, Summary: "卖点确认"},
		"SCONFLICT": {Symbol: "SCONFLICT",
			BuyPoints:  []ChanSignal{buySig("3B", "2026-10-10")},
			SellPoints: []ChanSignal{sellSig("2026-10-10")}, Summary: "同日冲突"},
		"SOBS": {Symbol: "SOBS", SignalType: "Observation", ZsCount: 2, Summary: "两中枢观察"},
		// SMISSING 故意不出现在 hits 中 -> 网关未返回该票路径
	}}
	g := newChanTestGenerator(fake)
	poolIn := make([]AggEntry, 0, len(syms))
	for _, s := range syms {
		poolIn = append(poolIn, chanStockEntry(s))
	}
	var warnings []string
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	out := g.ApplyChanLayer(context.Background(), poolIn, &warnings, now)

	// 池规模不变: 分层只打状态, 绝不删条目。
	if len(out) != len(syms) {
		t.Fatalf("池条目数被篡改: got %d, want %d", len(out), len(syms))
	}
	// 请求参数透传断言: 全部 7 只股票进入批量, cache_only 取配置值。
	if len(fake.gotSyms) != len(syms) || !fake.gotCacheO {
		t.Fatalf("ScreenBatch 入参异常: syms=%v cacheOnly=%v", fake.gotSyms, fake.gotCacheO)
	}

	wantStatus := map[string]ChanStatus{
		"S1B":       ChanBuy1Watch,
		"S2B":       ChanBuy2Confirm,
		"S3B":       ChanBuy3Confirm,
		"SSELL":     ChanInvalid,
		"SCONFLICT": ChanInvalid,
		"SOBS":      ChanWatch,
		"SMISSING":  ChanNoData,
	}
	wantTier := map[string]string{
		"S1B": "P1", "S2B": "P2", "S3B": "P3",
		"SSELL": "P4", "SCONFLICT": "P4", "SOBS": "P0", "SMISSING": "P0",
	}
	idx := map[string]*AggEntry{}
	for i := range out {
		idx[out[i].Symbol] = &out[i]
	}
	for sym, want := range wantStatus {
		e := idx[sym]
		if e == nil {
			t.Fatalf("%s 条目丢失", sym)
		}
		if e.ChanStatus != want {
			t.Errorf("%s ChanStatus=%s, want %s", sym, e.ChanStatus, want)
		}
		if e.CandidateTier != wantTier[sym] {
			t.Errorf("%s CandidateTier=%s, want %s", sym, e.CandidateTier, wantTier[sym])
		}
	}
	// 确认时间/止损证据回填断言（P1~P4 有信号者必须带 dt 与 stop, 防止"仅凭一根K线标确认"无审计）。
	if e := idx["S2B"]; e.BuyPointConfirmedAt != "2026-10-09" || e.StopLoss != 9.7 || e.EffectiveStop != 9.7 {
		t.Errorf("S2B 信号证据回填缺失: %+v", e)
	}
	// 同日冲突判 P4 时, 记录的是胜出卖点而非买点。
	if e := idx["SCONFLICT"]; e.BuyPointConfirmedAt != "2026-10-10" || e.BuyPointReason != "顶背驰卖点" {
		t.Errorf("SCONFLICT 应回填胜出卖点证据: %+v", e)
	}
	// 未返回票: 诚实 DATA_INSUFFICIENT + 明确 reason, 且不得残留伪造的确认字段。
	if e := idx["SMISSING"]; e.ChanReason == "" || e.BuyPointConfirmedAt != "" {
		t.Errorf("SMISSING 诚实标记不完整: %+v", e)
	}
	// 完成态统计 warning 必须逐层给出数量（可审计口径）。
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, "CHAN 分层完成") ||
		!strings.Contains(joined, "P0观察 1") || !strings.Contains(joined, "P1一买观察 1") ||
		!strings.Contains(joined, "P2二买确认 1") || !strings.Contains(joined, "P3三买确认 1") ||
		!strings.Contains(joined, "P4结构失效 2") || !strings.Contains(joined, "数据不足 1") {
		t.Errorf("完成统计告警文案不符: %q", joined)
	}
}

// ---------------------------------------------------------------------------
// 2. 网关整体失败 -> 整池保留 + markAll 告警文案断言
// ---------------------------------------------------------------------------

func TestApplyChanLayer_GatewayFailureKeepsWholePool(t *testing.T) {
	fake := &fakeChanClient{err: errors.New("gateway unreachable: dial tcp 127.0.0.1:8000")}
	g := newChanTestGenerator(fake)
	poolIn := []AggEntry{chanStockEntry("A001"), chanStockEntry("A002"), chanStockEntry("A003")}
	warnings := []string{"前置告警"} // 验证追加而非覆盖
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	out := g.ApplyChanLayer(context.Background(), poolIn, &warnings, now)

	// 弱依赖核心契约: 整体失败一条都不能少。
	if len(out) != 3 {
		t.Fatalf("网关失败导致条目丢失: len=%d, want 3", len(out))
	}
	for _, e := range out {
		if e.ChanStatus != ChanNoData {
			t.Errorf("%s 应为 DATA_INSUFFICIENT, got %s", e.Symbol, e.ChanStatus)
		}
		if e.CandidateTier != "P0(DATA)" {
			t.Errorf("%s CandidateTier 应为 P0(DATA), got %s", e.Symbol, e.CandidateTier)
		}
		if !strings.Contains(e.ChanReason, "网关失败") {
			t.Errorf("%s ChanReason 未记录失败原因: %q", e.Symbol, e.ChanReason)
		}
	}
	if len(warnings) != 2 || warnings[0] != "前置告警" {
		t.Fatalf("warnings 必须追加且保留既有内容, got %v", warnings)
	}
	wantWarn := "CHAN 分层未完成(网关失败:gateway unreachable: dial tcp 127.0.0.1:8000): 3 只股票标记 DATA_INSUFFICIENT, 全部保留在研究池"
	if warnings[1] != wantWarn {
		t.Errorf("markAll 告警文案不符:\n got: %q\nwant: %q", warnings[1], wantWarn)
	}
}

// 客户端未注入(nil)同样走 markAll 诚实退化路径。
func TestApplyChanLayer_NilClientMarksAll(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ChanEnabled = true
	g := NewGenerator(nil, cfg) // 不调 SetChanClient
	poolIn := []AggEntry{chanStockEntry("A001")}
	var warnings []string
	out := g.ApplyChanLayer(context.Background(), poolIn, &warnings, time.Now())
	if len(out) != 1 || out[0].ChanStatus != ChanNoData {
		t.Fatalf("nil 客户端应整池 DATA_INSUFFICIENT 且不删条目, got %+v", out)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "客户端未接入") || !strings.Contains(warnings[0], "全部保留在研究池") {
		t.Errorf("未接入告警文案不符: %v", warnings)
	}
}

// ---------------------------------------------------------------------------
// 3. P4 -> ComputeTradability 端到端重推导: BLOCKED / CHAN_STRUCTURE
// ---------------------------------------------------------------------------

func TestApplyChanLayer_P4BlocksTradabilityEndToEnd(t *testing.T) {
	fake := &fakeChanClient{hits: map[string]ChanHit{
		"P4SYM": {Symbol: "P4SYM", SellPoints: []ChanSignal{sellSig("2026-10-10")}},
		"P2SYM": {Symbol: "P2SYM", BuyPoints: []ChanSignal{buySig("2B", "2026-10-10")}},
	}}
	g := newChanTestGenerator(fake)
	poolIn := []AggEntry{chanStockEntry("P4SYM"), chanStockEntry("P2SYM")}
	var warnings []string
	out := g.ApplyChanLayer(context.Background(), poolIn, &warnings, time.Now())

	bySym := map[string]AggEntry{}
	for _, e := range out {
		bySym[e.Symbol] = e
	}
	p4 := bySym["P4SYM"]
	// 端到端断言: 分层完成后 tradability 必须与最新 ChanStatus 一致。
	if p4.Tradability != TradBlocked {
		t.Errorf("P4 条目 Tradability=%s, want BLOCKED", p4.Tradability)
	}
	if p4.BlockedBy != "CHAN_STRUCTURE" {
		t.Errorf("P4 条目 BlockedBy=%s, want CHAN_STRUCTURE", p4.BlockedBy)
	}
	// 对偶断言: P2 已确认买点不等于放行——tradability 仍由 L0/L5 独立判定,
	// 缠论层不得越权把非阻塞条目改成 BLOCKED。
	p2 := bySym["P2SYM"]
	if p2.Tradability == TradBlocked && p2.BlockedBy == "CHAN_STRUCTURE" {
		t.Errorf("P2 不应被缠论层阻塞: %+v", p2)
	}
	// BlocksTradability 单元锁定: 只有 P4 阻塞交易资格。
	for s, want := range map[ChanStatus]bool{
		ChanWatch: false, ChanBuy1Watch: false, ChanBuy2Confirm: false,
		ChanBuy3Confirm: false, ChanNoData: false, ChanInvalid: true,
		"": false, // 未接入缠论层时空值行为与旧版一致
	} {
		if got := s.BlocksTradability(); got != want {
			t.Errorf("BlocksTradability(%q)=%v, want %v", s, got, want)
		}
	}
}

// IsConfirmedBuy 口径锁定: P1 高风险观察不算确认买点队列成员。
func TestChanStatusQueueSemantics(t *testing.T) {
	if ChanBuy1Watch.IsConfirmedBuy() {
		t.Error("P1 一买观察不得计入已确认买点队列")
	}
	if !ChanBuy2Confirm.IsConfirmedBuy() || !ChanBuy3Confirm.IsConfirmedBuy() {
		t.Error("P2/P3 必须是已确认买点队列成员")
	}
	if ChanNoData.ChanTier() != "P0" || ChanWatch.ChanTier() != "P0" {
		t.Error("P0/DATA_INSUFFICIENT 均归属 P0 观察层")
	}
}
