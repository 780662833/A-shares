// patch_a_propagation_test.go —— 补丁 A（第 1 阶段裁定）：Transition() 的逐证券
// 决策与时间锚点必须经 Generate → ToGenData JSON round-trip 后仍对应正确证券，
// 且写入层拿到的锚点就是状态机产出的锚点（核对还原后的时间值，不要求 JSON
// 文本逐字节相同）。离线、确定性 fakeProvider，不触碰网络。
package pool

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// wireAggEntry 把一条状态机决策按生产同一编码器编码为 WatchEntry 的 JSON 子集并解码,
// 用于断言"载荷里确实有什么"（动作 + 锚点的逐证券传递）, 而不是依赖内存对象。
// 复用 LifecycleDecisionPayload 保证与 ToGenData 生产路径同一口径。
func wireAggEntry(t *testing.T, e AggEntry) WatchEntryView {
	t.Helper()
	raw, err := LifecycleDecisionPayload(e)
	if err != nil {
		t.Fatal(err)
	}
	var v WatchEntryView
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// WatchEntryView 是载荷 JSON 中补丁 A 字段的只读视图（与 tool.WatchEntry 的同名标签对齐）。
type WatchEntryView struct {
	LifecycleAction string     `json:"lifecycle_action"`
	LastSeenAt      *time.Time `json:"last_seen_at"`
	ExpireDate      *time.Time `json:"expire_date"`
}

func TestToGenData_PerSecurityActionAndAnchors(t *testing.T) {
	now := time.Date(2026, 10, 17, 15, 30, 45, 123456789, time.UTC)
	prevBase := now.AddDate(0, 0, -3)
	f := baseFake() // 本轮 SECTOR 成分: 600519(BK0488)/601127+300750(BK0438); ETF: 510300/159915; LRT: 300750
	// 人工白名单选 601127：聚合优先级 MANUAL(0) > SECTOR(2)，其 PrimarySource 必为
	// MANUAL——测试前提由此可证，不依赖 fixture 巧合。
	g := NewGenerator(f, DefaultConfig())
	g.PersistentStatus = func(context.Context) map[string]AggEntry {
		return map[string]AggEntry{
			// 在册且本轮命中 -> TOUCH（锚点刷新为状态机口径）
			"600519": {Symbol: "600519", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase},
			// MANUAL 主来源且本轮以人工白名单命中 -> PROTECT（即便命中也不得改写）
			"601127": {Symbol: "601127", PrimarySource: sourceManual, Sources: []SourceHit{{SourceType: sourceManual}}, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase},
			// 在册但本轮不再出现、未过截止 -> KEEP（ExtraDecisions 旁路，锚点=持久原值）。
			// P4 前提：本 fixture 的必需来源（SECTOR/ETF/FUND）经 baseFake 全部成功，
			// 缺席记录按"确认未命中"走自然截止日判定——未过期才允许 KEEP。
			"600002": {Symbol: "600002", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase, ExpireDate: ptrTimeWire(now.AddDate(0, 0, 3))},
			// 在册但本轮不再出现、已过期，且本轮必需来源**全部成功**（OK/SUCCESS_EMPTY）
			// -> EXPIRE（P4 统一口径：完整成功证据下缺席超期返回自然过期动作；F-2 冻结
			// 使写入层对 EXPIRE 仍只做审计保留，不自动失效）。失败证据下的 MARK_STALE
			// 保守路径由 TestAbsentCandidate_ConservativeUnderFailedSources 独立覆盖。
			"600003": {Symbol: "600003", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase, ExpireDate: ptrTimeWire(prevBase)},
		}
	}
	res, err := g.Generate(context.Background(), []string{"601127"}, now)
	if err != nil {
		t.Fatal(err)
	}
	data, err := res.ToGenData()
	if err != nil {
		t.Fatal(err)
	}
	actOf := map[string]string{}
	lsOf := map[string]*time.Time{}
	expOf := map[string]*time.Time{}
	for i := range data.Pool {
		e := &data.Pool[i]
		actOf[e.Symbol] = e.LifecycleAction
		lsOf[e.Symbol] = e.LastSeenAt
		expOf[e.Symbol] = e.ExpireDateP
	}

	// 0. 前提自证（fixture 契约核验，失败即说明测试前提不成立，须先修前提）：
	//    601127 的人工意图必须真的进入本轮池内记录的全部证据（Transition 的
	//    HasManualSource 保护按全部 Sources 检查，而非只看 PrimarySource）。
	var inPool601127 *AggEntry
	for i := range res.Pool {
		if res.Pool[i].Symbol == "601127" {
			inPool601127 = &res.Pool[i]
		}
	}
	if inPool601127 == nil || !inPool601127.HasManualSource() {
		t.Fatalf("前提失败：601127 未以 MANUAL 证据进入本轮池（人工白名单链路未生效），后续 PROTECT 断言无效")
	}

	// 1. 逐证券区分动作（审查点 1：不得用一个全局动作代表整池）。
	if actOf["600519"] != "TOUCH" {
		t.Fatalf("600519 action=%q want TOUCH", actOf["600519"])
	}
	if actOf["601127"] != "PROTECT" {
		t.Errorf("601127 action=%q want PROTECT（MANUAL 在册命中也不得改写）", actOf["601127"])
	}
	// 300750 走的是 SECTOR+LRT 两条自动来源、持久层无记录的新候选路径。按第 1 阶段
	// 边界裁定，该路径不属于五动作迁移（无既有记录可迁移），动作必须保持**空**——
	// 既不得继承其它证券的动作，也不得被预先标成 TOUCH 伪装成状态机决策；其入池
	// 初始化由写入层的新候选插入语义完成（见 tool 层 NewCandidateInsert 测试）。
	if actOf["300750"] != "" {
		t.Errorf("300750 新候选动作应为空（无既有记录不参与五动作迁移），实际 %q", actOf["300750"])
	}

	//  2. TOUCH 携带状态机锚点：last_seen_at = 注入运行时刻（round-trip 保留亚秒精度），
	//     expire_date = AddDate 自然日口径——不是写入层 now+7*24h 重算值。
	wantLS := now
	if lsOf["600519"] == nil || !lsOf["600519"].Equal(wantLS) {
		t.Errorf("600519 last_seen_at=%v want %v", lsOf["600519"], wantLS)
	}
	wantExp := wantLS.AddDate(0, 0, autoExpireDaysForPool)
	if expOf["600519"] == nil || !expOf["600519"].Equal(wantExp) {
		t.Errorf("600519 expire_date=%v want %v (AddDate 自然日口径)", expOf["600519"], wantExp)
	}

	// 3. ExtraDecisions 缺席候选：KEEP/EXPIRE 各自对应正确证券，锚点透传持久值。
	var d2 struct {
		LifecycleAction string     `json:"lifecycle_action"`
		LastSeenAt      *time.Time `json:"last_seen_at"`
		ExpireDate      *time.Time `json:"expire_date"`
	}
	raw2, okk := data.ExtraDecisions["600002"]
	if !okk {
		t.Fatal("缺席旧候选 600002 未进入 ExtraDecisions")
	}
	if err := json.Unmarshal(raw2, &d2); err != nil {
		t.Fatal(err)
	}
	if d2.LifecycleAction != "KEEP" {
		t.Errorf("600002 extra action=%q want KEEP（必需来源全部成功且未过截止 -> 自然保留）", d2.LifecycleAction)
	}
	if d2.LastSeenAt == nil || !d2.LastSeenAt.Equal(prevBase) {
		t.Errorf("600002 旁路锚点未按持久值透传: %v want %v", d2.LastSeenAt, prevBase)
	}
	var d3 struct {
		LifecycleAction string     `json:"lifecycle_action"`
		LastSeenAt      *time.Time `json:"last_seen_at"`
		ExpireDate      *time.Time `json:"expire_date"`
	}
	raw3, okk := data.ExtraDecisions["600003"]
	if !okk {
		t.Fatal("缺席旧候选 600003 未进入 ExtraDecisions")
	}
	if err := json.Unmarshal(raw3, &d3); err != nil {
		t.Fatal(err)
	}
	// P4 统一口径：同一轮运行中所有必需来源均成功完成时，两个缺席候选按各自
	// 截止日期产生**不同**动作——600002 未过期 KEEP、600003 已过期 EXPIRE。
	// F-2 继续冻结：EXPIRE 在写入层仍只是审计动作，不触发自动失效。
	if d3.LifecycleAction != "EXPIRE" {
		t.Errorf("600003 extra action=%q want EXPIRE（完整成功证据下缺席超期走自然过期判定；F-2 冻结仅影响写入效果）", d3.LifecycleAction)
	}
	if d3.LastSeenAt == nil || !d3.LastSeenAt.Equal(prevBase) {
		t.Errorf("600003 旁路锚点未按持久值透传: %v want %v", d3.LastSeenAt, prevBase)
	}

	// 1b. P3 新候选边界补充断言：既不得进入 ExtraDecisions/TransitionDecisions 旁路，
	//     也不得被赋予任何五动作迁移语义（初始化由插入路径负责）。
	if _, bad := data.ExtraDecisions["300750"]; bad {
		t.Errorf("300750 新候选不应出现在 ExtraDecisions 持久化决策旁路中")
	}
	for code := range res.TransitionDecisions {
		if code == "300750" {
			t.Errorf("300750 新候选不应进入 TransitionDecisions（无既有记录不参与五动作迁移）")
		}
	}

	// 4. 载荷中所有非空动作都属于五枚举；round-trip 至少有一条带动作（传递生效）。
	found := false
	for _, e := range data.Pool {
		switch e.LifecycleAction {
		case "", "TOUCH", "KEEP", "EXPIRE", "MARK_STALE", "PROTECT":
		default:
			t.Errorf("%s 携带非法动作 %q", e.Symbol, e.LifecycleAction)
		}
		if e.LifecycleAction != "" {
			found = true
			// 裁定：非空动作的条目必须同时透传锚点，写入层才无需（也不允许）重算。
			if e.LastSeenAt == nil {
				t.Errorf("%s 动作 %s 缺少 last_seen_at 锚点透传", e.Symbol, e.LifecycleAction)
			}
		}
	}
	if !found {
		t.Error("round-trip 后所有条目动作均为空——传递未生效")
	}
}

// TestAbsentCandidate_ConservativeUnderFailedSources —— P5（缺席候选失败证据场景）：
// 只要任一**必需发现来源**（SECTOR_LEADER / ETF_MOMENTUM / FUND_FLOW）本轮为
// PARTIAL / FAILED / STALE，或状态缺失、不可识别，即使候选已超过截止日期也必须
// 保守返回 MARK_STALE——绝不允许在证据不完整时走自然过期判定。
// 与主测试（全部来源成功 -> 600003 EXPIRE）构成同一规则的两个半边。
// 本函数只覆盖单元层（absentCandidateRefreshStatus 的输入形态矩阵）；端到端三个
// 单点失败场景见 TestAbsentCandidate_E2E_MarkStaleUnderEachFailedSource。
func TestAbsentCandidate_ConservativeUnderFailedSources(t *testing.T) {
	// 单元层：absentCandidateRefreshStatus 对每种失败形态都必须降级为 FAILED。
	unitCases := []struct {
		name string
		st   map[string]SourceStatus
	}{
		{"sector partial", map[string]SourceStatus{sourceSector: StatusPartial, sourceETF: StatusOK, sourceFund: StatusOK}},
		{"etf failed", map[string]SourceStatus{sourceSector: StatusOK, sourceETF: StatusFailed, sourceFund: StatusSuccessEmpty}},
		{"fund stale", map[string]SourceStatus{sourceSector: StatusSuccessEmpty, sourceETF: StatusOK, sourceFund: StatusStale}},
		{"required key missing (只带 LIQUIDITY_SCREEN 不算成功)", map[string]SourceStatus{"LIQUIDITY_SCREEN": StatusOK, sourceSector: StatusOK, sourceETF: StatusOK}},
		{"empty statuses", map[string]SourceStatus{}},
		{"nil statuses", nil},
		{"all ok", map[string]SourceStatus{sourceSector: StatusOK, sourceETF: StatusOK, sourceFund: StatusOK}},
		{"all success-empty", map[string]SourceStatus{sourceSector: StatusSuccessEmpty, sourceETF: StatusOK, sourceFund: StatusSuccessEmpty}},
	}
	for _, c := range unitCases {
		got := absentCandidateRefreshStatus(c.st)
		wantSuccess := c.name == "all ok" || c.name == "all success-empty"
		if wantSuccess {
			if got != StatusSuccessEmpty {
				t.Errorf("%s: absentCandidateRefreshStatus=%v want SUCCESS_EMPTY（完整成功证据应放行截止日判定）", c.name, got)
			}
		} else if got != StatusFailed {
			t.Errorf("%s: absentCandidateRefreshStatus=%v want FAILED（保守降级，禁止过期路径）", c.name, got)
		}
	}
}

// 端到端：三个必需来源各注入一条真实的本轮失败证据（SECTOR=PARTIAL / ETF=FAILED /
// FUND=FAILED），Generate 后缺席超期候选必须 MARK_STALE——绝不允许在证据不完整时
// 走 EXPIRE/KEEP 的自然过期判定（P4 保守语义）。
// 每个场景独立核验前提：
//   - 600003 确属持久层旧候选（PersistentStatus 注入）且本轮不在 res.Pool 中；
//   - 注入来源本轮状态与预期失败形态一致（防止 fixture 漂移测不到目标分支）；
//   - 另外两个必需来源本轮明确为成功（OK/SUCCESS_EMPTY）——失败是单点注入的，
//     不允许"整轮都坏"稀释断言强度。
//
// 错误注入全部走 generator_test.go 既有钩子（constituentsErr / fundListErr）与
// 本文件的测试专用 Provider 包装器（dragonTigerErrWrapper + failingFundProvider），
// 不修改通用 fixture、不给 fakeProvider 新增字段。
func TestAbsentCandidate_E2E_MarkStaleUnderEachFailedSource(t *testing.T) {
	now := time.Date(2026, 10, 17, 15, 30, 45, 0, time.UTC)
	prevBase := now.AddDate(0, 0, -3)
	absent := map[string]AggEntry{
		// 截止日已过（prevBase < now）的持久层旧候选——只有在"必需来源全部成功"时
		// 才允许 EXPIRE；任何单点失败证据都必须把它压回 MARK_STALE。
		"600003": {Symbol: "600003", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase, ExpireDate: ptrTimeWire(prevBase)},
	}

	cases := []struct {
		name    string
		injKey  string
		wantSt  SourceStatus // 注入来源的本轮期望状态
		otherOK []string     // 其余必须明确成功的必需来源
		mutate  func(*fakeProvider)
		wrap    func(Provider) Provider
	}{
		{
			name:   "SECTOR partial",
			injKey: sourceSector,
			// BK0488 成功 + BK0438/BK1234 取数失败 => 部分失败但有可用成分股 => PARTIAL。
			mutate: func(f *fakeProvider) {
				f.constituentsErr = map[string]error{
					"BK0438": errors.New("boom: sector constituent fetch failed"),
					"BK1234": errors.New("boom: sector constituent fetch failed"),
				}
			},
			wantSt:  StatusPartial,
			otherOK: []string{sourceETF, sourceFund},
		},
		{
			name:   "ETF failed",
			injKey: sourceETF,
			// FundList 为核心必要请求：失败即来源级 FAILED。
			mutate:  func(f *fakeProvider) { f.fundListErr = errors.New("boom: fund list failed") },
			wantSt:  StatusFailed,
			otherOK: []string{sourceSector, sourceFund},
		},
		{
			name:   "FUND failed (dragon-tiger error via test wrapper)",
			injKey: sourceFund,
			// fakeProvider.DragonTigerInstitution 没有错误钩子；用测试专用包装器只覆盖
			// 该方法并委托其余调用——保持通用 fixture 不动（范围控制裁定）。
			wrap:    func(p Provider) Provider { return &failingFundProvider{p} },
			wantSt:  StatusFailed,
			otherOK: []string{sourceSector, sourceETF},
		},
	}

	for _, tc := range cases {
		f := baseFake()
		if tc.mutate != nil {
			tc.mutate(f)
		}
		var prov Provider = f
		if tc.wrap != nil {
			prov = tc.wrap(prov)
		}
		g := NewGenerator(prov, DefaultConfig())
		g.PersistentStatus = func(context.Context) map[string]AggEntry { return absent }
		res, err := g.Generate(context.Background(), nil, now)
		if err != nil {
			t.Fatalf("%s: generate: %v", tc.name, err)
		}
		// 前提 1：注入来源本轮状态确实是预期的失败形态。
		if got := res.SourceStatuses[tc.injKey]; got != tc.wantSt {
			t.Fatalf("%s: 前提失败：注入来源 %s 本轮状态=%v want %v", tc.name, tc.injKey, got, tc.wantSt)
		}
		// 前提 2：其余必需来源必须**明确存在**且为 OK/SUCCESS_EMPTY。
		for _, k := range tc.otherOK {
			st, ok := res.SourceStatuses[k]
			if !ok || (st != StatusOK && st != StatusSuccessEmpty) {
				t.Fatalf("%s: 前提失败：其余必需来源 %s 状态=%v(ok=%v) 非明确成功", tc.name, k, st, ok)
			}
		}
		// 前提 3：600003 属持久层旧候选且本轮不在 Pool（缺席判定成立）。
		for i := range res.Pool {
			if res.Pool[i].Symbol == "600003" {
				t.Fatalf("%s: 前提失败：600003 意外出现在本轮 Pool", tc.name)
			}
		}
		data, err := res.ToGenData()
		if err != nil {
			t.Fatalf("%s: ToGenData: %v", tc.name, err)
		}
		rawd, okk := data.ExtraDecisions["600003"]
		if !okk {
			t.Fatalf("%s: 缺席候选 600003 未进入 ExtraDecisions", tc.name)
		}
		var d struct {
			LifecycleAction string `json:"lifecycle_action"`
		}
		if err := json.Unmarshal(rawd, &d); err != nil {
			t.Fatal(err)
		}
		if d.LifecycleAction != "MARK_STALE" {
			t.Errorf("%s: 600003 action=%q want MARK_STALE（任一必需来源带失败证据时禁止过期/保留路径）", tc.name, d.LifecycleAction)
		}
	}
}

// failingFundProvider 测试专用 Provider 包装器：只覆盖 DragonTigerInstitution
// （恒返回错误 -> FUND_FLOW 经生产 markSrc 路径记 FAILED），其余方法原样委托给
// 内层 Provider。避免为单个测试给通用 fakeProvider 增加错误钩子字段。
type failingFundProvider struct {
	Provider
}

func (w *failingFundProvider) DragonTigerInstitution(ctx context.Context, start, end time.Time) ([]InstitutionBuy, error) {
	return nil, errors.New("boom: dragon-tiger fetch failed (test injection)")
}
