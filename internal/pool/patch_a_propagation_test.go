// patch_a_propagation_test.go —— 补丁 A（第 1 阶段裁定）：Transition() 的逐证券
// 决策与时间锚点必须经 Generate → ToGenData JSON round-trip 后仍对应正确证券，
// 且写入层拿到的锚点就是状态机产出的锚点（核对还原后的时间值，不要求 JSON
// 文本逐字节相同）。离线、确定性 fakeProvider，不触碰网络。
package pool

import (
	"context"
	"encoding/json"
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
			// 在册但本轮不再出现、未过截止 -> KEEP（ExtraDecisions 旁路，锚点=持久原值）
			"600002": {Symbol: "600002", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase, ExpireDate: ptrTimeWire(now.AddDate(0, 0, 3))},
			// 在册但本轮不再出现、已过期 -> FAILED 最差证据口径 -> MARK_STALE（绝不借道失效）
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

	// 3. ExtraDecisions 缺席候选：KEEP/MARK_STALE 各自对应正确证券，锚点透传持久值。
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
		t.Errorf("600002 extra action=%q want KEEP", d2.LifecycleAction)
	}
	if d2.LastSeenAt == nil || !d2.LastSeenAt.Equal(prevBase) {
		t.Errorf("600002 旁路锚点未按持久值透传: %v want %v", d2.LastSeenAt, prevBase)
	}
	var d3 struct {
		LifecycleAction string `json:"lifecycle_action"`
	}
	raw3, okk := data.ExtraDecisions["600003"]
	if !okk {
		t.Fatal("缺席旧候选 600003 未进入 ExtraDecisions")
	}
	if err := json.Unmarshal(raw3, &d3); err != nil {
		t.Fatal(err)
	}
	if d3.LifecycleAction != "MARK_STALE" {
		t.Errorf("600003 extra action=%q want MARK_STALE（FAILED 证据下禁止 EXPIRE）", d3.LifecycleAction)
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
