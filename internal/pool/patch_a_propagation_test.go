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

func TestToGenData_PerSecurityActionAndAnchors(t *testing.T) {
	now := time.Date(2026, 10, 17, 15, 30, 45, 123456789, time.UTC)
	prevBase := now.AddDate(0, 0, -3)
	f := baseFake() // 本轮命中: 600519/601127(SECTOR), 300750(SECTOR+ETF), 无 FUND_FLOW 命中
	g := NewGenerator(f, DefaultConfig())
	g.PersistentStatus = func(context.Context) map[string]AggEntry {
		return map[string]AggEntry{
			// 在册且本轮命中 -> TOUCH（锚点刷新为状态机口径）
			"600519": {Symbol: "600519", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase},
			// MANUAL 主来源且在册 -> PROTECT（即便本轮命中也不得改写）
			"601127": {Symbol: "601127", PrimarySource: sourceManual, Sources: []SourceHit{{SourceType: sourceManual}}, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase},
			// 在册但本轮不再出现、未过截止 -> KEEP（ExtraDecisions 旁路，锚点=持久原值）
			"600002": {Symbol: "600002", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase, ExpireDate: ptrTimeWire(now.AddDate(0, 0, 3))},
			// 在册但本轮不再出现、已过期 -> FAILED 最差证据口径 -> MARK_STALE（绝不借道失效）
			"600003": {Symbol: "600003", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE", FirstSeenAt: prevBase, LastSeenAt: prevBase, ExpireDate: ptrTimeWire(prevBase)},
		}
	}
	res, err := g.Generate(context.Background(), nil, now)
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

	// 1. 逐证券区分动作（审查点 1：不得用一个全局动作代表整池）。
	if actOf["600519"] != "TOUCH" {
		t.Fatalf("600519 action=%q want TOUCH", actOf["600519"])
	}
	if actOf["601127"] != "PROTECT" {
		t.Errorf("601127 action=%q want PROTECT（MANUAL 在册命中也不得改写）", actOf["601127"])
	}
	if actOf["300750"] == "TOUCH" || actOf["300750"] == "PROTECT" {
		t.Errorf("300750 新候选不应继承其它证券的动作: %q", actOf["300750"])
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
