// watchlist_apply_test.go 覆盖 Layer 0.5 动态池落库生命周期规则：
// 首次插入 / MANUAL 不覆盖 / BLACKLIST 终态 / 重命中续期 / 过期清理 / 非法代码丢弃。
package tool

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"ashare-quant/internal/model"
)

func newWatchTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	gdb, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "w.db")), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := gdb.AutoMigrate(&model.WatchlistItem{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return gdb
}

func entry(sym, name, src string) WatchEntry {
	return WatchEntry{Symbol: sym, Name: name, SourceType: src}
}

func mustFind(t *testing.T, gdb *gorm.DB, code string) model.WatchlistItem {
	t.Helper()
	var it model.WatchlistItem
	if err := gdb.Where("code = ?", code).First(&it).Error; err != nil {
		t.Fatalf("find %s: %v", code, err)
	}
	return it
}

func TestApplyWatchlistPool_Lifecycle(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 12, 15, 0, 0, 0, time.UTC)

	// 预置：一条人工白名单 + 一条已过期未再命中的旧动态条目 + 一条黑名单
	seed := []model.WatchlistItem{
		{Code: "600519", Name: "贵州茅台", SourceType: model.WatchSourceManual, Status: model.WatchStatusActive, AddedDate: now.Add(-30 * 24 * time.Hour)},
		{Code: "000777", Name: "过期老票", SourceType: model.WatchSourceFund, Status: model.WatchStatusActive, AddedDate: now.Add(-30 * 24 * time.Hour), ExpireDate: ptrTime(now.Add(-time.Hour))},
		{Code: "000888", Name: "屏蔽票", SourceType: model.WatchSourceSector, Status: model.WatchStatusBlacklist, AddedDate: now.Add(-30 * 24 * time.Hour), ExpireDate: ptrTime(now.Add(-time.Hour))},
	}
	for i := range seed {
		if err := gdb.Create(&seed[i]).Error; err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	data := &WatchlistGenData{
		TradeDate: "2026-10-12",
		Pool: []WatchEntry{
			entry("600519", "贵州茅台改名?", model.WatchSourceETF), // 已存在且为 MANUAL -> 跳过不覆盖
			entry("300750", "宁德时代", model.WatchSourceETF),    // 新插入
			entry("000888", "屏蔽票", model.WatchSourceSector),  // BLACKLIST 终态 -> 跳过
			entry("123", "非法代码", model.WatchSourceFund),      // 非 6 位数字 -> 丢弃
			entry("300750", "重复条目", model.WatchSourceFund),   // 同日重复 -> seen 双保险丢弃
			entry("002594", "比亚迪", model.WatchSourceSector),  // 新插入
			entry("688117", "刚入池又出池", model.WatchSourceFund), // 本轮命中 -> 应 ACTIVE+续期
		},
		Warnings: []string{"FUND_FLOW 失败(已跳过): mock"},
	}

	res, err := ApplyWatchlistPool(gdb, data, now, nil)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if res.Inserted != 3 {
		t.Errorf("inserted=%d want 3", res.Inserted)
	}
	if res.SkippedMan != 2 { // 600519(MANUAL) + 000888(BLACKLIST)
		t.Errorf("skipped_manual=%d want 2", res.SkippedMan)
	}
	// F-2 冻结合同（经批准的语义变更）: 缺少可信 data_as_of 证据链时,
	// 自动过期 sweep 不再基于旧 expire_date 写 INACTIVE -> Inactivated 恒为 0。
	if res.Inactivated != 0 {
		t.Errorf("inactivated=%d want 0 (auto-expiry suspended, conservative keep)", res.Inactivated)
	}

	// MANUAL 未被改写
	m := mustFind(t, gdb, "600519")
	if m.SourceType != model.WatchSourceManual || m.Name != "贵州茅台" {
		t.Errorf("manual row overwritten: %+v", m)
	}
	// 新 ETF 条目：ACTIVE + 7 天过期
	e := mustFind(t, gdb, "300750")
	if e.SourceType != model.WatchSourceETF || e.Status != model.WatchStatusActive {
		t.Errorf("etf row wrong: %+v", e)
	}
	if e.ExpireDate == nil || !e.ExpireDate.After(now) {
		t.Errorf("etf expire missing/not future: %v", e.ExpireDate)
	}
	// 过期清理与黑名单豁免（F-2: 000777 虽 expire_date 已过, 但自动失效已暂停 -> 保持 ACTIVE）
	if got := mustFind(t, gdb, "000777"); got.Status != model.WatchStatusActive {
		t.Errorf("expired row must stay ACTIVE under F-2 conservative keep, got %s", got.Status)
	}
	if got := mustFind(t, gdb, "000888"); got.Status != model.WatchStatusBlacklist {
		t.Errorf("blacklist row mutated: %s", got.Status)
	}
	// 非法代码不入库
	var n int64
	gdb.Model(&model.WatchlistItem{}).Where("code = ?", "123").Count(&n)
	if n != 0 {
		t.Errorf("invalid symbol persisted")
	}

	// —— 第二轮：002594 跌出池子但尚未过期 -> 保持 ACTIVE；再次命中则续期 LastSeenAt ——
	firstSeen := mustFind(t, gdb, "002594").LastSeenAt
	later := now.Add(24 * time.Hour) // 002594 expire = now+7d > later，不应被清理
	res2, err := ApplyWatchlistPool(gdb, &WatchlistGenData{
		TradeDate: "2026-10-13",
		Pool:      []WatchEntry{entry("300750", "宁德时代", model.WatchSourceETF)},
	}, later, nil)
	if err != nil {
		t.Fatalf("apply round2: %v", err)
	}
	if res2.Inactivated != 0 {
		t.Errorf("round2 inactivated=%d want 0 (not yet expired)", res2.Inactivated)
	}
	if res2.Updated != 1 {
		t.Errorf("round2 updated=%d want 1 (300750 re-hit)", res2.Updated)
	}
	if got := mustFind(t, gdb, "002594"); got.Status != model.WatchStatusActive {
		t.Errorf("unexpired miss should stay ACTIVE, got %s", got.Status)
	}
	if got := mustFind(t, gdb, "300750"); got.LastSeenAt == nil || !got.LastSeenAt.After(*firstSeen) {
		t.Errorf("re-hit should refresh last_seen_at")
	}
}

func TestApplyWatchlistPool_NilGuards(t *testing.T) {
	gdb := newWatchTestDB(t)
	if _, err := ApplyWatchlistPool(nil, &WatchlistGenData{}, time.Now(), nil); err == nil {
		t.Error("nil db should error")
	}
	if _, err := ApplyWatchlistPool(gdb, nil, time.Now(), nil); err == nil {
		t.Error("nil data should error")
	}
}

// ---------------------------------------------------------------------------
// F-2 回归合同（第 4 步 E 审计 + 方案 B 裁定）：
//   - 过期时间已过但数据日期证据缺失 -> 保持 ACTIVE, 不因旧 expire_date 失效;
//   - PARTIAL/FAILED/STALE/缺失/未知来源状态 -> 一律不触发自动失效;
//   - MANUAL 行继续受保护;
//   - 数据库操作失败必须向上返回错误。
// 恢复时间驱动自动失效的前提是可信 data_as_of 证据链回归, 本文件同时锁定该边界。
// ---------------------------------------------------------------------------

func TestApplyWatchlistPool_F2_ExpiryKeepsRowsUnderAllStatuses(t *testing.T) {
	cases := []struct {
		name     string
		statuses map[string]string
	}{
		{"statuses nil (unknown)", nil},
		{"statuses empty", map[string]string{}},
		{"PARTIAL", map[string]string{"SECTOR_LEADER": "PARTIAL"}},
		{"FAILED", map[string]string{"FUND_FLOW": "FAILED"}},
		{"STALE", map[string]string{"ETF_MOMENTUM": "STALE"}},
		{"unknown enum", map[string]string{"FUND_FLOW": "WHAT_IS_THIS"}},
		{"OK only", map[string]string{"LIQUIDITY_SCREEN": "OK", "SECTOR_LEADER": "OK"}},
		{"SUCCESS_EMPTY only", map[string]string{"FUND_FLOW": "SUCCESS_EMPTY"}},
		{"mixed OK+PARTIAL", map[string]string{"FUND_FLOW": "OK", "SECTOR_LEADER": "PARTIAL"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gdb := newWatchTestDB(t)
			now := time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)
			expired := now.Add(-time.Hour)
			rows := []model.WatchlistItem{
				{Code: "000777", Name: "过期老票", SourceType: model.WatchSourceFund, Status: model.WatchStatusActive, AddedDate: now.Add(-30 * 24 * time.Hour), ExpireDate: &expired},
				{Code: "600519", Name: "人工白名单", SourceType: model.WatchSourceManual, Status: model.WatchStatusActive, AddedDate: now.Add(-30 * 24 * time.Hour), ExpireDate: &expired},
			}
			for i := range rows {
				if err := gdb.Create(&rows[i]).Error; err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			// 本轮池子不含上述任何代码 -> 两条都是"过期未命中"候选。
			res, err := ApplyWatchlistPool(gdb, &WatchlistGenData{
				TradeDate: "2026-10-17",
				Pool:      []WatchEntry{entry("300750", "宁德时代", model.WatchSourceETF)},
			}, now, tc.statuses)
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			if res.Inactivated != 0 {
				t.Errorf("inactivated=%d want 0 (no path may auto-inactivate under F-2)", res.Inactivated)
			}
			if got := mustFind(t, gdb, "000777"); got.Status != model.WatchStatusActive {
				t.Errorf("expired auto row must stay ACTIVE, got %s", got.Status)
			}
			if got := mustFind(t, gdb, "600519"); got.Status != model.WatchStatusActive || got.SourceType != model.WatchSourceManual {
				t.Errorf("manual row mutated: %+v", got)
			}
		})
	}
}

func TestApplyWatchlistPool_F2_DBFailureReturnsError(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)
	// 先正常建表并种一行, 再删除底层表 -> 后续读写必然报错。
	if err := gdb.Create(&model.WatchlistItem{Code: "000777", SourceType: model.WatchSourceFund, Status: model.WatchStatusActive, AddedDate: now}).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := gdb.Migrator().DropTable(&model.WatchlistItem{}); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	_, err := ApplyWatchlistPool(gdb, &WatchlistGenData{
		TradeDate: "2026-10-17",
		Pool:      []WatchEntry{entry("300750", "宁德时代", model.WatchSourceETF)},
	}, now, map[string]string{"FUND_FLOW": "OK"})
	if err == nil {
		t.Fatal("expected error propagated from failed DB operation, got nil (silent success is forbidden)")
	}
}

func TestExpirySkipReasonContract(t *testing.T) {
	// 保守保留原因必须与冻结合同一致: 任何输入都不得给出"允许失效"的结论。
	if r := expirySkipReason(nil); !strings.Contains(r, "missing/unknown") {
		t.Errorf("nil statuses reason=%q", r)
	}
	if r := expirySkipReason(map[string]string{"A": "PARTIAL"}); !strings.Contains(r, "PARTIAL") {
		t.Errorf("partial reason=%q", r)
	}
	if r := expirySkipReason(map[string]string{"A": "FAILED"}); !strings.Contains(r, "FAILED") {
		t.Errorf("failed reason=%q", r)
	}
	if r := expirySkipReason(map[string]string{"A": "STALE"}); !strings.Contains(r, "STALE") {
		t.Errorf("stale reason=%q", r)
	}
	if r := expirySkipReason(map[string]string{"A": "???"}); !strings.Contains(r, "unknown") {
		t.Errorf("unknown enum reason=%q", r)
	}
	// 完整成功也不放行失效——缺少数据日期证据链。
	if r := expirySkipReason(map[string]string{"A": "OK", "B": "SUCCESS_EMPTY"}); !strings.Contains(r, "data_as_of") {
		t.Errorf("complete-success reason must cite missing data_as_of, got %q", r)
	}
}

// F-2.5 语义变更（经批准的合同修正，非为消除失败而改断言）：
// 自动失效已暂停（ApplyWatchlistPool 不再依据旧 ExpireDate 写 INACTIVE），
// 读取路径必须与之保持一致——只按 status = ACTIVE 过滤，绝不因 ExpireDate
// 已过而排除 ACTIVE 行。否则写入侧保留、读取侧淘汰，同一记录在两个生产
// 环节得到矛盾结论，等于变相恢复未经证据链审批的自动过期。
func TestActiveWatchlistCodes_KeepsExpiredActiveRows_F25(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Now()
	rows := []model.WatchlistItem{
		{Code: "000001", SourceType: model.WatchSourceManual, Status: model.WatchStatusActive, AddedDate: now},
		// 过期但状态仍 ACTIVE：F-2.5 后必须继续返回（ExpireDate 不参与读取过滤）。
		{Code: "000002", SourceType: model.WatchSourceETF, Status: model.WatchStatusActive, AddedDate: now, ExpireDate: ptrTime(now.Add(-time.Minute))},
		// INACTIVE 行无论日期如何都不返回。
		{Code: "000003", SourceType: model.WatchSourceFund, Status: model.WatchStatusInactive, AddedDate: now},
		{Code: "000004", SourceType: model.WatchSourceSector, Status: model.WatchStatusActive, AddedDate: now, ExpireDate: ptrTime(now.Add(time.Hour))},
		// 过期且 INACTIVE：由 status 排除，而非 ExpireDate。
		{Code: "000005", SourceType: model.WatchSourceETF, Status: model.WatchStatusInactive, AddedDate: now, ExpireDate: ptrTime(now.Add(-24 * time.Hour))},
	}
	for i := range rows {
		if err := gdb.Create(&rows[i]).Error; err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	codes, err := ActiveWatchlistCodes(gdb, now)
	if err != nil {
		t.Fatalf("codes: %v", err)
	}
	set := map[string]bool{}
	for _, c := range codes {
		set[c] = true
	}
	// 期望：全部 ACTIVE 行返回（含 ExpireDate 已过的 000002）；INACTIVE 不返回。
	if !set["000001"] || !set["000002"] || !set["000004"] {
		t.Errorf("expired-but-ACTIVE rows must not be filtered out by ExpireDate (F-2.5), codes=%v", codes)
	}
	if set["000003"] || set["000005"] {
		t.Errorf("INACTIVE rows must never be returned, codes=%v", codes)
	}
	if len(codes) != 3 {
		t.Errorf("codes=%v want exactly {000001,000002,000004}", codes)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

// ---------------------------------------------------------------------------
// 补丁 A（第 1 阶段）：生命周期动作契约 —— 五动作数据库语义、锚点透传、
// 时间零漂移、旧载荷兼容分离、保护与未知值。断言对象是真实数据库行，
// 不是映射函数返回值。
// ---------------------------------------------------------------------------


// sameTime 比较两个时间的"同一时刻"语义（忽略时区/表示差异），符合裁定：
// 核对还原后的时间值，不要求 JSON 文本逐字节相同。
func sameTime(a, b time.Time) bool {
	if a.IsZero() != b.IsZero() {
		return false
	}
	return a.Equal(b)
}

func TestApplyLifecycleContract_FiveActions(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 17, 15, 30, 45, 123456789, time.UTC) // 故意带时分秒纳秒
	base := now.AddDate(0, 0, -3)                                   // 上次确认命中
	oldExp := base.AddDate(0, 0, 7)
	seed := func(code, src, status string, ls, exp time.Time) {
		it := model.WatchlistItem{Code: code, Name: "旧名", SourceType: src, Status: status,
			AddedDate: base, LastSeenAt: ptrTime(ls), ExpireDate: ptrTime(exp)}
		if err := gdb.Create(&it).Error; err != nil {
			t.Fatalf("seed %s: %v", code, err)
		}
	}
	seed("600001", model.WatchSourceSector, model.WatchStatusActive, base, oldExp) // TOUCH
	seed("600002", model.WatchSourceSector, model.WatchStatusActive, base, oldExp) // KEEP
	seed("600003", model.WatchSourceSector, model.WatchStatusActive, base, oldExp) // MARK_STALE
	seed("600004", model.WatchSourceSector, model.WatchStatusActive, base, oldExp) // EXPIRE
	seed("600005", model.WatchSourceManual, model.WatchStatusActive, base, oldExp)       // PROTECT(人工)
	seed("600006", model.WatchSourceSector, model.WatchStatusBlacklist, base, oldExp)
	seed("600007", model.WatchSourceSector, model.WatchStatusActive, base, oldExp) // 未知动作
	seed("600008", model.WatchSourceSector, model.WatchStatusActive, base, oldExp) // 空动作(旧载荷)
	seed("600009", model.WatchSourceSector, model.WatchStatusActive, base, oldExp) // TOUCH缺锚点

	// 状态机口径的 TOUCH 锚点：自然日 AddDate(0,0,7)，与写入层 now+7*24h 不同——
	// 若落库读回等于本值即证明使用了透传锚点而非重算值。
	smSeen := now.Truncate(time.Second)
	smExp := smSeen.AddDate(0, 0, 7)

	mk := func(sym, action string, ls, exp *time.Time) WatchEntry {
		e := WatchEntry{Symbol: sym, Name: "新名-" + sym, SourceType: string(model.WatchSourceSector), LifecycleAction: action}
		e.LastSeenAt, e.ExpireDateP = ls, exp
		return e
	}
	data := &WatchlistGenData{Pool: []WatchEntry{
		mk("600001", "TOUCH", ptrTime(smSeen), ptrTime(smExp)),
		mk("600002", "KEEP", ptrTime(base), ptrTime(oldExp)),
		mk("600003", "MARK_STALE", nil, nil),
		mk("600004", "EXPIRE", nil, nil),
		mk("600005", "PROTECT", nil, nil),
		mk("600006", "PROTECT", nil, nil),
		mk("600007", "TELEPORT", nil, nil), // 非空但非法枚举
		mk("600008", "", nil, nil),         // 旧版 Python 载荷：无动作字段
		mk("600009", "TOUCH", nil, nil),    // Go 链路矛盾输入：TOUCH 缺锚点
	}}
	res, err := ApplyWatchlistPool(gdb, data, now, map[string]string{"SECTOR_LEADER": "FAILED"})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	check := func(code string, wantLS, wantExp time.Time, wantStatus, wantSrc, wantName model.WatchlistItem) {}
	_ = check
	get := func(code string) model.WatchlistItem { return mustFind(t, gdb, code) }

	// 1. TOUCH：使用状态机透传锚点（含时分秒截断口径与自然日 AddDate 差），不是 now/Add(7*24h) 重算值。
	it := get("600001")
	if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, smSeen) {
		t.Errorf("TOUCH last_seen_at=%v want state-machine %v (not recomputed)", it.LastSeenAt, smSeen)
	}
	if it.ExpireDate == nil || !sameTime(*it.ExpireDate, smExp) {
		t.Errorf("TOUCH expire_date=%v want state-machine %v", it.ExpireDate, smExp)
	}
	recomputed := now.Add(WatchlistExpireDuration)
	if it.ExpireDate != nil && sameTime(*it.ExpireDate, recomputed) {
		t.Errorf("TOUCH expire_date 被写入层重算为 now+7*24h，违反透传裁定")
	}
	if it.Status != model.WatchStatusActive || it.Name != "新名-600001" {
		t.Errorf("TOUCH status/name = %s/%s", it.Status, it.Name)
	}

	// 2. KEEP：出现于本轮池也不续期——原锚点逐字段保持；仅元数据可更新。
	it = get("600002")
	if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) {
		t.Errorf("KEEP last_seen_at 被刷新: %v want %v", it.LastSeenAt, base)
	}
	if it.ExpireDate == nil || !sameTime(*it.ExpireDate, oldExp) {
		t.Errorf("KEEP expire_date 被刷新: %v", it.ExpireDate)
	}

	// 3. MARK_STALE：保留 ACTIVE 与锚点，绝不写 STALE 枚举。
	it = get("600003")
	if it.Status != model.WatchStatusActive {
		t.Errorf("MARK_STALE 改写了 status=%s（禁止写入模型外枚举）", it.Status)
	}
	if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) || it.ExpireDate == nil || !sameTime(*it.ExpireDate, oldExp) {
		t.Errorf("MARK_STALE 动了时间锚点: %v/%v", it.LastSeenAt, it.ExpireDate)
	}

	// 4. EXPIRE：F-2 冻结——保留记录，不写 INACTIVE/EXPIRED。
	it = get("600004")
	if it.Status != model.WatchStatusActive {
		t.Errorf("EXPIRE 借修复恢复了失效: status=%s", it.Status)
	}
	if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) {
		t.Errorf("EXPIRE 动了时间锚点: %v", it.LastSeenAt)
	}

	// 5. PROTECT：MANUAL 与 BLACKLIST 不可被自动逻辑改写（名称也不动）。
	it = get("600005")
	if it.SourceType != model.WatchSourceManual || it.Name != "旧名" || it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) {
		t.Errorf("PROTECT 改写了 MANUAL 行: %+v", it)
	}
	it = get("600006")
	if it.Status != model.WatchStatusBlacklist {
		t.Errorf("PROTECT 改写了 BLACKLIST: status=%s", it.Status)
	}

	// 6. 未知动作：保守保留，不退化为 TOUCH；产生警告。
	it = get("600007")
	if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) || it.Status != model.WatchStatusActive {
		t.Errorf("未知动作退化为续期: %+v", it)
	}
	if !strings.Contains(strings.Join(res.Warnings, "|"), "unknown lifecycle_action") {
		t.Errorf("未知动作缺少警告: %v", res.Warnings)
	}

	//  7. 空动作旧载荷：独立兼容分支"收到即命中"，以注入 now 续期（既有 Python 契约），
	//     与 Go TOUCH 路径互不掩盖。
	it = get("600008")
	if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, now) {
		t.Errorf("旧载荷兼容分支未生效: last_seen_at=%v want now=%v", it.LastSeenAt, now)
	}
	if it.ExpireDate == nil || !sameTime(*it.ExpireDate, now.Add(WatchlistExpireDuration)) {
		t.Errorf("旧载荷兼容分支续期口径变化: %v", it.ExpireDate)
	}

	// 8. Go 链路 TOUCH 缺锚点：不得静默续期——保守保留 + 明确审计警告。
	it = get("600009")
	if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) {
		t.Errorf("TOUCH 缺锚点被静默续期: %v", it.LastSeenAt)
	}
	if it.ExpireDate == nil || !sameTime(*it.ExpireDate, oldExp) {
		t.Errorf("TOUCH 缺锚点 expire_date 被改动: %v", it.ExpireDate)
	}
	if !strings.Contains(strings.Join(res.Warnings, "|"), "without state-machine anchor") {
		t.Errorf("TOUCH 缺锚点缺少警告: %v", res.Warnings)
	}
	if res.Inactivated != 0 {
		t.Errorf("F-2 冻结被破坏: Inactivated=%d", res.Inactivated)
	}
	if res.Updated != 2 || res.Kept != 1 || res.AuditOnly != 4 || res.SkippedMan != 2 {
		t.Errorf("统计不符: updated=%d kept=%d audit=%d prot=%d", res.Updated, res.Kept, res.AuditOnly, res.SkippedMan)
	}
}

func TestApplyLifecycleContract_TouchIdempotentNoDrift(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 17, 15, 30, 45, 0, time.UTC)
	base := now.AddDate(0, 0, -3)
	smSeen := now.Truncate(time.Second)
	smExp := smSeen.AddDate(0, 0, 7)
	if err := gdb.Create(&model.WatchlistItem{Code: "600001", Name: "x", SourceType: model.WatchSourceSector,
		Status: model.WatchStatusActive, AddedDate: base, LastSeenAt: ptrTime(base), ExpireDate: ptrTime(base.AddDate(0, 0, 7))}).Error; err != nil {
		t.Fatal(err)
	}
	entry := WatchEntry{Symbol: "600001", SourceType: string(model.WatchSourceSector), LifecycleAction: "TOUCH",
		LastSeenAt: ptrTime(smSeen), ExpireDateP: ptrTime(smExp)}
	first, err := ApplyWatchlistPool(gdb, &WatchlistGenData{Pool: []WatchEntry{entry}}, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Updated != 1 {
		t.Fatalf("first apply updated=%d", first.Updated)
	}
	a := mustFind(t, gdb, "600001")
	// 第二次在更晚的运行时刻应用同一份载荷：锚点必须仍是状态机的值，零漂移。
	later := now.Add(45 * time.Minute)
	if _, err := ApplyWatchlistPool(gdb, &WatchlistGenData{Pool: []WatchEntry{entry}}, later, nil); err != nil {
		t.Fatal(err)
	}
	b := mustFind(t, gdb, "600001")
	if a.LastSeenAt == nil || b.LastSeenAt == nil || !a.LastSeenAt.Equal(*b.LastSeenAt) {
		t.Errorf("重复应用 last_seen_at 漂移: %v -> %v", a.LastSeenAt, b.LastSeenAt)
	}
	if a.ExpireDate == nil || b.ExpireDate == nil || !a.ExpireDate.Equal(*b.ExpireDate) {
		t.Errorf("重复应用 expire_date 漂移: %v -> %v", a.ExpireDate, b.ExpireDate)
	}
	if !sameTime(*b.LastSeenAt, smSeen) || !sameTime(*b.ExpireDate, smExp) {
		t.Errorf("锚点不再是状态机值: %v/%v", b.LastSeenAt, b.ExpireDate)
	}
}

func TestApplyLifecycleContract_NewCandidateInsertUnchanged(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)
	// Go 链路新候选：库里没有记录，Transition 初始化锚点由插入路径落库（既有人工/自动初始化语义不变）。
	res, err := ApplyWatchlistPool(gdb, &WatchlistGenData{Pool: []WatchEntry{
		{Symbol: "600519", Name: "贵州茅台", SourceType: "SECTOR_LEADER"},
	}}, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 1 {
		t.Fatalf("inserted=%d", res.Inserted)
	}
	it := mustFind(t, gdb, "600519")
	if it.Status != model.WatchStatusActive || it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, now) {
		t.Errorf("新候选初始化语义改变: %+v", it)
	}
	// 人工新候选仍按既有人工语义：不设过期。
	res2, err := ApplyWatchlistPool(gdb, &WatchlistGenData{Pool: []WatchEntry{
		{Symbol: "600520", Name: "人工票", SourceType: "MANUAL"},
	}}, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Inserted != 1 || mustFind(t, gdb, "600520").ExpireDate != nil {
		t.Errorf("人工新候选不应携带过期锚点")
	}
}

func TestApplyLifecycleContract_ExtraDecisionsAbsentCandidates(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)
	base := now.AddDate(0, 0, -30)
	oldExp := base.AddDate(0, 0, 7)
	for _, c := range []string{"600001", "600002"} {
		if err := gdb.Create(&model.WatchlistItem{Code: c, Name: "旧", SourceType: model.WatchSourceSector,
			Status: model.WatchStatusActive, AddedDate: base, LastSeenAt: ptrTime(base), ExpireDate: ptrTime(oldExp)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	raw := func(s string) json.RawMessage { return json.RawMessage(s) }
	data := &WatchlistGenData{
		Pool: []WatchEntry{{Symbol: "600003", Name: "新", SourceType: "SECTOR_LEADER"}},
		ExtraDecisions: map[string]json.RawMessage{
			"600001": raw(`{"lifecycle_action":"MARK_STALE","last_seen_at":"` + base.Format(time.RFC3339Nano) + `"}`),
			"600002": raw(`{"lifecycle_action":"EXPIRE"}`),
			"600099": raw(`{"lifecycle_action":"KEEP"}`),       // 库里不存在: 不新建
			"600003": raw(`{"lifecycle_action":"MARK_STALE"}`), // 主循环已处理: 旁路跳过
		},
	}
	res, err := ApplyWatchlistPool(gdb, data, now, map[string]string{"SECTOR_LEADER": "FAILED"})
	if err != nil {
		t.Fatal(err)
	}
	if res.AuditOnly != 2 {
		t.Errorf("旁路 audit-only 计数=%d want 2", res.AuditOnly)
	}
	var cnt int64
	gdb.Model(&model.WatchlistItem{}).Where("code = ?", "600099").Count(&cnt)
	if cnt != 0 {
		t.Errorf("旁路不得为缺席代码新建记录")
	}
	it := mustFind(t, gdb, "600001")
	if it.Status != model.WatchStatusActive || it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) {
		t.Errorf("旁路 MARK_STALE 改写了生命周期: %+v", it)
	}
	it = mustFind(t, gdb, "600002")
	if it.Status != model.WatchStatusActive || it.ExpireDate == nil || !sameTime(*it.ExpireDate, oldExp) {
		t.Errorf("旁路 EXPIRE 恢复失效或改动锚点: %+v", it)
	}
	if res.Inactivated != 0 {
		t.Errorf("Inactivated=%d, F-2 冻结被破坏", res.Inactivated)
	}
}
