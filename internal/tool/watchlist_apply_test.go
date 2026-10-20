// watchlist_apply_test.go 覆盖 Layer 0.5 动态池落库生命周期规则：
// 首次插入 / MANUAL 不覆盖 / BLACKLIST 终态 / 重命中续期 / 过期清理 / 非法代码丢弃。
package tool

import (
	"encoding/json"
	"fmt"
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

	// 测试结束时关闭底层连接，避免 Windows 无法删除临时 SQLite 文件。
	sqlDB, err := gdb.DB()
	if err != nil {
		t.Fatalf("get underlying db: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close test db: %v", err)
		}
	})

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
	seed("600005", model.WatchSourceManual, model.WatchStatusActive, base, oldExp) // PROTECT(人工)
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

// ---------------------------------------------------------------------------
// 补丁 A P5（专项测试增补）：
//   A. MANUAL / BLACKLIST 受保护记录的动作矩阵 —— 任何生命周期动作（含空动作旧
//      载荷与未知动作）都不得改写受保护行，读回断言六个字段原值不变；
//   B. TOUCH 双锚点校验 —— 缺 LastSeenAt、缺 ExpireDateP、任一零时间三种形态
//      都必须保守保留 + 告警 + AuditOnly 计数，禁止半更新；
//   C. ExtraDecisions 旁路 EXPIRE 落库 —— F-2 冻结的直接数据库证据：审计动作
//      不得把 ACTIVE 改成 INACTIVE，也不得改动锚点。
// ---------------------------------------------------------------------------

// TestApplyLifecycleContract_ProtectedRowActionMatrix —— P1 护栏前置的数据库级
// 验证：TOUCH(带合法双锚点)/KEEP/EXPIRE/MARK_STALE/PROTECT/未知动作/空动作(旧载荷)
// 七种输入分别作用于 MANUAL 行与 BLACKLIST 行，六字段必须逐一原值保持。
func TestApplyLifecycleContract_ProtectedRowActionMatrix(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 17, 15, 30, 45, 123456789, time.UTC)
	base := now.AddDate(0, 0, -30)
	oldExp := base.AddDate(0, 0, 7)
	// 状态机口径的"合法" TOUCH 双锚点——即便载荷完全合规，受保护行也不得被写。
	smSeen := now.Truncate(time.Second)
	smExp := smSeen.AddDate(0, 0, 7)

	actions := []string{"TOUCH", "KEEP", "EXPIRE", "MARK_STALE", "PROTECT", "TELEPORT", ""}
	var pool []WatchEntry
	seeded := map[string][2]string{} // code -> {protectedKind, action}
	mkSeed := func(idx int, kind, status string) {
		// MANUAL 组占 6001xx，BLACKLIST 组占 6002xx（idx 为 actions 下标 0..6）。
		num := 100 + idx
		if kind == "BLACKLIST" {
			num = 200 + idx
		}
		code := fmt.Sprintf("60%04d", num)
		it := model.WatchlistItem{Code: code, Name: "受保护旧名", SourceType: model.WatchSourceSector,
			Status: status, SectorName: "旧板块", AddedDate: base,
			LastSeenAt: ptrTime(base), ExpireDate: ptrTime(oldExp)}
		if kind == "MANUAL" {
			it.SourceType = model.WatchSourceManual
		}
		if err := gdb.Create(&it).Error; err != nil {
			t.Fatalf("seed %s: %v", code, err)
		}
		seeded[code] = [2]string{kind, ""}
		pool = append(pool, mkProtectedEntry(code, actions[idx], smSeen, smExp))
		seeded[code] = [2]string{kind, actions[idx]}
	}
	for i, a := range actions {
		mkSeed(i, "MANUAL", model.WatchStatusActive) // 600100..600106: MANUAL 行
		mkSeed(i, "BLACKLIST", model.WatchStatusBlacklist)
		_ = a // BLACKLIST 组由 mkSeed 内部映射到 600200..600206
	}

	res, err := ApplyWatchlistPool(gdb, &WatchlistGenData{Pool: pool}, now, map[string]string{"SECTOR_LEADER": "OK"})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	for code, meta := range seeded {
		kind, action := meta[0], meta[1]
		it := mustFind(t, gdb, code)
		wantSrc := model.WatchSourceSector
		if kind == "MANUAL" {
			wantSrc = model.WatchSourceManual
		}
		wantStatus := model.WatchStatusActive
		if kind == "BLACKLIST" {
			wantStatus = model.WatchStatusBlacklist
		}
		// 六字段逐一原值断言：name / sector_name / last_seen_at / expire_date / status / source_type
		if it.Name != "受保护旧名" {
			t.Errorf("%s(%s, action=%q): name 被改写为 %q", code, kind, action, it.Name)
		}
		if it.SectorName != "旧板块" {
			t.Errorf("%s(%s, action=%q): sector_name 被改写为 %q", code, kind, action, it.SectorName)
		}
		if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) {
			t.Errorf("%s(%s, action=%q): last_seen_at 被改动: %v want %v", code, kind, action, it.LastSeenAt, base)
		}
		if it.ExpireDate == nil || !sameTime(*it.ExpireDate, oldExp) {
			t.Errorf("%s(%s, action=%q): expire_date 被改动: %v want %v", code, kind, action, it.ExpireDate, oldExp)
		}
		if it.Status != wantStatus {
			t.Errorf("%s(%s, action=%q): status 被改写: %s want %s", code, kind, action, it.Status, wantStatus)
		}
		if it.SourceType != wantSrc {
			t.Errorf("%s(%s, action=%q): source_type 被改写: %s want %s", code, kind, action, it.SourceType, wantSrc)
		}
	}

	// 计数断言：14 个受保护条目全部计入 SkippedMan（护栏先于一切动作分支），
	// 且没有任何一条走成写入路径。
	if res.SkippedMan != len(actions)*2 {
		t.Errorf("SkippedMan=%d want %d（每个动作×每类保护对象都须计入）", res.SkippedMan, len(actions)*2)
	}
	if res.Updated != 0 || res.Kept != 0 || res.Inserted != 0 {
		t.Errorf("受保护行不得产生任何写入计数: updated=%d kept=%d inserted=%d", res.Updated, res.Kept, res.Inserted)
	}
	// AuditOnly 只允许来自"非保护行的保守分支"——本测试全部条目受保护，应为 0。
	if res.AuditOnly != 0 {
		t.Errorf("AuditOnly=%d want 0（受保护行统一计 SkippedMan，不重复计入审计）", res.AuditOnly)
	}
	// 未知动作仍要留下警告（护栏跳过不吞掉契约违规信号）。
	if !strings.Contains(strings.Join(res.Warnings, "|"), "unknown lifecycle_action") {
		t.Errorf("未知动作缺少警告（护栏前置不得吞掉审计信号）: %v", res.Warnings)
	}
	if res.Inactivated != 0 {
		t.Errorf("Inactivated=%d, F-2 冻结被破坏", res.Inactivated)
	}
}

func mkProtectedEntry(sym, action string, smSeen, smExp time.Time) WatchEntry {
	e := WatchEntry{Symbol: sym, Name: "自动新名", SourceType: string(model.WatchSourceETF),
		SectorName: "自动新板块", LifecycleAction: action}
	if action == "TOUCH" { // 只有 TOUCH 才携带合法双锚点（其余动作给空锚点也进不了写入）
		e.LastSeenAt, e.ExpireDateP = ptrTime(smSeen), ptrTime(smExp)
	}
	return e
}

// TestApplyLifecycleContract_TouchDualAnchorMissing —— P2 双锚点校验的三种缺失
// 形态：缺 LastSeenAt / 缺 ExpireDateP / 任一为零时间。每种都必须：整条保守保留
// （两个时间字段都不动、status/name/sector/source_type 不写）、产生
// "without state-machine anchor" 警告、计入 AuditOnly——绝不允许半更新。
func TestApplyLifecycleContract_TouchDualAnchorMissing(t *testing.T) {
	cases := []struct {
		name string
		ls   func() *time.Time
		exp  func() *time.Time
	}{
		{"missing LastSeenAt", func() *time.Time { return nil }, func() *time.Time { x := time.Now().UTC(); return &x }},
		{"missing ExpireDateP", func() *time.Time { x := time.Now().UTC(); return &x }, func() *time.Time { return nil }},
		{"zero LastSeenAt", func() *time.Time { z := time.Time{}; return &z }, func() *time.Time { x := time.Now().UTC(); return &x }},
		{"zero ExpireDateP", func() *time.Time { x := time.Now().UTC(); return &x }, func() *time.Time { z := time.Time{}; return &z }},
		{"both zero", func() *time.Time { z := time.Time{}; return &z }, func() *time.Time { z := time.Time{}; return &z }},
	}
	for i, tc := range cases {
		gdb := newWatchTestDB(t)
		now := time.Date(2026, 10, 17, 15, 30, 45, 0, time.UTC).Add(time.Duration(i) * time.Hour)
		base := now.AddDate(0, 0, -3)
		oldExp := base.AddDate(0, 0, 7)
		code := "600001"
		if err := gdb.Create(&model.WatchlistItem{Code: code, Name: "旧名", SourceType: model.WatchSourceSector,
			Status: model.WatchStatusActive, SectorName: "旧板块", AddedDate: base,
			LastSeenAt: ptrTime(base), ExpireDate: ptrTime(oldExp)}).Error; err != nil {
			t.Fatal(err)
		}
		e := WatchEntry{Symbol: code, Name: "新名", SourceType: string(model.WatchSourceETF),
			SectorName: "新板块", LifecycleAction: "TOUCH", LastSeenAt: tc.ls(), ExpireDateP: tc.exp()}
		res, err := ApplyWatchlistPool(gdb, &WatchlistGenData{Pool: []WatchEntry{e}}, now, nil)
		if err != nil {
			t.Fatalf("%s: apply: %v", tc.name, err)
		}
		it := mustFind(t, gdb, code)
		if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) {
			t.Errorf("%s: last_seen_at 被半更新: %v want %v", tc.name, it.LastSeenAt, base)
		}
		if it.ExpireDate == nil || !sameTime(*it.ExpireDate, oldExp) {
			t.Errorf("%s: expire_date 被半更新: %v want %v", tc.name, it.ExpireDate, oldExp)
		}
		if it.Status != model.WatchStatusActive {
			t.Errorf("%s: status 被改写: %s", tc.name, it.Status)
		}
		if it.Name != "旧名" || it.SectorName != "旧板块" || it.SourceType != model.WatchSourceSector {
			t.Errorf("%s: 元数据/source_type 被改写: %+v", tc.name, it)
		}
		if !strings.Contains(strings.Join(res.Warnings, "|"), "without state-machine anchor") {
			t.Errorf("%s: 缺少保守保留警告: %v", tc.name, res.Warnings)
		}
		if res.AuditOnly != 1 {
			t.Errorf("%s: AuditOnly=%d want 1", tc.name, res.AuditOnly)
		}
		if res.Updated != 0 {
			t.Errorf("%s: 缺锚点 TOUCH 不应计入 Updated: %d", tc.name, res.Updated)
		}
	}
}

// TestApplyLifecycleContract_ExtraExpireStaysActiveF2Frozen —— F-2 冻结的**落库**
// 直接证据：ExtraDecisions 旁路的 EXPIRE 到达一个已超期的 ACTIVE 记录时，读回
// DB 必须仍是 ACTIVE + 原锚点 + 原元数据（审计动作不得恢复自动失效），同时
// Inactivated 恒为 0、AuditOnly 计数正确。此前该结论仅由代码注释推断，本用例
// 用真实数据库行证伪/证实。
func TestApplyLifecycleContract_ExtraExpireStaysActiveF2Frozen(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)
	base := now.AddDate(0, 0, -30)
	oldExp := base.AddDate(0, 0, 7) // 已过期：expire_date < now
	if err := gdb.Create(&model.WatchlistItem{Code: "600002", Name: "旧名", SourceType: model.WatchSourceSector,
		Status: model.WatchStatusActive, SectorName: "旧板块", AddedDate: base,
		LastSeenAt: ptrTime(base), ExpireDate: ptrTime(oldExp)}).Error; err != nil {
		t.Fatal(err)
	}
	data := &WatchlistGenData{
		Pool: []WatchEntry{{Symbol: "600003", Name: "本轮新候选", SourceType: "SECTOR_LEADER"}},
		ExtraDecisions: map[string]json.RawMessage{
			"600002": json.RawMessage(`{"lifecycle_action":"EXPIRE","last_seen_at":"` + base.Format(time.RFC3339Nano) + `","expire_date":"` + oldExp.Format(time.RFC3339Nano) + `"}`),
		},
	}
	res, err := ApplyWatchlistPool(gdb, data, now, map[string]string{"SECTOR_LEADER": "OK", "ETF_MOMENTUM": "SUCCESS_EMPTY", "FUND_FLOW": "OK"})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	it := mustFind(t, gdb, "600002")
	if it.Status != model.WatchStatusActive {
		t.Errorf("EXPIRE 审计动作恢复了自动失效: status=%s（F-2 冻结要求保持 ACTIVE）", it.Status)
	}
	if it.Status == model.WatchStatusInactive {
		t.Errorf("EXPIRE 旁路把 ACTIVE 改成了 INACTIVE，直接违反 F-2 冻结")
	}
	if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, base) || it.ExpireDate == nil || !sameTime(*it.ExpireDate, oldExp) {
		t.Errorf("EXPIRE 旁路改动了时间锚点: %v/%v", it.LastSeenAt, it.ExpireDate)
	}
	if it.Name != "旧名" || it.SectorName != "旧板块" {
		t.Errorf("EXPIRE 旁路改动了元数据: %+v", it)
	}
	if res.Inactivated != 0 {
		t.Errorf("Inactivated=%d want 0（F-2 冻结）", res.Inactivated)
	}
	if res.AuditOnly != 1 {
		t.Errorf("AuditOnly=%d want 1（EXPIRE 旁路应计入保守审计）", res.AuditOnly)
	}
}

// ---------------------------------------------------------------------------
// 补丁 A P1 收尾（ExtraDecisions 旁路护栏）：受保护行在旁路决策路径上同样
// 不得产生错误分类计数 —— MANUAL 收到 KEEP、BLACKLIST 收到 MARK_STALE/EXPIRE
// 都必须统一计入 SkippedMan；未知动作的审计警告在护栏之前解析阶段仍须保留。
// ---------------------------------------------------------------------------

func TestApplyLifecycleContract_ExtraDecisionsProtectedRowsAreSkipped(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Date(2026, 10, 17, 16, 0, 0, 0, time.UTC)
	base := now.AddDate(0, 0, -30)
	oldSeen := base.AddDate(0, 0, 2)
	oldExp := base.AddDate(0, 0, 7) // 未过期，避免混入 F-2 审计噪声

	seed := func(code, name, sector, sourceType, status string) {
		it := model.WatchlistItem{
			Code: code, Name: name, SectorName: sector, SourceType: sourceType,
			Status: status, AddedDate: base, LastSeenAt: ptrTime(oldSeen), ExpireDate: ptrTime(oldExp),
		}
		if err := gdb.Create(&it).Error; err != nil {
			t.Fatalf("seed %s: %v", code, err)
		}
	}
	seed("600301", "人工票", "人工板块", model.WatchSourceManual, model.WatchStatusActive)
	seed("600302", "屏蔽票", "屏蔽板块", model.WatchSourceSector, model.WatchStatusBlacklist)
	seed("600303", "屏蔽手工", "混合板块", model.WatchSourceManual, model.WatchStatusBlacklist)
	// 未知动作行同样使用受保护行（MANUAL）：验证警告先于护栏产生、护栏统一计入 SkippedMan。
	seed("600304", "未知动作票", "人工备用", model.WatchSourceManual, model.WatchStatusActive)

	raw := func(s string) json.RawMessage { return json.RawMessage(s) }
	data := &WatchlistGenData{
		Pool: []WatchEntry{{Symbol: "600305", Name: "本轮新候选", SourceType: "SECTOR_LEADER"}},
		ExtraDecisions: map[string]json.RawMessage{
			// MANUAL 行收到 KEEP：修复前会计入 Kept，现必须计入 SkippedMan。
			"600301": raw(`{"lifecycle_action":"KEEP","last_seen_at":"` + oldSeen.Format(time.RFC3339Nano) + `","expire_date":"` + oldExp.Format(time.RFC3339Nano) + `"}`),
			// BLACKLIST 行收到 MARK_STALE / EXPIRE：修复前计入 AuditOnly，现必须计入 SkippedMan。
			"600302": raw(`{"lifecycle_action":"MARK_STALE"}`),
			"600303": raw(`{"lifecycle_action":"EXPIRE"}`),
			// 非法动作落在受保护行：警告必须先于护栏产生，且仍零写入、计入 SkippedMan。
			"600304": raw(`{"lifecycle_action":"TELEPORT"}`),
		},
	}
	res, err := ApplyWatchlistPool(gdb, data, now, map[string]string{
		"SECTOR_LEADER": "OK", "ETF_MOMENTUM": "SUCCESS_EMPTY", "FUND_FLOW": "OK",
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	// 计数：4 个受保护旁路记录全部只贡献 SkippedMan；
	// 非保护的新候选 600305 走插入路径（Inserted=1），不进入 Kept/AuditOnly。
	if res.SkippedMan != 4 {
		t.Errorf("SkippedMan=%d want 4（旁路受保护记录必须统一计入）", res.SkippedMan)
	}
	if res.Kept != 0 {
		t.Errorf("Kept=%d want 0（MANUAL+KEEP 不得计为 Kept）", res.Kept)
	}
	if res.AuditOnly != 0 {
		t.Errorf("AuditOnly=%d want 0（BLACKLIST+MARK_STALE/EXPIRE 不得计为 AuditOnly）", res.AuditOnly)
	}
	if res.Inserted != 1 || res.Updated != 0 || res.Inactivated != 0 {
		t.Errorf("Inserted=%d Updated=%d Inactivated=%d want 1/0/0", res.Inserted, res.Updated, res.Inactivated)
	}

	// 未知动作警告不因护栏提前 continue 而丢失。
	foundWarn := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "unknown lifecycle_action") && strings.Contains(w, "600304") {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Errorf("未知动作 TELEPORT 落在受保护行时警告丢失, warnings=%v", res.Warnings)
	}

	// 数据库读回：六个字段原值保持不变。
	checkUnchanged := func(code, name, sector string, sourceType, status string) {
		it := mustFind(t, gdb, code)
		if it.Name != name || it.SectorName != sector {
			t.Errorf("%s 元数据被改写: name=%q sector=%q", code, it.Name, it.SectorName)
		}
		if it.SourceType != sourceType || it.Status != status {
			t.Errorf("%s 保护属性被改写: source_type=%q status=%q", code, it.SourceType, it.Status)
		}
		if it.LastSeenAt == nil || !sameTime(*it.LastSeenAt, oldSeen) {
			t.Errorf("%s last_seen_at 被改写: %v", code, it.LastSeenAt)
		}
		if it.ExpireDate == nil || !sameTime(*it.ExpireDate, oldExp) {
			t.Errorf("%s expire_date 被改写: %v", code, it.ExpireDate)
		}
	}
	checkUnchanged("600301", "人工票", "人工板块", model.WatchSourceManual, model.WatchStatusActive)
	checkUnchanged("600302", "屏蔽票", "屏蔽板块", model.WatchSourceSector, model.WatchStatusBlacklist)
	checkUnchanged("600303", "屏蔽手工", "混合板块", model.WatchSourceManual, model.WatchStatusBlacklist)
	checkUnchanged("600304", "未知动作票", "人工备用", model.WatchSourceManual, model.WatchStatusActive)
}
