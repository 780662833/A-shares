// watchlist_apply_test.go 覆盖 Layer 0.5 动态池落库生命周期规则：
// 首次插入 / MANUAL 不覆盖 / BLACKLIST 终态 / 重命中续期 / 过期清理 / 非法代码丢弃。
package tool

import (
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
