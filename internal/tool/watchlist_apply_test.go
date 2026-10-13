// watchlist_apply_test.go 覆盖 Layer 0.5 动态池落库生命周期规则：
// 首次插入 / MANUAL 不覆盖 / BLACKLIST 终态 / 重命中续期 / 过期清理 / 非法代码丢弃。
package tool

import (
	"path/filepath"
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

	res, err := ApplyWatchlistPool(gdb, data, now)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if res.Inserted != 3 {
		t.Errorf("inserted=%d want 3", res.Inserted)
	}
	if res.SkippedMan != 2 { // 600519(MANUAL) + 000888(BLACKLIST)
		t.Errorf("skipped_manual=%d want 2", res.SkippedMan)
	}
	if res.Inactivated != 1 { // 仅 000777（000888 因 status<>ACTIVE 豁免）
		t.Errorf("inactivated=%d want 1", res.Inactivated)
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
	// 过期清理与黑名单豁免
	if got := mustFind(t, gdb, "000777"); got.Status != model.WatchStatusInactive {
		t.Errorf("expired row should be INACTIVE, got %s", got.Status)
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
	}, later)
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
	if _, err := ApplyWatchlistPool(nil, &WatchlistGenData{}, time.Now()); err == nil {
		t.Error("nil db should error")
	}
	if _, err := ApplyWatchlistPool(gdb, nil, time.Now()); err == nil {
		t.Error("nil data should error")
	}
}

func TestActiveWatchlistCodes_ExcludesExpiredAndInactive(t *testing.T) {
	gdb := newWatchTestDB(t)
	now := time.Now()
	rows := []model.WatchlistItem{
		{Code: "000001", SourceType: model.WatchSourceManual, Status: model.WatchStatusActive, AddedDate: now},
		{Code: "000002", SourceType: model.WatchSourceETF, Status: model.WatchStatusActive, AddedDate: now, ExpireDate: ptrTime(now.Add(-time.Minute))}, // 漏网过期 -> 双保险剔除
		{Code: "000003", SourceType: model.WatchSourceFund, Status: model.WatchStatusInactive, AddedDate: now},
		{Code: "000004", SourceType: model.WatchSourceSector, Status: model.WatchStatusActive, AddedDate: now, ExpireDate: ptrTime(now.Add(time.Hour))},
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
	if !set["000001"] || !set["000004"] || set["000002"] || set["000003"] || len(codes) != 2 {
		t.Errorf("codes=%v want {000001,000004}", codes)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
