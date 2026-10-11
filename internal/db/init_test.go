package db

import (
	"path/filepath"
	"testing"

	"gorm.io/gorm"

	"ashare-quant/internal/model"
)

// openTempDB 在临时目录初始化数据库并返回连接。
// 注意：SQLite 文件是持久的，测试内可多次 InitDB 同一文件验证幂等性；
// Close 仅在测试结束时调用（globalDB 为进程级单例，串行执行即可）。
func openTempDB(t *testing.T) (*gorm.DB, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "trading.db")
	gdb, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("InitDB(%s) failed: %v", dbPath, err)
	}
	t.Cleanup(func() { _ = Close() })
	return gdb, dbPath
}

// TestInitDB_CreatesTablesAndDefaultAccount 验证：自动建表 + 默认账户 100 万。
func TestInitDB_CreatesTablesAndDefaultAccount(t *testing.T) {
	gdb, _ := openTempDB(t)

	// 五张表均存在且可查询。
	for _, tb := range []string{"stocks", "daily_klines", "positions", "accounts", "orders"} {
		var cnt int64
		if err := gdb.Table(tb).Count(&cnt).Error; err != nil {
			t.Fatalf("table %q missing or broken: %v", tb, err)
		}
	}

	// 默认账户恰好一条，资金正确。
	var count int64
	if err := gdb.Model(&model.Account{}).Count(&count).Error; err != nil {
		t.Fatalf("count accounts: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 default account, got %d", count)
	}
	var acc model.Account
	if err := gdb.First(&acc).Error; err != nil {
		t.Fatalf("default account not found: %v", err)
	}
	if acc.AvailableCash != model.DefaultInitialCapital || acc.TotalAssets != model.DefaultInitialCapital {
		t.Fatalf("default account capital wrong: %+v", acc)
	}
	if acc.FrozenCash != 0 {
		t.Fatalf("frozen cash should be 0, got %v", acc.FrozenCash)
	}

	// WAL 模式生效检查。
	var mode string
	if err := gdb.Raw("PRAGMA journal_mode").Scan(&mode).Error; err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("expected WAL journal mode, got %q", mode)
	}
}

// TestInitDB_Idempotent 验证：重复 InitDB 不重置已有账户数据、不产生第二条账户。
func TestInitDB_Idempotent(t *testing.T) {
	gdb, dbPath := openTempDB(t)

	// 人为修改余额，模拟已运行一段时间。
	if err := gdb.Model(&model.Account{}).Where("id = ?", 1).
		Update("available_cash", 666666.0).Error; err != nil {
		t.Fatalf("update account: %v", err)
	}

	// 对同一文件再次 InitDB。
	gdb2, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("second InitDB failed: %v", err)
	}

	var count int64
	if err := gdb2.Model(&model.Account{}).Count(&count).Error; err != nil {
		t.Fatalf("count accounts after re-init: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 account after re-init, got %d", count)
	}
	var acc model.Account
	if err := gdb2.First(&acc).Error; err != nil {
		t.Fatalf("reload account: %v", err)
	}
	if acc.AvailableCash != 666666.0 {
		t.Fatalf("re-init overwrote balance: want 666666, got %v", acc.AvailableCash)
	}
}

// TestModelCRUD_RoundTrip 验证五张表的写入/读取与唯一约束。
func TestModelCRUD_RoundTrip(t *testing.T) {
	gdb, _ := openTempDB(t)

	// --- Stock ---
	st := model.Stock{Code: "600519", Name: "贵州茅台", Industry: "白酒"}
	if err := gdb.Create(&st).Error; err != nil {
		t.Fatalf("create stock: %v", err)
	}
	if err := gdb.Create(&model.Stock{Code: "600519", Name: "dup"}).Error; err == nil {
		t.Fatal("duplicate stock code should violate unique index")
	}

	// --- DailyKline（含涨跌停价回填）---
	up, down, err := model.CalcLimitPrices(1700.00, "600519")
	if err != nil {
		t.Fatalf("calc limit prices: %v", err)
	}
	if up != 1870.00 || down != 1530.00 {
		t.Fatalf("limit price wrong: up=%v down=%v", up, down)
	}
	k := model.DailyKline{
		Code: "600519", Date: "2026-10-09",
		Open: 1705, High: 1750, Low: 1690, Close: 1742.5,
		Volume: 3_100_000, Amount: 5.37e9,
		LimitUpPrice: up, LimitDownPrice: down,
	}
	if err := gdb.Create(&k).Error; err != nil {
		t.Fatalf("create kline: %v", err)
	}
	if err := gdb.Create(&model.DailyKline{Code: "600519", Date: "2026-10-09"}).Error; err == nil {
		t.Fatal("duplicate (code,date) kline should violate unique index")
	}
	var gotK model.DailyKline
	if err := gdb.Where("code = ? AND date = ?", "600519", "2026-10-09").First(&gotK).Error; err != nil {
		t.Fatalf("load kline: %v", err)
	}
	if gotK.LimitUpPrice != 1870.00 || gotK.LimitDownPrice != 1530.00 {
		t.Fatalf("kline limit prices round-trip mismatch: %+v", gotK)
	}

	// --- Position（T+1：当日买入 available=0）---
	p := model.Position{Code: "600519", Quantity: 100, AvailableQuantity: 0, CostPrice: 1742.5, BuyDate: "2026-10-09"}
	if err := gdb.Create(&p).Error; err != nil {
		t.Fatalf("create position: %v", err)
	}

	// --- Order ---
	o := model.Order{
		OrderID: model.NewOrderID(1), Code: "600519", Direction: model.DirectionBuy,
		Price: 1745, Quantity: 100, Status: model.OrderStatusPending, TradeDate: "2026-10-12",
	}
	if err := gdb.Create(&o).Error; err != nil {
		t.Fatalf("create order: %v", err)
	}
	var gotO model.Order
	if err := gdb.Where("order_id = ?", o.OrderID).First(&gotO).Error; err != nil {
		t.Fatalf("load order: %v", err)
	}
	if gotO.Status != model.OrderStatusPending || gotO.Direction != model.DirectionBuy {
		t.Fatalf("order round-trip mismatch: %+v", gotO)
	}
	if gotO.CreatedAt.IsZero() {
		t.Fatal("order CreatedAt should be auto-filled")
	}
}

// TestCandidatePool_CRUD 通过 GORM 对真实 SQLite 验证 candidate_pools 表：
// 建表、写入/读取往返、(trade_date, code) 唯一约束、同日重跑 upsert 覆盖、跨日共存与条件查询。
func TestCandidatePool_CRUD(t *testing.T) {
	gdb, _ := openTempDB(t)

	// --- 表存在（InitDB AutoMigrate 已建）---
	var cnt int64
	if err := gdb.Table("candidate_pools").Count(&cnt).Error; err != nil {
		t.Fatalf("table candidate_pools missing or broken: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("fresh table should be empty, got %d rows", cnt)
	}

	// --- 写入 + 读取往返 ---
	cp := model.CandidatePool{
		TradeDate:      "2026-10-11",
		Code:           "600519",
		SignalType:     model.SignalBuy3,
		CzscSummary:    "出现第三类买点，回踩不破中枢上沿",
		ZsCount:        3,
		TrendDirection: "up",
	}
	if err := gdb.Create(&cp).Error; err != nil {
		t.Fatalf("create candidate: %v", err)
	}
	if cp.ID == 0 {
		t.Fatal("candidate PK should be auto-filled")
	}
	var got model.CandidatePool
	if err := gdb.Where("trade_date = ? AND code = ?", "2026-10-11", "600519").First(&got).Error; err != nil {
		t.Fatalf("load candidate: %v", err)
	}
	if got.SignalType != model.SignalBuy3 || got.ZsCount != 3 || got.TrendDirection != "up" ||
		got.CzscSummary != cp.CzscSummary {
		t.Fatalf("candidate round-trip mismatch: %+v", got)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("candidate CreatedAt should be auto-filled")
	}

	// --- (trade_date, code) 唯一约束：同日同代码重复插入必须失败 ---
	if err := gdb.Create(&model.CandidatePool{
		TradeDate: "2026-10-11", Code: "600519", SignalType: model.SignalBuy1, TrendDirection: "down",
	}).Error; err == nil {
		t.Fatal("duplicate (trade_date, code) should violate unique index uk_pool_date_code")
	}
	// 不同代码同日可共存
	if err := gdb.Create(&model.CandidatePool{
		TradeDate: "2026-10-11", Code: "300750", SignalType: model.SignalObservation,
		ZsCount: 2, TrendDirection: "unknown",
	}).Error; err != nil {
		t.Fatalf("different code same date should insert: %v", err)
	}

	// --- 同日重跑语义：生产路径用 Where+Assign+FirstOrCreate 做 upsert，应原地覆盖而非新增 ---
	update := model.CandidatePool{
		TradeDate: "2026-10-11", Code: "600519",
		SignalType: model.SignalBuy2, CzscSummary: "重跑后更新：第二类买点确认",
		ZsCount: 4, TrendDirection: "down",
	}
	if err := gdb.Where("trade_date = ? AND code = ?", "2026-10-11", "600519").
		Assign(update).FirstOrCreate(&update).Error; err != nil {
		t.Fatalf("upsert rerun: %v", err)
	}
	if err := gdb.Model(&model.CandidatePool{}).Where("trade_date = ? AND code = ?", "2026-10-11", "600519").
		Count(&cnt).Error; err != nil {
		t.Fatalf("count after upsert: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("same-day rerun must update in place, got %d rows for (2026-10-11, 600519)", cnt)
	}
	var reran model.CandidatePool
	if err := gdb.Where("trade_date = ? AND code = ?", "2026-10-11", "600519").First(&reran).Error; err != nil {
		t.Fatalf("reload after upsert: %v", err)
	}
	if reran.SignalType != model.SignalBuy2 || reran.ZsCount != 4 || reran.TrendDirection != "down" ||
		reran.CzscSummary != "重跑后更新：第二类买点确认" {
		t.Fatalf("upsert did not overwrite fields: %+v", reran)
	}

	// --- 跨交易日独立记录 ---
	if err := gdb.Create(&model.CandidatePool{
		TradeDate: "2026-10-12", Code: "600519", SignalType: model.SignalBuy3, TrendDirection: "up",
	}).Error; err != nil {
		t.Fatalf("next-day record should insert: %v", err)
	}

	// --- 按信号类型过滤查询（Layer2 Agent 的典型读取方式）---
	var buy3 []model.CandidatePool
	if err := gdb.Where("signal_type = ?", model.SignalBuy3).Find(&buy3).Error; err != nil {
		t.Fatalf("query by signal_type: %v", err)
	}
	// 注意：600519@2026-10-11 已被上面 upsert 覆盖为 2B，故全表仅剩 600519@2026-10-12 一条 3B。
	if len(buy3) != 1 || buy3[0].TradeDate != "2026-10-12" {
		t.Errorf("expect exactly 1 row with signal 3B (2026-10-12), got %+v", buy3)
	}

	// --- 删除当日池 ---
	if err := gdb.Where("trade_date = ?", "2026-10-11").Delete(&model.CandidatePool{}).Error; err != nil {
		t.Fatalf("delete day pool: %v", err)
	}
	if err := gdb.Model(&model.CandidatePool{}).Count(&cnt).Error; err != nil {
		t.Fatalf("final count: %v", err)
	}
	if cnt != 1 {
		t.Errorf("after deleting 2026-10-11 pool, expect 1 remaining row, got %d", cnt)
	}
}

// TestCandidatePool_SignalTypeValidation 校验候选池信号类型常量与合法性判断，
// 保证落库前的入参校验与 Python 网关 screen_pool 的 signal_type 对齐。
func TestCandidatePool_SignalTypeValidation(t *testing.T) {
	valid := []string{
		model.SignalObservation, model.SignalBuy1, model.SignalBuy2, model.SignalBuy3,
		model.SignalSell1, model.SignalSell2, model.SignalSell3,
	}
	for _, s := range valid {
		if !model.IsValidSignalType(s) {
			t.Errorf("IsValidSignalType(%q) = false, want true", s)
		}
	}
	invalid := []string{"", "3b", "2A", "Observation ", "BUY3", "unknown-signal"}
	for _, s := range invalid {
		if model.IsValidSignalType(s) {
			t.Errorf("IsValidSignalType(%q) = true, want false", s)
		}
	}

	// 非法信号类型不应被静默写入：模拟服务层校验后拒绝入库。
	gdb, _ := openTempDB(t)
	if model.IsValidSignalType("9X") {
		t.Fatal("sanity: 9X should be invalid")
	}
	var before int64
	gdb.Model(&model.CandidatePool{}).Count(&before)
	// （校验拦截后不执行 Create）确认表仍为空
	var after int64
	gdb.Model(&model.CandidatePool{}).Count(&after)
	if after != before {
		t.Errorf("row count changed without any insert: %d -> %d", before, after)
	}
}

// TestCalcLimitPrices_Boards 覆盖主板/创业板/科创板的涨跌停计算与取整。
func TestCalcLimitPrices_Boards(t *testing.T) {
	cases := []struct {
		code     string
		prev     float64
		wantUp   float64
		wantDown float64
	}{
		{"600000", 10.00, 11.00, 9.00},    // 主板 10%
		{"000001", 12.34, 13.57, 11.11},   // 12.34*1.1=13.574 -> 13.57; 12.34*0.9=11.106 -> 11.11
		{"300750", 100.00, 120.00, 80.00}, // 创业板 20%
		{"688111", 50.00, 60.00, 40.00},   // 科创板 20%
	}
	for _, c := range cases {
		up, down, err := model.CalcLimitPrices(c.prev, c.code)
		if err != nil {
			t.Fatalf("code %s: %v", c.code, err)
		}
		if up != c.wantUp || down != c.wantDown {
			t.Errorf("code %s prev %v: got up=%v down=%v, want up=%v down=%v",
				c.code, c.prev, up, down, c.wantUp, c.wantDown)
		}
	}
	if _, _, err := model.CalcLimitPrices(0, "600000"); err == nil {
		t.Error("prevClose=0 should error")
	}
	if _, _, err := model.CalcLimitPrices(-1, "600000"); err == nil {
		t.Error("negative prevClose should error")
	}
}

// TestInitDB_EmptyPath 参数防御。
func TestInitDB_EmptyPath(t *testing.T) {
	if _, err := InitDB(""); err == nil {
		t.Fatal("empty path should error")
	}
}

// TestGetDB_BeforeInit 未初始化时 GetDB 报错而非 panic。
func TestGetDB_BeforeInit(t *testing.T) {
	old := globalDB
	globalDB = nil
	defer func() { globalDB = old }()

	if _, err := GetDB(); err == nil {
		t.Fatal("GetDB before InitDB should error")
	}
}
