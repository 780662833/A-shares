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
