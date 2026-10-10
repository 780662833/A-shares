// Package db 负责 SQLite 数据库的初始化、连接与 AutoMigrate。
package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/glebarez/sqlite" // 纯 Go SQLite 驱动（modernc.org/sqlite，无 CGO 依赖）
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"ashare-quant/internal/model"
)

// globalDB 进程级单例。InitDB 成功后可用 GetDB() 获取。
var globalDB *gorm.DB

// InitDB 打开（或创建）SQLite 数据库，执行建表迁移，并保证存在一个
// 初始资金 1,000,000 元的默认账户。返回配置好 WAL/busy_timeout 的 *gorm.DB。
//
// 幂等性：可安全重复调用；已存在的默认账户不会被重置。
func InitDB(dbPath string) (*gorm.DB, error) {
	if dbPath == "" {
		return nil, errors.New("db: dbPath is empty")
	}

	// 1. 确保数据库文件的父目录存在（如 ./data/trading.db -> ./data）。
	if dir := filepath.Dir(dbPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("db: create directory %q: %w", dir, err)
		}
	}

	// 2. 打开连接。DSN 参数说明（glebarez/sqlite 支持 _pragma 语法）：
	//   - _pragma=journal_mode(WAL)  : 读写并发友好，配合连接池更安全
	//   - _pragma=busy_timeout(5000) : 写锁冲突最多等待 5s，避免 SQLITE_BUSY 直接报错
	//   - _pragma=synchronous(NORMAL): WAL 下兼顾性能与持久性
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", dbPath)

	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Warn), // 生产可调 Silent，仅报慢查询/错误
	})
	if err != nil {
		return nil, fmt.Errorf("db: open sqlite %q: %w", dbPath, err)
	}

	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, fmt.Errorf("db: get underlying sql.DB: %w", err)
	}
	// SQLite 单文件库：写者唯一，限制连接数防止锁竞争；WAL 下读可适度放宽。
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("db: ping sqlite: %w", err)
	}

	// 3. 自动建表 / 增量列迁移。
	if err := autoMigrate(gdb); err != nil {
		return nil, err
	}

	// 4. 初始化默认账户（幂等）。
	if err := ensureDefaultAccount(gdb); err != nil {
		return nil, err
	}

	globalDB = gdb
	log.Printf("db: initialized sqlite at %q (default account ready)", dbPath)
	return gdb, nil
}

// autoMigrate 统一维护需要建表的 Model 列表。新增 Model 时在此登记。
func autoMigrate(gdb *gorm.DB) error {
	models := []interface{}{
		&model.Stock{},
		// DailyKline：模型与 internal/sync 落库链路仍在使用（MockBroker 撮合依赖
		// daily_klines 的开盘/涨跌停价，复盘前由 SyncDailyKlines 从网关行情落库），
		// 因此新库仍需建表。漏斗模型仅表示"AI 分析不再消费 Go 端原始 K 线"，
		// 不代表撮合层可以脱离行情数据。
		&model.DailyKline{},
		&model.Position{},
		&model.Account{},
		&model.Order{},
		&model.CandidatePool{},
		&model.AIDecisionLog{},
	}
	if err := gdb.AutoMigrate(models...); err != nil {
		return fmt.Errorf("db: automigrate: %w", err)
	}
	return nil
}

// ensureDefaultAccount 若 accounts 表为空，插入初始资金 100 万的默认账户；
// 否则跳过（不覆盖已有资金数据，保证重启幂等）。
func ensureDefaultAccount(gdb *gorm.DB) error {
	var count int64
	if err := gdb.Model(&model.Account{}).Count(&count).Error; err != nil {
		return fmt.Errorf("db: count accounts: %w", err)
	}
	if count > 0 {
		return nil
	}

	acc := model.Account{
		TotalAssets:   model.DefaultInitialCapital,
		AvailableCash: model.DefaultInitialCapital,
		FrozenCash:    0,
	}
	if err := gdb.Create(&acc).Error; err != nil {
		return fmt.Errorf("db: create default account: %w", err)
	}
	log.Printf("db: created default account id=%d with capital %.2f", acc.ID, model.DefaultInitialCapital)
	return nil
}

// GetDB 返回最近一次 InitDB 成功建立的连接；未初始化时报错而非 panic。
func GetDB() (*gorm.DB, error) {
	if globalDB == nil {
		return nil, errors.New("db: InitDB has not been called")
	}
	return globalDB, nil
}

// MustGetDB 供 main 启动路径使用：未初始化直接 panic（快速失败）。
func MustGetDB() *gorm.DB {
	gdb, err := GetDB()
	if err != nil {
		panic(err)
	}
	return gdb
}

// Close 关闭底层连接池（进程退出前调用，确保 WAL 落盘）。
func Close() error {
	if globalDB == nil {
		return nil
	}
	sqlDB, err := globalDB.DB()
	if err != nil {
		return fmt.Errorf("db: get sql.DB for close: %w", err)
	}
	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("db: close sql.DB: %w", err)
	}
	globalDB = nil
	return nil
}
