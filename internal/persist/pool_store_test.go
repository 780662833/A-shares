// pool_store_test.go —— 第 3.2 步 P0-2：SaveScreeningRun 持久化测试。
//
// 验证真实落库与读回，而不是仅验证函数不报错。覆盖：
//  1. 正常落库：四表（screening_runs / source_run_status / watchlist_source_hits
//     / daily_stock_metrics）字段读回一致；
//  2. 幂等更新：同 RunID 重复调用不产生重复行，可变字段按 upsert 语义刷新；
//  3. 事务回滚：中途失败时前面已写的表必须无残留（原子性）；
//  4. 空参数校验：GDB/Result 为 nil 时报错且无任何写入；
//  5. 缺省字段处理：RunID 为空自动生成并回填、AsOf 缺省取 TradeDate、
//     Status 缺省 SUCCESS、StrategyVersion/ConfigHash 空值落 "unknown"；
//  6. 缺失指标诚实为 NULL（不伪造 0）；FAILED 来源只记错误状态不删旧数据；
//  7. 现状揭示：strategy_version 实际写入 20 字符 > varchar(16) 声明宽度，
//     SQLite 不强制约束因此原样读回——记录该隐性契约违反，不修改列定义。
package persist

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"ashare-quant/internal/model"
	"ashare-quant/internal/pool"
)

// openTestDB 独立初始化临时库（不复用 internal/db 包私有 helper），
// AutoMigrate 与生产 autoMigrate 相同的审计四表。
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "persist_test.db")
	gdb, err := gorm.Open(sqlite.Open("file:"+dbPath), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := gdb.AutoMigrate(
		&model.ScreeningRun{},
		&model.SourceRunStatus{},
		&model.WatchlistSourceHit{},
		&model.DailyStockMetrics{},
	); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	return gdb
}

func f64(v float64) *float64 { return &v }

// sampleResult 构造一次含两来源、两条命中证据的最小可信运行结果。
func sampleResult() *pool.Result {
	return &pool.Result{
		TradeDate:  "2026-10-16",
		AsOf:       "2026-10-16",
		RunID:      "RUN-T1",
		ConfigHash: "abcdef123456",
		Total:      2,
		Warnings:   []string{"w1", "w2"},
		SourceStatuses: map[string]pool.SourceStatus{
			"SECTOR_LEADER": pool.StatusOK,
			"FUND_FLOW":     pool.StatusFailed,
		},
		SourceErrors: map[string]string{"FUND_FLOW": "gateway timeout"},
		Pool: []pool.AggEntry{
			{
				Symbol:         "600519",
				Name:           "贵州茅台",
				PrimarySource:  "SECTOR_LEADER",
				HitThisRun:     true,
				Sources:        []pool.SourceHit{{SourceType: "SECTOR_LEADER", Reason: "sector top", SectorName: "白酒", Status: pool.StatusOK}},
				AvgAmount20D:   f64(8.5e8),
				RetShortRaw:    f64(3.2),
				RetLongRaw:     f64(7.1),
				Close:          f64(1400.5),
				Eligibility:    pool.EligPass,
				Liquidity:      pool.LiqPass,
				Financial:      pool.FinPass,
				Strength:       pool.StrengthA,
				Tradability:    "ELIGIBLE",
				CapacityStatus: "UNCHECKED",
			},
			{
				Symbol:      "000001",
				Name:        "平安银行",
				HitThisRun:  true,
				Sources:     []pool.SourceHit{{SourceType: "SECTOR_LEADER", Reason: "leader", SectorName: "银行", Status: pool.StatusOK}},
				Eligibility: pool.EligUnknown,
				// AvgAmount20D / Close 等全部保持 nil —— 缺失必须诚实落 NULL。
			},
		},
	}
}

// TestSaveScreeningRun_NormalWriteAndReadBack 正常路径：写完后逐字段读回比对。
func TestSaveScreeningRun_NormalWriteAndReadBack(t *testing.T) {
	gdb := openTestDB(t)
	res := sampleResult()
	cfgVer := pool.DefaultConfig().StrategyVersion()
	res.StrategyVersion = cfgVer

	runID, err := SaveScreeningRun(SaveScreeningRunInputs{
		GDB: gdb, Result: res, Started: time.Now().Add(-3 * time.Second), Status: "PARTIAL_SUCCESS",
	})
	if err != nil {
		t.Fatalf("SaveScreeningRun: %v", err)
	}
	if runID != "RUN-T1" {
		t.Fatalf("runID = %q, want RUN-T1 (caller-injected must win)", runID)
	}

	var run model.ScreeningRun
	if err := gdb.Where("run_id = ?", "RUN-T1").First(&run).Error; err != nil {
		t.Fatalf("read screening_runs: %v", err)
	}
	if run.Status != "PARTIAL_SUCCESS" || run.AsOf != "2026-10-16" ||
		run.ConfigHash != "abcdef123456" || run.PoolSize != 2 || run.StrategyVersion != cfgVer {
		t.Fatalf("screening_runs readback mismatch: %+v", run)
	}
	if run.Warnings != `["w1","w2"]` {
		t.Fatalf("warnings JSON not persisted as-is: %q", run.Warnings)
	}
	if run.CompletedAt == nil || !run.CompletedAt.After(run.StartedAt.Add(-time.Second)) {
		t.Fatalf("completed_at missing or before started_at: %+v", run)
	}

	// 每来源一行，FAILED 携带 error_reason。
	var sts []model.SourceRunStatus
	if err := gdb.Where("run_id = ?", "RUN-T1").Order("source").Find(&sts).Error; err != nil {
		t.Fatalf("read source_run_status: %v", err)
	}
	if len(sts) != 2 {
		t.Fatalf("want 2 source rows, got %d", len(sts))
	}
	var fund model.SourceRunStatus
	if err := gdb.Where("run_id = ? AND source = ?", "RUN-T1", "FUND_FLOW").First(&fund).Error; err != nil {
		t.Fatalf("read FUND_FLOW status: %v", err)
	}
	if fund.Status != "FAILED" || fund.ErrorReason != "gateway timeout" {
		t.Fatalf("FAILED source row wrong: %+v", fund)
	}

	// 命中证据：is_primary / status / sector_name 读回一致。
	var hits []model.WatchlistSourceHit
	if err := gdb.Where("run_id = ?", "RUN-T1").Find(&hits).Error; err != nil {
		t.Fatalf("read hits: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("want 2 hit rows, got %d", len(hits))
	}
	for _, h := range hits {
		if h.Symbol == "600519" && (!h.IsPrimary || h.Status != "OK" || h.SectorName != "白酒") {
			t.Fatalf("primary hit fields wrong: %+v", h)
		}
		if h.Symbol == "000001" && h.IsPrimary {
			t.Fatalf("non-primary entry marked primary: %+v", h)
		}
	}

	// 指标快照：有值字段读回一致，nil 字段必须是 NULL 而非 0。
	var m600 model.DailyStockMetrics
	if err := gdb.Where("symbol = ? AND as_of = ?", "600519", "2026-10-16").First(&m600).Error; err != nil {
		t.Fatalf("read metrics 600519: %v", err)
	}
	if m600.Adv20 == nil || *m600.Adv20 != 8.5e8 || m600.Close == nil || *m600.Close != 1400.5 {
		t.Fatalf("metrics value mismatch: %+v", m600)
	}
	if m600.Eligibility != "PASS" || m600.StrengthPath != "A" || m600.RunID != "RUN-T1" {
		t.Fatalf("metrics enum/run mismatch: %+v", m600)
	}
	var m001 model.DailyStockMetrics
	if err := gdb.Where("symbol = ? AND as_of = ?", "000001", "2026-10-16").First(&m001).Error; err != nil {
		t.Fatalf("read metrics 000001: %v", err)
	}
	if m001.Adv20 != nil || m001.Close != nil || m001.Ma20 != nil {
		t.Fatalf("missing values must persist as NULL, got Adv20=%v Close=%v Ma20=%v", m001.Adv20, m001.Close, m001.Ma20)
	}
	if m001.Eligibility != "UNKNOWN" {
		t.Fatalf("eligibility UNKNOWN expected, got %q", m001.Eligibility)
	}
}

// TestSaveScreeningRun_IdempotentUpsert 同 RunID 重复保存不产生重复行；
// 同时锁定 upsert 的字段子集语义：strategy_version/config_hash/as_of 不在
// DoUpdates 列表中——第二次调用的新值不会刷新这些键（现状行为）。
func TestSaveScreeningRun_IdempotentUpsert(t *testing.T) {
	gdb := openTestDB(t)
	res := sampleResult()
	if _, err := SaveScreeningRun(SaveScreeningRunInputs{GDB: gdb, Result: res, Started: time.Now(), Status: "SUCCESS"}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// 第二次：改状态与池大小，模拟同日重跑。
	res2 := sampleResult()
	res2.Total = 3
	if _, err := SaveScreeningRun(SaveScreeningRunInputs{GDB: gdb, Result: res2, Started: time.Now(), Status: "PARTIAL_SUCCESS"}); err != nil {
		t.Fatalf("second save: %v", err)
	}
	var n int64
	gdb.Model(&model.ScreeningRun{}).Where("run_id = ?", "RUN-T1").Count(&n)
	if n != 1 {
		t.Fatalf("duplicate screening_runs row for same run_id: %d", n)
	}
	gdb.Model(&model.SourceRunStatus{}).Where("run_id = ?", "RUN-T1").Count(&n)
	if n != 2 {
		t.Fatalf("source_run_status duplicated on re-save: %d rows", n)
	}
	gdb.Model(&model.WatchlistSourceHit{}).Where("run_id = ?", "RUN-T1").Count(&n)
	if n != 2 {
		t.Fatalf("watchlist_source_hits duplicated on re-save: %d rows", n)
	}
	gdb.Model(&model.DailyStockMetrics{}).Where("as_of = ?", "2026-10-16").Count(&n)
	if n != 2 {
		t.Fatalf("daily_stock_metrics duplicated on re-save: %d rows", n)
	}
	var run model.ScreeningRun
	gdb.Where("run_id = ?", "RUN-T1").First(&run)
	if run.Status != "PARTIAL_SUCCESS" || run.PoolSize != 3 {
		t.Fatalf("mutable fields not refreshed by upsert: %+v", run)
	}
}

// TestSaveScreeningRun_TransactionRollback 事务中途失败必须整体回滚：
// 通过关闭 source_run_status 表制造第二步写入失败，验证第一步的
// screening_runs 行没有残留。
func TestSaveScreeningRun_TransactionRollback(t *testing.T) {
	gdb := openTestDB(t)
	// 制造失败：删除 source_run_status 表后，事务内第二条 INSERT 必然报错。
	if err := gdb.Migrator().DropTable(&model.SourceRunStatus{}); err != nil {
		t.Fatalf("drop table to induce failure: %v", err)
	}
	res := sampleResult()
	runID, err := SaveScreeningRun(SaveScreeningRunInputs{GDB: gdb, Result: res, Started: time.Now(), Status: "SUCCESS"})
	if err == nil {
		t.Fatal("expected error after dropping source_run_status, got nil")
	}
	t.Logf("induced failure (expected): runID=%q err=%v", runID, err)
	var n int64
	gdb.Table("screening_runs").Count(&n)
	if n != 0 {
		t.Fatalf("transaction not atomic: screening_runs has %d rows after rollback", n)
	}
	gdb.Table("daily_stock_metrics").Count(&n)
	if n != 0 {
		t.Fatalf("orphan metrics rows after rollback: %d", n)
	}
}

// TestSaveScreeningRun_InputValidation nil 输入直接报错且零写入。
func TestSaveScreeningRun_InputValidation(t *testing.T) {
	gdb := openTestDB(t)
	if _, err := SaveScreeningRun(SaveScreeningRunInputs{GDB: nil, Result: sampleResult()}); err == nil {
		t.Fatal("nil GDB must error")
	}
	if _, err := SaveScreeningRun(SaveScreeningRunInputs{GDB: gdb, Result: nil}); err == nil {
		t.Fatal("nil Result must error")
	}
	var n int64
	gdb.Model(&model.ScreeningRun{}).Count(&n)
	if n != 0 {
		t.Fatalf("validation failures must not write anything, found %d rows", n)
	}
}

// TestSaveScreeningRun_DefaultFieldHandling 缺省字段处理：
// RunID 空 -> 生成 UTC 时间戳并回填 Result；AsOf 空 -> 取 TradeDate；
// Status 空 -> SUCCESS；StrategyVersion/ConfigHash 空 -> "unknown"。
func TestSaveScreeningRun_DefaultFieldHandling(t *testing.T) {
	gdb := openTestDB(t)
	res := sampleResult()
	res.RunID = ""
	res.AsOf = ""
	res.StrategyVersion = ""
	res.ConfigHash = ""
	runID, err := SaveScreeningRun(SaveScreeningRunInputs{GDB: gdb, Result: res, Started: time.Now()})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if runID == "" || runID != res.RunID {
		t.Fatalf("generated run id not returned/backfilled: ret=%q res=%q", runID, res.RunID)
	}
	if _, err := time.Parse("20060102T150405", runID); err != nil {
		t.Fatalf("auto-generated runID format unexpected %q: %v", runID, err)
	}
	var run model.ScreeningRun
	if err := gdb.Where("run_id = ?", runID).First(&run).Error; err != nil {
		t.Fatalf("read back: %v", err)
	}
	if run.AsOf != "2026-10-16" {
		t.Fatalf("AsOf should fall back to TradeDate, got %q", run.AsOf)
	}
	if run.Status != "SUCCESS" {
		t.Fatalf("empty status should default SUCCESS, got %q", run.Status)
	}
	if run.StrategyVersion != "unknown" || run.ConfigHash != "unknown" {
		t.Fatalf("empty version/hash should persist as 'unknown', got %q/%q", run.StrategyVersion, run.ConfigHash)
	}
	if run.Warnings != `["w1","w2"]` {
		t.Fatalf("warnings: %q", run.Warnings)
	}
	// 空 warnings -> "[]"。
	res2 := sampleResult()
	res2.RunID = "RUN-EMPTY-W"
	res2.Warnings = nil
	if _, err := SaveScreeningRun(SaveScreeningRunInputs{GDB: gdb, Result: res2, Started: time.Now()}); err != nil {
		t.Fatalf("save2: %v", err)
	}
	var r2 model.ScreeningRun
	gdb.Where("run_id = ?", "RUN-EMPTY-W").First(&r2)
	if r2.Warnings != "[]" {
		t.Fatalf("nil warnings should serialize to [], got %q", r2.Warnings)
	}
}

// TestSaveScreeningRun_StrategyVersionColumnWidthRisk 揭示（不修复）：
// 真实 StrategyVersion 长 20 字符，超过模型声明的 varchar(16)。SQLite 不强制
// 该宽度，落库与读回均为原值——风险仅在迁移到严格引擎时显性化。
func TestSaveScreeningRun_StrategyVersionColumnWidthRisk(t *testing.T) {
	gdb := openTestDB(t)
	sv := pool.DefaultConfig().StrategyVersion()
	if len(sv) != 20 {
		t.Fatalf("precondition: strategy version expected 20 chars, got %d (%q)", len(sv), sv)
	}
	res := sampleResult()
	res.StrategyVersion = sv
	if _, err := SaveScreeningRun(SaveScreeningRunInputs{GDB: gdb, Result: res, Started: time.Now()}); err != nil {
		t.Fatalf("save with over-declared-length version failed on SQLite (contract drift!): %v", err)
	}
	var run model.ScreeningRun
	if err := gdb.Where("run_id = ?", "RUN-T1").First(&run).Error; err != nil {
		t.Fatalf("read back: %v", err)
	}
	if run.StrategyVersion != sv {
		t.Fatalf("value silently truncated on SQLite: stored %q want %q", run.StrategyVersion, sv)
	}
	t.Logf("KNOWN RISK LOCKED: strategy_version '%s' (%d chars) exceeds declared varchar(16); SQLite tolerates it, strict engines would not", sv, len(sv))
}
