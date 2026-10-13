// Package db 的候选池落库层（阶段 8）。
//
// SaveScreeningRun 把一次 Go 原生池运行（pool.Result）的审计与快照数据写入
// SQLite 五表：screening_runs / source_run_status / watchlist_source_hits /
// daily_stock_metrics（当前池状态表沿用既有 watchlist_items，由
// tool.ApplyWatchlistPool 负责，本文件不重复写名单）。
//
// 幂等性：全部写入走唯一约束 upsert——
//   - screening_runs:        UNIQUE(run_id)
//   - source_run_status:     UNIQUE(run_id, source)
//   - watchlist_source_hits: UNIQUE(run_id, symbol, source)
//   - daily_stock_metrics:   UNIQUE(symbol, as_of, strategy_version)
//
// 同一次运行重复调用不会产生重复记录；失败来源只更新其错误状态，
// 绝不把 FAILED 解释成"今天没有命中"而批量失效旧数据。
package db

import (
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"ashare-quant/internal/model"
	"ashare-quant/internal/pool"
)

// SaveScreeningRunInputs 一次运行的落库输入。
type SaveScreeningRunInputs struct {
	GDB       *gorm.DB
	Result    *pool.Result // Go 原生链路生成结果（含审计字段）
	Started   time.Time    // 运行开始时间（cron 传入）
	Status    string       // SUCCESS / PARTIAL_SUCCESS / FALLBACK / FAILED
	ErrReason string       // FAILED/FALLBACK 的整体原因（可空）
}

// SaveScreeningRun 在一个事务内写入运行审计、来源状态、来源命中证据与日度快照。
// 返回 run_id（Result.RunID 为空时自动生成并回填到 Result）。
func SaveScreeningRun(in SaveScreeningRunInputs) (string, error) {
	if in.GDB == nil || in.Result == nil {
		return "", fmt.Errorf("db: SaveScreeningRun requires gdb and result")
	}
	res := in.Result
	now := time.Now()
	runID := res.RunID
	if runID == "" {
		runID = now.UTC().Format("20060102T150405")
		res.RunID = runID
	}
	asOf := res.AsOf
	if asOf == "" {
		asOf = res.TradeDate
	}
	status := in.Status
	if status == "" {
		status = "SUCCESS"
	}
	warnJSON := "[]"
	if len(res.Warnings) > 0 {
		if b, err := json.Marshal(res.Warnings); err == nil {
			warnJSON = string(b)
		}
	}

	err := in.GDB.Transaction(func(tx *gorm.DB) error {
		// ---- 1. screening_runs（UNIQUE run_id upsert）----
		run := model.ScreeningRun{
			RunID:           runID,
			StrategyVersion: orDefault(res.StrategyVersion, "unknown"),
			ConfigHash:      orDefault(res.ConfigHash, "unknown"),
			AsOf:            asOf,
			StartedAt:       in.Started,
			CompletedAt:     &now,
			Status:          status,
			PoolSize:        res.Total,
			LiquidityPassed: res.LiquidityPassed,
			Warnings:        warnJSON,
			ErrorReason:     in.ErrReason,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "run_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"completed_at", "status", "pool_size", "liquidity_passed", "warnings", "error_reason", "updated_at"}),
		}).Create(&run).Error; err != nil {
			return fmt.Errorf("save screening_runs: %w", err)
		}

		// ---- 2. source_run_status（每来源一行, FAILED 只记错误不删旧数据）----
		for src, st := range res.SourceStatuses {
			rec := model.SourceRunStatus{
				RunID:       runID,
				Source:      src,
				Status:      string(st),
				AsOf:        asOf,
				HitCount:    countHitsBySource(res, src),
				ErrorReason: res.SourceErrors[src],
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "run_id"}, {Name: "source"}},
				DoUpdates: clause.AssignmentColumns([]string{"status", "as_of", "hit_count", "error_reason"}),
			}).Create(&rec).Error; err != nil {
				return fmt.Errorf("save source_run_status[%s]: %w", src, err)
			}
		}

		// ---- 3. watchlist_source_hits（证据保全: 每个 symbol×source 一行）----
		for i := range res.Pool {
			e := &res.Pool[i]
			if !e.HitThisRun {
				continue // 本轮未命中的存量条目不产生新证据行
			}
			for _, h := range e.Sources {
				hit := model.WatchlistSourceHit{
					RunID:      runID,
					Symbol:     e.Symbol,
					Source:     h.SourceType,
					IsPrimary:  h.SourceType == e.PrimarySource,
					Reason:     h.Reason,
					SectorName: h.SectorName,
					AsOf:       orDefault(h.AsOf, asOf),
					Status:     string(h.Status),
				}
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "run_id"}, {Name: "symbol"}, {Name: "source"}},
					DoUpdates: clause.AssignmentColumns([]string{"is_primary", "reason", "sector_name", "as_of", "status"}),
				}).Create(&hit).Error; err != nil {
					return fmt.Errorf("save source_hits[%s/%s]: %w", e.Symbol, h.SourceType, err)
				}
			}
		}

		// ---- 4. daily_stock_metrics（缺失诚实为 NULL, 绝不伪造）----
		for i := range res.Pool {
			e := &res.Pool[i]
			m := model.DailyStockMetrics{
				Symbol:          e.Symbol,
				AsOf:            asOf,
				StrategyVersion: orDefault(res.StrategyVersion, "unknown"),
				RunID:           runID,
				Adv20:           e.AvgAmount20D,
				R20:             e.RetShortRaw,
				R60:             e.RetLongRaw,
				RS20Index:       e.RS20IndexExcess,
				RS20Sector:      e.RS20SectorExcess,
				Close:           e.Close,
				Ma20:            e.MA20,
				Ma60:            e.MA60,
				RankScore:       e.RankScore,
				StrengthPath:    string(e.Strength),
				Eligibility:     string(e.Eligibility),
				Liquidity:       string(e.Liquidity),
				Financial:       string(e.Financial),
				Tradability:     e.Tradability,
				CapacityStatus:  e.CapacityStatus,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "symbol"}, {Name: "as_of"}, {Name: "strategy_version"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"run_id", "adv20", "r20", "r60", "rs20_index", "rs20_sector",
					"close", "ma20", "ma60", "rank_score", "strength_path",
					"eligibility", "liquidity", "financial", "tradability", "capacity_status",
				}),
			}).Create(&m).Error; err != nil {
				return fmt.Errorf("save daily_metrics[%s]: %w", e.Symbol, err)
			}
		}
		return nil
	})
	if err != nil {
		return runID, err
	}
	return runID, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// countHitsBySource 统计某来源在本轮池内的命中条数（含非主来源证据）。
func countHitsBySource(res *pool.Result, src string) int {
	n := 0
	for i := range res.Pool {
		e := &res.Pool[i]
		if !e.HitThisRun {
			continue
		}
		for _, h := range e.Sources {
			if h.SourceType == src {
				n++
				break
			}
		}
	}
	return n
}
