package model

import "time"

// ---------------------------------------------------------------------------
// 阶段 8：候选池 SQLite 落库模型（五表 + 唯一约束）
//
// 设计原则（对齐实施计划第九章）：
//   - watchlist_items 仍是当前池子的权威状态表（生命周期规则见 WatchlistItem），
//     本文件不重复建"第二套名单"，而是补齐审计与快照维度；
//   - 每次成功运行保存 config_hash / strategy_version，供日后比较不同规则
//     （ADV20 门槛、RS 权重、财务阈值）对候选池的影响；
//   - 唯一约束防止同一 run 重复写入：
//       watchlist_source_hits: UNIQUE(run_id, symbol, source)
//       daily_stock_metrics:   UNIQUE(symbol, as_of, strategy_version)
//       screening_runs:        UNIQUE(run_id)
//       source_run_status:     UNIQUE(run_id, source)
// ---------------------------------------------------------------------------

// ScreeningRun 一次筛选运行的审计记录。
//
// status 语义：
//   - SUCCESS         全部来源 OK/SUCCESS_EMPTY，正常完成；
//   - PARTIAL_SUCCESS 存在 PARTIAL 来源，池子可用但带缺口警告；
//   - FALLBACK        Go 原生链路失败回退 Python 网关（触发 deprecated 监控）；
//   - FAILED          整体失败——此时不落任何来源命中，旧池子原样保留。
type ScreeningRun struct {
	ID              uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	RunID           string     `gorm:"type:varchar(32);not null;uniqueIndex:uk_run_id" json:"run_id"`
	StrategyVersion string     `gorm:"type:varchar(16);not null" json:"strategy_version"`
	ConfigHash      string     `gorm:"type:varchar(16);not null;index:idx_run_cfghash" json:"config_hash"` // sha256 前 12 位
	AsOf            string     `gorm:"type:varchar(10);not null;index:idx_run_asof" json:"as_of"`          // 业务截止日 YYYY-MM-DD
	StartedAt       time.Time  `json:"started_at"`
	CompletedAt     *time.Time `gorm:"type:datetime" json:"completed_at,omitempty"`
	Status          string     `gorm:"type:varchar(24);not null;default:RUNNING;index:idx_run_status" json:"status"`
	PoolSize        int        `gorm:"not null;default:0" json:"pool_size"`
	LiquidityPassed int        `gorm:"not null;default:0" json:"liquidity_passed"` // 全市场初筛通过数（诊断）
	Warnings        string     `gorm:"type:text" json:"warnings,omitempty"`        // JSON 数组
	ErrorReason     string     `gorm:"type:text" json:"error_reason,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName 固定表名。
func (ScreeningRun) TableName() string { return "screening_runs" }

// SourceRunStatus 每个数据来源在一次运行中的执行状态。
//
// 关键红线（阶段 2/8 契约）：FAILED 只更新该来源的错误状态并保留最后一次
// 有效结果——绝不把请求失败解释成"今天没有任何命中股票"，更不能因此把
// 对应记录批量置 INACTIVE。SUCCESS_EMPTY 与 FAILED 必须严格区分。
type SourceRunStatus struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	RunID       string    `gorm:"type:varchar(32);not null;uniqueIndex:uk_srcrun,priority:1" json:"run_id"`
	Source      string    `gorm:"type:varchar(24);not null;uniqueIndex:uk_srcrun,priority:2" json:"source"` // SECTOR_LEADER/ETF_MOMENTUM/FUND_FLOW/MANUAL/LIQUIDITY_BASE
	Status      string    `gorm:"type:varchar(16);not null" json:"status"`                                  // OK/SUCCESS_EMPTY/PARTIAL/STALE/FAILED
	AsOf        string    `gorm:"type:varchar(10)" json:"as_of,omitempty"`                                  // 本来源数据的业务截止日期
	HitCount    int       `gorm:"not null;default:0" json:"hit_count"`                                      // 本来源本轮命中条数
	ErrorReason string    `gorm:"type:text" json:"error_reason,omitempty"`                                  // FAILED/PARTIAL 的具体原因
	CreatedAt   time.Time `json:"created_at"`
}

// TableName 固定表名。
func (SourceRunStatus) TableName() string { return "source_run_status" }

// WatchlistSourceHit 每次运行中某证券命中某来源的证据记录。
//
// 去重是合并证券身份，不是删除来源证据：同一 symbol 的多来源在聚合层合并为
// primary_source + sources[]，但每条命中理由都完整落到本表（UNIQUE(run_id,
// symbol, source) 防同轮重复），供事后审计"这只票当初为什么进池"。
type WatchlistSourceHit struct {
	ID         uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	RunID      string    `gorm:"type:varchar(32);not null;uniqueIndex:uk_hitrun,priority:1;index:idx_hit_symbol_run,priority:2" json:"run_id"`
	Symbol     string    `gorm:"type:varchar(16);not null;uniqueIndex:uk_hitrun,priority:2;index:idx_hit_symbol_run,priority:1" json:"symbol"`
	Source     string    `gorm:"type:varchar(24);not null;uniqueIndex:uk_hitrun,priority:3" json:"source"`
	IsPrimary  bool      `gorm:"not null;default:false" json:"is_primary"` // 聚合后的主来源标记
	Reason     string    `gorm:"type:text" json:"reason,omitempty"`        // 人类可读命中理由
	SectorName string    `gorm:"type:varchar(64)" json:"sector_name,omitempty"`
	AsOf       string    `gorm:"type:varchar(10)" json:"as_of,omitempty"`  // 数据截止日期
	Status     string    `gorm:"type:varchar(16)" json:"status,omitempty"` // 本条命中证据的来源刷新状态
	CreatedAt  time.Time `json:"created_at"`
}

// TableName 固定表名。
func (WatchlistSourceHit) TableName() string { return "watchlist_source_hits" }

// DailyStockMetrics 候选池成员的日度指标快照。
//
// 缺失诚实为 NULL：样本不足或来源失败的字段不得用 0 或前值伪造。
// 金额单位统一为人民币元（适配层收敛），收益率单位为百分数（%）。
type DailyStockMetrics struct {
	ID              uint64 `gorm:"primaryKey;autoIncrement" json:"id"`
	Symbol          string `gorm:"type:varchar(16);not null;uniqueIndex:uk_metrics,priority:1" json:"symbol"`
	AsOf            string `gorm:"type:varchar(10);not null;uniqueIndex:uk_metrics,priority:2" json:"as_of"`
	StrategyVersion string `gorm:"type:varchar(16);not null;uniqueIndex:uk_metrics,priority:3" json:"strategy_version"`
	RunID           string `gorm:"type:varchar(32);not null;index:idx_metrics_run" json:"run_id"`

	Adv20          *float64 `gorm:"type:real" json:"adv20,omitempty"`       // 20日平均成交额（元）
	R20            *float64 `gorm:"type:real" json:"r20,omitempty"`         // 个股 R20（%，复权收盘口径）
	R60            *float64 `gorm:"type:real" json:"r60,omitempty"`         // 个股 R60（%）
	RS20Index      *float64 `gorm:"type:real" json:"rs20_index,omitempty"`  // R20 - 基准指数同期（%）
	RS20Sector     *float64 `gorm:"type:real" json:"rs20_sector,omitempty"` // R20 - 行业同期（%; nil=行业不可确认）
	Close          *float64 `gorm:"type:real" json:"close,omitempty"`       // 最新有效收盘价（元）
	Ma20           *float64 `gorm:"type:real" json:"ma20,omitempty"`        // 20日均价（元）
	Ma60           *float64 `gorm:"type:real" json:"ma60,omitempty"`        // 60日均价（元）
	RankScore      float64  `gorm:"type:real;not null;default:0" json:"rank_score"`
	StrengthPath   string   `gorm:"type:varchar(8)" json:"strength_path,omitempty"` // A/B/NONE
	Eligibility    string   `gorm:"type:varchar(16)" json:"eligibility_status,omitempty"`
	Liquidity      string   `gorm:"type:varchar(20)" json:"liquidity_status,omitempty"`
	Financial      string   `gorm:"type:varchar(20)" json:"financial_status,omitempty"`
	Tradability    string   `gorm:"type:varchar(16)" json:"tradability_status,omitempty"`
	CapacityStatus string   `gorm:"type:varchar(16)" json:"capacity_status,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

// TableName 固定表名。
func (DailyStockMetrics) TableName() string { return "daily_stock_metrics" }
