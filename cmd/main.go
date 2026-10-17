// Command main 是 A 股日线级别 AI 模拟交易系统的进程入口（Phase 6）。
//
// 职责：
//  1. 初始化 SQLite（Phase 2 db.InitDB，幂等，默认账户 100 万）；
//  2. 注册 robfig/cron 四个工作日任务：
//     - 0  15 * * 1-5 : 动态自选股池更新（Layer 0.5，ASHARE_DYNAMIC_WATCHLIST=1 时启用）；
//     - 10 15 * * 1-5 : czsc 缓存增量预热（当日 ACTIVE 池 ∪ 持仓股）；
//     - 30 15 * * 1-5 : 触发 Phase 5 Eino 复盘 Graph，生成次日 PENDING 计划；
//     - 25 09 * * 1-5 : 调用 Phase 3 MockBroker.SimulateNextDayOpen 撮合昨日计划；
//  3. 提供调试用 HTTP 手动触发接口（/trigger/review、/trigger/simulate、/trigger/refresh、/trigger/watchlist、/healthz）；
//  4. 监听 SIGINT/SIGTERM 优雅退出（停 cron + 关 HTTP + 关 DB）。
//
// 环境变量：
//
//	ASHARE_DB_PATH      SQLite 文件路径，默认 ./data/trading.db
//	ASHARE_GATEWAY_URL  Python 数据网关地址，默认 http://127.0.0.1:8000
//	ASHARE_HTTP_ADDR    手动触发服务监听地址，默认 :8080（设为 "off" 可禁用）
//	ASHARE_WATCHLIST    候选股票池，逗号分隔，如 "000001,600519"，默认空（仅复盘持仓股）；
//	                    动态池模式下兼作人工白名单（MANUAL）与降级保底名单
//	ASHARE_DYNAMIC_WATCHLIST   =1 启用动态池模式（watchlist_items 表为 symbols 主来源）
//	ASHARE_WATCHLIST_MAX       动态池参与粗筛的最大股票数，默认 100（持仓股不占配额）
//	ASHARE_CRON_WATCHLIST      动态池更新 cron，默认 "0 15 * * 1-5"
//	ASHARE_POOL_SOURCE         go | python（默认 python，灰度开关）；=go 时动态池改由
//	                           Go 原生 internal/pool + stock-go SDK 生成，不再调用网关
//	                           /api/watchlist/generate；Go 生成失败时自动回退 Python 链路
//	ASHARE_POOL_TIMEOUT        Go 原生池单次生成超时，默认 6m
//	ASHARE_LLM_BASE_URL / ASHARE_MODEL / ASHARE_API_KEY
//	                    LLM 接入配置。MVP 未强依赖 eino-ext provider：
//	                    未配置时系统以「降级模式」运行 —— 复盘任务将跳过 LLM 决策
//	                    并记录告警，撮合/查询等其他功能完全可用。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"

	"ashare-quant/internal/broker"
	"ashare-quant/internal/db"
	dbmodel "ashare-quant/internal/model"
	"ashare-quant/internal/persist"
	"ashare-quant/internal/pool"
	datasync "ashare-quant/internal/sync"
	"ashare-quant/internal/tool"
)

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

type appConfig struct {
	DBPath       string
	GatewayURL   string
	HTTPAddr     string
	Watchlist    []string
	CronReview   string        // 日终复盘 cron 表达式
	CronSimul    string        // 次日撮合 cron 表达式
	CronRefresh  string        // 收盘后 czsc 缓存预计算 cron 表达式（长期方案 C）
	CronWatch    string        // 动态自选股池生成 cron 表达式
	DynWatchOn   bool          // ASHARE_DYNAMIC_WATCHLIST=1 时启用动态池模式
	DynWatchMax  int           // 每日动态池参与粗筛的最大股票数（持仓股不占配额）
	WatchTimeout time.Duration // 网关 /api/watchlist/generate 单次调用超时
	PoolSource   string        // ASHARE_POOL_SOURCE: go | python（默认 python, 灰度开关）
	PoolTimeout  time.Duration // Go 原生池单次生成超时
	PoolCfg      pool.Config   // Go 原生池精筛管线参数（ASHARE_REFINE_* / ASHARE_RS_* 环境变量覆盖 DefaultConfig）
}

// applyPoolEnvOverrides 用环境变量覆盖精筛管线参数；非法值告警并保持原值。
func applyPoolEnvOverrides(c *pool.Config) {
	if v := strings.TrimSpace(os.Getenv("ASHARE_REFINE_ENABLED")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.QualifyEnabled = b
		} else {
			log.Printf("WARN: invalid ASHARE_REFINE_ENABLED=%q, keep %v", v, c.QualifyEnabled)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_MIN_LISTED_DAYS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.MinListedDays = n
		} else {
			log.Printf("WARN: invalid ASHARE_MIN_LISTED_DAYS=%q, keep %d", v, c.MinListedDays)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_REQUIRE_POSITIVE_EPS")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.RequirePositiveEPS = b
		} else {
			log.Printf("WARN: invalid ASHARE_REQUIRE_POSITIVE_EPS=%q, keep %v", v, c.RequirePositiveEPS)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_MIN_ROE")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			c.MinROE = f
		} else {
			log.Printf("WARN: invalid ASHARE_MIN_ROE=%q, keep %v", v, c.MinROE)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_RS_ENABLED")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.RSEnabled = b
		} else {
			log.Printf("WARN: invalid ASHARE_RS_ENABLED=%q, keep %v", v, c.RSEnabled)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_RS_WINDOW")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.RSWindow = n
		} else {
			log.Printf("WARN: invalid ASHARE_RS_WINDOW=%q, keep %d", v, c.RSWindow)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_RS_BENCHMARK")); v != "" {
		c.RSBenchmark = v
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_RS_TOPK")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.RSTopK = n
		} else {
			log.Printf("WARN: invalid ASHARE_RS_TOPK=%q, keep %d", v, c.RSTopK)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_RS_CONCURRENCY")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.RSConcurrency = n
		} else {
			log.Printf("WARN: invalid ASHARE_RS_CONCURRENCY=%q, keep %d", v, c.RSConcurrency)
		}
	}
	// ---- 阶段 4：流动性过滤与交易容量（ASHARE_LIQ_*）----
	if v := strings.TrimSpace(os.Getenv("ASHARE_LIQ_ENABLED")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.LiquidityEnabled = b
		} else {
			log.Printf("WARN: invalid ASHARE_LIQ_ENABLED=%q, keep %v", v, c.LiquidityEnabled)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_MIN_ADV20_STOCK")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			c.MinADV20Stock = f
		} else {
			log.Printf("WARN: invalid ASHARE_MIN_ADV20_STOCK=%q, keep %v", v, c.MinADV20Stock)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_MIN_ADV20_ETF")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			c.MinADV20ETF = f
		} else {
			log.Printf("WARN: invalid ASHARE_MIN_ADV20_ETF=%q, keep %v", v, c.MinADV20ETF)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_PLAN_NOTIONAL")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			c.PlanNotional = f
		} else {
			log.Printf("WARN: invalid ASHARE_PLAN_NOTIONAL=%q, keep %v", v, c.PlanNotional)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_MAX_PARTICIPATION_PCT")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			c.MaxParticipationPct = f
		} else {
			log.Printf("WARN: invalid ASHARE_MAX_PARTICIPATION_PCT=%q, keep %v", v, c.MaxParticipationPct)
		}
	}
	// ---- 阶段 5：双窗口相对强度（R20 + R60）----
	if v := strings.TrimSpace(os.Getenv("ASHARE_RS_SHORT_WINDOW")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.RSShortWindow = n
		} else {
			log.Printf("WARN: invalid ASHARE_RS_SHORT_WINDOW=%q, keep %d", v, c.RSShortWindow)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_RS_LONG_WINDOW")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.RSLongWindow = n
		} else {
			log.Printf("WARN: invalid ASHARE_RS_LONG_WINDOW=%q, keep %d", v, c.RSLongWindow)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_RS_SHORT_WEIGHT")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 100 {
			c.RSShortWeightPct = f
		} else {
			log.Printf("WARN: invalid ASHARE_RS_SHORT_WEIGHT=%q, keep %v", v, c.RSShortWeightPct)
		}
	}
	// ---- 候选池来源规模上限（遗留问题修复：默认全量不截断，显式设置才恢复旧 Top-N 口径）----
	// 注意: Ovr<=0 表示"沿用生效默认(全量)", 因此这里用负数哨兵区分"未设置"与"显式设 0"。
	if v := strings.TrimSpace(os.Getenv("ASHARE_SECTOR_TOP_N")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.SectorTopNOvr = n
		} else {
			log.Printf("WARN: invalid ASHARE_SECTOR_TOP_N=%q, keep full(不截断)", v)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_SECTOR_LEADERS_K")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.SectorLeadersKOvr = n
		} else {
			log.Printf("WARN: invalid ASHARE_SECTOR_LEADERS_K=%q, keep full(不截断)", v)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_ETF_TOP_M")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.ETFTopMOvr = n
		} else {
			log.Printf("WARN: invalid ASHARE_ETF_TOP_M=%q, keep full(不截断)", v)
		}
	}
	// 显式把三个覆盖键全部设为 0 => 视为调用方主动要求"按字段原值执行"，
	// 关闭 DefaultConfig 的全量兜底（用于精确复现历史 3/3/8 之外口径的调试场景）。
	if c.IsFullSourceDefault() &&
		c.SectorTopNOvr == 0 && c.SectorLeadersKOvr == 0 && c.ETFTopMOvr == 0 &&
		(os.Getenv("ASHARE_SECTOR_TOP_N") == "0" || os.Getenv("ASHARE_SECTOR_LEADERS_K") == "0" || os.Getenv("ASHARE_ETF_TOP_M") == "0") {
		c.DisableFullSourceDefault()
	}
	// ---- L3/L4 缠论结构分层（chan.go, 复用 python czsc; ASHARE_CHAN_*）----
	if v := strings.TrimSpace(os.Getenv("ASHARE_CHAN_ENABLED")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.ChanEnabled = b
		} else {
			log.Printf("WARN: invalid ASHARE_CHAN_ENABLED=%q, keep %v", v, c.ChanEnabled)
		}
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_CHAN_CACHE_ONLY")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.ChanCacheOnly = b
		} else {
			log.Printf("WARN: invalid ASHARE_CHAN_CACHE_ONLY=%q, keep %v", v, c.ChanCacheOnly)
		}
	}
}

func loadConfig() appConfig {
	cwd, _ := os.Getwd()
	cfg := appConfig{
		DBPath:       envOr("ASHARE_DB_PATH", filepath.Join(cwd, "data", "trading.db")),
		GatewayURL:   envOr("ASHARE_GATEWAY_URL", "http://127.0.0.1:8000"),
		HTTPAddr:     envOr("ASHARE_HTTP_ADDR", ":8080"),
		CronReview:   "30 15 * * 1-5",
		CronSimul:    "25 9 * * 1-5",
		CronRefresh:  "10 15 * * 1-5", // 晚于 15:00 池子更新、先于复盘 20 分钟，保证 15:30 粗筛全部命中缓存
		CronWatch:    "0 15 * * 1-5",  // 收盘即更新动态池（Layer 0.5），10 分钟后 refresh 只对当日 ACTIVE 池增量预热
		DynWatchMax:  100,
		WatchTimeout: 4 * time.Minute,
		PoolSource:   "python",
		PoolTimeout:  6 * time.Minute,
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_CRON_REFRESH")); v != "" {
		cfg.CronRefresh = v
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_CRON_WATCHLIST")); v != "" {
		cfg.CronWatch = v
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("ASHARE_POOL_SOURCE"))); v == "go" {
		cfg.PoolSource = "go"
	} else if v != "" && v != "python" {
		log.Printf("WARN: invalid ASHARE_POOL_SOURCE=%q, keep %q", v, cfg.PoolSource)
	}
	if v := strings.TrimSpace(os.Getenv("ASHARE_POOL_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.PoolTimeout = d
		} else {
			log.Printf("WARN: invalid ASHARE_POOL_TIMEOUT=%q, keep %s", v, cfg.PoolTimeout)
		}
	}
	cfg.DynWatchOn = strings.TrimSpace(os.Getenv("ASHARE_DYNAMIC_WATCHLIST")) == "1"
	if v := strings.TrimSpace(os.Getenv("ASHARE_WATCHLIST_MAX")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.DynWatchMax = n
		} else {
			log.Printf("WARN: invalid ASHARE_WATCHLIST_MAX=%q, keep %d", v, cfg.DynWatchMax)
		}
	}
	cfg.PoolCfg = pool.DefaultConfig()
	applyPoolEnvOverrides(&cfg.PoolCfg)
	for _, s := range strings.Split(envOr("ASHARE_WATCHLIST", ""), ",") {
		if s = strings.TrimSpace(s); s != "" {
			cfg.Watchlist = append(cfg.Watchlist, s)
		}
	}
	return cfg
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// ---------------------------------------------------------------------------
// 应用
// ---------------------------------------------------------------------------

type app struct {
	cfg    appConfig
	gdb    *gorm.DB
	broker *broker.MockBroker
	syncer *datasync.DataSyncer // Phase 5.5：复盘前行情落库（修复数据与撮合脱节断层）

	// lastRunSourceStatuses 最近一次 Go 原生 Generate() 的来源刷新状态（F-2: 传给
	// ApplyWatchlistPool 做保守过期审计; Python 网关/FALLBACK 链路无法提供 -> nil 即状态未知）。
	lastRunSourceStatuses map[string]string

	mu       sync.Mutex // 防止手动触发与 cron 同时跑同一流程
	running  map[string]bool
	reviewFn reviewRunner // Phase 5 Graph 执行器（LLM 未配置时为降级实现）
}

// reviewRunner 抽象复盘执行，便于在缺少 LLM 凭据时注入降级实现。
type reviewRunner func(ctx context.Context, tradeDate string, watchlist []string) (map[string]any, error)

func newApp(cfg appConfig) (*app, error) {
	gdb, err := db.InitDB(cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("init db: %w", err)
	}
	br, err := broker.NewMockBroker(gdb)
	if err != nil {
		return nil, fmt.Errorf("init broker: %w", err)
	}
	syncer, err := datasync.New(gdb, datasync.Config{GatewayBaseURL: cfg.GatewayURL})
	if err != nil {
		return nil, fmt.Errorf("init data syncer: %w", err)
	}
	a := &app{
		cfg:     cfg,
		gdb:     gdb,
		broker:  br,
		syncer:  syncer,
		running: make(map[string]bool),
	}
	a.reviewFn = buildReviewRunner(cfg, gdb)
	return a, nil
}

// tryLock/unlock 简单互斥：同名流程已在运行时直接拒绝后来者（cron 与手动触发互斥）。
func (a *app) tryLock(name string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running[name] {
		return false
	}
	a.running[name] = true
	return true
}

func (a *app) unlock(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.running, name)
}

// positionCodes 返回当前持仓（quantity > 0）股票代码，去重。
func (a *app) positionCodes() []string {
	var positions []dbmodel.Position
	if err := a.gdb.Where("quantity > 0").Find(&positions).Error; err != nil {
		log.Printf("WARN: load positions failed: %v", err)
	}
	seen := make(map[string]bool)
	var out []string
	for _, p := range positions {
		if !seen[p.Code] {
			seen[p.Code] = true
			out = append(out, p.Code)
		}
	}
	return out
}

// syncSymbols 返回本轮需要同步落库的股票集合：
// scanSymbols()（动态池模式下 = watchlist_items ACTIVE ∪ MANUAL ∪ 持仓股；
// 否则 = ASHARE_WATCHLIST ∪ 持仓股）。持仓股必须同步——次日撮合卖出时
// Broker 依赖 daily_klines 的涨跌停价判定一字板，缺数据会导致订单被拒。
func (a *app) syncSymbols() []string {
	return a.scanSymbols()
}

// scanSymbols 返回每日 czsc 粗筛 / 缓存预热的 symbols 集合。
//
// 静态模式（默认）：ASHARE_WATCHLIST ∪ 持仓股，行为与历史版本完全一致。
// 动态模式（ASHARE_DYNAMIC_WATCHLIST=1）：以 watchlist_items 表为唯一主来源 ——
//   - MANUAL/BLACKLIST 等人工终态不受配额约束；
//   - 持仓股必进且不占 DynWatchMax 配额（撮合依赖其行情缓存）；
//   - 其余 ACTIVE 条目按来源优先级 MANUAL > ETF_MOMENTUM > SECTOR_LEADER > FUND_FLOW
//     截断到 DynWatchMax，防止上游异动接口异常导致池子暴涨拖垮 refresh 时限；
//   - 表为空（首次启用/网关从未成功）自动回退 ASHARE_WATCHLIST ∪ 持仓股，保证首日不断流。
func (a *app) scanSymbols() []string {
	seen := make(map[string]bool)
	var out []string
	add := func(s string) bool {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			return false
		}
		seen[s] = true
		out = append(out, s)
		return true
	}
	pos := a.positionCodes()

	if !a.cfg.DynWatchOn {
		for _, s := range a.cfg.Watchlist {
			add(s)
		}
		for _, p := range pos {
			add(p)
		}
		return out
	}

	var items []dbmodel.WatchlistItem
	if err := a.gdb.Where("status = ?", dbmodel.WatchStatusActive).Find(&items).Error; err != nil {
		log.Printf("WARN: load dynamic watchlist failed (%v), fallback to static ASHARE_WATCHLIST", err)
	}
	if len(items) == 0 {
		log.Printf("[watchlist] dynamic pool empty (table has no ACTIVE rows), fallback to static watchlist")
		for _, s := range a.cfg.Watchlist {
			add(s)
		}
		for _, p := range pos {
			add(p)
		}
		return out
	}

	// 第一遍：人工白名单 + 持仓股（不占配额）
	for _, it := range items {
		if it.SourceType == dbmodel.WatchSourceManual {
			add(it.Code)
		}
	}
	for _, p := range pos {
		add(p)
	}
	// 第二遍：动态来源按优先级填充剩余配额
	rank := map[string]int{
		dbmodel.WatchSourceETF:    0,
		dbmodel.WatchSourceSector: 1,
		dbmodel.WatchSourceFund:   2,
	}
	quota := a.cfg.DynWatchMax
	for quota > 0 {
		picked := false
		for r := 0; r < len(rank) && quota > 0; r++ {
			for _, it := range items {
				if rank[it.SourceType] == r && add(it.Code) {
					quota--
					picked = true
				}
			}
		}
		if !picked {
			break // 未知来源类型兜底退出，防止死循环
		}
	}
	if len(out) > len(pos)+len(a.cfg.Watchlist) {
		log.Printf("[watchlist] dynamic mode: scanned=%d (cap=%d, positions=%d not counted against cap)",
			len(out), a.cfg.DynWatchMax, len(pos))
	}
	return out
}

// ---------------------------------------------------------------------------
// 任务 1：日终复盘（15:30）—— 调用 Phase 5 Eino Graph
// ---------------------------------------------------------------------------

const flowReview = "review"

func (a *app) runDailyReview(trigger string) error {
	if !a.tryLock(flowReview) {
		return errors.New("review already running, skip")
	}
	defer a.unlock(flowReview)

	start := time.Now()
	tradeDate := today()
	log.Printf("[review] ===== 日终复盘开始 trigger=%s date=%s watchlist=%v =====", trigger, tradeDate, a.cfg.Watchlist)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// ---- Phase 5.5：先把今日行情落库，再触发 Graph ----
	// 修复断层：撮合引擎次日 09:25 依赖 daily_klines 表中的涨停/跌停价判定一字板；
	// MarketDataTool 的数据只进 LLM 上下文不落库，故此处显式同步。
	// 落库失败不阻断复盘（LLM 分析仍可用 Tool 实时数据），但会打 WARN 供运维感知。
	symbols := a.syncSymbols()
	if syncRes, err := a.syncer.SyncDailyKlines(ctx, symbols, tradeDate); err != nil {
		log.Printf("[review] WARN: pre-review data sync failed (broker may lack klines for tomorrow): %v", err)
	} else if syncRes != nil && syncRes.Failed > 0 {
		log.Printf("[review] WARN: data sync partial failures: %+v", syncRes.Failures)
	}

	state, err := a.reviewFn(ctx, tradeDate, symbols)
	if err != nil {
		log.Printf("[review] FAILED date=%s elapsed=%s err=%v", tradeDate, time.Since(start), err)
		return err
	}

	// 打印生成的计划摘要（题设要求）
	printPlanSummary(state)
	log.Printf("[review] ===== 复盘完成 date=%s elapsed=%s =====", tradeDate, time.Since(start))
	return nil
}

func printPlanSummary(state map[string]any) {
	if state == nil {
		return
	}
	if plans, ok := state["llm_plans"].([]interface{}); ok {
		log.Printf("[review] plan items total=%d", len(plans))
	}
	if rej, ok := state["rejected"]; ok {
		b, _ := json.Marshal(rej)
		log.Printf("[review] risk-rejected: %s", b)
	}
	if rep, ok := state["exec_report"]; ok {
		b, _ := json.Marshal(rep)
		log.Printf("[review] exec report: %s", b)
	}
}

// ---------------------------------------------------------------------------
// 任务 2：次日撮合（09:25）—— 调用 Phase 3 MockBroker
// ---------------------------------------------------------------------------

const flowSimulate = "simulate"

func (a *app) runSimulation(trigger string) error {
	if !a.tryLock(flowSimulate) {
		return errors.New("simulation already running, skip")
	}
	defer a.unlock(flowSimulate)

	tradeDate := today()
	log.Printf("[simulate] ===== 开盘撮合开始 trigger=%s date=%s =====", trigger, tradeDate)

	summary, err := a.broker.SimulateNextDayOpen(tradeDate)
	if err != nil {
		log.Printf("[simulate] FAILED date=%s err=%v", tradeDate, err)
		return err
	}

	// 成交明细已由 Broker 内部逐单打日志；此处补充账户最新资金（题设要求）
	var acc dbmodel.Account
	if qerr := a.gdb.Order("id asc").First(&acc).Error; qerr == nil {
		log.Printf("[simulate] account snapshot: available=%.2f frozen=%.2f total(last known)=%.2f",
			acc.AvailableCash, acc.FrozenCash, acc.TotalAssets)
	} else {
		log.Printf("[simulate] WARN load account failed: %v", qerr)
	}
	log.Printf("[simulate] ===== 撮合完成 %s =====", summary.String())
	return nil
}

// ---------------------------------------------------------------------------
// 任务 3（长期方案 C）：收盘后 czsc 缓存预计算（15:10）
// ---------------------------------------------------------------------------

const flowRefresh = "refresh"

// refreshCachePayload 与 Python 网关 POST /api/czsc/refresh 的请求体对齐。
type refreshCachePayload struct {
	Symbols []string `json:"symbols"`
	Period  string   `json:"period"`
	Force   bool     `json:"force"`
}

// runCzscRefresh 调用数据网关把「自选股 ∪ 持仓股」的缠论分析结果批量预计算并落盘。
//
// 意义：AKShare 限速导致的分钟级扫描全部挪到本时段离线完成；20 分钟后 (15:30)
// 复盘 Graph 的 FetchPool/screen_pool 直接命中当日缓存，毫秒级返回且不再实时联网。
// force=true 全量重拉可顺带消除增量拼接在除权日引入的 qfq 漂移近似。
func (a *app) runCzscRefresh(trigger string) error {
	if !a.tryLock(flowRefresh) {
		return errors.New("czsc cache refresh already running, skip")
	}
	defer a.unlock(flowRefresh)

	start := time.Now()
	symbols := a.syncSymbols() // 自选股 ∪ 持仓股，与复盘前行情同步同一集合
	if len(symbols) == 0 {
		log.Printf("[refresh] no symbols (watchlist & positions both empty), skip czsc cache refresh")
		return nil
	}
	log.Printf("[refresh] ===== czsc 缓存预计算开始 trigger=%s symbols=%d =====", trigger, len(symbols))

	payload, err := json.Marshal(refreshCachePayload{Symbols: symbols, Period: "daily", Force: true})
	if err != nil {
		return fmt.Errorf("marshal refresh payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.GatewayURL+"/api/czsc/refresh", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[refresh] FAILED gateway unreachable: %v", err)
		return fmt.Errorf("call gateway /api/czsc/refresh: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read refresh response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("[refresh] FAILED status=%d body=%.512s", resp.StatusCode, body)
		return fmt.Errorf("gateway returned HTTP %d: %.256s", resp.StatusCode, string(body))
	}

	// 解析统计字段做结构化日志（total/stats/elapsed_seconds），失败不影响成功判定
	var parsed struct {
		Code int `json:"code"`
		Data struct {
			Total          int            `json:"total"`
			Stats          map[string]int `json:"stats"`
			ElapsedSeconds float64        `json:"elapsed_seconds"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		log.Printf("[refresh] WARN cannot parse stats (%v), raw=%.256s", err, body)
	} else {
		log.Printf("[refresh] done total=%d stats=%v gateway_elapsed=%.2fs",
			parsed.Data.Total, parsed.Data.Stats, parsed.Data.ElapsedSeconds)
	}
	log.Printf("[refresh] ===== 预计算完成 elapsed=%s =====", time.Since(start))
	return nil
}

// ---------------------------------------------------------------------------
// 任务 0（Layer 0.5）：动态自选股池更新（15:00，ASHARE_DYNAMIC_WATCHLIST=1 时启用）
// ---------------------------------------------------------------------------

const flowWatchlist = "watchlist"

// runWatchlistUpdate 生成今日动态池并按生命周期规则落库 watchlist_items
// （新增/续期/过期清理，MANUAL 与 BLACKLIST 不受侵蚀）。
//
// 双链路（灰度迁移）：
//   - ASHARE_POOL_SOURCE=go：优先走 Go 原生 internal/pool（stock-go SDK 直连,
//     全市场流动性初筛 + 行业资金流 + 龙虎榜叠加），结果经 ToGenData 转同构载荷后
//     复用同一套 ApplyWatchlistPool 落库逻辑；Go 链路彻底失败时自动回退 Python 网关。
//   - 默认（python）：调用网关 POST /api/watchlist/generate（原链路, 行为不变）。
//
// 降级语义：两条链路均失败时返回 error 但**绝不清空现有池子**——scanSymbols 会继续使用
// 昨日 ACTIVE 集合（未过期部分），复盘链路零感知。
func (a *app) runWatchlistUpdate(trigger string) error {
	if !a.tryLock(flowWatchlist) {
		return errors.New("watchlist update already running, skip")
	}
	defer a.unlock(flowWatchlist)

	start := time.Now()

	var data *tool.WatchlistGenData
	var err error
	if a.cfg.PoolSource == "go" {
		data, err = a.generatePoolNative(start)
		if err != nil {
			log.Printf("[watchlist] WARN go-native pool failed trigger=%s err=%v -> fallback to python gateway", trigger, err)
			a.saveFallbackRun(start, err) // 阶段 8: FALLBACK 审计行（screening_runs.status=FALLBACK）
			ctx, cancel := context.WithTimeout(context.Background(), a.cfg.WatchTimeout+30*time.Second)
			defer cancel()
			data, err = tool.FetchGeneratedWatchlist(ctx, a.cfg.GatewayURL, a.cfg.Watchlist, a.cfg.WatchTimeout)
			a.lastRunSourceStatuses = nil // FALLBACK 链路无本轮 Go 来源状态 -> 按未知保守处理
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), a.cfg.WatchTimeout+30*time.Second)
		defer cancel()
		data, err = tool.FetchGeneratedWatchlist(ctx, a.cfg.GatewayURL, a.cfg.Watchlist, a.cfg.WatchTimeout)
		a.lastRunSourceStatuses = nil // Python 网关载荷不含来源状态 -> 按未知保守处理
	}
	if err != nil {
		log.Printf("[watchlist] FAILED trigger=%s err=%v (沿用现有池子)", trigger, err)
		return fmt.Errorf("update watchlist: %w", err)
	}

	res, err := tool.ApplyWatchlistPool(a.gdb, data, time.Now(), a.lastRunSourceStatuses)
	if res != nil {
		log.Printf("[watchlist] applied trigger=%s trade_date=%s generated=%d inserted=%d updated=%d skipped_manual=%d inactivated=%d by_source=%v warnings=%d elapsed=%s",
			trigger, res.TradeDate, res.Generated, res.Inserted, res.Updated,
			res.SkippedMan, res.Inactivated, data.BySource, len(res.Warnings), time.Since(start))
		for _, w := range res.Warnings {
			log.Printf("[watchlist] WARN source step: %s", w)
		}
	}
	if err != nil {
		log.Printf("[watchlist] apply partial failure after %d ops: %v", res.Inserted+res.Updated, err)
		return fmt.Errorf("apply watchlist pool: %w", err)
	}
	log.Printf("[watchlist] ===== 池子更新完成 elapsed=%s =====", time.Since(start))
	return nil
}

// generatePoolNative Go 原生动态池链路（ASHARE_POOL_SOURCE=go）：
// stock-go SDK 直连生成 -> ToGenData 转 WatchlistGenData 同构载荷, 供既有
// ApplyWatchlistPool 复用落库。整体超时 ASHARE_POOL_TIMEOUT（默认 6m）。
func (a *app) generatePoolNative(now time.Time) (*tool.WatchlistGenData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.PoolTimeout)
	defer cancel()

	gen := pool.NewGenerator(pool.NewSDKProvider(15*time.Second), a.cfg.PoolCfg)
	// L3/L4 缠论分层: 复用 Python 网关 czsc 引擎（/api/czsc/screen_pool）。
	// 构造失败/未启用时不注入 -> ApplyChanLayer 整池诚实 DATA_INSUFFICIENT, 不删条目。
	if a.cfg.PoolCfg.ChanEnabled {
		cli, err := pool.NewHTTPChanClient(pool.HTTPChanConfig{GatewayBaseURL: a.cfg.GatewayURL})
		if err != nil {
			log.Printf("[watchlist] WARN chan client unavailable (%v): 缠论分层将标记 DATA_INSUFFICIENT", err)
		} else {
			gen.SetChanClient(cli)
		}
	}
	// 阶段 8: run_id 由 cron 注入（时间戳口径, 幂等键——同一次运行重复落库不产生重复行）。
	gen.SetRunID(now.UTC().Format("20060102T150405"))
	// 阶段 2: 把持久层事实位（BLACKLIST / first_seen / last_seen / expire）回填进
	// 聚合器, 使黑名单优先压制 MANUAL、FAILED/STALE 不批量失效等生命周期规则
	// 基于真实 DB 状态而非仅本次运行内存态。
	if gdb := a.gdb; gdb != nil {
		gen.PersistentStatus = func(ctx context.Context) map[string]pool.AggEntry {
			m := make(map[string]pool.AggEntry)
			var items []dbmodel.WatchlistItem
			if err := gdb.WithContext(ctx).Find(&items).Error; err != nil {
				log.Printf("[watchlist] WARN load persistent status failed: %v", err)
				return m
			}
			for _, it := range items {
				agg := pool.AggEntry{Symbol: it.Code, Blacklisted: it.Status == dbmodel.WatchStatusBlacklist}
				if !it.AddedDate.IsZero() {
					agg.FirstSeenAt = it.AddedDate
				}
				if it.LastSeenAt != nil {
					agg.LastSeenAt = *it.LastSeenAt
				}
				if it.ExpireDate != nil {
					exp := *it.ExpireDate
					agg.ExpireDate = &exp
				}
				m[it.Code] = agg
			}
			return m
		}
	}
	res, err := gen.Generate(ctx, a.cfg.Watchlist, now)
	if err != nil {
		return nil, fmt.Errorf("go-native pool generate: %w", err)
	}
	// 阶段 8: 运行审计与快照落库（screening_runs / source_run_status /
	// watchlist_source_hits / daily_stock_metrics）。落库失败不影响池子生成
	// 结果——审计缺失要报警但不能反向阻断交易链路。
	runStatus := deriveRunStatus(res.SourceStatuses)
	if a.gdb != nil {
		runID, serr := persist.SaveScreeningRun(persist.SaveScreeningRunInputs{
			GDB: a.gdb, Result: res, Started: now, Status: runStatus,
		})
		if serr != nil {
			log.Printf("[watchlist] WARN save screening run failed (pool result still applied): %v", serr)
		} else {
			log.Printf("[watchlist] screening run saved run_id=%s status=%s config_hash=%s version=%s pool=%d",
				runID, runStatus, res.ConfigHash, res.StrategyVersion, res.Total)
		}
	}
	a.lastRunSourceStatuses = stringifySourceStatuses(res.SourceStatuses)
	data, err := res.ToGenData()
	if err != nil {
		return nil, err
	}
	log.Printf("[watchlist] go-native generated=%d liquidity_passed=%d by_source=%v warnings=%d",
		res.Total, res.LiquidityPassed, res.BySource, len(res.Warnings))
	return data, nil
}

// deriveRunStatus 从各数据源运行状态推导整次筛选的运行级状态。
//
// 实现的是第 3.3 步冻结契约（表驱动用例见 cmd/runstatgap_test.go
// TestDeriveRunStatusContract）：
//
//	可用来源 = OK / SUCCESS_EMPTY（正常完成且可接受）。
//	PARTIAL / FAILED / STALE 均为降级状态；降级仅在存在可用来源时
//	将运行级判为 PARTIAL_SUCCESS。
//
//	SourceStatuses 为空            -> FAILED   （无法审计，不得视为成功）
//	全部 OK / SUCCESS_EMPTY        -> SUCCESS
//	全部 FAILED                    -> FAILED
//	仅 FAILED/STALE（无可用来源）  -> FAILED   （STALE 单独出现按不可用处理）
//	有可用来源 + 任一 PARTIAL      -> PARTIAL_SUCCESS
//	有可用来源 + 任一 FAILED       -> PARTIAL_SUCCESS
//	有可用来源 + 任一 STALE        -> PARTIAL_SUCCESS
//
// FALLBACK 不在本函数推导——继续由备用流程显式写入（saveFallbackRun），
// 避免把正常状态推导与备用运行审计混为一谈。
func deriveRunStatus(statuses map[string]pool.SourceStatus) string {
	if len(statuses) == 0 {
		return "FAILED" // 空状态集 = 无法审计，不得判成功
	}
	var hasUsable, hasDegraded bool
	for _, st := range statuses {
		switch st {
		case pool.StatusOK, pool.StatusSuccessEmpty:
			hasUsable = true
		case pool.StatusPartial, pool.StatusFailed, pool.StatusStale:
			hasDegraded = true
		default:
			// 未知枚举值不参与推导，也不提升状态。
			hasDegraded = true
		}
	}
	if !hasUsable {
		return "FAILED" // 全部 FAILED / STALE（或未知状态），没有可用来源
	}
	if hasDegraded {
		return "PARTIAL_SUCCESS" // 存在可用来源且至少一个降级状态
	}
	return "SUCCESS"
}

// stringifySourceStatuses 把 pool 层的来源状态枚举转成 ApplyWatchlistPool 需要的
// 字符串口径（map[string]string）。空输入返回 nil —— 调用方以 nil 表达"状态未知"。
func stringifySourceStatuses(statuses map[string]pool.SourceStatus) map[string]string {
	if len(statuses) == 0 {
		return nil
	}
	m := make(map[string]string, len(statuses))
	for k, v := range statuses {
		m[k] = string(v)
	}
	return m
}

// ---------------------------------------------------------------------------
// HTTP 手动触发接口（白天调试用）
// ---------------------------------------------------------------------------

func (a *app) newHTTPServer() *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "time": time.Now().Format(time.RFC3339)})
	})

	mux.HandleFunc("/trigger/review", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "use POST"})
			return
		}
		if err := a.runDailyReview("manual"); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "review finished"})
	})

	mux.HandleFunc("/trigger/simulate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "use POST"})
			return
		}
		if err := a.runSimulation("manual"); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "simulation finished"})
	})

	mux.HandleFunc("/trigger/refresh", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "use POST"})
			return
		}
		if err := a.runCzscRefresh("manual"); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "czsc cache refresh finished"})
	})

	mux.HandleFunc("/trigger/watchlist", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "use POST"})
			return
		}
		if err := a.runWatchlistUpdate("manual"); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": "watchlist update finished"})
	})

	return &http.Server{
		Addr:              a.cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("http: write json failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// main：启动 + cron 注册 + 优雅退出
// ---------------------------------------------------------------------------

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[ashare] ")

	cfg := loadConfig()
	log.Printf("starting: db=%s gateway=%s http=%s watchlist=%v", cfg.DBPath, cfg.GatewayURL, cfg.HTTPAddr, cfg.Watchlist)

	appInstance, err := newApp(cfg)
	if err != nil {
		log.Fatalf("FATAL: bootstrap failed: %v", err)
	}

	// ---- cron 调度（周一到周五）----
	loc := shanghaiLocation()
	c := cron.New(cron.WithLocation(loc), cron.WithChain(cron.SkipIfStillRunning(cron.DefaultLogger), cron.Recover(cron.DefaultLogger)))

	if _, err := c.AddFunc(cfg.CronReview, func() {
		if err := appInstance.runDailyReview("cron"); err != nil {
			log.Printf("cron review job error: %v", err)
		}
	}); err != nil {
		log.Fatalf("FATAL: add review cron (%q): %v", cfg.CronReview, err)
	}
	if _, err := c.AddFunc(cfg.CronSimul, func() {
		if err := appInstance.runSimulation("cron"); err != nil {
			log.Printf("cron simulate job error: %v", err)
		}
	}); err != nil {
		log.Fatalf("FATAL: add simulate cron (%q): %v", cfg.CronSimul, err)
	}
	// 长期方案 C：收盘后 15:10 预计算 czsc 缓存，15:30 复盘的粗筛环节直接命中缓存
	if _, err := c.AddFunc(cfg.CronRefresh, func() {
		if err := appInstance.runCzscRefresh("cron"); err != nil {
			log.Printf("cron czsc refresh job error: %v", err)
		}
	}); err != nil {
		log.Fatalf("FATAL: add czsc refresh cron (%q): %v", cfg.CronRefresh, err)
	}
	// Layer 0.5：15:00 动态自选股池更新（仅动态模式启用；早于 15:10 预热，
	// 保证当日新入池股票当晚即被增量/全量预热，次日复盘零联网成本）
	watchEnabled := cfg.DynWatchOn
	if watchEnabled {
		if _, err := c.AddFunc(cfg.CronWatch, func() {
			if err := appInstance.runWatchlistUpdate("cron"); err != nil {
				log.Printf("cron watchlist job error: %v", err)
			}
		}); err != nil {
			log.Fatalf("FATAL: add watchlist cron (%q): %v", cfg.CronWatch, err)
		}
	}
	c.Start()
	log.Printf("cron registered: review=%q simulate=%q czsc_refresh=%q watchlist=%q(enabled=%v) (Asia/Shanghai)",
		cfg.CronReview, cfg.CronSimul, cfg.CronRefresh, cfg.CronWatch, watchEnabled)

	// ---- HTTP 调试接口 ----
	var httpSrv *http.Server
	httpErr := make(chan error, 1)
	if strings.EqualFold(cfg.HTTPAddr, "off") {
		log.Printf("http trigger server disabled (ASHARE_HTTP_ADDR=off)")
	} else {
		httpSrv = appInstance.newHTTPServer()
		go func() {
			log.Printf("http trigger server listening on %s (POST /trigger/review, POST /trigger/simulate)", cfg.HTTPAddr)
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				httpErr <- err
			}
		}()
	}

	// ---- 优雅退出 ----
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down gracefully ...", sig)
	case err := <-httpErr:
		log.Printf("http server crashed: %v, shutting down ...", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. 停止接收新的 cron 触发并等待正在运行的任务结束
	stopCtx := c.Stop()
	select {
	case <-stopCtx.Done():
		log.Printf("cron stopped (all running jobs finished)")
	case <-shutdownCtx.Done():
		log.Printf("WARN: cron stop timeout, forcing exit path")
	}

	// 2. 关闭 HTTP
	if httpSrv != nil {
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown error: %v", err)
		} else {
			log.Printf("http server closed")
		}
	}

	// 3. 关闭 DB（flush WAL）
	if sqlDB, err := appInstance.gdb.DB(); err == nil {
		if err := sqlDB.Close(); err != nil {
			log.Printf("db close error: %v", err)
		} else {
			log.Printf("database closed")
		}
	}
	log.Printf("bye.")
}

func today() string { return time.Now().Format("2006-01-02") }

// shanghaiLocation 返回 Asia/Shanghai 时区（A 股所有调度时点均以北京时间为准），
// tzdata 缺失时退回固定 +8:00，保证容器环境无 /usr/share/zoneinfo 也能正确对齐。
func shanghaiLocation() *time.Location {
	if loc, err := time.LoadLocation("Asia/Shanghai"); err == nil {
		return loc
	}
	log.Printf("WARN: tzdata unavailable, falling back to fixed UTC+8 for cron scheduling")
	return time.FixedZone("CST", 8*3600)
}

// saveFallbackRun 阶段 8: Go 原生链路整体失败时写一条 status=FALLBACK 的审计行。
// 注意：此时不落任何 source_hits/metrics——FAILED 不是"今天没有命中"，旧池数据
// 原样保留（生命周期红线），仅记录本次回退事实供监控 deprecated 触发率。
func (a *app) saveFallbackRun(start time.Time, cause error) {
	if a.gdb == nil {
		return
	}
	now := time.Now()
	run := dbmodel.ScreeningRun{
		RunID:           now.UTC().Format("20060102T150405") + "F",
		StrategyVersion: a.cfg.PoolCfg.StrategyVersion(),
		ConfigHash:      a.cfg.PoolCfg.Hash(),
		AsOf:            now.Format("2006-01-02"),
		StartedAt:       start,
		CompletedAt:     &now,
		Status:          "FALLBACK",
		ErrorReason:     cause.Error(),
	}
	if err := a.gdb.Create(&run).Error; err != nil {
		log.Printf("[watchlist] WARN save fallback run failed: %v", err)
	}
}
