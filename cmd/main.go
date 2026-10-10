// Command main 是 A 股日线级别 AI 模拟交易系统的进程入口（Phase 6）。
//
// 职责：
//  1. 初始化 SQLite（Phase 2 db.InitDB，幂等，默认账户 100 万）；
//  2. 注册 robfig/cron 两个工作日任务：
//     - 30 15 * * 1-5 : 触发 Phase 5 Eino 复盘 Graph，生成次日 PENDING 计划；
//     - 25 09 * * 1-5 : 调用 Phase 3 MockBroker.SimulateNextDayOpen 撮合昨日计划；
//  3. 提供调试用 HTTP 手动触发接口（/trigger/review、/trigger/simulate、/healthz）；
//  4. 监听 SIGINT/SIGTERM 优雅退出（停 cron + 关 HTTP + 关 DB）。
//
// 环境变量：
//
//	ASHARE_DB_PATH      SQLite 文件路径，默认 ./data/trading.db
//	ASHARE_GATEWAY_URL  Python 数据网关地址，默认 http://127.0.0.1:8000
//	ASHARE_HTTP_ADDR    手动触发服务监听地址，默认 :8080（设为 "off" 可禁用）
//	ASHARE_WATCHLIST    候选股票池，逗号分隔，如 "000001,600519"，默认空（仅复盘持仓股）
//	ASHARE_LLM_BASE_URL / ASHARE_MODEL / ASHARE_API_KEY
//	                    LLM 接入配置。MVP 未强依赖 eino-ext provider：
//	                    未配置时系统以「降级模式」运行 —— 复盘任务将跳过 LLM 决策
//	                    并记录告警，撮合/查询等其他功能完全可用。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"gorm.io/gorm"

	"ashare-quant/internal/broker"
	"ashare-quant/internal/db"
	dbmodel "ashare-quant/internal/model"
	datasync "ashare-quant/internal/sync"
)

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

type appConfig struct {
	DBPath     string
	GatewayURL string
	HTTPAddr   string
	Watchlist  []string
	CronReview string // 日终复盘 cron 表达式
	CronSimul  string // 次日撮合 cron 表达式
}

func loadConfig() appConfig {
	cwd, _ := os.Getwd()
	cfg := appConfig{
		DBPath:     envOr("ASHARE_DB_PATH", filepath.Join(cwd, "data", "trading.db")),
		GatewayURL: envOr("ASHARE_GATEWAY_URL", "http://127.0.0.1:8000"),
		HTTPAddr:   envOr("ASHARE_HTTP_ADDR", ":8080"),
		CronReview: "30 15 * * 1-5",
		CronSimul:  "25 9 * * 1-5",
	}
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

// syncSymbols 返回本轮需要同步落库的股票集合：
// 自选股（ASHARE_WATCHLIST）∪ 当前持仓股。持仓股必须同步——次日撮合卖出时
// Broker 依赖 daily_klines 的涨跌停价判定一字板，缺数据会导致订单被拒。
func (a *app) syncSymbols() []string {
	seen := make(map[string]bool)
	var out []string
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, s := range a.cfg.Watchlist {
		add(s)
	}
	var positions []dbmodel.Position
	if err := a.gdb.Where("quantity > 0").Find(&positions).Error; err != nil {
		log.Printf("WARN: load positions for sync failed: %v", err)
	}
	for _, p := range positions {
		add(p.Code)
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
	c.Start()
	log.Printf("cron registered: review=%q simulate=%q (Asia/Shanghai)", cfg.CronReview, cfg.CronSimul)

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
