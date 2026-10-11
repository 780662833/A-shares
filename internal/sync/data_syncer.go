// Package sync 提供行情数据同步能力（Phase 5.5）。
//
// 背景（致命断层修复）：MarketDataTool 拉取的 K 线只存在于 LLM 上下文，
// 从未写入 DailyKline 表；而 Phase 3 撮合引擎在次日开盘时必须从 DB 读取
// 当日 K 线的涨停价/跌停价来判定一字板。本包在复盘开始前把网关数据落库，
// 打通「数据获取 → 落库 → 次日撮合」链路。
package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	dbmodel "ashare-quant/internal/model"
)

// ---------------------------------------------------------------------------
// 配置与结果类型
// ---------------------------------------------------------------------------

// Config 数据同步器配置。
type Config struct {
	// GatewayBaseURL Python 数据网关地址，如 "http://127.0.0.1:8000"。必填。
	GatewayBaseURL string
	// HTTPTimeout 单次网关请求超时。<=0 时默认 15s。
	HTTPTimeout time.Duration
	// LookbackDays 每只股票向回多取多少个自然日的数据（保证能拿到
	// 「前一交易日收盘价」来计算涨跌停价）。<=0 时默认 15。
	LookbackDays int
	// MaxConcurrency 并发拉取的最大 goroutine 数。<=0 时默认 4。
	MaxConcurrency int
}

// Failure 记录单只股票的同步失败原因。
type Failure struct {
	Symbol string
	Err    string
}

// SyncResult 单轮同步的汇总结果。
type SyncResult struct {
	Date       string    // 目标交易日 "YYYY-MM-DD"
	Total      int       // 计划同步的股票数（含格式非法项）
	Succeeded  int       // 成功落库的股票数
	Failed     int       // 失败被跳过的股票数
	RowsUpsert int       // 累计写入/更新的 K 线条数
	Failures   []Failure // 每只失败股票的错误信息
	StartedAt  time.Time
	FinishedAt time.Time
}

// Summary 返回适合打日志的一行摘要。
func (r SyncResult) Summary() string {
	return fmt.Sprintf("date=%s total=%d ok=%d fail=%d rows=%d elapsed=%s",
		r.Date, r.Total, r.Succeeded, r.Failed, r.RowsUpsert,
		r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond))
}

// DataSyncer 通过 HTTP 调用 Python 网关拉取日 K 线并 Upsert 到 daily_klines 表。
type DataSyncer struct {
	cfg    Config
	client *http.Client
	gdb    *gorm.DB
	// writeMu 串行化批量写库（SQLite 单写者友好，避免锁竞争 busy）。
	writeMu sync.Mutex
}

// New 构造同步器；配置非法时返回 error。gdb 必须已由 db.InitDB 完成迁移。
func New(gdb *gorm.DB, cfg Config) (*DataSyncer, error) {
	if gdb == nil {
		return nil, fmt.Errorf("sync: gorm.DB is required")
	}
	base := strings.TrimRight(strings.TrimSpace(cfg.GatewayBaseURL), "/")
	if base == "" {
		return nil, fmt.Errorf("sync: GatewayBaseURL is required")
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		return nil, fmt.Errorf("sync: invalid GatewayBaseURL %q", cfg.GatewayBaseURL)
	}
	cfg.GatewayBaseURL = base
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 15 * time.Second
	}
	if cfg.LookbackDays <= 0 {
		cfg.LookbackDays = 15
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}
	return &DataSyncer{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.HTTPTimeout},
		gdb:    gdb,
	}, nil
}

// ---------------------------------------------------------------------------
// 网关响应解析结构（对齐 data_gateway /api/daily_kline 信封）
// ---------------------------------------------------------------------------

type klineEnvelope struct {
	Code int        `json:"code"`
	Data []klineRow `json:"data"`
}

type klineRow struct {
	Date   string  `json:"date"`
	Open   float64 `json:"open"`
	Close  float64 `json:"close"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Volume float64 `json:"volume"` // 网关口径：手
	Amount float64 `json:"amount"` // 元
}

var symbolRe = regexp.MustCompile(`^\d{6}$`)

// ---------------------------------------------------------------------------
// 核心方法
// ---------------------------------------------------------------------------

// SyncDailyKlines 同步指定股票列表在 date（含）之前若干交易日的日 K 线，
// 补算涨跌停价后按 (code,date) 唯一键批量 Upsert 到 daily_klines 表。
//
//   - symbols：6 位代码列表；自动去重，重复/空串忽略，格式非法计入 Failed。
//   - date：目标交易日，接受 "YYYYMMDD" 或 "YYYY-MM-DD"；传空则取今天。
//
// 契约：任何单只股票的失败（网络、HTTP 500、解析、无数据、DB 写失败）
// 只会被记录进 SyncResult.Failures 并跳过，绝不影响其他股票；函数仅在
// 系统性错误（日期非法、ctx 取消）时返回 error。
func (s *DataSyncer) SyncDailyKlines(ctx context.Context, symbols []string, date string) (*SyncResult, error) {
	res := &SyncResult{StartedAt: time.Now()}

	target, err := normalizeTradeDate(date)
	if err != nil {
		return nil, fmt.Errorf("sync: invalid date %q: %w", date, err)
	}
	res.Date = target

	dedup := make(map[string]bool, len(symbols))
	var list []string
	for _, sym := range symbols {
		sym = strings.TrimSpace(sym)
		if sym == "" || dedup[sym] {
			continue
		}
		if !symbolRe.MatchString(sym) {
			res.Total++
			res.Failed++
			res.Failures = append(res.Failures, Failure{Symbol: sym, Err: "invalid symbol format, want 6 digits"})
			log.Printf("data_sync: SKIP symbol=%q err=invalid symbol format", sym)
			continue
		}
		dedup[sym] = true
		res.Total++
		list = append(list, sym)
	}
	if len(list) == 0 {
		res.FinishedAt = time.Now()
		log.Printf("data_sync: nothing to sync (%s)", res.Summary())
		return res, nil
	}

	startDate := shiftDateCompact(target, -s.cfg.LookbackDays)
	endDate := toCompact(target)

	// ---- 阶段 1：并发拉取 + 转换（纯 IO/CPU，不碰 DB），信号量限流 ----
	type job struct {
		sym    string
		bars   []dbmodel.DailyKline
		errMsg string
	}
	jobs := make(chan job, len(list))
	sem := make(chan struct{}, s.cfg.MaxConcurrency)

	var wg sync.WaitGroup
	for _, sym := range list {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				jobs <- job{sym: sym, errMsg: "canceled before fetch"}
				return
			}

			rows, err := s.fetchKlines(ctx, sym, startDate, endDate)
			if err != nil {
				jobs <- job{sym: sym, errMsg: err.Error()}
				return
			}
			bars, err := buildDailyKlines(sym, rows, target)
			if err != nil {
				jobs <- job{sym: sym, errMsg: err.Error()}
				return
			}
			jobs <- job{sym: sym, bars: bars}
		}(sym)
	}
	wg.Wait()
	close(jobs)

	// ---- 阶段 2：串行批量 Upsert（SQLite 单写者）----
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	for j := range jobs {
		if j.errMsg != "" {
			res.Failed++
			res.Failures = append(res.Failures, Failure{Symbol: j.sym, Err: j.errMsg})
			log.Printf("data_sync: SKIP symbol=%s err=%s", j.sym, j.errMsg)
			continue
		}
		if err := s.upsertKlines(ctx, j.bars); err != nil {
			res.Failed++
			res.Failures = append(res.Failures, Failure{Symbol: j.sym, Err: fmt.Sprintf("db upsert: %v", err)})
			log.Printf("data_sync: SKIP symbol=%s db upsert failed: %v", j.sym, err)
			continue
		}
		res.Succeeded++
		res.RowsUpsert += len(j.bars)
		log.Printf("data_sync: OK symbol=%s rows=%d latest=%s", j.sym, len(j.bars), j.bars[len(j.bars)-1].Date)
	}
	res.FinishedAt = time.Now()
	log.Printf("data_sync: DONE %s", res.Summary())
	return res, nil
}

// fetchKlines 调网关 /api/daily_kline 并解析出原始行（保持网关返回顺序）。
func (s *DataSyncer) fetchKlines(ctx context.Context, symbol, start, end string) ([]klineRow, error) {
	url := fmt.Sprintf("%s/api/daily_kline?symbol=%s&start_date=%s&end_date=%s",
		s.cfg.GatewayBaseURL, symbol, start, end)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gateway unreachable: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20)) // 8MB 上限防御异常大响应
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway HTTP %d: %.200s", resp.StatusCode, string(body))
	}

	var env klineEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("decode gateway payload: %w", err)
	}
	if len(env.Data) == 0 {
		return nil, fmt.Errorf("no kline data returned (range %s~%s)", start, end)
	}
	return env.Data, nil
}

// ---------------------------------------------------------------------------
// 业务转换：补算涨跌停价 + 组装模型
// ---------------------------------------------------------------------------

// buildDailyKlines 将网关行转换为 DailyKline 模型。
//
// 关键逻辑：AKShare 前复权 K 线接口不返回涨跌停价字段，按交易所规则由
// 「前一交易日收盘价 × (1±板块比例)，四舍五入到分」推算（CalcLimitPrices）：
//   - 第 i 根 K 线的昨收 = 第 i-1 根的 close（同一复权口径下序列自洽）；
//   - 首根 K 线缺少昨收参照，无法可靠计算 → 跳过不落库（宁缺毋错）；
//   - 仅保留 date <= target 的行；输出按日期升序。
func buildDailyKlines(symbol string, rows []klineRow, target string) ([]dbmodel.DailyKline, error) {
	// 按日期升序稳定排序（不信任网关顺序）
	sorted := make([]klineRow, 0, len(rows))
	dates := make([]string, 0, len(rows))
	for _, r := range rows {
		d := normalizeBarDate(r.Date)
		if d == "" {
			continue
		}
		sorted = append(sorted, r)
		dates = append(dates, d)
	}
	for i := 1; i < len(sorted); i++ { // 插入排序，N≈lookback 足够
		for j := i; j > 0 && dates[j] < dates[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
			dates[j], dates[j-1] = dates[j-1], dates[j]
		}
	}

	var bars []dbmodel.DailyKline
	for i := 1; i < len(sorted); i++ {
		r := sorted[i]
		d := dates[i]
		if d > target {
			continue
		}
		prevClose := sorted[i-1].Close
		lu, ld, err := dbmodel.CalcLimitPrices(prevClose, symbol)
		if err != nil {
			return nil, fmt.Errorf("calc limit prices at %s: %w", d, err)
		}
		if badPrice(r.Open) || badPrice(r.High) || badPrice(r.Low) || badPrice(r.Close) {
			return nil, fmt.Errorf("invalid OHLC in gateway payload at %s", d)
		}
		vol := int64(math.Round(r.Volume * 100)) // 手 → 股，对齐模型注释口径
		bars = append(bars, dbmodel.DailyKline{
			Code:           symbol,
			Date:           d,
			Open:           r.Open,
			High:           math.Max(r.High, r.Low), // 防御上游偶发 high<low 脏数据
			Low:            math.Min(r.High, r.Low),
			Close:          r.Close,
			Volume:         vol,
			Amount:         r.Amount,
			LimitUpPrice:   lu,
			LimitDownPrice: ld,
		})
	}
	if len(bars) == 0 {
		return nil, fmt.Errorf("all %d rows unusable (missing prev close or out of date range)", len(rows))
	}
	return bars, nil
}

// upsertKlines 按唯一键 (code,date) Upsert：存在则更新行情与涨跌停字段，不存在则插入。
func (s *DataSyncer) upsertKlines(ctx context.Context, bars []dbmodel.DailyKline) error {
	return s.gdb.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "code"}, {Name: "date"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"open", "high", "low", "close", "volume", "amount",
				"limit_up_price", "limit_down_price", "updated_at",
			}),
		}).
		CreateInBatches(&bars, 200).Error
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

func badPrice(v float64) bool {
	return v <= 0 || math.IsNaN(v) || math.IsInf(v, 0)
}

// normalizeTradeDate 接受 YYYYMMDD / YYYY-MM-DD / 空(今天)，统一为 "YYYY-MM-DD"。
func normalizeTradeDate(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Now().Format("2006-01-02"), nil
	}
	t, err := time.Parse("20060102", strings.ReplaceAll(s, "-", ""))
	if err != nil {
		return "", err
	}
	return t.Format("2006-01-02"), nil
}

func toCompact(dashDate string) string { return strings.ReplaceAll(dashDate, "-", "") }

func shiftDateCompact(dashDate string, days int) string {
	t, err := time.Parse("2006-01-02", dashDate)
	if err != nil {
		return dashDate
	}
	return t.AddDate(0, 0, days).Format("20060102")
}

// normalizeBarDate 网关日期可能为 "2026-10-09" 或 "20261009"，统一为 "YYYY-MM-DD"。
func normalizeBarDate(s string) string {
	s = strings.TrimSpace(s)
	if len(s) == 8 {
		if _, err := strconv.Atoi(s); err == nil {
			if t, err := time.Parse("20060102", s); err == nil {
				return t.Format("2006-01-02")
			}
		}
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.Format("2006-01-02")
	}
	return ""
}
