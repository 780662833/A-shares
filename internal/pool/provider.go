// Package pool 实现动态自选股池（Layer 0.5）的 Go 原生生成器。
//
// 架构迁移说明：候选池生成已从 Python 网关上移至 Go 层，直接使用
// github.com/ceheng-io/stock-go SDK（无需 Token，公开行情源）完成——
//   - 全市场流动性初筛（Quotes.AllCN 批量行情：价格 / 20 日均成交额 / 市值 / ST 剔除）；
//   - 行业资金流叠加（FundFlow.SectorRank + Board.IndustryConstituents）；
//   - 龙虎榜机构净买入叠加（DragonTiger.Institution）；
//   - ETF 动量（CodesFund + Quotes.Fund 快照粗筛 + Kline.CN 精算 20 日涨幅）。
//
// Python 端仅保留 czsc 形态确认（/api/czsc/screen_pool、/api/czsc/refresh）。
//
// 可测试性：所有 SDK 调用收敛到 Provider 接口，单测注入 fakeProvider
// 即可离线全链路验证，不触网。
package pool

import (
	"context"
	"time"

	stock "github.com/ceheng-io/stock-go"
)

// ---------------------------------------------------------------------------
// 领域 DTO（与 stock-go types 解耦，字段口径对齐原 Python watchlist_generator）
// ---------------------------------------------------------------------------

// StockQuote 全市场单只股票的实时快照（流动性初筛输入）。
type StockQuote struct {
	Code           string  // 6 位数字代码
	Name           string  // 证券名称
	Price          float64 // 最新价（元）
	AvgAmount20D   float64 // 近 20 个交易日日均成交额（元, 由 K 线补齐; 0 表示未取到）
	TotalMarketCap float64 // 总市值（元, 0 表示缺失）
}

// SectorFlow 行业板块当日资金流排行一行。
type SectorFlow struct {
	Code          string  // 板块代码, 如 BK0438
	Name          string  // 板块名称
	MainNetInflow float64 // 今日主力净流入（元）
}

// ConstituentStock 板块成分股一行。
//
// 注意：stock-go 的 BoardConstituent 不携带市值字段（无 TotalMarketCap），
// 因此成分股层面**不做**市值过滤/排序——流动性门槛统一由全市场初筛集合
// liquidSet 叠加承担，龙头排序回退为"最新价 Top K"口径（见 generator.go）。
type ConstituentStock struct {
	Code  string
	Name  string
	Price float64
}

// InstitutionBuy 龙虎榜机构席位净买入一行。
type InstitutionBuy struct {
	Code         string
	Name         string
	Date         string
	OrgNetAmount float64 // 机构净买入额（元）
}

// FundSnapshot ETF/基金快照一行。
type FundSnapshot struct {
	Code   string
	Name   string
	Price  float64
	Amount float64 // 今日成交额（元, 0 表示数据源未提供）
}

// DailyClose 一只 ETF 的日线收盘序列（按日期升序）。
type DailyClose struct {
	Dates  []string
	Closes []float64
}

// ---------------------------------------------------------------------------
// Provider 接口
// ---------------------------------------------------------------------------

// Provider 抽象候选池生成所需的全部上游数据能力。
// 生产实现为 SDKProvider（stock-go），测试实现见 provider_fake_test.go。
type Provider interface {
	// AllStocks 返回全市场 A 股快照（含沪深京），用于流动性初筛。
	AllStocks(ctx context.Context) ([]StockQuote, error)
	// SectorFundFlowRank 返回行业板块资金流排行（今日口径），按净流入降序。
	SectorFundFlowRank(ctx context.Context) ([]SectorFlow, error)
	// IndustryConstituents 返回指定行业板块的成分股列表（boardCode 支持 BKxxxx 或名称）。
	IndustryConstituents(ctx context.Context, boardCode string) ([]ConstituentStock, error)
	// DragonTigerInstitution 返回 [start,end] 窗口内龙虎榜机构席位净买入统计。
	DragonTigerInstitution(ctx context.Context, start, end time.Time) ([]InstitutionBuy, error)
	// FundList 返回全市场公募基金（含场内 ETF）代码清单。
	FundList(ctx context.Context) ([]string, error)
	// FundBatchQuote 批量获取基金快照（名称/净值）。
	FundBatchQuote(ctx context.Context, codes []string) ([]FundSnapshot, error)
	// ETFCloseSeries 返回一只 ETF 最近 lookback 个交易日的收盘价序列（前复权）。
	ETFCloseSeries(ctx context.Context, code string, lookback int, end time.Time) (*DailyClose, error)
}

// ---------------------------------------------------------------------------
// SDKProvider: stock-go 生产实现
// ---------------------------------------------------------------------------

// SDKProvider 基于 stock-go 根入口 *stock.Client 的 Provider 实现。零值不可用。
type SDKProvider struct {
	sdk         *stock.Client
	batchSize   int
	concurrency int
	klineWindow int // 补算 20 日均成交额时拉取的 K 线条数
}

// NewSDKProvider 创建生产 Provider。timeout<=0 时取默认 15s。
func NewSDKProvider(timeout time.Duration) *SDKProvider {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	sdk := stock.New(
		stock.WithTimeout(timeout),
		stock.WithRetry(stock.RetryOptions{MaxRetries: 2}),
	)
	return &SDKProvider{sdk: sdk, batchSize: 200, concurrency: 4, klineWindow: 30}
}

var _ Provider = (*SDKProvider)(nil)

func f64(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

// AllStocks 全市场行情快照 -> StockQuote 列表。
//
// 注意：腾讯批量行情只有"当日成交额"，无 20 日均值；为控制网络开销，
// 这里先用当日成交额作为初筛代理（收盘后当日值≈近端均值），再由
// liquidityFilter 对通过第一道闸的股票按需精算（见 generator.go）。
func (p *SDKProvider) AllStocks(ctx context.Context) ([]StockQuote, error) {
	quotes, err := p.sdk.Quotes.AllCN(ctx, stock.CodeListOptions{}, stock.BatchOptions{
		BatchSize:   p.batchSize,
		Concurrency: p.concurrency,
	})
	if err != nil {
		return nil, err
	}
	out := make([]StockQuote, 0, len(quotes))
	for _, q := range quotes {
		out = append(out, StockQuote{
			Code:           q.Code,
			Name:           q.Name,
			Price:          q.Price,
			AvgAmount20D:   q.Amount, // 当日成交额做初筛代理值
			TotalMarketCap: f64(q.TotalMarketCap),
		})
	}
	return out, nil
}

// AvgAmountByKline 用东财日 K 精算单只股票近 window 个交易日日均成交额。
// 失败返回 err, 由调用方决定回退策略（保留代理值）。
func (p *SDKProvider) AvgAmountByKline(ctx context.Context, code string, window int, end time.Time) (float64, error) {
	start := end.AddDate(0, 0, int(float64(window)*1.8)+10)
	rows, err := p.sdk.Kline.CN(ctx, code, stock.HistoryKlineOptions{
		Period:    stock.KlinePeriodDaily,
		Adjust:    stock.AdjustNone,
		StartDate: start.Format("2006-01-02"),
		EndDate:   end.Format("2006-01-02"),
	})
	if err != nil {
		return 0, err
	}
	var sum float64
	var n int
	for _, r := range rows[len(rows)-minInt(len(rows), window):] {
		if a := f64(r.Amount); a > 0 {
			sum += a
			n++
		}
	}
	if n == 0 {
		return 0, context.DeadlineExceeded // 语义: 无有效样本
	}
	return sum / float64(n), nil
}

// SectorFundFlowRank 行业资金流今日排行（东财 clist, 已按净流入降序返回）。
func (p *SDKProvider) SectorFundFlowRank(ctx context.Context) ([]SectorFlow, error) {
	rows, err := p.sdk.FundFlow.SectorRank(ctx, stock.FundFlowRankOptions{
		Indicator:  stock.FundFlowRankToday,
		SectorType: stock.FundFlowSectorIndustry,
	})
	if err != nil {
		return nil, err
	}
	out := make([]SectorFlow, 0, len(rows))
	for _, r := range rows {
		out = append(out, SectorFlow{Code: r.Code, Name: r.Name, MainNetInflow: f64(r.MainNetInflow)})
	}
	return out, nil
}

// IndustryConstituents 行业板块成分股。
func (p *SDKProvider) IndustryConstituents(ctx context.Context, boardCode string) ([]ConstituentStock, error) {
	rows, err := p.sdk.Board.IndustryConstituents(ctx, boardCode)
	if err != nil {
		return nil, err
	}
	out := make([]ConstituentStock, 0, len(rows))
	for _, r := range rows {
		out = append(out, ConstituentStock{Code: r.Code, Name: r.Name, Price: f64(r.Price)})
	}
	return out, nil
}

// DragonTigerInstitution 龙虎榜机构席位净买入统计。
func (p *SDKProvider) DragonTigerInstitution(ctx context.Context, start, end time.Time) ([]InstitutionBuy, error) {
	rows, err := p.sdk.DragonTiger.Institution(ctx, stock.DragonTigerDateOptions{
		StartDate: start.Format("2006-01-02"),
		EndDate:   end.Format("2006-01-02"),
	})
	if err != nil {
		return nil, err
	}
	out := make([]InstitutionBuy, 0, len(rows))
	for _, r := range rows {
		out = append(out, InstitutionBuy{Code: r.Code, Name: r.Name, Date: r.Date, OrgNetAmount: f64(r.OrgNetAmount)})
	}
	return out, nil
}

// FundList 全市场基金代码清单。
func (p *SDKProvider) FundList(ctx context.Context) ([]string, error) {
	return p.sdk.Data.CodesFund(ctx)
}

// FundBatchQuote 批量基金快照（分片防止上游 URL 过长）。
func (p *SDKProvider) FundBatchQuote(ctx context.Context, codes []string) ([]FundSnapshot, error) {
	var out []FundSnapshot
	for i := 0; i < len(codes); i += p.batchSize {
		end := i + p.batchSize
		if end > len(codes) {
			end = len(codes)
		}
		chunk := codes[i:end]
		quotes, err := p.sdk.Quotes.Fund(ctx, chunk)
		if err != nil {
			return nil, err
		}
		for _, q := range quotes {
			out = append(out, FundSnapshot{Code: q.Code, Name: q.Name, Price: q.NAV})
		}
	}
	return out, nil
}

// ETFCloseSeries ETF 日线收盘价（前复权）。
func (p *SDKProvider) ETFCloseSeries(ctx context.Context, code string, lookback int, end time.Time) (*DailyClose, error) {
	start := end.AddDate(0, 0, int(float64(lookback)*1.8)+10)
	rows, err := p.sdk.Kline.CN(ctx, code, stock.HistoryKlineOptions{
		Period:    stock.KlinePeriodDaily,
		Adjust:    stock.AdjustQFQ,
		StartDate: start.Format("2006-01-02"),
		EndDate:   end.Format("2006-01-02"),
	})
	if err != nil {
		return nil, err
	}
	dc := &DailyClose{}
	for _, r := range rows {
		c := f64(r.Close)
		if c <= 0 {
			continue
		}
		dc.Dates = append(dc.Dates, r.Date)
		dc.Closes = append(dc.Closes, c)
	}
	if len(dc.Closes) > lookback {
		dc.Dates = dc.Dates[len(dc.Dates)-lookback:]
		dc.Closes = dc.Closes[len(dc.Closes)-lookback:]
	}
	return dc, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
