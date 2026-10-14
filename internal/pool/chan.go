// chan.go —— L3/L4 缠论结构识别与买点分层（P0~P4）。
//
// 架构定位（六层候选池）：
//   - L1 宽口径发现、L2 趋势分类、流动性闸门等 Go 原生链路照常执行；
//   - L3 缠论结构识别 + L4 买点分层 **复用 Python 网关 czsc 引擎**
//     （data_gateway/czsc_engine.py 的结构驱动信号），Go 端不重复造轮子。
//     本文件只做"调用 + 状态映射 + 审计统计"，不实现任何笔/线段/中枢算法。
//
// 分层语义（业务状态定义, 与 czsc 信号一一对应）：
//   - P0 STRUCTURE_WATCH    : 尚无可确认买点(或仅卖点/观察信号), 继续跟踪, 不代表买入;
//   - P1 BUY1_WATCH         : 出现 1B(疑似底背驰)候选, 背驰条件仍需确认 -> 高风险观察队列;
//   - P2 BUY2_CONFIRMED     : 2B 确认(一买后首次回调不创新低) -> 重点候选, 仍需交易风控;
//   - P3 BUY3_CONFIRMED     : 3B 确认(向上离开中枢后回试不入枢) -> 重点候选;
//   - P4 STRUCTURE_INVALID  : 关键结构被破坏(出现 1S/2S/3S 卖点)或原买点不再成立 ->
//                             退出买点队列, 记录保留(历史可审计);
//   - DATA_INSUFFICIENT     : czsc 无法给出结构(行情缺失/K线不足/网关失败) -> 诚实标记,
//                             绝不伪造通过, 也不因此把股票从研究池删除。
//
// 弱依赖原则（与全池一致）：
//   - 网关不可达 / 单票计算失败 -> 该票 DATA_INSUFFICIENT, 整池保持原样;
//   - 分层结果只决定 candidate_tier 与确认队列归属, 不翻转 eligibility/tradability
//     契约状态（那些仍由 L0 资格闸门与 L5 风控独立判定）。
package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// 契约类型
// ---------------------------------------------------------------------------

// ChanStatus L3/L4 缠论结构状态枚举（禁止裸字符串比较, 对齐 contract.go 规范）。
type ChanStatus string

const (
	ChanWatch       ChanStatus = "STRUCTURE_WATCH"   // P0: 无确认买点, 观察跟踪
	ChanBuy1Watch   ChanStatus = "BUY1_WATCH"        // P1: 一买候选(疑似底背驰), 待确认
	ChanBuy2Confirm ChanStatus = "BUY2_CONFIRMED"    // P2: 二买确认
	ChanBuy3Confirm ChanStatus = "BUY3_CONFIRMED"    // P3: 三买确认
	ChanInvalid     ChanStatus = "STRUCTURE_INVALID" // P4: 结构失效(卖点出现/买点被破坏)
	ChanNoData      ChanStatus = "DATA_INSUFFICIENT" // 数据/结构不足, 诚实标记不伪造
)

// ChanTier 买点分层号（P0~P4; ChanNoData 归入 P0 观察但单独标注数据状态）。
func (s ChanStatus) ChanTier() string {
	switch s {
	case ChanBuy1Watch:
		return "P1"
	case ChanBuy2Confirm:
		return "P2"
	case ChanBuy3Confirm:
		return "P3"
	case ChanInvalid:
		return "P4"
	case ChanWatch:
		return "P0"
	default:
		return "P0"
	}
}

// IsConfirmedBuy P2/P3 才是"已确认买点"队列成员；P1 是高风险观察，不算确认。
func (s ChanStatus) IsConfirmedBuy() bool {
	return s == ChanBuy2Confirm || s == ChanBuy3Confirm
}

// BlocksTradability L4 分层对交易资格的影响（统一口径, ComputeTradability 消费）：
//   - P4 STRUCTURE_INVALID -> true（结构被破坏, 禁止新订单, 记录保留）;
//   - P0/P1/P2/P3/DATA_INSUFFICIENT -> false（研究/观察资格不受影响;
//     "无买点"不等于"可交易", 放行与否仍由 L0/L2/L5 各闸门独立决定）。
func (s ChanStatus) BlocksTradability() bool { return s == ChanInvalid }

// ChanLayerClient 抽象对 Python 网关 /api/czsc/screen_pool 的一次批量调用。
// 生产实现为 HTTPChanClient；单测注入 fake 即可离线验证映射逻辑。
type ChanLayerClient interface {
	// ScreenBatch 对 symbols 批量执行 czsc 结构分析, 返回 symbol->命中记录。
	// 返回 error 表示整批失败（网络/网关 5xx）, 调用方按 DATA_INSUFFICIENT 退化。
	ScreenBatch(ctx context.Context, symbols []string, cacheOnly bool) (map[string]ChanHit, error)
}

// ChanSignal czsc 一个买/卖点信号（字段与 data_gateway/czsc_engine.py 输出对齐）。
type ChanSignal struct {
	Type          string  `json:"type"`      // 1B/2B/3B 或 1S/2S/3S
	NameCn        string  `json:"name_cn"`   // 中文名
	Price         float64 `json:"price"`     // 信号价（回抽笔极值）
	Dt            string  `json:"dt"`        // 信号确认日 YYYY-MM-DD（结构确认时间）
	Reason        string  `json:"reason"`    // 人类可读理由（审计）
	PullbackLow   float64 `json:"pullback_low"`
	StopLoss      float64 `json:"stop_loss"`
	Invalidation  float64 `json:"invalidation"`
	EffectiveStop float64 `json:"effective_stop"`
}

// ChanHit 网关 screen_pool 对单只证券的返回（含未命中路径所需的信息）。
type ChanHit struct {
	Symbol         string       `json:"symbol"`
	SignalType     string       `json:"signal_type"` // 3B/2B/1B/Observation/3S...
	BuyPoints      []ChanSignal `json:"buy_points"`
	SellPoints     []ChanSignal `json:"sell_points"`
	ZsCount        int          `json:"zs_count"`
	BiCount        int          `json:"bi_count"`
	TrendDirection string       `json:"trend_direction"`
	Summary        string       `json:"summary"`
	BlockedReasons []string     `json:"blocked_reasons,omitempty"`
}

// ---------------------------------------------------------------------------
// HTTPChanClient: Python 网关生产实现
// ---------------------------------------------------------------------------

// HTTPChanConfig 构造参数。
type HTTPChanConfig struct {
	GatewayBaseURL string        // 如 http://127.0.0.1:8000
	HTTPTimeout    time.Duration // 批量扫描超时, <=0 取默认 5m
	BatchSize      int           // 单次 POST 最大代码数（网关 _SCREEN_MAX_SYMBOLS 保护）, <=0 取 100
}

// HTTPChanClient 经 /api/czsc/screen_pool 复用 czsc 引擎的客户端。零值不可用。
type HTTPChanClient struct {
	cfg    HTTPChanConfig
	client *http.Client
}

// NewHTTPChanClient 构造客户端（校验 URL 合法性, 口径与 tool 包一致）。
func NewHTTPChanClient(cfg HTTPChanConfig) (*HTTPChanClient, error) {
	cfg.GatewayBaseURL = strings.TrimRight(strings.TrimSpace(cfg.GatewayBaseURL), "/")
	if cfg.GatewayBaseURL == "" {
		return nil, fmt.Errorf("chan layer: GatewayBaseURL is required")
	}
	if !strings.HasPrefix(cfg.GatewayBaseURL, "http://") && !strings.HasPrefix(cfg.GatewayBaseURL, "https://") {
		return nil, fmt.Errorf("chan layer: invalid GatewayBaseURL %q, expect http(s)://...", cfg.GatewayBaseURL)
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 5 * time.Minute
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > 100 {
		cfg.BatchSize = 100 // 网关单次上限 100（AKShare 限流保护）
	}
	return &HTTPChanClient{cfg: cfg, client: &http.Client{Timeout: cfg.HTTPTimeout}}, nil
}

var _ ChanLayerClient = (*HTTPChanClient)(nil)

type chanPoolEnvelope struct {
	Code    int `json:"code"`
	Data    *struct {
		Pool    []ChanHit `json:"pool"`
		Blocked []ChanHit `json:"blocked"`
		Errors  []struct {
			Symbol string `json:"symbol"`
			Error  string `json:"error"`
		} `json:"errors"`
	} `json:"data"`
	Message string `json:"message"`
}

// ScreenBatch 分批 POST /api/czsc/screen_pool（cache_only=true: 只读每日 refresh
// 落盘的 czsc 缓存, 零联网毫秒级返回; 某批未命中缓存导致空结果时自动降级回源一批）。
func (c *HTTPChanClient) ScreenBatch(ctx context.Context, symbols []string, cacheOnly bool) (map[string]ChanHit, error) {
	out := make(map[string]ChanHit, len(symbols))
	for start := 0; start < len(symbols); start += c.cfg.BatchSize {
		end := start + c.cfg.BatchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[start:end]
		hits, err := c.postOnce(ctx, batch, cacheOnly)
		if err != nil {
			return nil, err
		}
		// cache_only 模式下整批空命中多半是当日 refresh 未跑/缓存过期, 回源补一次。
		if cacheOnly && len(hits) == 0 && len(batch) > 0 {
			log.Printf("[chan] WARN cache_only batch empty (%d symbols), retry with live fallback", len(batch))
			hits, err = c.postOnce(ctx, batch, false)
			if err != nil {
				return nil, err
			}
		}
		for _, h := range hits {
			out[h.Symbol] = h
		}
	}
	return out, nil
}

func (c *HTTPChanClient) postOnce(ctx context.Context, batch []string, cacheOnly bool) ([]ChanHit, error) {
	body := map[string]any{
		"symbols":    batch,
		"period":     "daily",
		"cache_only": cacheOnly,
		// 排雷/负期望属于 L5 独立风控职责, L3/L4 只要纯结构结论:
		// mine_sweep=false 保证 blocked 不为空时不会丢结构证据(pool/blocked 都解析)。
		"mine_sweep": false,
		"edge_check": false,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("chan layer: marshal request: %w", err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, c.cfg.HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.cfg.GatewayBaseURL+"/api/czsc/screen_pool", strings.NewReader(string(payload)))
	if err != nil {
		return nil, fmt.Errorf("chan layer: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("chan layer: gateway unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20)) // 8MB 防御上限
	if err != nil {
		return nil, fmt.Errorf("chan layer: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("chan layer: gateway HTTP %d: %.256s", resp.StatusCode, string(raw))
	}
	var env chanPoolEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("chan layer: decode response: %w", err)
	}
	if env.Data == nil {
		return nil, fmt.Errorf("chan layer: gateway no data (message=%s)", env.Message)
	}
	hits := make([]ChanHit, 0, len(env.Data.Pool)+len(env.Data.Blocked))
	hits = append(hits, env.Data.Pool...)
	hits = append(hits, env.Data.Blocked...) // blocked 也带完整结构证据, 分层只看缠论结论
	return hits, nil
}

// ---------------------------------------------------------------------------
// 信号 -> 分层映射（唯一口径, 可测试）
// ---------------------------------------------------------------------------

// classifyChanHit 把一只证券的 czsc 命中记录映射为分层状态。
//
// 优先级依据 P4 > P3 > P2 > P1 > P0：
//   - 同向冲突时以最新确认(dt 最大)的信号为准; 买卖点同日冲突视为结构失效(P4)——
//     "不能仅凭一根 K 线就把买点标为已确认"的对偶纪律;
//   - Observation 信号(>=2 中枢且最后一笔向上)不是三类买点, 归 P0 观察;
//   - 只有卖点(1S/2S/3S) -> P4 结构失效, 退出买点队列但保留记录。
func classifyChanHit(hit *ChanHit) (status ChanStatus, sig *ChanSignal) {
	if hit == nil {
		return ChanNoData, nil
	}
	type cand struct {
		s ChanSignal
		d string
	}
	var buys, sells []cand
	for _, b := range hit.BuyPoints {
		buys = append(buys, cand{b, b.Dt})
	}
	for _, s := range hit.SellPoints {
		sells = append(sells, cand{s, s.Dt})
	}
	lastest := func(list []cand) *cand {
		if len(list) == 0 {
			return nil
		}
		best := 0
		for i := range list {
			if list[i].d > list[best].d {
				best = i
			}
		}
		return &list[best]
	}
	lb, ls := lastest(buys), lastest(sells)
	switch {
	case lb != nil && ls != nil && ls.d >= lb.d:
		return ChanInvalid, &ls.s // 卖点更新(或同日冲突) -> 结构失效
	case ls != nil && lb == nil:
		return ChanInvalid, &ls.s
	case lb != nil:
		switch lb.s.Type {
		case "3B":
			return ChanBuy3Confirm, &lb.s
		case "2B":
			return ChanBuy2Confirm, &lb.s
		case "1B":
			return ChanBuy1Watch, &lb.s
		}
		return ChanWatch, &lb.s // 未知买点类型保守归观察, 不冒充确认
	default:
		return ChanWatch, nil // Observation / 无信号 -> P0 结构观察
	}
}

// ---------------------------------------------------------------------------
// Generator 接线
// ---------------------------------------------------------------------------

// SetChanClient 注入缠论分层客户端（cmd 层用 ASHARE_GATEWAY_URL 构造）。
// nil 或未注入时 ApplyChanLayer 整池标记 DATA_INSUFFICIENT 并告警——绝不静默跳过。
func (g *Generator) SetChanClient(c ChanLayerClient) { g.chanCli = c }

// ApplyChanLayer 对精筛后的股票类条目执行 L3/L4 缠论分层（在流动性闸门之后、
// 强度二次精化之前调用）。ETF/MANUAL-only 不参与（czsc 形态确认面向个股漏斗）。
//
// 返回修改后的池；任何整体失败都不删条目, 只打 DATA_INSUFFICIENT 状态。
func (g *Generator) ApplyChanLayer(ctx context.Context, pool []AggEntry, warnings *[]string, now time.Time) []AggEntry {
	if !g.cfg.ChanEnabled {
		return pool
	}
	syms := make([]string, 0, len(pool))
	for _, e := range pool {
		if e.PrimarySource != sourceSector && e.PrimarySource != sourceFund {
			continue
		}
		syms = append(syms, e.Symbol)
	}
	markAll := func(reason string) {
		for i := range pool {
			if pool[i].PrimarySource != sourceSector && pool[i].PrimarySource != sourceFund {
				continue
			}
			pool[i].ChanStatus = ChanNoData
			pool[i].CandidateTier = ChanNoData.ChanTier() + "(DATA)"
			pool[i].ChanReason = reason
		}
		*warnings = append(*warnings, fmt.Sprintf("CHAN 分层未完成(%s): %d 只股票标记 DATA_INSUFFICIENT, 全部保留在研究池", reason, len(syms)))
	}
	if len(syms) == 0 {
		return pool
	}
	if g.chanCli == nil {
		markAll("客户端未接入")
		return pool
	}
	hits, err := g.chanCli.ScreenBatch(ctx, syms, g.cfg.ChanCacheOnly)
	if err != nil {
		log.Printf("[chan] WARN screen batch failed: %v", err)
		markAll("网关失败:"+err.Error())
		return pool
	}
	nP := map[ChanStatus]int{}
	for i := range pool {
		e := &pool[i]
		if e.PrimarySource != sourceSector && e.PrimarySource != sourceFund {
			continue
		}
		hit, ok := hits[e.Symbol]
		if !ok {
			// 网关 errors 列出的票(行情不足/取数失败)与正常"无结构"区分不开,
			// 统一诚实标记 DATA_INSUFFICIENT——不伪造 P0 通过, 也不删除记录。
			e.ChanStatus = ChanNoData
			e.CandidateTier = "P0"
			e.ChanReason = "czsc 未返回该票结构(行情不足或计算失败)"
			nP[ChanNoData]++
			continue
		}
		status, sig := classifyChanHit(&hit)
		e.ChanStatus = status
		e.CandidateTier = status.ChanTier()
		e.ChanSummary = hit.Summary
		if sig != nil {
			e.BuyPointConfirmedAt = sig.Dt
			e.BuyPointReason = sig.Reason
			e.StopLoss = sig.StopLoss
			e.EffectiveStop = sig.EffectiveStop
		} else {
			e.BuyPointConfirmedAt = ""
			e.BuyPointReason = ""
			e.StopLoss = 0
			e.EffectiveStop = 0
		}
		nP[status]++
	}
	// ChanStatus 是 ComputeTradability 的输入(P4 -> BLOCKED/CHAN_INVALID),
	// 而本层在 refinePool 之后执行, 必须重推导一次使派生字段与最新状态一致。
	for i := range pool {
		pool[i].ComputeTradability(g.StrengthKnown())
	}
	*warnings = append(*warnings, fmt.Sprintf(
		"CHAN 分层完成(L3/L4, 复用 python czsc): P0观察 %d, P1一买观察 %d, P2二买确认 %d, P3三买确认 %d, P4结构失效 %d, 数据不足 %d(as_of=%s, cache_only=%v)",
		nP[ChanWatch], nP[ChanBuy1Watch], nP[ChanBuy2Confirm], nP[ChanBuy3Confirm], nP[ChanInvalid], nP[ChanNoData],
		now.Format("2006-01-02"), g.cfg.ChanCacheOnly))
	return pool
}
