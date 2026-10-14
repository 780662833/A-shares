// Package graph —— 漏斗图与旧复盘图共用的基础工具（统一执行计划 Step 5）。
//
// 本文件从孤儿旧图 review_graph.go 中迁移而来：删除旧图前，先把仍被
// funnel_graph.go / order_writer.go 引用的共享符号原样搬到这里，
// 保证行为逐字节一致（round2/ratioPct/buyFee/sellFee/lastCloseOf/
// extractJSONCandidate/lastAssistantContent/normalizeSymbols/appendSymbol/truncate）。
package graph

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/cloudwego/eino/schema"

	dbmodel "ashare-quant/internal/model"
)

// lotSize A 股一手 = 100 股。
const lotSize = 100

// codeBlockRe 匹配 markdown 代码围栏（```json ... ```）。
var codeBlockRe = regexp.MustCompile("(?s)```(?:json|JSON)?\\s*(.*?)\\s*```")

// ---------------------------------------------------------------------------
// LLM 脏输出预清洗
// ---------------------------------------------------------------------------

// extractJSONCandidate 预清洗：若模型套了 markdown 围栏，先把围栏内容提出来。
func extractJSONCandidate(content string) string {
	if m := codeBlockRe.FindStringSubmatch(content); len(m) == 2 {
		return strings.TrimSpace(m[1])
	}
	return strings.TrimSpace(content)
}

// lastAssistantContent 取 history 中最后一条 assistant 消息正文（错误回灌用）。
func lastAssistantContent(history []*schema.Message) string {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == schema.Assistant {
			return history[i].Content
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// K 线信封轻量解析
// ---------------------------------------------------------------------------

type klineEnvelopeLite struct {
	Klines []struct {
		Date  string  `json:"date"`
		Close float64 `json:"close"`
	} `json:"klines"`
}

// lastCloseOf 从行情 JSON 中取最后一根 K 线的收盘价。
func lastCloseOf(rawJSON string) (float64, bool) {
	if rawJSON == "" {
		return 0, false
	}
	var env klineEnvelopeLite
	if err := json.Unmarshal([]byte(rawJSON), &env); err != nil || len(env.Klines) == 0 {
		return 0, false
	}
	c := env.Klines[len(env.Klines)-1].Close
	if c <= 0 || math.IsNaN(c) || math.IsInf(c, 0) {
		return 0, false
	}
	return c, true
}

// limitUpOf 由 K 线 JSON 的昨收计算次日涨停价（板块比例按代码前缀判定）。
func limitUpOf(klineJSON string) (float64, bool) {
	close, ok := lastCloseOf(klineJSON)
	if !ok {
		return 0, false
	}
	var env struct {
		Symbol string `json:"symbol"`
	}
	_ = json.Unmarshal([]byte(klineJSON), &env)
	ratio := dbmodel.PriceLimitRatio(env.Symbol)
	return dbmodel.RoundToCent(close * (1 + ratio)), true
}

// ---------------------------------------------------------------------------
// 费用与数值工具
// ---------------------------------------------------------------------------

func buyFee(amount float64) float64 {
	f := amount * 0.00025
	if f < 5 {
		f = 5
	}
	return round2(f)
}

func sellFee(amount float64) float64 {
	return round2(buyFee(amount) + amount*0.0005)
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func ratioPct(part, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return part / total * 100
}

// truncate 截断字符串到 n 个 rune（日志展示用）。
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

// ---------------------------------------------------------------------------
// symbol 归一化
// ---------------------------------------------------------------------------

// normalizeSymbols 去空白、去重、剔除空串，保持输入顺序。
func normalizeSymbols(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// appendSymbol 向列表追加去重后的 symbol。
func appendSymbol(list []string, s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return list
	}
	for _, x := range list {
		if x == s {
			return list
		}
	}
	return append(list, s)
}

// summary 供 ExecutionReport 打印的人类可读摘要（随类型迁移至此）。
func reportSummary(submitted, failed int, orderIDs []string) string {
	return fmt.Sprintf("submitted=%d failed=%d orders=%v", submitted, failed, orderIDs)
}

// ---------------------------------------------------------------------------
// ExecutionReport —— Layer4 下单结果汇总（原定义在 review_graph.go，随旧图删除迁移）
// ---------------------------------------------------------------------------

// ExecutionReport PlaceOrders/ExecutePlan 节点的产出汇总。
type ExecutionReport struct {
	Submitted int      `json:"submitted"`
	Failed    int      `json:"failed"`
	OrderIDs  []string `json:"order_ids,omitempty"`
	Errors    []string `json:"errors,omitempty"`
}

// Summary 生成人类可读的单段摘要（供 main 打印 / HTTP 接口返回）。
func (r *ExecutionReport) Summary() string {
	if r == nil {
		return "<nil>"
	}
	return reportSummary(r.Submitted, r.Failed, r.OrderIDs)
}

// ---------------------------------------------------------------------------
// 账户快照 —— FetchData(旧)/RiskBudget(新) 共用的账户事实口径
// ---------------------------------------------------------------------------

// PositionView 持仓视图（喂给 LLM 与风控共用的口径）。
type PositionView struct {
	Code              string  `json:"code"`
	Quantity          int64   `json:"quantity"`
	AvailableQuantity int64   `json:"available_quantity"`
	CostPrice         float64 `json:"cost_price"`
	LastClose         float64 `json:"last_close"` // 最新收盘价（来自行情信封）
	MarketValue       float64 `json:"market_value"`
	FloatProfitPct    float64 `json:"float_profit_pct"`
}

// accountSnapshot 账户+市值快照（含最新价，供风控复用）。
type accountSnapshot struct {
	AvailableCash float64        `json:"available_cash"`
	FrozenCash    float64        `json:"frozen_cash"`
	Positions     []PositionView `json:"positions"`
	HoldValue     float64        `json:"hold_value"`   // Σ 持仓市值
	TotalAssets   float64        `json:"total_assets"` // available + frozen + hold_value（复盘时点恒等）
}

// nextWeekdayDate T 日复盘生成的订单归属 T+1 工作日（周末顺延到周一）。
// MVP 简化：不查交易日历（节假日顺延由撮合侧 PENDING 单滞留兜底）。
func nextWeekdayDate(tradeDate string) (string, error) {
	t, err := time.Parse("2006-01-02", strings.TrimSpace(tradeDate))
	if err != nil {
		return "", fmt.Errorf("next_weekday: invalid trade_date %q", tradeDate)
	}
	for {
		t = t.AddDate(0, 0, 1)
		if wd := t.Weekday(); wd != time.Saturday && wd != time.Sunday {
			return t.Format("2006-01-02"), nil
		}
	}
}
