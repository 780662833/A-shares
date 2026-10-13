// refine.go —— Layer 0.75 精筛管线：证券资格 → 财务状态 → 相对强度(RS)排序。
//
// 在四来源合并去重（Layer 0.5）之后、落库之前执行，对股票类条目
// （SECTOR_LEADER / FUND_FLOW）施加三道闸门并按 RS 降序重排整池：
//
//	① 证券资格闸门：剔除上市未满 MinListedDays 自然日的次新股；
//	   ListingDate 缺失视为"未知放行"（上游 F10 偶发缺字段，不误杀）。
//	② 财务状态闸门：RequirePositiveEPS 时最近一期基本 EPS < 0 剔除；
//	   MinROE 语义——默认 0 = 仅剔除"明确负 ROE"的亏损公司；
//	   显式设为 >0 则要求 ROE ≥ 该值；负阈值可完全关闭本项。
//	③ 相对强度排序：RS = 个股 RSWindow 日区间涨跌幅 − 基准指数同期涨跌幅
//	   （百分点，前复权口径保证跨票可比）；按 RS 降序记录 RankScore/RankPos，
//	   RSTopK>0 时截断弱尾。MANUAL/ETF 不参与闸门与截断。
//
// 弱依赖原则（与全池一致）：
//   - 单只股票资格/RS 查询失败 → 该股"未知放行"（保留、不打分），记聚合 warning；
//   - 基准指数不可得 → 跳过 RS 排序，池子保持原顺序，绝不因此清空；
//   - 闸门把全部股票类条目杀光 → 视为数据事故，整体退化为不设闸门并告警。
package pool

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// refinePool 对去重后的池执行资格/财务闸门 + RS 排序。warnings 追加写回。
func (g *Generator) refinePool(ctx context.Context, pool []AggEntry, warnings *[]string, now time.Time) []AggEntry {
	// 股票类下标（参与闸门与 RS）; MANUAL/ETF 直通。
	stockIdx := make([]int, 0, len(pool))
	for i, e := range pool {
		switch e.PrimarySource {
		case sourceSector, sourceFund:
			stockIdx = append(stockIdx, i)
		}
	}
	if len(stockIdx) == 0 {
		return pool
	}

	type outcome struct {
		rejected bool   // 被闸门剔除
		reason   string // 剔除原因（诊断用）
		unknown  bool   // 上游数据缺失 -> 未知放行
		scored   bool   // 已实际执行资格/财务闸门判定
		finBlock bool   // 财务维度明确否决（EPS<0 / ROE 不足）
		chg      *float64
	}
	results := make([]outcome, len(stockIdx))

	var wg sync.WaitGroup
	sem := make(chan struct{}, g.cfg.RSConcurrency)
	for k, idx := range stockIdx {
		wg.Add(1)
		go func(k, idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			code := pool[idx].Symbol
			res := outcome{}

			q, err := g.prov.StockQualification(ctx, code)
			if err != nil || q == nil {
				res.unknown = true
			} else {
				res.scored = true
				// 画像整体可得, 但关键字段缺失 -> 该维度"未知放行"并计入可观测统计
				if q.ListingDate == "" || q.BasicEPS == nil || q.ROEWeighted == nil {
					res.unknown = true
				}
				if reason, reject := g.qualifyReject(q, now); reject {
					res.rejected, res.reason = true, reason
					res.finBlock = strings.Contains(reason, "EPS") || strings.Contains(reason, "ROE")
				}
			}

			if !res.rejected && g.cfg.RSEnabled {
				bars, err := g.prov.StockBars(ctx, code, g.cfg.RSWindow, now)
				if err != nil || len(bars) < needBars(g.cfg.RSWindow) {
					res.unknown = true
				} else {
					res.chg = pctChangeOver(bars, g.cfg.RSWindow)
					if res.chg == nil {
						res.unknown = true
					}
				}
			}
			results[k] = res
		}(k, idx)
	}
	wg.Wait()

	// ---- 基准指数同期涨跌幅 ----
	var benchChg *float64
	if g.cfg.RSEnabled {
		bars, err := g.prov.IndexBars(ctx, g.cfg.RSBenchmark, g.cfg.RSWindow, now)
		if err == nil && len(bars) >= needBars(g.cfg.RSWindow) {
			benchChg = pctChangeOver(bars, g.cfg.RSWindow)
		}
		if benchChg == nil {
			*warnings = append(*warnings, fmt.Sprintf("RS_BENCHMARK 基准指数 %s 不可得(已跳过 RS 排序)", g.cfg.RSBenchmark))
		}
	}

	// ---- 应用闸门与打分 ----
	var unknownN, rejectedN int
	kept := make([]AggEntry, 0, len(stockIdx))
	dropped := make([]AggEntry, 0, len(stockIdx))
	for k, idx := range stockIdx {
		e := pool[idx]
		res := results[k]
		// 契约状态回填（独立保存, 不受主来源优先级影响）
		switch {
		case res.rejected && res.finBlock:
			e.Financial = FinBlocked
			e.Eligibility = EligPass
		case res.rejected:
			e.Eligibility = EligBlocked
		case res.unknown || !res.scored:
			e.Eligibility = EligUnknown
			e.Financial = FinDataInsufficient
		default:
			e.Eligibility = EligPass
			e.Financial = FinPass
		}
		if res.rejected {
			rejectedN++
			dropped = append(dropped, e)
			continue
		}
		if res.unknown {
			unknownN++
		}
		if res.chg != nil && benchChg != nil {
			e.RankScore = round2(*res.chg - *benchChg)
			e.Scored = true // 参与 RS 打分（RankPos/Strength 仅对 Scored 条目赋值）
		}
		kept = append(kept, e)
	}
	if unknownN > 0 {
		*warnings = append(*warnings, fmt.Sprintf("QUALIFY/RS %d 只股票数据缺失(已按未知放行, 不参与打分)", unknownN))
	}
	// 可观测性: 只要实际执行过闸门判定就输出统计 warning（即使 0 剔除/0 缺失）,
	// 使每次运行的通过/拒绝计数可审计。
	if benchChg != nil {
		*warnings = append(*warnings, fmt.Sprintf("QUALIFY 闸门完成: %d 只参与判定, 剔除 %d, 未知放行 %d, 保留 %d", len(stockIdx), rejectedN, unknownN, len(kept)))
	} else {
		*warnings = append(*warnings, fmt.Sprintf("QUALIFY 闸门完成(未打分): %d 只参与判定, 剔除 %d, 未知放行 %d, 保留 %d", len(stockIdx), rejectedN, unknownN, len(kept)))
	}
	// 保险丝（收紧版）: 仅当"没有任何一条经过实际闸门判定"（全部因上游故障未知、
	// 且无明确拒绝证据）时才视为数据事故整体退化放行；存在明确拒绝证据时尊重判定，
	// 绝不复活被剔除的股票。
	var judged int
	for _, res := range results {
		if res.scored && !res.rejected {
			judged++
		}
	}
	if len(kept) == 0 && len(dropped) > 0 && judged == 0 {
		*warnings = append(*warnings, fmt.Sprintf("QUALIFY 闸门剔除了全部 %d 只股票类条目(疑似数据事故, 已整体退化为不设闸门)", len(dropped)))
		kept = dropped
	} else if len(kept) == 0 && len(dropped) > 0 {
		*warnings = append(*warnings, fmt.Sprintf("QUALIFY 闸门基于明确证据剔除全部 %d 只股票类条目(不复活)", len(dropped)))
	}

	// ---- RS 降序排序 + TopK 截断（仅在基准可得时执行排序）----
	if benchChg != nil {
		sort.SliceStable(kept, func(i, j int) bool { return kept[i].RankScore > kept[j].RankScore })
		rankedN := 0
		for i := range kept {
			if !kept[i].Scored {
				continue // ETF / 无 K 线样本者不参与 RS 打分 -> Strength/RankPos 保持未赋值
			}
			rankedN++
			kept[i].RankPos = rankedN
		}
		for i := range kept {
			if !kept[i].Scored {
				continue
			}
			// 强度路径：A=短期强(灰度期以单窗口得分前 1/3 近似 R20 口径), B=中期强(其余正超额), NONE=打分后落选
			switch {
			case kept[i].RankScore <= 0:
				kept[i].Strength = StrengthNone
			case kept[i].RankPos*3 <= rankedN:
				kept[i].Strength = StrengthA
			default:
				kept[i].Strength = StrengthB
			}
		}
		if g.cfg.RSTopK > 0 && len(kept) > g.cfg.RSTopK {
			*warnings = append(*warnings, fmt.Sprintf("RS_TOPK 截断弱尾 %d 只(保留前 %d)", len(kept)-g.cfg.RSTopK, g.cfg.RSTopK))
			kept = kept[:g.cfg.RSTopK]
		}
	}

	// ---- 重组整池: 非股票条目保持原位相对顺序, 股票类整体插到其后 ----
	// （优先级去重已在 Layer 0.5 完成, 此处重排不影响 symbol 唯一性。）
	out := make([]AggEntry, 0, len(pool))
	for i, e := range pool {
		isStock := false
		for _, si := range stockIdx {
			if si == i {
				isStock = true
				break
			}
		}
		if !isStock {
			out = append(out, e)
		}
	}
	out = append(out, kept...)
	// 交易资格统一推导（黑名单优先; 强度路径 NONE 的纯自动股票条目软阻塞）
	for i := range out {
		out[i].ComputeTradability()
	}
	return out
}

// qualifyReject 证券资格 + 财务状态闸门。返回 (拒绝原因, 是否拒绝)。
// 所有"数据缺失"分支一律放行（配合调用方 unknown 统计），只有拿到明确
// 不合格证据时才拒绝。
func (g *Generator) qualifyReject(q *StockQualification, now time.Time) (string, bool) {
	// ① 证券资格: 上市满 MinListedDays 自然日
	if g.cfg.MinListedDays > 0 && q.ListingDate != "" {
		t, err := time.Parse("2006-01-02", q.ListingDate[:10])
		if err == nil {
			if now.Sub(t).Hours()/24 < float64(g.cfg.MinListedDays) {
				return fmt.Sprintf("次新股(上市%.0f天 < %d天)", now.Sub(t).Hours()/24, g.cfg.MinListedDays), true
			}
		}
		// 解析失败 -> 未知放行
	}
	// ② 财务状态: 最近一期 EPS / ROE
	if g.cfg.RequirePositiveEPS && q.BasicEPS != nil && *q.BasicEPS < 0 {
		return fmt.Sprintf("亏损(最近一期EPS=%.2f<0)", *q.BasicEPS), true
	}
	if q.ROEWeighted != nil {
		// 显式阈值模式（MinROE != 0）: 要求 ROE >= MinROE; 负阈值视为关闭本项。
		// 默认模式（MinROE == 0）: 仅剔除明确负 ROE 的亏损公司。
		if g.cfg.MinROE != 0 {
			if g.cfg.MinROE > 0 && *q.ROEWeighted < g.cfg.MinROE {
				return fmt.Sprintf("ROE不足(%.1f%% < %.1f%%)", *q.ROEWeighted, g.cfg.MinROE), true
			}
		} else if *q.ROEWeighted < 0 {
			return fmt.Sprintf("负ROE(%.1f%%)", *q.ROEWeighted), true
		}
	}
	return "", false
}

// needBars RS 计算所需最少 K 线根数（容忍小幅缺数据, 75% 覆盖）。
func needBars(window int) int {
	n := window * 3 / 4
	if n < 2 {
		n = 2
	}
	return n
}

// pctChangeOver 最近 window 个交易日区间涨跌幅 %（首尾收盘）。
func pctChangeOver(bars []DailyBar, window int) *float64 {
	if len(bars) <= window {
		return nil
	}
	base := bars[len(bars)-1-window].Close
	last := bars[len(bars)-1].Close
	if base <= 0 || last <= 0 {
		return nil
	}
	v := (last/base - 1) * 100
	return &v
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
