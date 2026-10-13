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

// ---------------------------------------------------------------------------
// 阶段 3：证券资格硬条件（eligibility ≠ tradability, 见 contract.go 注释）
// ---------------------------------------------------------------------------

// LimitState 常量：涨跌停/可交易状态。SDK 当前公开行情类型未提供权威
// IsST / 停牌 / 涨跌停字段（见 provider.go 顶部说明），因此第一版采取
// "诚实 UNKNOWN"策略：无法确认的状态一律 LimitUnknown —— 禁止生成可执行
// 订单（tradability=UNKNOWN），但保留研究资格与观察记录，等待接入可靠的
// 证券主数据 Provider 后升级为权威判定。绝不以名称含 "ST" 作为最终依据。
const (
	LimitNormal  = "NORMAL"
	LimitUp      = "UP"
	LimitDown    = "DOWN"
	LimitUnknown = "UNKNOWN"
)

// barOHLC 日 K 一行完整 OHLC（阶段 3 完整性校验输入）。
type barOHLC struct {
	Date  string
	Open  float64
	High  float64
	Low   float64
	Close float64
}

// ohlcValid 单根 K 线完整性：High >= max(Open, Close) 且 Low <= min(Open, Close)，
// 且各价为正。异常记录不能参与计算。
func ohlcValid(o, h, l, c float64) bool {
	if o <= 0 || h <= 0 || l <= 0 || c <= 0 {
		return false
	}
	if h < math.Max(o, c) || l > math.Min(o, c) {
		return false
	}
	return l <= h
}

// ohlcSeriesValid 近窗口内所有 K 线均满足完整性（任一异常即整条记录标记 OhlcInvalid）。
func ohlcSeriesValid(bars []barOHLC) bool {
	if len(bars) == 0 {
		return false // 无数据 -> 未知（调用方按 unknown 处理）
	}
	for _, b := range bars {
		if !ohlcValid(b.Open, b.High, b.Low, b.Close) {
			return false
		}
	}
	return true
}

// quoteAsOfMatches 行情时点校验：最近一根 K 线日期与本次 as_of 同一自然日
// （或允许范围内——灰度期先取同日口径；周末/节假日运行由调用方传入交易日对齐的 as_of）。
// 不一致 -> StaleQuote=true，tradability=UNKNOWN（禁止把过期行情当成当前有效信号）。
func quoteAsOfMatches(bars []barOHLC, asOf time.Time) bool {
	if len(bars) == 0 {
		return false
	}
	last := bars[len(bars)-1].Date
	if len(last) >= 10 {
		last = last[:10]
	}
	return last == asOf.Format("2006-01-02")
}

// listingCalendarDays 上市至今的自然日数; ListingDate 为空返回 -1（未知）。
func listingCalendarDays(listingDate string, now time.Time) int {
	if listingDate == "" {
		return -1
	}
	t, err := time.Parse("2006-01-02", listingDate[:10])
	if err != nil {
		return -1
	}
	return int(now.Sub(t).Hours() / 24)
}

// refinePool 对去重后的池执行资格/财务闸门 + RS 排序。warnings 追加写回。
// 阶段 5: 委托给带显式窗口覆盖的 refinePoolWith（cfg 值传 0 -> 走配置解析）。
func (g *Generator) refinePool(ctx context.Context, pool []AggEntry, warnings *[]string, now time.Time) []AggEntry {
	return g.refinePoolWith(ctx, pool, warnings, now, 0, 0)
}

// refinePoolWith 双窗口口径可注入版本：shortOverride/longOverride > 0 时直接采用
// （仅供测试精确构造"基准长窗样本不足"等退化场景），否则按配置解析并做非法值回落。
func (g *Generator) refinePoolWith(ctx context.Context, pool []AggEntry, warnings *[]string, now time.Time, shortOverride, longOverride int) []AggEntry {
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
		hardFlag bool   // 阶段3硬条件异常标记（OHLC/时点/价格）——不剔除, 只降级 tradability
		chg      *float64
		chgShort *float64 // 阶段5: R20 区间涨跌幅（短期动量）
	}
	results := make([]outcome, len(stockIdx))

	// ---- 阶段 5: 双窗口口径解析（R20 短期 + R60 中期）----
	longWin := g.cfg.RSLongWindow
	if longWin <= 0 {
		longWin = g.cfg.RSWindow // 兼容旧配置键
	}
	shortWin := g.cfg.RSShortWindow
	if shortWin <= 0 || shortWin >= longWin {
		shortWin = 20 // 非法配置回落契约默认，绝不静默改变长期窗口
	}
	if longOverride > 0 {
		longWin = longOverride
	}
	if shortOverride > 0 {
		shortWin = shortOverride
	}
	maxWin := longWin
	if shortWin > maxWin {
		maxWin = shortWin
	}

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

			// ---- 阶段 3: 证券资格硬条件明细（OHLC 完整性 / 行情时点 / 价格下限）----
			// 停牌与涨跌停状态：stock-go 公开行情类型无权威 IsST/停牌/涨跌停字段，
			// 第一版诚实标记 LimitUnknown（tradability=UNKNOWN 禁止可执行订单），
			// 等待证券主数据 Provider 接入后升级——见 contract.go / aggregate.go 注释。
			// 注意：本段直接复用上方已取的 q（避免重复 RPC 两次画像不一致）。
			e := &pool[idx]
			e.LimitState = LimitUnknown // SDK 无权威来源 -> 诚实 UNKNOWN（不伪造 NORMAL）
			if q != nil {
				e.ListingDays = listingCalendarDays(q.ListingDate, now)
			}
			if bars, err := g.prov.BarsOHLC(ctx, code, 20, now); err == nil && len(bars) > 0 {
				if !ohlcSeriesValid(bars) {
					e.OhlcInvalid = true // 异常记录不能参与计算（保留观察, tradability=UNKNOWN）
				}
				if !quoteAsOfMatches(bars, now) {
					e.StaleQuote = true // 行情截止日 != as_of -> STALE
				}
				if last := bars[len(bars)-1].Close; last > 0 && last < g.cfg.MinPrice {
					e.PriceBelowMin = true // 最新有效价 < 1 元 -> 不入普通候选排名
				}
			} else {
				res.unknown = true // OHLC 不可得 -> 无法校验, 按未知处理（不误杀）
			}
			if e.OhlcInvalid || e.StaleQuote || e.PriceBelowMin {
				res.hardFlag = true
			}

			if !res.rejected && g.cfg.RSEnabled {
				// 阶段 5: 一次取 max(R20,R60)+1 根 K 线, 同序列上计算双窗口收益（避免两次 RPC 口径不一致）
				bars, err := g.prov.StockBars(ctx, code, maxWin, now)
				if err != nil || len(bars) < needBars(shortWin) {
					res.unknown = true
				} else {
					// 各窗口独立判定样本充分性: 短窗口可得即打短分; 长窗口缺样仅置 nil(单窗口退化),
					// 绝不因长窗口样本不足而把整只股票踢出打分。
					res.chgShort = pctChangeOver(bars, shortWin)
					if len(bars) >= needBars(longWin) {
						res.chg = pctChangeOver(bars, longWin)
					}
					if res.chg == nil && res.chgShort == nil {
						res.unknown = true
					}
				}
			}
			results[k] = res
		}(k, idx)
	}
	wg.Wait()

	// ---- 基准指数同期涨跌幅（阶段 5: 双窗口, 与个股同一条 K 线序列计算）----
	var benchChg, benchChgShort *float64
	if g.cfg.RSEnabled {
		bars, err := g.prov.IndexBars(ctx, g.cfg.RSBenchmark, maxWin, now)
		if err == nil && len(bars) >= needBars(shortWin) {
			benchChgShort = pctChangeOver(bars, shortWin)
			if len(bars) >= needBars(longWin) {
				benchChg = pctChangeOver(bars, longWin)
			}
		}
		if benchChg == nil && benchChgShort == nil {
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
		if res.chg != nil || res.chgShort != nil {
			// 阶段 5: 双窗口加权总分。缺哪个窗口就只用另一个（单窗口退化），
			// 权重按实际可得窗口归一化, 绝不把缺失当 0 参与加权。
			var sExcess, lExcess float64
			var haveS, haveL bool
			if res.chgShort != nil && benchChgShort != nil {
				sExcess = *res.chgShort - *benchChgShort
				haveS = true
			}
			if res.chg != nil && benchChg != nil {
				lExcess = *res.chg - *benchChg
				haveL = true
			}
			switch {
			case haveS && haveL:
				w := g.cfg.RSShortWeightPct / 100
				if w < 0 {
					w = 0
				}
				if w > 1 {
					w = 1
				}
				e.RankScore = round2(w*sExcess + (1-w)*lExcess)
				e.Scored = true
			case haveS:
				e.RankScore = round2(sExcess)
				e.Scored = true
			case haveL:
				e.RankScore = round2(lExcess)
				e.Scored = true
			}
			if haveS {
				v := round2(sExcess)
				e.RetShortExcess = &v
			}
			if haveL {
				v := round2(lExcess)
				e.RetLongExcess = &v
			}
		}
		kept = append(kept, e)
	}
	if unknownN > 0 {
		*warnings = append(*warnings, fmt.Sprintf("QUALIFY/RS %d 只股票数据缺失(已按未知放行, 不参与打分)", unknownN))
	}
	// 可观测性: 只要实际执行过闸门判定就输出统计 warning（即使 0 剔除/0 缺失）,
	// 使每次运行的通过/拒绝计数可审计。
	if benchChg != nil || benchChgShort != nil {
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

	// ---- RS 降序排序 + TopK 截断（任一基准窗口可得即执行排序）----
	if benchChg != nil || benchChgShort != nil {
		sort.SliceStable(kept, func(i, j int) bool { return kept[i].RankScore > kept[j].RankScore })
		rankedN := 0
		for i := range kept {
			if !kept[i].Scored {
				continue // ETF / 无 K 线样本者不参与 RS 打分 -> Strength/RankPos 保持未赋值
			}
			rankedN++
			kept[i].RankPos = rankedN
		}
		// 短期维度单独排名: RankPos 是加权总分的排名位——若用总分名次判"短期排名",
		// 会出现"短强中极弱被总分挤出前 1/3 -> 误判 NONE"的自相矛盾。因此 A 路径的
		// "位于前列"必须按 R20 超额降序单独排名（仅在参与打分的股票类条目中比较）。
		shortRank := map[int]int{} // kept 下标 -> 短期超额名次(1 起)
		shortN := 0
		{
			order := make([]int, 0, len(kept))
			for i := range kept {
				if kept[i].Scored {
					order = append(order, i)
				}
			}
			sort.SliceStable(order, func(a, b int) bool {
				ea, eb := &kept[order[a]], &kept[order[b]]
				va, vb := -1e18, -1e18
				if ea.RetShortExcess != nil {
					va = *ea.RetShortExcess
				}
				if eb.RetShortExcess != nil {
					vb = *eb.RetShortExcess
				}
				return va > vb
			})
			for pos, idxK := range order {
				if kept[idxK].RetShortExcess != nil {
					shortN++
					shortRank[idxK] = pos + 1
				}
			}
		}
		for i := range kept {
			if !kept[i].Scored {
				continue
			}
			// 阶段 5: 强度路径按契约语义判定（看窗口超额, 不看加权总分符号）：
			//   A = R20 超额为正 且 位于"短期超额降序"列的前 1/3
			//       （单窗口退化时唯一正超额窗口即视为短期强）
			//   B = 非 A 但 R60 超额为正（中期强）
			//   NONE = 两窗口超额均不为正（或不可得）
			// 总分只用于排序与 TopK 截断；短期强（R20 超额为正）即使中期
			// 弱于基准拉负总分，仍应标 A（czsc 形态确认层会二次把关）。
			onlyShort := kept[i].RetLongExcess == nil && kept[i].RetShortExcess != nil
			inShortTopThird := false
			if r, ok := shortRank[i]; ok && shortN > 0 {
				inShortTopThird = r*3 <= shortN
			}
			switch {
			case kept[i].RetShortExcess != nil && *kept[i].RetShortExcess > 0 &&
				(onlyShort || inShortTopThird):
				kept[i].Strength = StrengthA; t1 := true; _ = t1
			case kept[i].RetLongExcess != nil && *kept[i].RetLongExcess > 0:
				kept[i].Strength = StrengthB
			default:
				kept[i].Strength = StrengthNone
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

// needBars RS 计算所需最少 K 线根数。注意：pctChangeOver 要求 len(bars) > window，
// 即至少 window+1 根——若只按窗口的 75% 放行，短窗口(R20)在长窗口取数(maxWin=60)下
// 会因基期与现期落在同一平台价上而恒得 0% 收益，属静默口径错误，绝不允许。
func needBars(window int) int {
	return window + 1
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
