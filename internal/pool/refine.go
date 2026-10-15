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
		rejected   bool     // 被闸门剔除
		reason     string   // 剔除原因（诊断用）
		unknown    bool     // 上游数据缺失 -> 未知放行
		scored     bool     // 已实际执行资格/财务闸门判定
		finBlock   bool     // 财务维度明确否决（EPS<0 / ROE 不足）
		hardFlag   bool     // 阶段3硬条件异常标记（OHLC/时点/价格）——不剔除, 只降级 tradability
		chg        *float64 // R60 个股收益率 %（原始值, 非超额）
		shortKnown bool     // 短窗样本充分（端点可计算，即使收益为 0 也算可得）
		longKnown  bool     // 长窗样本充分（R60 端点在样本内）
		chgShort   *float64 // 阶段5: R20 区间涨跌幅（短期动量）
		// 阶段 8: MA 快照透传（daily_stock_metrics 落库输入）
		closePx, ma20, ma60 *float64
	}
	results := make([]outcome, len(stockIdx))

	// 阶段 7 接线：记录每只股票打分时**实际使用**的基准窗口可得性（单票样本不足
	// 退化口径）。路径 A/B 判定必须与打分共用同一基准视图，否则会出现"分数按短窗
	// 算、判定却要求 R60>0"的口径分裂。nil 切片在接线处判空，无副作用。
	stockBenchShort := make([]*float64, len(stockIdx))
	stockBenchLong := make([]*float64, len(stockIdx))

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
	trendRes := make([]trendState, len(stockIdx)) // kept 下标(k)对齐的均线结构判定
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
					// finBlock 仅标记"财务维度"否决(画像 EPS/ROE); 上市时长等资格闸门不算财务结论
					res.finBlock = strings.Contains(reason, "EPS") || strings.Contains(reason, "ROE中位数")
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
				bars, err := g.prov.StockBars(ctx, code, needBars(maxWin), now)
				if err == nil && len(bars) > 0 {
					closes := make([]float64, len(bars))
					for bi, bb := range bars {
						closes[bi] = bb.Close
					}
					cge, mge, known := maTrendFromBars(closes)
					trendRes[k] = trendState{CloseGeMA20: cge, MA20GeMA60: mge, Known: known}
					results[k].closePx, results[k].ma20, results[k].ma60 = maValuesFromBars(closes)
				}
				if err != nil || len(bars) < needBars(shortWin) {
					res.unknown = true
				} else {
					// 各窗口独立判定样本充分性: 短窗口可得即打短分; 长窗口缺样仅置 nil(单窗口退化),
					// 绝不因长窗口样本不足而把整只股票踢出打分。
					res.chgShort = pctChangeOver(bars, shortWin)
					if res.chgShort != nil {
						res.shortKnown = true
					}
					if len(bars) >= needBars(longWin) {
						res.chg = pctChangeOver(bars, longWin)
						if res.chg != nil {
							res.longKnown = true
						}
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
		// 请求 needBars(maxWin)=maxWin+1 根: lookback 与 pctChangeOver 的样本口径
		// 统一为 needBars(window)，fake/SDK 按请求条数精确截断也不会移动窗口端点。
		bars, err := g.prov.IndexBars(ctx, g.cfg.RSBenchmark, needBars(maxWin), now)
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
	// 阶段 6: 独立 FinancialProvider 并发取财务快照（弱依赖：供应商未接入/取数
	// 失败一律 FinDataInsufficient, 绝不默认"已通过"; 也不阻断整池生成）。
	finStatus := make([]FinancialStatus, len(stockIdx))
	finReasons := make([][]string, len(stockIdx))
	for k := range finStatus {
		finStatus[k] = FinDataInsufficient
	}
	if g.cfg.QualifyEnabled {
		var fwg sync.WaitGroup
		fsem := make(chan struct{}, g.cfg.RSConcurrency)
		for k, idx := range stockIdx {
			fwg.Add(1)
			go func(k, idx int) {
				defer fwg.Done()
				fsem <- struct{}{}
				defer func() { <-fsem }()
				if g.finProv == nil {
					finReasons[k] = []string{ErrFinancialNotWired.Error()}
					return
				}
				s, err := g.finProv.GetFinSnapshot(ctx, pool[idx].Symbol, now)
				if err != nil || s == nil {
					finReasons[k] = []string{fmt.Sprintf("财务供应商取数失败: %v", err)}
					return
				}
				st, rs := classifyFinSnapshot(s, now)
				finStatus[k] = st
				finReasons[k] = rs
				pe, pa, pd, pp := s.metaFields()
				pool[idx].FinReportPeriod = pe
				pool[idx].FinAnnounceDate = pa
				pool[idx].FinDataAsOf = pd
				pool[idx].FinProviderSource = pp
			}(k, idx)
		}
		fwg.Wait()
		var nBlocked, nWarn, nInsuf int
		for _, st := range finStatus {
			switch st {
			case FinBlocked:
				nBlocked++
			case FinWarn:
				nWarn++
			case FinDataInsufficient:
				nInsuf++
			}
		}
		*warnings = append(*warnings, fmt.Sprintf("FINANCIAL 闸门完成: BLOCKED %d, WARN(降权) %d, DATA_INSUFFICIENT %d%s",
			nBlocked, nWarn, nInsuf, map[bool]string{true: ", provider 未接入(全部按数据不足处理)", false: ""}[g.finProv == nil]))
	}

	var unknownN, rejectedN int
	kept := make([]AggEntry, 0, len(stockIdx))
	dropped := make([]AggEntry, 0, len(stockIdx))
	// 阶段5 打分集合与粗排快照镜像：在闸门循环内逐票填充（e 是值拷贝、Symbol
	// 唯一），供下方 strengthWiring 构造时直接采用。ScoredSymbols 的判据必须是
	// "实际完成 RS 打分"而非"存在于 kept"——未知放行条目也在 kept 中但不得进入
	// 四因子百分位链路（否则伪造打分结论，违反诚实性契约）。
	wiringPreScore := map[string]float64{}
	wiringScored := map[string]bool{}
	for k, idx := range stockIdx {
		e := pool[idx]
		res := results[k]
		// 阶段 8: MA/收盘价快照透传（供 daily_stock_metrics 落库; nil = 样本不足, 诚实缺失）
		e.Close, e.MA20, e.MA60 = res.closePx, res.ma20, res.ma60
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
			// 阶段 6: 精筛开启时旧画像启发式不再自动给出"财务 PASS"结论 ——
			// 权威快照不可得(provider 未接入/取数失败)保持 DATA_INSUFFICIENT;
			// 接入后由上方 finStatus 覆盖规则统一回填。仅精筛关闭(无 provider
			// 链路, 兼容迁移版行为)时才沿用默认 PASS。
			if g.cfg.QualifyEnabled {
				e.Financial = FinDataInsufficient
			} else {
				e.Financial = FinPass
			}
		}
		// ---- 阶段 6 覆盖规则（精筛开启即生效; provider 未接入时快照循环已把全部
		// 条目诚实初始化为 FinDataInsufficient + "provider 未接入"理由）----
		// 旧画像 EPS/ROE 启发式保留为兜底; 权威财务快照的判定优先。
		if g.cfg.QualifyEnabled {
			// provider 未接入时快照不可得, 不得用旧启发式的"默认 PASS"吞掉未知;
			// 其余状态（WARN/BLOCKED/PASS）来自真实快照, 一律覆盖旧启发式结论。
			if !(g.finProv == nil && finStatus[k] == FinDataInsufficient) {
				e.Financial = finStatus[k]
			}
			if len(finReasons[k]) > 0 {
				e.FinReasons = strings.Join(finReasons[k], "; ")
			}
			// 重大风险确认 -> BLOCKED 硬排除（不进可交易队列, 记录保留）。
			// 注意: 不翻转 Eligibility —— 股票可以符合研究资格却因财务被禁新单
			// （eligibility 与 tradability 分离，见阶段 3 契约）。
			if finStatus[k] == FinBlocked {
				res.rejected, res.reason = true, "财务重大风险:"+e.FinReasons
			}
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
			// 基准某窗口不可得时, 个股对应窗口端点必须同步失效——否则
			// "haveS/haveL"判定会随 lookback 透传方式漂移, 同一序列在不同
			// 用例里得出不同口径（伪精度）。统一以 maxWin 端点存在性为准。
			var sExcess, lExcess float64
			var haveS, haveL bool
			benchHasLong := benchChg != nil
			benchHasShort := benchChgShort != nil
			if res.chgShort != nil && benchHasShort {
				sExcess = *res.chgShort - *benchChgShort
				haveS = true
			}
			if res.chg != nil && benchHasLong {
				lExcess = *res.chg - *benchChg
				haveL = true
			}
			// 接线：记录该票打分时实际参与的基准窗口可得性（与 haveS/haveL 同语义），
			// 供阶段 7 路径判定复用同一基准视图——单票长窗样本不足时不得再用全局
			// R60 基准去要求 "R60>0" 硬门槛（口径分裂 bug）。
			if haveS {
				v := *benchChgShort
				stockBenchShort[k] = &v
			}
			if haveL {
				v := *benchChg
				stockBenchLong[k] = &v
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
			// 阶段 7: 保存个股原始双窗口收益率（路径 A/B 硬门槛看的是绝对收益正负, 不是超额）
			if res.chgShort != nil {
				v := round2(*res.chgShort)
				e.RetShortRaw = &v
			}
			if res.chg != nil {
				v := round2(*res.chg)
				e.RetLongRaw = &v
			}
			// 阶段 6: WARN 财务质量弱项 -> 排序降权（PASS=1.0, WARN=0.5）。
			// 只影响名次, 不删除记录；BLOCKED 已在上方硬排除, 不会走到这里。
			if g.cfg.QualifyEnabled {
				if p := finRankPenalty(e.Financial); p != 1.0 {
					e.RankScore = round2(e.RankScore * p)
				}
			}
			// 阶段5 口径快照：RS 双窗口加权总分（超额收益百分点）。下游强度管线
			// 会把 RankScore 重写为四因子百分位口径（0-100）——两者量纲不同、语义
			// 互补，必须分别保留，供审计与"非法窗口回落契约默认值"类断言精确区分：
			// RankScore 跟随最终精化口径, RSScore 恒为本阶段粗排结论。
			// 注意必须直接写入 kept 尾部条目: 循环变量 e 是 pool[idx] 的值拷贝,
			// 若只记在 results[k].rsScore 上再回填, 会因下方"同 Symbol 去重取最后
			// 得分"策略而丢失快照（重复代码场景）——e.RSScore 与 e.RankScore 同源同值,
			// 天然与最终保留条目一致, 无需回填。
			e.RSScore = e.RankScore
			wiringPreScore[e.Symbol] = e.RSScore
			wiringScored[e.Symbol] = true
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

	// ---- RS 降序粗排 + TopK 截断（任一基准窗口可得即执行）----
	// 唯一最终评分写入口径（阶段 5 锁定语义）：RankScore = 双窗口加权超额收益
	// （RSShortWeightPct·R20超额 + (1-w)·R60超额，缺窗归一化），路径判定与分段
	// 输出由下方 applyStrengthPathsAndScores 负责——该函数只在精筛主链路内以
	// 显式 legacy 模式运行（legacy=true），不再回写 RankScore/RankPos；百分位
	// 四因子口径作为独立实验管线保留（strength_test.go 直接调用验证），避免
	// "双评分口径互相覆盖"的回归。
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
		if g.cfg.RSTopK > 0 && len(kept) > g.cfg.RSTopK {
			// 统计口径修正：RSTopK 是"股票类确认队列名额"，ETF/未打分条目本就不参与
			// RS 排名（旧实现用 len(kept)-TopK 把 ETF 也计入"截断数"，虚报剔除量）。
			nStock := 0
			for i := range kept {
				if kept[i].Scored {
					nStock++
				}
			}
			truncN := nStock - g.cfg.RSTopK
			if truncN <= 0 {
				truncN = 0
			}
			if !g.cfg.TieredPipeline {
				// 旧模式: 物理截断（按名次裁掉股票类弱尾, 非股票条目直通段保留）。
				if truncN > 0 {
					*warnings = append(*warnings, fmt.Sprintf("RS_TOPK 截断弱尾 %d 只(保留前 %d)", truncN, g.cfg.RSTopK))
					trimmed := make([]AggEntry, 0, len(kept))
					for _, e := range kept {
						if e.Scored && e.RankPos > g.cfg.RSTopK {
							continue // 弱尾股票类: 移出输出
						}
						trimmed = append(trimmed, e)
					}
					kept = trimmed
				}
			} else if truncN > 0 {
				// 分层口径(TieredPipeline=true): RSTopK 是"确认队列名额"而非删除开关——
				// 弱尾降级保留在观察队列尾部(L4 P0 研究池)。RankPos 清零使其退出
				// 确认段位展示；但 Scored 必须保持 true：该标志同时是
				// ComputeTradability QUOTE_STATE 分支的判据（精筛恒置 LimitUnknown），
				// 若在此翻转会把被剔除条目的诚实结论从 QUOTE_STATE/FINANCIAL_DATA
				// 吞成泛化 STRENGTH，并污染 RefineScores 幂等快照重建。
				for i := range kept {
					if !kept[i].Scored {
						continue
					}
					if kept[i].RankPos > g.cfg.RSTopK {
						kept[i].RankPos = 0        // 移出确认段位, 保留记录、分数快照与打分标记(观察队列)
						kept[i].DemotedToWatch = true // 独立标志: 阶段7 重组会重编 RankPos, 清零不可靠
					}
				}
				*warnings = append(*warnings, fmt.Sprintf("L2 分层保留: RS_TOPK=%d 之外的 %d 只弱尾降级进入观察队列(不删除)", g.cfg.RSTopK, truncN))
			}
		}
	}

	// ---- 阶段 7: 契约口径的 A/B/NONE 路径判定 + 四因子百分位评分 + 分段排序 ----
	// 每次运行先重置强度包：精筛关闭/基准不可得时保持 nil（StrengthKnown=false）,
	// 防止上一轮的 wiring 指针残留把"管线未执行"误标成"打分后落选 STRENGTH"。
	g.strengthWiring = nil
	// 输入全部来自本次已采集数据（趋势结构 trendRes、基准 benchChgShort/benchChg、
	// 行业指数经 fetchSector 复取）；ADV20 属流动性侧数据, 由调用方在 liquidityGatePool
	// 之后通过 RefineScores 补齐——本函数先用"可得因子"打分,
	// 后续补算会覆盖 RankScore 为完整四因子口径。
	{
		w := strengthWiring{BenchR20: benchChgShort, BenchR60: benchChg, SingleWindowShortOnly: benchChgShort != nil && benchChg == nil,
			StockBenchR20: map[string]*float64{}, StockBenchR60: map[string]*float64{},
			StockShortKnown: map[string]bool{}, StockLongKnown: map[string]bool{},
			SectorR20: map[string]*float64{}, Adv: map[string]*float64{},
			Trend: map[string]trendState{}, RSPrescore: wiringPreScore, ScoredSymbols: wiringScored}
		for k, idxK := range stockIdx {
			if trendRes[k].Known || trendRes[k].CloseGeMA20 || trendRes[k].MA20GeMA60 {
				w.Trend[pool[idxK].Symbol] = trendRes[k]
			}
		}
		for k, idxK := range stockIdx {
			if v := stockBenchShort[k]; v != nil {
				w.StockBenchR20[pool[idxK].Symbol] = v
			}
			if v := stockBenchLong[k]; v != nil {
				w.StockBenchR60[pool[idxK].Symbol] = v
			}
			// 样本可得性标志：区分"窗口端点存在但收益恰为 0%"与"样本不足不可得(nil)"。
			// 仅凭 nil 指针无法表达前者（0% 也是有效测量），退化判定必须用显式标志。
			if results[k].shortKnown {
				w.StockShortKnown[pool[idxK].Symbol] = true
			}
			if results[k].longKnown {
				w.StockLongKnown[pool[idxK].Symbol] = true
			}
		}
		// ---- 生产 bug 修复：行业相对强度 RS20_sector 的权威来源是**行业指数**。
		// 旧实现把中文行业名当指数代码传给 IndexBars, 真实供应商必然取数失败, 且失败
		// 结果被 sectorCache 永久缓存 nil，SectorR20 恒空 → "跑赢行业"硬门槛永远无法
		// 确认 → A 路径进不了、四因子评分恒缺行业侧权重。现改为按行业名向 Provider
		// 请求行业指数 K 线（needBars(shortWin) 口径, 与个股/宽基窗口端点语义一致）;
		// 取不到诚实置 nil（"无法确认"→ 最高归 B, 评分走权重归一, 绝不猜测为正）。
		// 同一行业名在批次内只请求一次。
		type secResult struct {
			chg *float64
		}
		sectorCache := map[string]secResult{}
		fetchSector := func(name string) *float64 {
			if c, hit := sectorCache[name]; hit {
				return c.chg
			}
			var v *float64
			if name != "" {
				// 与个股/宽基同口径请求 maxWin+1 根：fake/SDK 按尾部定位时 lookback
				// 差异不会移动窗口端点；长窗样本不足时短窗仍诚实可得。
				if bars, err := g.prov.IndexBars(ctx, name, needBars(maxWin), now); err == nil && len(bars) >= needBars(shortWin) {
					v = pctChangeOver(bars, shortWin)
				}
			}
			sectorCache[name] = secResult{chg: v}
			return v
		}
		// 生产 bug 修复：行业侧取数覆盖全部股票类条目（SECTOR_LEADER 与 FUND_FLOW），
		// 按各自携带的 SectorName 归类——旧实现只给 SECTOR_LEADER 取行业指数，资金流
		// 来源即便带行业名也恒缺 RS20_sector 因子，A 路径"跑赢行业"硬门槛与四因子评分
		// 的行业权重被静默清零。同行业名批次内只请求一次。
		for i := range kept {
			e := &kept[i]
			if (e.PrimarySource != sourceSector && e.PrimarySource != sourceFund) || e.SectorName == "" {
				continue
			}
			if _, has := w.SectorR20[e.SectorName]; !has {
				w.SectorR20[e.SectorName] = fetchSector(e.SectorName)
			}
		}
		// 切换至非 legacy（百分位四因子终局口径）：阶段 5 加权超额仅作粗排输入，
		// 最终 RankScore/RankPos/截断由 applyStrengthPathsAndScores 统一重写，
		// 避免 trunc 截断在主链路成为死代码（P0-5 定案更新）。
		kept = g.applyStrengthPathsAndScores(kept, w, warnings)
		g.strengthWiring = &w // 供 Generate() 在流动性闸门后二次精化(ADV因子)复用
	}

	// ---- 重组整池: 非股票条目保持原位相对顺序, 股票类整体插到其后 ----
	// （优先级去重已在 Layer 0.5 完成, 此处重排不影响 symbol 唯一性。）
	// 生产 bug 修复: kept 已经过 applyStrengthPathsAndScores 分段重排(A→B→其余)，
	// 必须按 kept 的**当前切片顺序**输出。旧实现用下标遍历原 pool 取条目，而 kept
	// 与 pool 共享底层数组——读 pool[i] 会拿到重排前的原始位置版本，把分段排序
	// 静默还原成输入顺序（A/B/NONE 段位与 RankPos 全部错位）。
	out := make([]AggEntry, 0, len(pool))
	// kept 中每个条目都源自 stockIdx 指向的位置；被剔除者(dropped)不回填，
	// 非股票条目保持原相对顺序在前。
	isStockIdx := func(i int) bool {
		for _, si := range stockIdx {
			if si == i {
				return true
			}
		}
		return false
	}
	for i, e := range pool {
		if isStockIdx(i) {
			continue // 股票类统一由下方 kept 段以重排后的值输出
		}
		out = append(out, e)
	}
	out = append(out, kept...)
	// 交易资格统一推导（黑名单优先; 强度路径 NONE 的纯自动股票条目软阻塞）
	for i := range out {
		out[i].ComputeTradability(g.StrengthKnown())
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
