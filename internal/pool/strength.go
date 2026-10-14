// strength.go —— 阶段 7：相对强度计算与路径 A / B 排序（确定性 Go 计算，不依赖 LLM）。
//
// 与实施计划 6.1~6.4 的对应关系：
//
//	6.1 统一价格口径：R20 = C_t/C_{t-20} - 1、R60 = C_t/C_{t-60} - 1，在同一 as_of、
//	    同一前复权收盘序列上计算（refine.go 已保证一次取 maxWin+1 根、needBars(window)=window+1，
//	    即 R20 至少 21 个有效收盘价、R60 至少 61 个——不足则该窗口诚实置 nil，绝不静默按 0）。
//	    RS_index = (1+R_stock)/(1+R_index) - 1；RS_sector = (1+R_stock)/(1+R_sector) - 1。
//	    本文件以"超额收益"近似比值口径：excess ≈ R_stock - R_bench（百分比点），
//	    在 |R| < ~10% 时两者一阶等价（差项为二阶小量 R_s*R_b/(1+R_b)）。第一版沿用
//	    既有 RetShortExcess/RetLongExcess 字段与测试基线不做破坏性变更；比值精确口径
//	    列为后续样本外验证项（见 contract.go 差距清单）。
//	    基准指数固定配置 RSBenchmark（默认沪深300 "000300"）；行业指数使用条目自身
//	    SectorName（SECTOR_LEADER 来源携带），分类体系由数据源固定，截止日期与个股一致。
//
//	6.2 路径 A（趋势强势股，全部条件为硬门槛，不用评分抵消）：
//	    R20 > 0 ∧ R60 > 0 ∧ RS20_index > 0 ∧ RS20_sector > 0 ∧ Close ≥ MA20 ∧ MA20 ≥ MA60
//	    ∧ 证券资格通过（Eligibility != BLOCKED）∧ 流动性通过（Liquidity ∈ {PASS, WARN}）
//	    ∧ 财务不得为 BLOCKED；DATA_INSUFFICIENT 允许入选但 tradability 保持 UNKNOWN
//	    （aggregate.ComputeTradability 已实现该语义——缺失不默认为通过）。
//	    注意：RS20_sector 无法确认（非行业来源 / 行业指数不可得）时 **不能猜测为 >0**，
//	    该票最高只能归入 B；A 是完整趋势确认路径，宁缺毋滥。
//
//	6.3 路径 B（趋势修复候选，低优先级观察队列）：
//	    R20 > 0 ∧ RS20_index > 0 ∧ 资格/流动性硬检查通过 ∧ 财务未默认通过；
//	    中短期趋势可以尚未达到 A 的完整条件（MA20 未上穿 MA60 等不排除 B）。
//	    B 只进入较低优先级的观察与 CZSC 分析队列——反弹不等于可执行买入建议。
//
//	6.4 统一评分公式（百分位排名消除量纲，仅用于候选排序，不代表预测收益概率）：
//	    Score = 0.35·P(RS20_index) + 0.25·P(R60) + 0.25·P(RS20_sector) + 0.15·P(ADV20)，
//	    P 在同一批通过基础资格检查的候选内计算，范围 0–100。
//	    R20 已用于路径准入，总分不再重复加入 P(R20)（避免短期动量重复计权；
//	    后续可在样本外测试比较加入版本）。
//	    因子不可得时的权重处理：把可得因子的权重按比例归一化（缺失 ≠ 0 分，也 ≠ 满分），
//	    并在 Reason 里如实记录参与因子数。
//	    路径 A 与路径 B **分别排序、各自独立 TopN 上限**（PathATopN / PathBTopN），
//	    防止早期修复股挤占高优先级名额；B 整体排在 A 之后进入输出池。
package pool

import (
	"fmt"
	"math"
	"sort"
)

// 评分权重（6.4 表）。声明为变量便于灰度期做参数实验，默认值锁定契约口径。
var (
	WeightRS20Index   = 0.35 // 个股是否跑赢大盘
	WeightR60         = 0.25 // 中期趋势持续性
	WeightRS20Sector  = 0.25 // 个股是否跑赢所属行业
	WeightADV20       = 0.15 // 流动性和交易容量优先级
	PctileNonePenalty = true // 因子缺失走权重归一（false=按 0 分参与, 仅供回测对比）
)

// strengthOutcome 单票强度路径判定所需的全部输入（由 refinePool 收集后传入，
// 保证打分函数本身是纯函数——确定性、可单测、无网络）。
type strengthOutcome struct {
	R20Stock       *float64 // 个股 20 日收益率 %
	R60Stock       *float64 // 个股 60 日收益率 %
	R20Index       *float64 // 基准指数同期收益率 %
	R60Index       *float64 // 基准指数 60 日收益率 %
	R20Sector      *float64 // 所属行业指数同期收益率 %（nil=无法确认）
	CloseAboveMA20 bool     // Close ≥ MA20
	MA20AboveMA60  bool     // MA20 ≥ MA60
	TrendKnown     bool     // 均线数据是否足够（不足时两个 bool 均不可信）
	EligPass       bool     // 证券资格硬检查通过（非 BLOCKED）
	LiqPass        bool     // 流动性硬检查通过（PASS/WARN）
	FinBlocked     bool     // 财务明确 BLOCKED
}

// rsExcess 计算相对强度超额（百分点）。任一侧不可得 -> nil（无法确认，绝不猜正）。
func rsExcess(stock, bench *float64) *float64 {
	if stock == nil || bench == nil {
		return nil
	}
	v := *stock - *bench
	return &v
}

// pathABResult 路径判定结果。
type pathABResult struct {
	path       StrengthPath
	rs20Index  *float64 // 供评分因子 1
	rs20Sector *float64 // 供评分因子 3（可能 nil）
	r60        *float64 // 供评分因子 2
	reason     string   // 人类可读判定依据（审计用）
}

// classifyPathA_B 按 6.2/6.3 硬门槛判定 strength_path。
// ADV20 不参与路径准入（它是评分因子与流动性闸门职责），此处不引用。
func classifyPathA_B(o strengthOutcome) pathABResult {
	rsIdx20 := rsExcess(o.R20Stock, o.R20Index)
	rsSec20 := rsExcess(o.R20Stock, o.R20Sector)

	hardOK := o.EligPass && o.LiqPass && !o.FinBlocked
	// 共同准入：R20 > 0 且 RS20_index > 0（能确认才谈路径; 指数侧未知则两条路径都不给）
	shortPositive := o.R20Stock != nil && *o.R20Stock > 0
	rsIdxPositive := rsIdx20 != nil && *rsIdx20 > 0

	res := pathABResult{rs20Index: rsIdx20, rs20Sector: rsSec20, r60: o.R60Stock, path: StrengthNone}
	if !hardOK {
		switch {
		case o.FinBlocked:
			res.reason = "财务BLOCKED不入路径"
		case !o.LiqPass:
			res.reason = "流动性硬检查未过"
		default:
			res.reason = "证券资格硬检查未过"
		}
		return res
	}
	if !shortPositive || !rsIdxPositive {
		res.reason = "R20≤0或RS20_index未确认为正(B需二者皆正)"
		return res
	}
	// ---- 路径 A：完整趋势确认（所有条件必须"确认成立"，未知一律降级 B/NONE）----
	aConds := []bool{
		o.R60Stock != nil && *o.R60Stock > 0,
		rsIdx20 != nil && *rsIdx20 > 0, // 已在上方共同准入
		rsSec20 != nil && *rsSec20 > 0, // 行业侧未知 -> 不为真 -> 进不了 A
		o.TrendKnown && o.CloseAboveMA20,
		o.TrendKnown && o.MA20AboveMA60,
	}
	allA := true
	for _, c := range aConds {
		if !c {
			allA = false
			break
		}
	}
	if allA {
		res.path = StrengthA
		res.reason = "A: R20/R60/RS20_index/RS20_sector均为正且Close≥MA20≥MA60"
		return res
	}
	// ---- 路径 B：修复候选（R20>0 ∧ RS20_index>0 ∧ 硬检查过；趋势结构可以未达标）----
	res.path = StrengthB
	res.reason = "B: 短期强于大盘但趋势结构/行业强度未达A完整条件"
	return res
}

// percentileScores 对一组值（下标对齐 entries）计算 0–100 百分位排名。
// NaN/nil 位置返回 -1（不可得，交给调用方做权重归一）。
// 并列取平均秩（midrank），百分位 = (rank-1)/(n-1)*100，n==1 时恒 50。
func percentileScores(vals []*float64) []float64 {
	out := make([]float64, len(vals))
	type pv struct {
		v float64
		i int
	}
	var ok []pv
	for i, p := range vals {
		if p == nil || math.IsNaN(*p) {
			out[i] = -1
			continue
		}
		ok = append(ok, pv{*p, i})
	}
	n := len(ok)
	if n == 0 {
		return out
	}
	sort.SliceStable(ok, func(a, b int) bool { return ok[a].v < ok[b].v })
	ranks := make([]float64, n)
	for i := 0; i < n; {
		j := i
		for j+1 < n && ok[j+1].v == ok[i].v {
			j++
		}
		mid := float64(i+j)/2 + 1 // 并列组平均秩（1 起）
		for k := i; k <= j; k++ {
			ranks[k] = mid
		}
		i = j + 1
	}
	for k, p := range ok {
		if n == 1 {
			// 单样本百分位口径终裁：一个样本不构成分布, 取中位锚点 50。
			// 若取 100, WARN×0.5 降权分与 PASS 原始分恰相等, 并列破坏"WARN 必须排后"稳定序。
			out[p.i] = 50
		} else {
			out[p.i] = (ranks[k] - 1) / float64(n-1) * 100
		}
	}
	return out
}

// weightedScore 按 6.4 公式合成总分；不可得因子按剩余权重归一化。
// 返回 (score, usedFactors)。全部因子不可得 -> (0, 0)，调用方据此保持未打分。
func weightedScore(f [4]*float64, pct [4]float64) (float64, int) {
	w := [4]float64{WeightRS20Index, WeightR60, WeightRS20Sector, WeightADV20}
	var sum, wsum float64
	used := 0
	for i := range f {
		if f[i] == nil || pct[i] < 0 {
			if !PctileNonePenalty {
				// 回测对比模式：缺失按 0 分参与, 权重照算
				sum += 0
				wsum += w[i]
			}
			continue
		}
		sum += w[i] * pct[i]
		wsum += w[i]
		used++
	}
	if wsum == 0 {
		return 0, 0
	}
	return round2(sum / wsum), used
}

// strengthWiring 是 refinePool 传给强度管线的输入包（全部为已采集数据，
// 本文件不再发起任何 RPC —— 保证纯函数语义与单次运行口径一致）。
// StrengthKnown 强度管线是否对本池实际执行过判定（refinePool 接线时置位）。
// AggEntry 是纯 DTO, 运行期标志由 Generator 持有——供 ComputeTradability 的 STRENGTH
// 软阻塞分支区分"打分后落选(应阻塞)"与"管线未执行(不得猜测为落选)"。
func (g *Generator) StrengthKnown() bool { return g.strengthWiring != nil }

type strengthWiring struct {
	BenchR20, BenchR60    *float64              // 基准指数双窗口收益率 %
	SingleWindowShortOnly bool                  // 基准 R60 不可得而 R20 可得（阶段 5 退化口径）
	SectorR20             map[string]*float64   // 行业名 -> R20 %（缺失/失败 -> nil 或键不存在）
	Trend                 map[string]trendState // symbol -> 均线结构（缺键=未知）
	Adv                   map[string]*float64   // symbol -> ADV20（元; 缺键=nil）
	// RSPrescore symbol -> 阶段5 RS 双窗口加权总分快照（超额收益百分点口径）。
	// applyStrengthPathsAndScores 重写 RankScore 为百分位口径时，RankScore 的
	// WARN×0.5 降权必须同步镜像到该快照——否则粗排量纲字段与最终名次脱节
	// （WARN 票在旧版 RS 口径下名次应低于同分 PASS 票）。
	RSPrescore map[string]float64
	// ScoredSymbols 本轮实际进入四因子打分的股票集合。RefineScores 二次精化
	// （整池输入、含未经 refinePool 打分链路的条目）据此把"未参与打分"与
	// "打分后落选"区分开：不在集合内的股票类条目保持 Strength=Unset，
	// 绝不伪造 NONE 结论（诚实性契约）。
	ScoredSymbols map[string]bool
}

// trendState Close≥MA20 / MA20≥MA60 判定结果及可判性。
type trendState struct {
	CloseGeMA20 bool
	MA20GeMA60  bool
	Known       bool
}

// applyStrengthPathsAndScores 在 refinePool 完成闸门与双窗口收益采集后执行：
//  1. 逐票判定 A/B/NONE（classifyPathA_B 硬门槛）；
//  2. 在同一批"通过基础资格检查"的股票类候选内计算四因子百分位并合成 Score；
//  3. A、B 分别排序，各按 PathATopN / PathBTopN 截断，NONE 保留在后；
//  4. 输出顺序 A 段 → B 段 → NONE/非股票段，RankPos 全局连续编号。
//
// 分层口径（TieredPipeline=true, L2 趋势分类降级）：R20/R60/RS 是"分类与评分"
// 特征而不是统一淘汰条件——下跌末端潜在反转股（缠论一买观察对象）R20≤0 时
// 仍保留在研究观察队列（Strength=NONE, 不截断、不删除），仅不能进入 A/B 确认
// 路径；tradability 由聚合层 STRENGTH 软阻塞保证不会误标可执行。
// 同时输出 TREND_CLASS 审计统计（强势/转强/震荡/弱势/数据不足五档）。
//
// kept 必须已按旧版总分粗排（上游职责），本函数会重写 RankScore 为百分位口径。
func (g *Generator) applyStrengthPathsAndScores(kept []AggEntry, w strengthWiring, warnings *[]string) []AggEntry {
	if !g.cfg.RSEnabled || (w.BenchR20 == nil && w.BenchR60 == nil) {
		return kept // RS 管线未实际执行 -> Strength 保持 Unset, 不伪装成 NONE
	}

	isStock := func(e AggEntry) bool {
		return e.PrimarySource == sourceSector || e.PrimarySource == sourceFund
	}

	// ---- 1. 路径判定 ----
	for i := range kept {
		e := &kept[i]
		if !isStock(*e) {
			continue
		}
		// RefineScores 二次精化以整池为输入（含流动性闸门 FAIL/未知放行等未经
		// refinePool 打分链路的股票条目）：不在本轮打分集合内的条目保持
		// Strength=Unset，绝不伪造"打分后落选 NONE"结论。
		if w.ScoredSymbols != nil && !w.ScoredSymbols[e.Symbol] {
			continue
		}
		ts := w.Trend[e.Symbol]
		o := strengthOutcome{
			R20Stock: e.RetShortRaw, R60Stock: e.RetLongRaw,
			R20Index: w.BenchR20, R60Index: w.BenchR60,
			R20Sector:      w.SectorR20[e.SectorName],
			CloseAboveMA20: ts.CloseGeMA20, MA20AboveMA60: ts.MA20GeMA60, TrendKnown: ts.Known,
			EligPass:   e.Eligibility != EligBlocked,
			LiqPass:    e.Liquidity == "" || e.Liquidity == LiqPass || e.Liquidity == LiqWarn,
			FinBlocked: e.Financial == FinBlocked,
		}
		det := classifyPathA_B(o)
		if w.SingleWindowShortOnly && det.path == StrengthB && o.R20Stock != nil && o.R60Stock == nil {
			if ex := rsExcess(o.R20Stock, o.R20Index); ex != nil && *ex > 0 {
				// 阶段 5 锁定语义：基准长窗不可得、短期强于大盘 -> 视为短期强势 A（退化口径）。
				det.path = StrengthA
				det.reason = "A(单窗口退化): 基准长窗不可得, 短期强于大盘即视为短期强势"
			}
		}
		e.Strength = det.path
		e.StrengthReason = det.reason
		v := round2(ptrOr(det.rs20Index, 0))
		e.RS20IndexExcess = &v
		if det.rs20Sector != nil {
			v2 := round2(*det.rs20Sector)
			e.RS20SectorExcess = &v2
		}
	}

	// ---- 2. 四因子百分位（只在股票类候选集合内比较, 消除量纲）----
	partIdx := make([]int, 0, len(kept))
	fIdx := [4][]*float64{{}, {}, {}, {}}
	for i := range kept {
		if !isStock(kept[i]) {
			continue
		}
		if w.ScoredSymbols != nil && !w.ScoredSymbols[kept[i].Symbol] {
			continue // 未经本轮打分链路 -> 不参与百分位, 保持 Unset（不伪造结论）
		}
		fIdx[0] = append(fIdx[0], kept[i].RS20IndexExcess)
		fIdx[1] = append(fIdx[1], kept[i].RetLongRaw)
		fIdx[2] = append(fIdx[2], kept[i].RS20SectorExcess)
		fIdx[3] = append(fIdx[3], w.Adv[kept[i].Symbol])
		partIdx = append(partIdx, i)
	}
	var pcts [4][]float64
	for fi := 0; fi < 4; fi++ {
		pcts[fi] = percentileScores(fIdx[fi])
	}
	// ---- 阶段 6/7 时序修复：WARN 降权必须在百分位合成之前施加 ----
	// 旧实现把降权放在百分位重写之后（对最终分 ×0.5），n=2 时看似成立；但 n≥3
	// 时百分位会彻底抹平阶段5 已施加的 ×0.5 降权（WARN 票 RS 超额居中 → 百分位
	// 反而高于低 RS 的 PASS 票），"WARN 名次低于同分 PASS"契约随候选数量失真。
	// 正确做法：以"阶段5 快照 RSScore × 惩罚"作为第 0 因子(RS20_index 超额)的
	// 相对质量输入参与百分位——惩罚信息被百分位保留且量纲统一；随后再对最终分
	// 显式施加一次惩罚作双保险。不会平方衰减：百分位输入读的是上一阶段快照，
	// 本轮改写后的 RankScore 不回灌该列（RefineScores 幂等重建同理）。
	penaltyOf := map[string]float64{}
	for _, i := range partIdx {
		penaltyOf[kept[i].Symbol] = finRankPenalty(kept[i].Financial)
	}
	if len(partIdx) > 0 {
		col0 := make([]*float64, len(partIdx))
		for m, i := range partIdx {
			base := fIdx[0][m]
			p := penaltyOf[kept[i].Symbol]
			if base == nil {
				continue
			}
			v := *base
			if p != 1.0 {
				if pre, ok := w.RSPrescore[kept[i].Symbol]; ok {
					v = pre // 有阶段5快照者: 用快照作相对质量输入（快照已含降权, 不再乘 p）
				} else {
					vd := round2(v * p)
					v = vd
				}
				base = &v
			}
			col0[m] = base
		}
		pcts[0] = percentileScores(col0)
	}
	nA, nB, nNone, nUnscored := 0, 0, 0, 0
	for m, i := range partIdx {
		f := [4]*float64{fIdx[0][m], fIdx[1][m], fIdx[2][m], fIdx[3][m]}
		p := [4]float64{pcts[0][m], pcts[1][m], pcts[2][m], pcts[3][m]}
		score, used := weightedScore(f, p)
		if used == 0 {
			kept[i].Scored = false
			nUnscored++
			continue // 全部因子不可得 -> 保持未打分（区别于"打分后落选 NONE"）
		}
		pp := penaltyOf[kept[i].Symbol]
		if w.RSPrescore != nil {
			if v, ok := w.RSPrescore[kept[i].Symbol]; ok {
				kept[i].RSScore = round2(v * pp) // 快照同步镜像降权（两口径名次关系一致）
			}
		}
		kept[i].RankScore = round2(score * pp) // 最终分显式施加惩罚（WARN×0.5）
		kept[i].Scored = true
		switch kept[i].Strength {
		case StrengthA:
			nA++
		case StrengthB:
			nB++
		default:
			nNone++
		}
	}
	*warnings = append(*warnings, fmt.Sprintf("STRENGTH 路径判定完成: A %d, B %d, NONE %d, 未打分 %d(评分口径 0.35·P(RS20_index)+0.25·P(R60)+0.25·P(RS20_sector)+0.15·P(ADV20), 百分位0-100)", nA, nB, nNone, nUnscored))

	// ---- L2 趋势分类审计（分层架构: R20/R60/RS 是分类特征, 不是统一淘汰条件）----
	// 五档口径：强势(A 完整确认) / 转强(B 短期修复) / 震荡(R20≤0 但 R60>0 或结构未知)
	// / 弱势(R20、R60 均为负——潜在一买观察对象, 保留研究资格) / 数据不足。
	if g.cfg.TieredPipeline {
		nStrong, nTurn, nRange, nWeak, nInsuf2 := 0, 0, 0, 0, 0
		for m, i := range partIdx {
			_ = m
			e := &kept[i]
			switch {
			case e.Strength == StrengthA:
				nStrong++
			case e.Strength == StrengthB:
				nTurn++
			case e.RetShortRaw == nil && e.RetLongRaw == nil:
				nInsuf2++
			case ptrOr(e.RetShortRaw, 0) <= 0 && ptrOr(e.RetLongRaw, 0) <= 0:
				nWeak++
			default:
				nRange++
			}
		}
		*warnings = append(*warnings, fmt.Sprintf("TREND_CLASS 分类(L2, 不淘汰): 强势 %d, 转强 %d, 震荡 %d, 弱势 %d(潜在反转观察, 保留入池), 数据不足 %d",
			nStrong, nTurn, nRange, nWeak, nInsuf2))
	}

	// ---- 3. A/B 分别排序 + 独立 TopN 截断（分层口径下被截者降级保留, 不删除）----
	var segA, segB, segRest []AggEntry
	for i := range kept {
		switch {
		case !isStock(kept[i]):
			segRest = append(segRest, kept[i]) // ETF/MANUAL 直通段保持原相对顺序
		case kept[i].Strength == StrengthA:
			segA = append(segA, kept[i])
		case kept[i].Strength == StrengthB:
			segB = append(segB, kept[i])
		default:
			segRest = append(segRest, kept[i])
		}
	}
	// WARN 财务降权必须同时决定段内次序：百分位评分在只有少数候选时极易并列
	// （n=2 时两票各自因子不是 0 就是 100, 归一化后同为 50）——若只靠 SliceStable
	// 依赖输入顺序, "WARN×0.5 排后"契约会被并列静默吞掉。显式以 RankScore(已含
	// ×0.5 降权)为主键、RSScore(阶段5 粗排快照, 同样镜像了降权)为次键排序,
	// 原始输入顺序作最终 tiebreak, 保证同分 WARN 稳定排在 PASS 之后。
	downrankLess := func(a, b AggEntry) bool {
		if a.RankScore != b.RankScore {
			return a.RankScore > b.RankScore
		}
		if a.RSScore != b.RSScore {
			return a.RSScore > b.RSScore
		}
		return false // SliceStable: 保持原相对顺序
	}
	sort.SliceStable(segA, func(i, j int) bool { return downrankLess(segA[i], segA[j]) })
	sort.SliceStable(segB, func(i, j int) bool { return downrankLess(segB[i], segB[j]) })
	demote := func(list []AggEntry, topN int, label string) ([]AggEntry, []AggEntry) {
		if topN > 0 && len(list) > topN {
			*warnings = append(*warnings, fmt.Sprintf("STRENGTH 路径%s 截断弱尾 %d 只(保留前 %d)", label, len(list)-topN, topN))
			return list[:topN], list[topN:]
		}
		return list, nil
	}
	var demotedA, demotedB []AggEntry
	segA, demotedA = demote(segA, g.cfg.PathATopN, "A")
	segB, demotedB = demote(segB, g.cfg.PathBTopN, "B")
	if g.cfg.TieredPipeline && (len(demotedA)+len(demotedB)) > 0 {
		// 分层口径：PathA/PathB TopN 是"确认队列名额", 不是删除开关——被截掉的
		// 强势/转强股票降级保留在观察队列尾部（segRest, 不占 A/B 段位与连续 RankPos）。
		// 非分层模式维持旧行为（直接丢弃）以兼容既有回归测试基线。
		*warnings = append(*warnings, fmt.Sprintf("L2 分层保留: A/B TopN 截断的 %d 只降级进入观察队列(不删除)", len(demotedA)+len(demotedB)))
		segRest = append(append(segRest, demotedA...), demotedB...)
	}

	// ---- 4. 重组输出: A 段 -> B 段 -> 其余段; RankPos 只对打分股票连续编号 ----
	// 分层保留标志（DemotedToWatch）优先：被 RSTopK/TopN 移出确认段位的条目即使
	// 落入 NONE 段也不得重新获得确认名次（否则阶段5 的降级会被重编号静默撤销）。
	out := make([]AggEntry, 0, len(kept))
	pos := 0
	assign := func(e AggEntry, numbered bool) AggEntry {
		if e.DemotedToWatch {
			e.RankPos = 0
			return e
		}
		if numbered {
			pos++
			e.RankPos = pos
		} else {
			e.RankPos = 0
		}
		return e
	}
	for _, seg := range [][]AggEntry{segA, segB} {
		for _, e := range seg {
			out = append(out, assign(e, true))
		}
	}
	for _, e := range segRest {
		out = append(out, assign(e, isStock(e) && e.Scored))
	}
	return out
}

func ptrOr(p *float64, def float64) float64 {
	if p == nil {
		return def
	}
	return *p
}

// maTrendFromBars 从升序前复权收盘序列计算 Close≥MA20 / MA20≥MA60。
// 样本不足（<61 根）-> known=false（趋势结构不可确认, A 路径自动落空, 不影响 B）。
// maValuesFromBars 返回 (最新收盘价, MA20, MA60)。样本不足对应窗口时该项返回 nil
// （阶段 8 daily_stock_metrics 快照字段; 缺失诚实为 NULL, 绝不伪造）。
func maValuesFromBars(closes []float64) (last, m20, m60 *float64) {
	ma := func(n int) *float64 {
		if len(closes) < n {
			return nil
		}
		s := 0.0
		for _, c := range closes[len(closes)-n:] {
			s += c
		}
		v := s / float64(n)
		return &v
	}
	if len(closes) > 0 {
		v := closes[len(closes)-1]
		last = &v
	}
	return last, ma(20), ma(60)
}

func maTrendFromBars(closes []float64) (closeGeMa20, ma20GeMa60, known bool) {
	if len(closes) < 61 {
		return false, false, false
	}
	ma := func(n int) float64 {
		s := 0.0
		for _, c := range closes[len(closes)-n:] {
			s += c
		}
		return s / float64(n)
	}
	c20, c60 := ma(20), ma(60)
	last := closes[len(closes)-1]
	return last >= c20, c20 >= c60, true
}

// RefineScores 阶段 7 二次精化：liquidityGatePool 已把 ADV20 写入条目快照
// (AvgAmount20D, 单位元)后调用。若 refinePool 未产出强度包（精筛关闭/基准不可得），
// 原样返回。幂等设计：重复调用只会用最新 Adv 快照重算百分位与分段排序。
func (g *Generator) RefineScores(pool []AggEntry, warnings *[]string) []AggEntry {
	if g.strengthWiring == nil {
		return pool
	}
	for i := range pool {
		if pool[i].AvgAmount20D != nil {
			g.strengthWiring.Adv[pool[i].Symbol] = pool[i].AvgAmount20D
		}
	}
	w := *g.strengthWiring
	out := g.applyStrengthPathsAndScores(pool, w, warnings)
	// 幂等性关键：applyStrengthPathsAndScores 会把 RankScore 重写为百分位口径，
	// 若不把本轮结果镜像回 wiring，二次调用（如 Generate 的 ADV 精化）会用已被
	// 改写的分数刷新 RSPrescore 快照，导致 WARN×0.5 降权被重复施加（平方衰减），
	// 名次关系随之脱节。因此每次精化后同步重建两个 map（以最新一轮打分集合为准）。
	newPre := make(map[string]float64, len(out))
	newScored := make(map[string]bool, len(out))
	for i := range out {
		// 判据必须是 Scored && Strength!=Unset: RSScore==0 是合法粗排快照(个股与基准涨幅相等,
		// 或 WARN×0.5 归零)——旧实现用 RSScore!=0 作代理判据会把这类票静默移出打分集合,
		// 下一轮因 ScoredSymbols 门控保持 Strength=Unset 落入 segRest 直通段, 破坏
		// "A→B→其余" 分段排序契约(TestPipelineWarnDownrankAndProvenance 锁定)。
		if !out[i].Scored || out[i].Strength == StrengthUnset {
			continue // 未经阶段5打分者（ETF/未知放行/未打分）不进快照
		}
		newPre[out[i].Symbol] = out[i].RSScore
		newScored[out[i].Symbol] = true
	}
	w.RSPrescore = newPre
	w.ScoredSymbols = newScored
	g.strengthWiring = &w
	return out
}
