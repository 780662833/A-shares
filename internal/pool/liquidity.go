// liquidity.go —— 阶段 4：流动性过滤与交易容量。
//
// 两个独立判断（不互相替代）：
//
//	① 日常流动性 ADV20 = (1/20) * Σ Amount_i，Amount_i 为单个交易日成交额（人民币元），
//	   取最近 20 个"有效样本日"。第一版门槛：普通 A 股 ≥ StockMinADV20(5e7)，
//	   ETF ≥ ETFMinADV20(2e7)——ETF 使用单独配置，不套用股票规则；最新价格下限
//	   同样只对普通股票生效（StockMinPrice=1 元）。
//	② 交易容量 Participation = 计划下单金额 / ADV20，第一版上限 MaxParticipationPct=1%。
//	   候选池阶段 PlanNotional<=0（不知道最终仓位）时只输出 CAPACITY_UNCHECKED——
//	   "流动性通过"不等于"执行一定可行"，真正下单前必须依据最终计划数量重算。
//
// 三条必须落入测试的硬规则（见 liquidity_test.go）：
//
//	R1 成交额缺失不能自动视为零，也不能把缺失样本从分母中悄悄删除后仍宣称数据完整：
//	   有效样本 = Amount > 0 的行；缺失行计入 observed（观测窗口覆盖度诊断），
//	   分母恒为固定 20（ADV20 定义即 /20），因此缺样必然拉低均值并被阈值诚实拒绝，
//	   而不是"删掉分母假装达标"。
//	R2 最近 20 个有效交易日不足 20 个 → LiquidityStatus=DATA_INSUFFICIENT（不是 FAIL、更不是 PASS）。
//	R3 单位与复权口径：ADV 使用 Provider.StockAmounts 返回的"未复权实际成交额"（元），
//	   收益率/R20/R60 使用统一前复权收盘序列（refine.go），两套口径不得混用；
//	   单位换算只允许发生在适配层（provider.go StockAmounts / contract.go normalizeYiAmount），
//	   本文件内所有比较一律以"元"为单位。
package pool

import (
	"context"
	"fmt"
	"time"
)

// AdvResult ADV20 计算结果快照（每日指标快照的最小形态，供排序与风险模块复用）。
type AdvResult struct {
	Adv      float64 // 20 日均成交额（元）；Valid=false 时无意义
	Samples  int     // 有效样本数（Amount>0 的交易日）
	Observed int     // 实际观测到的日历行数（含缺失行，覆盖度诊断）
	Valid    bool    // 是否满足"至少 20 个有效样本"
}

// advWindow ADV20 固定观察窗（20 个交易日）。
const advWindow = 20

// adv20 按契约公式计算 20 日平均成交额。输入必须是升序、未复权、单位为元的日成交额行。
// 剔除无效数据（Amount<=0 视为缺失而非零，R1）→ 检查有效样本数（R2）→ 除以固定分母 20。
func adv20(rows []DailyAmountRow) AdvResult {
	res := AdvResult{}
	// 只看最近 advWindow 个观测行；行本身不足 20 → 连"观测满 20 天"都不成立
	recent := rows
	if len(recent) > advWindow {
		recent = recent[len(recent)-advWindow:]
	}
	for _, r := range recent {
		if r.Amount > 0 {
			res.Samples++
			res.Adv += r.Amount
		}
		// Amount<=0：缺失，跳过累加但保留在 Observed 中（不许当零参与均值）
	}
	res.Observed = len(recent)
	res.Valid = res.Samples >= advWindow
	res.Adv = res.Adv / float64(advWindow) // 分母恒为 20（R1：不随样本数缩水）
	return res
}

// minADVFor 按资产类型选择流动性门槛（元）。ETF 与普通股票分开配置（4.1 表）。
func (g *Generator) minADVFor(isETF bool) float64 {
	if isETF {
		if g.cfg.MinADV20ETF > 0 {
			return g.cfg.MinADV20ETF
		}
		return ContractETFMinADV20
	}
	if g.cfg.MinADV20Stock > 0 {
		return g.cfg.MinADV20Stock
	}
	return ContractStockMinADV20
}

// capacityStatus 交易容量判定：Participation = plan/ADV20，超过 MaxParticipationPct(%) 即 OVER。
// plan<=0（候选池阶段不知道最终仓位）→ UNCHECKED，只记录 ADV 供下游使用，绝不伪装成通过。
func capacityStatus(plan, adv float64, maxPct float64) string {
	if plan <= 0 || adv <= 0 {
		return CapacityUnchecked
	}
	part := plan / adv * 100
	if part > maxPct {
		return CapacityOver
	}
	return CapacityOK
}

// 交易容量状态常量（capacity_status，供排序与风险模块消费；下单前必须重算）。
const (
	CapacityOK        = "OK"        // 参与率 ≤ 上限
	CapacityOver      = "OVER"      // 超限：需降低数量或分析执行成本
	CapacityUnchecked = "UNCHECKED" // 候选池阶段无计划金额，未校验
)

// liquidityGatePool 对精筛池逐只执行阶段 4 流动性闸门 + 容量标注。
// 与 refinePool 同级的弱依赖原则：单票取数失败 → DATA_INSUFFICIENT（未知），
// 不剔除记录（研究资格保留），tradability 经 QUOTE_STATE 路径自然降级为 UNKNOWN。
// 被明确判 FAIL 的股票类条目剔除并记聚合 warning；全军覆没视为数据事故退化放行。
//
// A.4 新鲜度透传链（tradeDate/srcDates 参数）：ADV20 计算窗口内最后一个通过
// normalizeBusinessDate 验证的成交额行日期，写入条目 LiqDataAsOf（内存态诊断
// 字段，round-trip 丢弃、不扩 schema）。任何一根未通过验证的行都不参与该结论；
// 全部不可证时保持空串——诚实未知，绝不兜底成运行日。srcDates 当前仅作调用
// 侧接线保留（来源级日期与本闸门的行级日期语义不同，不互相替代）。
func (g *Generator) liquidityGatePool(ctx context.Context, pool []AggEntry, warnings *[]string, tradeDate string, srcDates map[string]string) []AggEntry {
	if !g.cfg.LiquidityEnabled {
		return pool
	}
	_ = srcDates // 接线保留：来源级可信日期映射不参与本闸门的行级日期判定。
	bound := tradeDate
	if _, err := time.Parse("2006-01-02", bound); err != nil {
		bound = tradeDateOf(time.Now()) // 评估日非法时以上海时区当日为上界，仅用于未来边界校验
	}
	type outcome struct {
		res   AdvResult
		fetch bool // 取数成功
		fail  bool // 明确 FAIL（样本足但额不足）
	}
	results := make([]outcome, len(pool))

	// 同步实现（池子规模 ≤ 数百，串行足够；保持与既有代码一致的确定性）
	for i := range pool {
		e := &pool[i]
		isETF := e.PrimarySource == sourceETF || e.AssetType == "ETF"
		rows, err := g.prov.StockAmounts(ctx, e.Symbol, advWindow, time.Now())
		if err != nil || len(rows) == 0 {
			e.Liquidity = LiqDataInsufficient
			e.CapacityStatus = CapacityUnchecked
			continue
		}
		res := adv20(rows)
		v := res.Adv
		e.AvgAmount20D = &v
		// A.4：ADV20 窗口内最后一个有效样本行（Amount>0，即实际参与均值计算）
		// 的业务日期，逐行验证后取最大者；不可证则留空（诚实未知）。
		for j := len(rows) - 1; j >= 0; j-- {
			if rows[j].Amount <= 0 {
				continue
			}
			if d, dErr := normalizeBusinessDate(rows[j].Date, bound); dErr == nil {
				e.LiqDataAsOf = d
			}
			break
		}
		switch {
		case !res.Valid:
			// R2：有效样本 < 20 → DATA_INSUFFICIENT（不 FAIL、不 PASS）
			e.Liquidity = LiqDataInsufficient
		case res.Adv < g.minADVFor(isETF):
			e.Liquidity = LiqFail
			results[i].fail = true
		case res.Adv < g.minADVFor(isETF)*1.2:
			e.Liquidity = LiqWarn // 达标但接近阈值（余量 <20%）
		default:
			e.Liquidity = LiqPass
		}
		results[i].res = res
		results[i].fetch = true
		e.CapacityStatus = capacityStatus(g.cfg.PlanNotional, res.Adv, g.cfg.MaxParticipationPct)
		if res.Observed < advWindow && res.Valid {
			*warnings = append(*warnings, fmt.Sprintf("LIQ %s 观测行%d<20但样本满(疑似上游补齐)", e.Symbol, res.Observed))
		}
	}

	kept := make([]AggEntry, 0, len(pool))
	var failN, insN int
	for i, e := range pool {
		if results[i].fail && e.PrimarySource != sourceManual {
			failN++
			continue
		}
		if e.Liquidity == LiqDataInsufficient {
			insN++
		}
		kept = append(kept, e)
	}
	if failN > 0 {
		*warnings = append(*warnings, fmt.Sprintf("LIQ 流动性闸门剔除 %d 只(ADV20不足), 数据不足 %d 只(保留标DATA_INSUFFICIENT)", failN, insN))
	} else if insN > 0 {
		// 可审计性：即使 0 剔除也必须输出 DATA_INSUFFICIENT 计数——否则"闸门执行过
		// 但全部放行"与"闸门未执行"在日志里不可区分（L4 分层统计契约）。
		*warnings = append(*warnings, fmt.Sprintf("LIQ 流动性闸门完成: 剔除 0 只(ADV20均达标或未知放行), 数据不足 %d 只(保留标DATA_INSUFFICIENT)", insN))
	}
	if len(kept) == 0 && failN > 0 {
		*warnings = append(*warnings, fmt.Sprintf("LIQ 流动性闸门剔除了全部 %d 条(疑似数据事故, 已整体退化为不设闸门)", failN))
		return pool
	}
	return kept
}
