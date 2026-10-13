// contract.go —— 阶段 0：固定数据契约（枚举状态 + 统一参数表 + 单位适配层约定）。
//
// 本文件只定义"数据契约"，不含筛选算法，防止后续实现出现格式不一致、
// 成交额单位错位和"空数据当作通过"三类事故：
//
//  1. 状态字段一律使用下方固定枚举类型（禁止裸字符串比较）；
//     "获取成功但结果为空" = SUCCESS_EMPTY，与 FAILED/PARTIAL 严格区分，
//     任何闸门逻辑不得把非 OK/SUCCESS_EMPTY 的状态当作"通过"。
//  2. 全部阈值集中在 Params()（第一版默认值），数值为研究起点而非历史最优；
//     ETF 与普通 A 股使用不同的流动性门槛（ETFMinADV20 / StockMinADV20）。
//  3. 成交额/市值单位契约：Provider 边界输出一律为"元"。stock-go 上游若以
//     "万元/亿元"返回，必须在 SDKProvider 适配层（provider.go）完成换算，
//     严禁把单位转换散落在筛选逻辑中。normalizeYiAmount 是适配层唯一入口。

package pool

import "fmt"

// ---------------------------------------------------------------------------
// 固定枚举：来源获取状态
// ---------------------------------------------------------------------------

// SourceStatus 描述单个数据来源一次取数的结果状态。
// 注意：SUCCESS_EMPTY 是"正常空结果"（如当日无满足阈值的龙虎榜买入），
// 与 FAILED（取数失败）语义完全不同——FAILED 不能伪装成空结果参与池子生成。
type SourceStatus string

const (
	StatusOK           SourceStatus = "OK"            // 成功且有数据
	StatusSuccessEmpty SourceStatus = "SUCCESS_EMPTY" // 成功且确认结果为空
	StatusPartial      SourceStatus = "PARTIAL"       // 部分分片成功（可信度降级）
	StatusStale        SourceStatus = "STALE"         // 成功但数据日期早于 as_of
	StatusFailed       SourceStatus = "FAILED"        // 取数失败（禁止当作空通过）
)

// Usable 报告该状态的数据是否可参与筛选判定。
// STALE 可用但调用方应记 warning；FAILED 绝对不可用。
func (s SourceStatus) Usable() bool { return s != StatusFailed }

// ---------------------------------------------------------------------------
// 固定枚举：证券资格 / 流动性 / 财务状态
// ---------------------------------------------------------------------------

// EligibilityStatus 证券交易资格状态（ST、退市风险等权威判定属于主数据 Provider，
// 名称含 "ST" 只是启发式代理，不能作为最终唯一依据 —— 见 provider.go 注释）。
type EligibilityStatus string

const (
	EligPass    EligibilityStatus = "PASS"    // 明确具备资格
	EligBlocked EligibilityStatus = "BLOCKED" // 明确不具备（ST/退市整理/暂停上市）
	EligUnknown EligibilityStatus = "UNKNOWN" // 主数据缺失 -> 按策略放行并计入统计
)

// LiquidityStatus 流动性闸门结果。DATA_INSUFFICIENT 表示样本不足无法计算 ADV，
// 绝不能等同于 PASS。
type LiquidityStatus string

const (
	LiqPass             LiquidityStatus = "PASS"
	LiqWarn             LiquidityStatus = "WARN"              // 达标但接近阈值
	LiqFail             LiquidityStatus = "FAIL"              // 明确不达标
	LiqDataInsufficient LiquidityStatus = "DATA_INSUFFICIENT" // 无法计算
)

// FinancialStatus 财务状态闸门结果。行业差异化阈值（银行/保险 vs 制造业）
// 在后续版本按 sector 覆盖，第一版仅做全局 EPS/ROE 底线。
type FinancialStatus string

const (
	FinPass             FinancialStatus = "PASS"
	FinWarn             FinancialStatus = "WARN"
	FinBlocked          FinancialStatus = "BLOCKED"
	FinDataInsufficient FinancialStatus = "DATA_INSUFFICIENT"
)

// StrengthPath 相对强度入选路径：A=短期强(R20)，B=中期强(R60)，NONE=未入选。
type StrengthPath string

const (
	// StrengthUnset 空值语义：RS 管线未执行（精筛关闭 / 基准不可得退化 / ETF / MANUAL-only）。
	// 必须与 StrengthNone（管线执行过但两条路径均未入选）区分, 否则"未打分"会被
	// ComputeTradability 吞成"打分后落选"而误判 BLOCKED。
	StrengthUnset StrengthPath = ""
	StrengthA     StrengthPath = "A"
	StrengthB     StrengthPath = "B"
	StrengthNone  StrengthPath = "NONE"
)

// ---------------------------------------------------------------------------
// 单位契约：成交额/金额一律"元"
// ---------------------------------------------------------------------------

// 单位换算常量（仅在 SDK 适配层使用）。
const (
	YuanPerWan = 1e4 // 万元 -> 元
	YuanPerYi  = 1e8 // 亿元 -> 元
)

// normalizeYiAmount 把"亿元"口径的上游金额统一换算为"元"。
// 适配层唯一入口：SDKProvider 中所有 Amount / NetInflow / OrgNetAmount /
// TotalMarketCap 字段若上游以亿元计，必须经此函数出界；筛选逻辑只见"元"。
func normalizeYiAmount(vYi float64) float64 { return vYi * YuanPerYi }

// formatYi 诊断输出用：元 -> 亿元字符串。
func formatYi(vYuan float64) string { return fmt.Sprintf("%.2f亿", vYuan/YuanPerYi) }

// ---------------------------------------------------------------------------
// 第一版参数表（研究起点，允许环境变量覆盖 / 回测再校准）
// ---------------------------------------------------------------------------

// Params 返回阶段 0 约定的第一版默认参数。
// 与 DefaultConfig() 的关系：DefaultConfig 即以此为准；差异项在注释中标明
// 当前实现口径与目标契约的差距，供阶段 1 Provider 重构时收敛。
func Params() Config {
	c := DefaultConfig()
	// —— 与契约表逐项对齐（数值口径均为"元"）——
	// SectorTopN=3, SectorLeadersK(SectorTopK)=3      已一致
	// ETFTopM=8                                        已一致
	// FundFlowMinNetBuy(LHBMinNetBuy)=5000万=5e7       已一致
	// FundFlowMinMarketCap(MinMarketCap)=50亿=5e9      已一致
	// StockMinPrice(MinPrice)=1元                      已一致
	// MinListingTradingDays=120个有效交易日; 当前 MinListedDays=120 自然日,
	//   自然日≈交易日*1.4, 阶段 1 由主数据 Provider 提供交易日计数后收紧。
	// R20/R60: 阶段 5 已实现双窗口打分（RSShortWindow=20 + RSLongWindow=60, 加权 RSShortWeightPct=60%）。
	// StrengthTopN: 对应 RSTopK, 建议区间 50~100; 默认 0(不截断)保持灰度期全量观察。
	c.RSTopK = 0 // 验收口径: 上线初期不截断, 回测后再设 50~100
	return c
}

// ETFMinADV20 / StockMinADV20 契约口径（元）。ETF 与股票分开：
//
//	StockMinADV20 = 5000万 (5e7) —— 普通 A 股最低 20 日平均成交额
//	ETFMinADV20   = 2000万 (2e7) —— ETF 最低 20 日平均成交额
//
// 当前实现的 MinAvgAmount=1e8 是迁移期沿用的旧 Python 口径（偏严），
// 契约值作为阶段 1 起点对齐；两者均可被 ASHARE_MIN_AVG_AMOUNT 覆盖。
const (
	ContractStockMinADV20  = 5e7 // 元
	ContractETFMinADV20    = 2e7 // 元
	ContractETFHistoryBars = 90  // ETF 日线观察窗口（当前 ETFLookback=60, 阶段 1 扩至 90）
)
