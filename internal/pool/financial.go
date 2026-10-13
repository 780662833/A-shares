package pool

// ============================================================================
// 阶段 6：财务状态与重大风险过滤（独立 FinancialProvider）
//
// 设计原则（对齐用户契约）：
//  1. stock-go 公开 API 矩阵只有行情/资金流/龙虎榜/K线，没有完整财报、ROE、
//     经营现金流、审计意见服务 —— 因此财务数据必须走独立供应商接口，本文件
//     只定义契约 + 纯判定逻辑，SDKProvider 不实现它（返回 ErrNotWired 诚实失败）。
//  2. 不把一般财务弱项设为硬性排除：质量问题 -> WARN（排序降权），
//     有充分依据的重大风险（造假/审计非标/重大违规担保）-> BLOCKED。
//  3. DATA_INSUFFICIENT 绝不默认"财务已通过"：字段缺失/过期/时点未知一律
//     FinDataInsufficient，tradability=UNKNOWN（见 ComputeTradability）。
//  4. 防未来函数：回测只能使用 announcement_date <= as_of 的快照；
//     data_as_of 超出 MaxAgeDays 视为 STALE -> DATA_INSUFFICIENT。
//  5. 金融行业（银行/保险/证券）不适用制造业阈值（如 ROE>=8%），第一版对
//     金融股仅执行重大风险检查，质量指标记 WARN(行业规则待单独校准)。
// ============================================================================

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrFinancialNotWired 未接入财务数据供应商时的诚实错误（不得伪造空成功）。
var ErrFinancialNotWired = errors.New("pool: financial provider not wired (stock-go 无财报 API, 需另接供应商)")

// AuditOpinionKind 最近年度审计意见类型（固定枚举）。
type AuditOpinionKind string

const (
	AuditUnset      AuditOpinionKind = ""
	AuditStandard   AuditOpinionKind = "STANDARD_UNQUALIFIED" // 标准无保留
	AuditEmphasis   AuditOpinionKind = "EMPHASIS_ONLY"        // 带强调事项段的无保留
	AuditQualified  AuditOpinionKind = "QUALIFIED"            // 保留意见
	AuditAdverse    AuditOpinionKind = "ADVERSE"              // 否定意见
	AuditDisclaimer AuditOpinionKind = "DISCLAIMER"           // 无法表示意见
)

// MajorRiskFlag 已核实的重大风险标志。
type MajorRiskFlag struct {
	Code         string `json:"code"`
	Kind         string `json:"kind"`          // FRAUD / MAJOR_LITIGATION / ABNORMAL_GUARANTEE / DELIST_RISK ...
	Evidence     string `json:"evidence"`      // 核实依据（公告编号/处罚文号），不允许空口断言
	AnnounceDate string `json:"announce_date"` // 风险披露日期
}

// FinSnapshot 单只证券的财务快照。四个时间/来源字段是契约硬要求：
// report_period（报告期）、announcement_date（实际公告日）、data_as_of（数据截止）、provider（供应商）。
type FinSnapshot struct {
	Symbol string `json:"symbol"`

	// ---- 质量指标（非金融类适用）----
	TtmNetProfit   *float64 `json:"ttm_net_profit"` // 最近滚动12个月净利润（元）
	OpCFYearly     *float64 `json:"op_cf_yearly"`   // 最近年度经营现金流（元）
	OpCFPrevYearly *float64 `json:"op_cf_prev"`     // 上一年度经营现金流（元）
	ROE3yMedian    *float64 `json:"roe_3y_median"`  // 过去3年 ROE 中位数（%）
	RevenueYoY     *float64 `json:"revenue_yoy"`    // 营收同比（%）
	ProfitYoY      *float64 `json:"profit_yoy"`     // 归母净利同比（%）
	DeclineYears   int      `json:"decline_years"`  // 收入&利润连续双降年数（-1=未知）

	// ---- 风险与元数据 ----
	Audit          AuditOpinionKind `json:"audit_opinion"`
	MajorRisks     []MajorRiskFlag  `json:"major_risks"`
	IsFinancialSec bool             `json:"is_financial_sector"` // 银行/保险/证券 -> 单独规则集
	ReportPeriod   string           `json:"report_period"`       // e.g. "2025Q4"/"2025-12-31"
	AnnouncementDt string           `json:"announcement_date"`   // e.g. "2026-03-31" 实际公告日
	DataAsOf       string           `json:"data_as_of"`          // 供应商数据截止日
	Source         string           `json:"provider"`            // 数据来源供应商标识
}

// FinancialProvider 独立财务数据接口。生产环境由外部财报供应商实现；
// 未接入时 GetFinSnapshot 必须返回错误（ErrFinancialNotWired），绝不能返回
// 空快照伪装 SUCCESS_EMPTY —— 财务缺失与财务合格是两回事。
type FinancialProvider interface {
	GetFinSnapshot(ctx context.Context, code string, asOf time.Time) (*FinSnapshot, error)
}

// ---------------------------------------------------------------------------
// 第一版参数（研究起点，非历史验证最优值；金额单位一律"元"）
// ---------------------------------------------------------------------------

const (
	FinMinROE3yMedianPct = 8.0 // 3年ROE中位数研究阈值（非金融）
	FinWarnParticipation = 0.5 // WARN 相对 PASS 的排序降权系数（乘在 RankScore 上）
	FinMaxAgeDays        = 400 // 快照 data_as_of 距 as_of 超过该自然日 -> STALE/DATA_INSUFFICIENT
	FinMinProfitYuan     = 0.0 // TTM 净利润 > 0 作为质量加分条件（<=0 -> WARN）
)

// finThresholds 允许按行业覆盖的阈值组（第一版仅 非金融/金融 两组）。
type finThresholds struct {
	minROEMedian float64 // <=0 关闭该项
	checkQuality bool    // 金融板块第一版跳过质量阈值组，只做重大风险
}

func thresholdsFor(isFinancial bool) finThresholds {
	if isFinancial {
		// 银行/保险的 ROE、现金流口径与制造业不可比 —— 单独规则集尚未校准，
		// 第一版仅执行重大风险/审计意见检查，质量维度不判 FAIL/WARN。
		return finThresholds{minROEMedian: 0, checkQuality: false}
	}
	return finThresholds{minROEMedian: FinMinROE3yMedianPct, checkQuality: true}
}

// ---------------------------------------------------------------------------
// 判定核心（纯函数，全部可单测）
// ---------------------------------------------------------------------------

// classifyFinSnapshot 按契约四态判定财务状态。优先级：
// BLOCKED（重大风险/审计否定或无法表示）> DATA_INSUFFICIENT（关键时点缺失/过期/
// 公告日晚于 as_of 的未来数据）> WARN（质量弱项）> PASS。
// 返回 (状态, 原因列表)。
func classifyFinSnapshot(s *FinSnapshot, asOf time.Time) (FinancialStatus, []string) {
	var reasons []string
	if s == nil {
		return FinDataInsufficient, []string{"财务快照缺失(provider 未返回)"}
	}

	// ---- ① BLOCKED：已核实的重大风险（必须有 evidence，否则降级为 UNKNOWN 线索）----
	for _, r := range s.MajorRisks {
		if strings.TrimSpace(r.Evidence) == "" {
			reasons = append(reasons, fmt.Sprintf("重大风险线索缺核实依据(%s) -> 不计入BLOCKED", r.Kind))
			continue
		}
		switch r.Kind {
		case "FRAUD", "MAJOR_LITIGATION", "ABNORMAL_GUARANTEE", "DELIST_RISK":
			return FinBlocked, append(reasons, fmt.Sprintf("重大风险[%s] 依据:%s", r.Kind, r.Evidence))
		}
	}
	// 审计意见：否定/无法表示/保留 -> BLOCKED（硬条件，第一版不用评分抵消——
	// 保留意见意味着审计师对报表可信度存疑，属"审计结论核实后禁止新开仓"范畴；
	// 带强调事项段仍属无保留意见 -> WARN 提示关注）。
	switch s.Audit {
	case AuditAdverse, AuditDisclaimer, AuditQualified:
		return FinBlocked, append(reasons, fmt.Sprintf("审计意见%s(报告期%s)", s.Audit, s.ReportPeriod))
	case AuditEmphasis:
		reasons = append(reasons, fmt.Sprintf("审计带强调事项段(报告期%s)", s.ReportPeriod))
	}

	// ---- ② DATA_INSUFFICIENT：时点纪律 ----
	// 公告日校验：晚于本次 as_of 的财报属于未来数据，禁止用于当期决策（也说明
	// 该记录本身不应出现在实时链路 —— 供应商侧过滤失误，同样按数据不足处理）。
	if d, err := time.Parse("2006-01-02", dateOnly(s.AnnouncementDt)); err != nil || d.After(asOf) {
		return FinDataInsufficient, append(reasons, fmt.Sprintf("公告时点缺失或晚于as_of(ann=%q)", s.AnnouncementDt))
	}
	if d, err := time.Parse("2006-01-02", dateOnly(s.DataAsOf)); err != nil || asOf.Sub(d).Hours()/24 > float64(FinMaxAgeDays) {
		return FinDataInsufficient, append(reasons, fmt.Sprintf("数据过期/as_of缺失(data_as_of=%q, max_age=%d天)", s.DataAsOf, FinMaxAgeDays))
	}
	if strings.TrimSpace(s.Source) == "" {
		return FinDataInsufficient, append(reasons, "缺少 provider 溯源标识")
	}

	th := thresholdsFor(s.IsFinancialSec)

	// ---- 金融板块第一版：不做质量阈值判定；只要走到这里即视为通过风险检查 ----
	if !th.checkQuality {
		return FinPass, append(reasons, "金融板块: 仅执行重大风险/审计检查(质量阈值另行校准)")
	}

	// ---- ③ WARN：一般质量弱项（不排除、只降权）----
	warned := false
	if s.Audit == AuditEmphasis {
		warned = true // 强调事项段本身即构成观察级风险（即使质量指标全优）
	}
	if s.TtmNetProfit == nil || s.ROE3yMedian == nil {
		// 关键字段缺失：不是"弱"，而是"不知道" —— 诚实归 DATA_INSUFFICIENT
		return FinDataInsufficient, append(reasons, "TTM净利或3年ROE中位数缺失")
	}
	if *s.TtmNetProfit <= FinMinProfitYuan {
		warned = true
		reasons = append(reasons, fmt.Sprintf("TTM净利%.0f万<=0(亏损成长股:保留观察,仓位从严)", *s.TtmNetProfit/1e4))
	}
	if *s.ROE3yMedian < th.minROEMedian {
		warned = true
		reasons = append(reasons, fmt.Sprintf("3年ROE中位数%.1f%%<%.0f%%", *s.ROE3yMedian, th.minROEMedian))
	}
	// 经营现金流持续明显为负：两年均负且绝对量级不小 -> WARN（比单年负更重）
	negCF := func(v *float64) bool { return v != nil && *v < 0 }
	if negCF(s.OpCFYearly) && negCF(s.OpCFPrevYearly) {
		warned = true
		reasons = append(reasons, fmt.Sprintf("经营现金流连续两年为负(%.0f万/%.0f万)",
			*s.OpCFYearly/1e4, *s.OpCFPrevYearly/1e4))
	} else if negCF(s.OpCFYearly) {
		warned = true
		reasons = append(reasons, fmt.Sprintf("最近年度经营现金流为负(%.0f万)", *s.OpCFYearly/1e4))
	}
	// 收入利润趋势：连续双降 >= 2 年，或单期双双大幅下滑
	if s.DeclineYears >= 2 {
		warned = true
		reasons = append(reasons, fmt.Sprintf("收入与利润连续%d年双降", s.DeclineYears))
	} else if s.RevenueYoY != nil && s.ProfitYoY != nil && *s.RevenueYoY < -20 && *s.ProfitYoY < -30 {
		warned = true
		reasons = append(reasons, fmt.Sprintf("营收/净利异常下滑(%.1f%%/%.1f%%)", *s.RevenueYoY, *s.ProfitYoY))
	}
	if warned {
		return FinWarn, reasons
	}
	return FinPass, reasons
}

// finRankPenalty WARN 排序降权系数：PASS/其他=1.0，WARN=0.5。
// （能否交易不由这里决定 —— tradability 由 ComputeTradability 统一推导。）
func finRankPenalty(st FinancialStatus) float64 {
	if st == FinWarn {
		return FinWarnParticipation
	}
	return 1.0
}

// snapshotMetaFields 提取契约要求的四个溯源字段，写入 AggEntry 供落库/审计。
func (s *FinSnapshot) metaFields() (period, announced, dataAsOf, provider string) {
	if s == nil {
		return "", "", "", ""
	}
	return s.ReportPeriod, s.AnnouncementDt, s.DataAsOf, s.Source
}

func dateOnly(s string) string {
	if len(s) >= 10 {
		return s[:10]
	}
	return s
}
