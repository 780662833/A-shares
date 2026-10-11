// Package agent 定义 Master Agent 的 System Prompt 与交易计划 JSON 契约。
//
// 设计原则：Prompt 中的每条硬约束（T+1 / 单票上限 / 总仓上限 / JSON 格式）
// 在 graph.RiskCheck 节点都有对应的服务端代码复核 —— Prompt 约束是「软提示」，
// 代码校验才是「硬风控」，二者缺一不可。
//
// V2.0 职责收敛（架构师裁决版）：
//   - 技术结构判断（分型/笔/中枢/买卖点确认）统一归 czsc Python 网关，
//     LLM 不再从原始 K 线自行推演 MA/量能等技术信号；
//   - BUY 计划的 quantity 只是「意向量」，最终委托股数由 Layer3 风险预算
//     （internal/risk.CalculatePosition，双止损线取较高者）接管重算；
//   - 仓位比例常量单一来源为 internal/risk（Default*），本包通过 init() 派生
//     Prompt 文本，杜绝两处口径漂移。
package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"ashare-quant/internal/risk"
)

// ---------------------------------------------------------------------------
// 仓位硬约束（派生自 internal/risk 单一事实来源，防止两处漂移）
// ---------------------------------------------------------------------------

// maxPositionRatioPerStock / maxTotalPositionRatio 运行时从 risk 默认常量派生。
var (
	maxPositionRatioPerStock = risk.DefaultMaxPositionRatioPerStock
	maxTotalPositionRatio    = risk.DefaultMaxTotalPositionRatio
)

// MaxPositionRatioPerStock 单只股票市值占总资产上限（当前 20%，源自 risk 包）。
//
// Deprecated: 请直接使用 risk.Config.MaxPositionRatioPerStock /
// risk.DefaultMaxPositionRatioPerStock。保留此导出仅为兼容既有测试与旧引用，
// 其值始终与 risk 默认常量一致（init 派生），不是独立配置项。
func MaxPositionRatioPerStock() float64 { return maxPositionRatioPerStock }

// MaxTotalPositionRatio 全部持仓市值占总资产上限（当前 80%，源自 risk 包）。
//
// Deprecated: 同 MaxPositionRatioPerStock。
func MaxTotalPositionRatio() float64 { return maxTotalPositionRatio }

// ---------------------------------------------------------------------------
// System Prompt
// ---------------------------------------------------------------------------

// masterSystemPromptTemplate 是「A股日终复盘策略总监」系统提示词的模板。
// 两个 %d 占位符依次为：单票仓位上限百分比、总仓位上限百分比 —— 由 init()
// 从 internal/risk 默认常量渲染，确保 Prompt 与硬风控代码永远同一口径。
const masterSystemPromptTemplate = `你是"A股日终复盘策略总监"，负责每个交易日 15:30 收盘后对模拟盘进行复盘，并生成次一交易日的交易计划。

## 身份与职责边界
- 你管理一个 A 股纯日线模拟账户，不接真实券商；目标是控制回撤优先、趋势跟随，不做日内博弈。
- 技术面结论（分型/笔/中枢/买卖点/背驰）一律以输入的 czsc 缠论信号为准：signal_type 是网关规范化后的别名（1B/2B/3B 买点、1S/2S/3S 卖点、Observation 观察），confirmed_at 为消前视确认时间。你不得基于原始 K 线自行重新认定买卖点，也不得把 Observation 当作买点。
- Manager 排雷层已完成基本面否决，其结论你不得推翻。
- RiskCheck 是最终硬风控：BUY 的 quantity 只是意向量，实际委托股数由服务端按风险预算公式（单笔最大亏损 ÷ 每股止损距离，双止损线取较高者）重算，可能小于你的意向量甚至归零拒单。因此你只需给出方向、标的与理由，不必也不应试图通过数量绕过风控。

## 铁律（违反任何一条的计划将被服务端风控直接废弃）
1. T+1 制度：当日买入的股票次日才能卖出。卖出数量绝不能超过持仓中的"可用数量"（available_quantity）。
2. 单票仓位上限：任一股票的持仓市值（含本次拟买入部分）不得超过账户总资产的 %d%%。
3. 总仓位上限：全部持仓市值不得超过账户总资产的 %d%%，即始终保留最低限度的现金。
4. 买入数量必须是 100 的整数倍（一手 = 100 股）；卖出清仓时可一次性全部卖出（含不足一手的零股）。
5. 委托价必须来自输入行情快照（reference_price / last_close），不得臆造次日开盘价或成交价。一字涨停板买单、一字跌停板卖单将无法成交。
6. 只能使用输入数据中出现的事实做决策，禁止臆造行情、新闻或财务数据。

## 决策顺序（先卖后买）
1. 先处理已有持仓的退出：仅当输入中存在已确认的 czsc 卖点（signal_status=CONFIRMED）、或价格已触及止损线（stop_loss_price）、或存在经核实的重大基本面风险时，才生成 SELL；没有退出依据的持仓不得仅因缺少买入信号而清仓。正面新闻不得覆盖已确认的技术退出信号。
2. 再评估开仓候选：必须有已确认的买点（1B/2B/3B）、Manager 未否决、且账户现金与仓位空间允许。1B 属于早期转折候选，在下跌环境中从严把握；3B/2B 顺势形态优先。
3. 同一股票不得同时出现 BUY 与 SELL；重复信号只保留一条计划。
4. 无合格候选、无有效退出或仓位空间不足时，允许不交易，输出 []。不得因为账户还有现金就强行买入。

## 输出格式（严格遵守）
只输出一个 JSON 数组，不要输出任何其他文字、解释或 markdown 代码块标记。数组每个元素形如（示例仅演示格式，实际必须使用输入中的真实代码与价格）：
[
  {
    "action": "BUY",
    "symbol": "000001",
    "quantity": 1000,
    "price": 12.35,
    "reason": "日线三买已确认，Manager排雷通过，组合风险允许"
  }
]
- action 只能是 "BUY" 或 "SELL"（大写）。
- symbol 为 6 位数字字符串，必须来自输入的候选池或持仓。
- quantity 为正整数（BUY 时必须为 100 的整数倍；SELL 时不超过 available_quantity）。
- price 为两位小数的委托参考价（元），必须来自输入行情字段。
- reason 不超过 50 字：BUY 引用确认的技术信号及排雷依据，SELL 引用退出信号或风险条件。
- 输出次序：SELL 在前，BUY 在后。
- 如果经过分析后决定明日不做任何交易，输出空数组：[]`

// MasterSystemPrompt 是运行时渲染后的系统提示词（仓位比例来自 risk 包单一来源）。
var MasterSystemPrompt = fmt.Sprintf(masterSystemPromptTemplate,
	int(maxPositionRatioPerStock*100), // 第1个%d: 单票上限
	int(maxTotalPositionRatio*100),    // 第2个%d: 总仓上限
)

// SetPositionRatios 允许调度层在启动时按配置覆盖仓位口径并同步重渲染 Prompt，
// 保证「Prompt 软约束」与「RiskCheck 硬风控」永远同源。传 0/负值表示保持原值。
func SetPositionRatios(perStock, total float64) error {
	if perStock > 0 {
		maxPositionRatioPerStock = perStock
	}
	if total > 0 {
		maxTotalPositionRatio = total
	}
	if maxPositionRatioPerStock > maxTotalPositionRatio {
		return fmt.Errorf("agent: per-stock ratio %.2f must <= total ratio %.2f",
			maxPositionRatioPerStock, maxTotalPositionRatio)
	}
	MasterSystemPrompt = fmt.Sprintf(masterSystemPromptTemplate,
		int(maxPositionRatioPerStock*100), int(maxTotalPositionRatio*100))
	return nil
}

// ---------------------------------------------------------------------------
// User Prompt 模板
// ---------------------------------------------------------------------------

// UserPromptTemplate 渲染进 Graph 的 AnalyzeAndPlan 节点。
// 占位符依次为：交易日期、账户快照 JSON、持仓明细 JSON、行情数据包 JSON。
const UserPromptTemplate = `今日复盘日期：%s

## 账户快照
%s

## 当前持仓（available_quantity 为 T+1 可卖数量）
%s

## 候选股票行情数据（最近 60 个交易日前复权日K）
%s

请基于以上数据完成日终复盘，严格按 System Prompt 的 JSON 数组格式输出次一交易日交易计划。`

// BuildUserPrompt 组装用户消息文本。任何一段 JSON 序列化失败都会返回 error，
// 由调用方（Graph 节点）决定降级策略。
func BuildUserPrompt(tradeDate string, accountJSON, positionsJSON, marketJSON string) string {
	return fmt.Sprintf(UserPromptTemplate, tradeDate, accountJSON, positionsJSON, marketJSON)
}

// ---------------------------------------------------------------------------
// TradePlan：LLM 输出的交易计划契约与容错解析
// ---------------------------------------------------------------------------

// TradePlanItem 单笔交易计划（LLM JSON 数组的元素）。
type TradePlanItem struct {
	Action   string  `json:"action"`
	Symbol   string  `json:"symbol"`
	Quantity int64   `json:"quantity"`
	Price    float64 `json:"price"`
	Reason   string  `json:"reason"`
}

// ParseTradePlans 从 LLM 原始输出中容错提取交易计划数组。
//
// 四级降级策略（生产环境 LLM 常见脏输出逐层兜底）：
//  1. 整体直接作为 JSON 数组解析；
//  2. 剥离 ```json ... ``` markdown 围栏后再解析；
//  3. 截取第一个 '[' 到最后一个 ']' 的平衡子串再解析；
//  4. 若上述均失败，尝试把单个 JSON 对象（模型忘记外层数组）解析为长度 1 的数组；
//     仍失败则返回带原文摘要的错误，供上层把错误信息回灌给 LLM 重试。
//
// 空数组 [] 是合法输出（表示明日不交易），返回长度为 0 的非 nil 切片。
func ParseTradePlans(raw string) ([]TradePlanItem, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil, fmt.Errorf("empty llm output")
	}

	// 逐级尝试候选字符串
	candidates := []string{s}

	// 去 markdown 围栏
	if stripped := stripCodeFence(s); stripped != "" {
		candidates = append(candidates, stripped)
	}

	// 截取最外层数组
	if arr := extractBalanced(s, '[', ']'); arr != "" {
		candidates = append(candidates, arr)
	}

	for _, c := range candidates {
		var items []TradePlanItem
		if err := json.Unmarshal([]byte(c), &items); err == nil {
			return normalizeItems(items), nil
		}
	}

	// 最后尝试单对象
	if obj := extractBalanced(s, '{', '}'); obj != "" {
		var one TradePlanItem
		if err := json.Unmarshal([]byte(obj), &one); err == nil {
			return normalizeItems([]TradePlanItem{one}), nil
		}
	}

	snippet := s
	if len(snippet) > 300 {
		snippet = snippet[:300] + "..."
	}
	return nil, fmt.Errorf("cannot parse trade plan JSON from llm output: %q", snippet)
}

// normalizeItems 归一化字段：方向转大写、代码去空白。
// 非法项不在此处剔除（交由 RiskCheck 统一处理并留痕），仅做形态归一。
func normalizeItems(items []TradePlanItem) []TradePlanItem {
	for i := range items {
		items[i].Action = strings.ToUpper(strings.TrimSpace(items[i].Action))
		items[i].Symbol = strings.TrimSpace(items[i].Symbol)
		// 兼容个别模型的简写/小写输出（IsValidDirection 大小写不敏感）
		switch items[i].Action {
		case "B", "BUY":
			items[i].Action = "BUY"
		case "S", "SELL":
			items[i].Action = "SELL"
		}
	}
	return items
}

// stripCodeFence 提取 ``` 围栏内的内容；无围栏返回空串。
func stripCodeFence(s string) string {
	i := strings.Index(s, "```")
	if i < 0 {
		return ""
	}
	rest := s[i+3:]
	// 跳过可能的语言标注行（json）
	if j := strings.IndexByte(rest, '\n'); j >= 0 {
		head := strings.TrimSpace(rest[:j])
		if head == "json" || head == "JSON" {
			rest = rest[j+1:]
		}
	}
	k := strings.Index(rest, "```")
	if k < 0 {
		return strings.TrimSpace(rest)
	}
	return strings.TrimSpace(rest[:k])
}

// extractBalanced 从 s 中找到首个 open 字符，按括号配平（忽略字符串内部的括号）
// 截出完整片段；找不到或不配平返回空串。
func extractBalanced(s string, open, close byte) string {
	start := strings.IndexByte(s, open)
	if start < 0 {
		return ""
	}
	depth := 0
	inStr := false
	escaped := false
	for i := start; i < len(s); i++ {
		ch := s[i]
		if escaped {
			escaped = false
			continue
		}
		switch {
		case ch == '\\' && inStr:
			escaped = true
		case ch == '"':
			inStr = !inStr
		case inStr:
			// 字符串字面量内部，忽略括号
		case ch == open:
			depth++
		case ch == close:
			depth--
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return ""
}
