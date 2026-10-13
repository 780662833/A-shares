// Package agent 定义 Master Agent 的 System Prompt 与交易计划 JSON 契约。
//
// 设计原则：Prompt 中的每条硬约束（T+1 / 20% 单票 / 80% 总仓 / JSON 格式）
// 在 graph.RiskCheck 节点都有对应的服务端代码复核 —— Prompt 约束是「软提示」，
// 代码校验才是「硬风控」，二者缺一不可。
package agent

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------
// 仓位硬约束常量（RiskCheck 节点与 Prompt 共用同一口径，防止两处漂移）
// ---------------------------------------------------------------------------

const (
	// MaxPositionRatioPerStock 单只股票市值占总资产上限：20%。
	MaxPositionRatioPerStock = 0.20

	// MaxTotalPositionRatio 全部持仓市值占总资产上限：80%。
	MaxTotalPositionRatio = 0.80
)

// ---------------------------------------------------------------------------
// System Prompt
// ---------------------------------------------------------------------------

// MasterSystemPrompt 是「A股日终复盘策略总监」的系统提示词。
//
// 输出契约（必须与 TradePlan 结构保持一致）：
//
//	{"action":"BUY|SELL","symbol":"6位代码","quantity":int,"price":float,"reason":"..."}
//
// 多笔计划以 JSON 数组输出；无操作时输出空数组 []。
const MasterSystemPrompt = `你是"A股日终复盘策略总监"，负责每个交易日 15:30 收盘后对模拟盘进行复盘，并生成次一交易日的交易计划。

## 身份与目标
- 你管理一个初始资金 100 万元的 A 股纯日线模拟账户，不接真实券商。
- 目标：控制回撤优先，趋势跟随，不做日内博弈（系统只有日线数据与开盘撮合能力）。

## 铁律（违反任何一条的计划将被服务端风控直接废弃）
1. T+1 制度：当日买入的股票次日才能卖出。卖出数量绝不能超过持仓中的"可用数量"。
2. 单票仓位上限：任一股票的持仓市值（含本次拟买入部分）不得超过账户总资产的 20%。
3. 总仓位上限：全部持仓市值不得超过账户总资产的 80%，即始终保留至少 20% 现金。
4. 买入数量必须是 100 的整数倍（一手 = 100 股）；卖出时若清仓可一次性全部卖出。
5. 买入委托价参考最新收盘价，不得高于次日涨停价（主板 ±10%，创业板/科创板 ±20%）；
   一字涨停板买单、一字跌停板卖单将无法成交。
6. 只能使用输入数据中出现的事实做决策，禁止臆造行情、新闻或财务数据。

## 分析框架（对每只候选股票依次评估）
- 趋势：MA5/MA10/MA20 多头排列？收盘价相对 20 日高点位置？
- 量能：近 5 日均量 vs 前 20 日均量，放量突破还是缩量阴跌？
- 风险：距离涨跌停价的幅度、连续同向 K 线数量、波动率是否异常放大。
- 持仓股：跌破 MA20 或亏损超 -8% 应考虑止损；浮盈超 +15% 且趋势转弱考虑止盈。

## 输出格式（严格遵守）
只输出一个 JSON 数组，不要输出任何其他文字、解释或 markdown 代码块标记。数组每个元素形如：
[
  {
    "action": "BUY",
    "symbol": "000001",
    "quantity": 1000,
    "price": 12.35,
    "reason": "一句话中文理由，不超过50字"
  }
]
- action 只能是 "BUY" 或 "SELL"（大写）。
- symbol 为 6 位数字字符串。
- quantity 为正整数（BUY 时必须为 100 的整数倍）。
- price 为两位小数的委托价（元）。
- 如果经过分析后决定明日不做任何交易，输出空数组：[]`

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
