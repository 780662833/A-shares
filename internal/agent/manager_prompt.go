// Package agent —— 漏斗模型 Layer2「基金经理排雷」Prompt 与决策 JSON 契约。
//
// 与旧版 MasterSystemPrompt（让 LLM 直接看 K 线做计划）的本质区别：
//   - LLM 不再"看图"，技术面找位置的工作已由 czsc（Python 网关）完成；
//   - LLM 的职责收敛为它真正擅长的事：读新闻/公告，对候选池进行基本面排雷，
//     输出 BUY / SELL / DROP + 信心分数；
//   - 仓位、数量、价格等执行细节由下游硬风控层决定，本 Prompt 不要求 LLM 给出。
package agent

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------
// ManagerSystemPrompt：公募基金经理排雷人设（Step 4 题设原文落地）
// ---------------------------------------------------------------------------

// ManagerSystemPrompt 是「AI 精选排雷」节点的 System Prompt。
//
// 输出契约（必须与 DecisionItem 结构保持一致）：
//
//	[{"symbol":"6位代码","action":"BUY|SELL|DROP","reason":"...","confidence_score":0-100}]
const ManagerSystemPrompt = `你是一位严谨的 A 股公募基金经理。你的工作不是预测股价，而是对量化团队（czsc 缠论引擎）提供的候选股票池进行基本面排雷和逻辑验证。

## 输入说明
你将收到两部分信息：
1. 候选股票池：每只股票附带 czsc 技术信号（signal_type 如 3B=第三类买点、1B=第一类买点、3S=第三类卖点、Observation=趋势观察）与中文摘要；
2. 这些股票的最新新闻/公告（含情绪标签 sentiment 与 red_flag 硬雷标记，red_flag=true 表示命中预亏/减持/立案调查/退市等关键词）。

## 决策规则（严格遵守）
- 如果 czsc 提示买点（1B/2B/3B），但新闻中存在「业绩预亏、高管减持、立案调查、行业重大利空」等负面信息，必须标记为 DROP（放弃），无论技术形态多好。
- 如果技术面买点与新闻利好共振（业绩增长、回购、中标等），标记为 BUY，并给出 0-100 的 confidence_score（共振越强分数越高；仅中性新闻支撑的买点不超过 70）。
- 如果 czsc 提示卖点（1S/2S/3S），且新闻面平庸或偏空，标记为 SELL；若持仓该股应止盈/止损离场。
- Observation（纯趋势观察池）没有明确买卖点，除非新闻有强利好，否则一律 DROP 或低分观望（confidence_score < 50 时按 DROP 处理）。
- 只能使用输入中出现的事实，禁止臆造新闻或财务数据。宁可错过，不可踩雷。

## 输出格式（严格遵守）
只输出一个 JSON 数组，不要输出任何其他文字、解释或 markdown 代码块标记。数组每个元素形如：
[
  {
    "symbol": "603236",
    "action": "BUY",
    "reason": "一句话中文理由，不超过60字",
    "confidence_score": 85
  }
]
- action 只能是 "BUY"、"SELL" 或 "DROP"（大写）。
- symbol 为 6 位数字字符串，必须来自候选池输入。
- confidence_score 为 0-100 的整数。
- 严禁输出 quantity / price / 股数 / 金额等任何仓位数量字段——买卖数量由下游风控层按风险预算公式硬编码计算，你的输出中出现这些字段一律视为格式违规。
- 候选池中每一只股票都必须出现在输出数组中（一视同仁地给出结论）。
- 如果候选池为空，输出空数组：[]`

// ManagerUserPromptTemplate 渲染进 ManagerDecision 节点。
// 占位符依次为：交易日期、候选池 JSON、新闻上下文 JSON。
const ManagerUserPromptTemplate = `今日决策日期：%s

## 候选股票池（czsc 缠论粗筛结果）
%s

## 候选池新闻/公告上下文
%s

请对上述候选池逐只完成排雷与精选，严格按 System Prompt 的 JSON 数组格式输出决策。`

// BuildManagerPrompt 组装用户消息文本。
func BuildManagerPrompt(tradeDate, poolJSON, newsJSON string) string {
	return fmt.Sprintf(ManagerUserPromptTemplate, tradeDate, poolJSON, newsJSON)
}

// ---------------------------------------------------------------------------
// DecisionItem：LLM 决策 JSON 契约与容错解析
// ---------------------------------------------------------------------------

// DecisionItem 单只股票的 AI 决策（LLM JSON 数组的元素）。
type DecisionItem struct {
	Symbol          string `json:"symbol"`
	Action          string `json:"action"` // BUY / SELL / DROP
	Reason          string `json:"reason"`
	ConfidenceScore int    `json:"confidence_score"`
}

// ParseDecisions 从 LLM 原始输出中容错提取决策数组。
//
// 复用与 ParseTradePlans 相同的四级降级策略：
//  1. 整体 JSON 数组；2. 剥离 markdown 围栏；3. 截取最外层 [ ... ]；
//  4. 单对象兜底。非法 action / 越界 score 在此归一化（见 normalizeDecisions）。
//
// 空数组 [] 合法（候选池为空时），返回长度 0 的非 nil 切片。
func ParseDecisions(raw string) ([]DecisionItem, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil, fmt.Errorf("empty llm output")
	}

	candidates := []string{s}
	if stripped := stripCodeFence(s); stripped != "" {
		candidates = append(candidates, stripped)
	}
	if arr := extractBalanced(s, '[', ']'); arr != "" {
		candidates = append(candidates, arr)
	}

	for _, c := range candidates {
		var items []DecisionItem
		if err := json.Unmarshal([]byte(c), &items); err == nil {
			return normalizeDecisions(items), nil
		}
	}

	if obj := extractBalanced(s, '{', '}'); obj != "" {
		var one DecisionItem
		if err := json.Unmarshal([]byte(obj), &one); err == nil {
			return normalizeDecisions([]DecisionItem{one}), nil
		}
	}

	snippet := s
	if len(snippet) > 300 {
		snippet = snippet[:300] + "..."
	}
	return nil, fmt.Errorf("cannot parse decision JSON from llm output: %q", snippet)
}

// normalizeDecisions 归一化：action 大写并把常见简写映射到 BUY/SELL/DROP，
// 非法 action 统一置为 DROP（保守处理——排雷场景下不确定即放弃）；
// confidence_score 裁剪到 [0,100]。
func normalizeDecisions(items []DecisionItem) []DecisionItem {
	for i := range items {
		it := &items[i]
		it.Symbol = strings.TrimSpace(it.Symbol)
		a := strings.ToUpper(strings.TrimSpace(it.Action))
		switch a {
		case "BUY", "B", "买入":
			it.Action = "BUY"
		case "SELL", "S", "卖出":
			it.Action = "SELL"
		case "DROP", "D", "放弃", "HOLD", "OBSERVE", "WATCH":
			it.Action = "DROP"
		default:
			// 未知动作 -> 保守降级为 DROP，同时压低置信度供审计识别脏输出
			it.Action = "DROP"
			it.ConfidenceScore = 0
		}
		if it.ConfidenceScore < 0 {
			it.ConfidenceScore = 0
		} else if it.ConfidenceScore > 100 {
			it.ConfidenceScore = 100
		}
	}
	return items
}
