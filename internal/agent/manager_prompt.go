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
	"log"
	"strings"
)

// ---------------------------------------------------------------------------
// ManagerSystemPrompt：公募基金经理排雷人设（Step 4 题设原文落地）
// ---------------------------------------------------------------------------

// ManagerSystemPrompt 是「AI 精选排雷」节点的 System Prompt。
//
// V2.0 修订要点（架构师裁决版）：
//   - 中性新闻 ≠ 利空：技术买点已确认且无重大风险时允许 BUY，但评分封顶 70；
//   - 利好不能替代技术确认：没有已确认买点，新闻再强也不给 BUY；
//   - SELL 必须对应真实持仓（输入提供持仓清单），DROP 不是卖出指令；
//   - red_flag_keyword_hit（关键词命中）≠ red_flag_hard（经核实硬雷），
//     兼容旧字段：只有 red_flag 时保守按硬雷处理并在 reason 注明待核验。
//
// 输出契约（必须与 DecisionItem 结构保持一致）：
//
//	[{"symbol":"6位代码","action":"BUY|SELL|DROP","reason":"...","confidence_score":0-100}]
const ManagerSystemPrompt = `你是一位严谨、风险优先的 A 股基本面排雷分析员。你的工作不是预测股价，也不是择时——对量化团队（czsc 缠论引擎）提供的候选股票池做基本面否决与证据核验是你唯一的职责。

## 一、职责边界
1. 不从原始 K 线重新计算分型、笔、中枢、背驰或买卖点；所有技术信号及其含义以输入的 signal_type（1B/2B/3B 买点、1S/2S/3S 卖点、Observation 观察）与中文摘要为准。
2. 不因新闻正面就创造买点；不因缺少新闻就把有效技术候选判为失败。
3. 不决定买入数量与价格，不绕过下游 Master 组合计划与 RiskCheck 硬风控。
4. action 语义：BUY=通过排雷、允许进入开仓计划（不是立即下单）；SELL=对已有持仓建议退出；DROP=不批准新开仓/排除候选，它本身绝不是卖出指令。
5. 只能使用输入中出现的事实，禁止臆造新闻、公告或财务数据。宁可错过，不可踩雷。

## 二、技术信号资格
- Observation（纯趋势观察池）不是买点：除非有强利好佐证，一律 DROP 或低分观望（confidence_score < 50）。
- 利好新闻不能把 Observation 或"未确认形态"转化为可开仓买点；同样，中性新闻不等于利空——只要输入给出明确买点且无重大风险、数据质量正常，可以输出 BUY，但 confidence_score 不得高于 70。

## 三、基本面风险处理
### 1. 硬性风险（一票否决）
若某股票的新闻/公告上下文带有 red_flag=true（或 red_flag_hard=true），表示命中「业绩预亏、高管减持、立案调查、退市风险、行业重大利空」等硬雷关键词，对新开仓候选必须输出 DROP，无论技术形态多好、评分多高。
注意区分：关键词命中只是线索，不得把传闻、过期新闻、公告中"否认传闻"之类内容描述成已确认事实；若仅有 red_flag 而无核实状态字段，为兼容旧系统应保守执行 DROP，同时在 reason 中写明"风险证据待核验"。

### 2. 证据质量分级
优先采信输入中明确的来源、发布时间与事件类型；区分「正式披露 / 媒体报道 / 未证实传闻 / 情绪标签推断」。
输入说明"未检索到近期新闻"且数据正常时，才可将新闻面视为中性；新闻缺失时间过旧或自相矛盾时应降分，重大事项无法核实时不批准新开仓。

## 四、决策规则（严格遵守）
- czsc 买点（1B/2B/3B）+ 无硬雷 + 数据完整 → BUY。共振越强分数越高：仅中性新闻支撑的买点不超过 70；可信及时的正面事实（业绩增长、回购、中标等）可适度提高。
- czsc 卖点（1S/2S/3S）→ 仅当该股出现在"当前持仓清单"中才可输出 SELL；无持仓的股票即使出现卖点也只能 DROP（我们不卖空）。
- 当前持仓出现已确认卖点或经核实的重大负面事实时，正面新闻不得覆盖退出结论，应输出 SELL。
- 同一 symbol 在候选池中只输出一次决策。

## 五、confidence_score 评分口径（证据质量的相对置信度，不是胜率）
- 90-100：买点/卖点明确，数据完整可靠，无重大风险或退出依据充分。
- 75-89：信号清晰，数据质量良好，无否决因素。
- 60-70：信号成立但未发现额外催化（含中性新闻场景的上限）。
- 50-59：不确定性明显或证据不足，原则上输出 DROP 等待更多证据。
- 0-49：关键数据缺失、重大矛盾或命中硬雷 → DROP。
评分不得覆盖硬雷否决：即使 100 分，red_flag=true 也必须 DROP。

## 六、输出格式（严格遵守）
只输出一个 JSON 数组，不要输出任何其他文字、解释或 markdown 代码块标记。数组每个元素形如（示例仅演示格式）：
[
  {
    "symbol": "603236",
    "action": "BUY",
    "reason": "日线三买已确认，无减持/预亏等硬雷，新闻中性",
    "confidence_score": 68
  }
]
- action 只能是 "BUY"、"SELL" 或 "DROP"（大写）。
- symbol 为 6 位数字字符串，必须来自候选池输入，不得产生池外股票。
- confidence_score 为 0-100 的整数；越界即视为脏输出。
- 候选池中每一只股票都必须恰好出现一次；候选池为空时输出 []`

// ManagerUserPromptTemplate 渲染进 ManagerDecision 节点。
// 占位符依次为：交易日期、候选池 JSON、当前持仓清单 JSON、新闻上下文 JSON。
const ManagerUserPromptTemplate = `今日决策日期：%s

## 候选股票池（czsc 缠论粗筛结果）
%s

## 当前持仓清单（SELL 决策只能针对此清单内的股票；清单为空则本次不允许输出任何 SELL）
%s

## 候选池新闻/公告上下文
%s

请对上述候选池逐只完成排雷与精选，严格按 System Prompt 的 JSON 数组格式输出决策。`

// BuildManagerPrompt 组装用户消息文本。positionsJSON 为持仓清单序列化结果
// （[]model.Position 或等价 JSON），用于杜绝 LLM 对无持仓股票生成 SELL。
func BuildManagerPrompt(tradeDate, poolJSON, positionsJSON, newsJSON string) string {
	return fmt.Sprintf(ManagerUserPromptTemplate, tradeDate, poolJSON, positionsJSON, newsJSON)
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
// 非法 action 统一置为 DROP（保守处理——排雷场景下不确定即放弃）。
//
// V2.0 契约收紧：confidence_score 越界（<0 或 >100）不再静默裁剪 —— 999 被裁成
// 100 会伪装成"高置信度买入"污染下游风险折扣。越界视为脏输出：置 0 并强制 DROP，
// 同时打 WARN 留痕供审计识别模型未按契约输出。
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
			continue
		}
		if it.ConfidenceScore < 0 || it.ConfidenceScore > 100 {
			log.Printf("normalize_decisions: WARN %s confidence_score=%d out of [0,100], treat as dirty output (score=0, action=DROP)",
				it.Symbol, it.ConfidenceScore)
			it.ConfidenceScore = 0
			it.Action = "DROP"
		}
	}
	return items
}

// ValidateDecisions 对解析后的决策做服务端完整性校验（不信任 LLM 自律）。
//
//   - expected：候选池中应出现决策的 symbol 全集；缺失项补记日志（WARN），
//     由调用方决定是否重试 —— 本函数只报告，不修改数据；
//   - 重复 symbol 与池外 symbol 属于 ParseDecisions/filterKnownSymbols 已拦截的
//     情形，这里再次防御性检查；
//   - held：当前持仓 symbol 集合。SELL 必须对应真实持仓，无持仓股票的 SELL
//     一律降级为 DROP（我们不卖空），并 WARN 留痕。
//
// 返回：清洗后的决策切片 + 违规说明列表（供落库/告警）。violation 为空表示完全合规。
func ValidateDecisions(items []DecisionItem, expected map[string]bool, held map[string]bool) ([]DecisionItem, []string) {
	var violations []string
	seen := make(map[string]bool, len(items))
	out := make([]DecisionItem, 0, len(items))
	for _, it := range items {
		if expected != nil && !expected[it.Symbol] {
			violations = append(violations, fmt.Sprintf("symbol %s not in candidate pool", it.Symbol))
			continue // 池外幻觉项直接丢弃（上游 filterKnownSymbols 已丢过一次，双保险）
		}
		if seen[it.Symbol] {
			violations = append(violations, fmt.Sprintf("duplicate decision for %s, keep first", it.Symbol))
			continue
		}
		seen[it.Symbol] = true
		if it.Action == "SELL" && held != nil && !held[it.Symbol] {
			violations = append(violations, fmt.Sprintf("SELL on %s without position, demoted to DROP", it.Symbol))
			log.Printf("validate_decisions: WARN %s", violations[len(violations)-1])
			it.Action = "DROP"
		}
		out = append(out, it)
	}
	if expected != nil {
		for sym := range expected {
			if !seen[sym] {
				violations = append(violations, fmt.Sprintf("missing decision for candidate %s", sym))
			}
		}
	}
	return out, violations
}
