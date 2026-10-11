// V2.0 Prompt 职责收敛与决策契约收紧的回归测试（MP.1 - MP.9）。
package agent

import (
	"strings"
	"testing"
)

// --- Master Prompt：仓位口径来自 risk 单一来源 --------------------------------

func TestMasterPromptRatiosDerivedFromRisk(t *testing.T) { // MP.1
	if !strings.Contains(MasterSystemPrompt, "20%") || !strings.Contains(MasterSystemPrompt, "80%") {
		t.Fatalf("default prompt must embed 20%%/80%% from risk defaults, got:\n%.200s", MasterSystemPrompt)
	}
	oldPS, oldTot := maxPositionRatioPerStock, maxTotalPositionRatio
	defer func() {
		maxPositionRatioPerStock, maxTotalPositionRatio = oldPS, oldTot
		MasterSystemPrompt = strings.Replace(MasterSystemPrompt, "30%", "20%", 1) // 还原即可，值域测试后重建
		_ = SetPositionRatios(oldPS, oldTot)
	}()
	if err := SetPositionRatios(0.30, 0.70); err != nil {
		t.Fatalf("SetPositionRatios: %v", err)
	}
	if !strings.Contains(MasterSystemPrompt, "30%") || !strings.Contains(MasterSystemPrompt, "70%") {
		t.Errorf("prompt not re-rendered after SetPositionRatios")
	}
}

func TestSetPositionRatiosRejectsInvertedBounds(t *testing.T) { // MP.2
	oldPS, oldTot := maxPositionRatioPerStock, maxTotalPositionRatio
	defer func() { _, _ = oldPS, oldTot; _ = SetPositionRatios(oldPS, oldTot) }()
	if err := SetPositionRatios(0.50, 0.30); err == nil {
		t.Fatal("per-stock > total must error")
	}
}

func TestMasterPromptNoSelfTechAnalysis(t *testing.T) { // MP.3
	// 旧版让 LLM 看 K 线推演 MA/量能；新版必须删除该框架并改为 czsc 信号为准
	for _, banned := range []string{"MA5/MA10/MA20 多头排列", "近 5 日均量"} {
		if strings.Contains(MasterSystemPrompt, banned) {
			t.Errorf("master prompt still contains self tech-analysis instruction %q", banned)
		}
	}
	for _, want := range []string{"czsc", "signal_type", "confirmed_at", "意向量", "风险预算"} {
		if !strings.Contains(MasterSystemPrompt, want) {
			t.Errorf("master prompt missing %q", want)
		}
	}
}

// --- Manager Prompt：中性新闻 / SELL 持仓前提 / DROP 语义 ----------------------

func TestManagerPromptContractKeywords(t *testing.T) { // MP.4
	for _, want := range []string{
		"中性新闻不等于利空",                // 修正"无利好即否决"的错杀
		"不得高于 70",                  // 中性买点评分封顶
		"当前持仓清单",                   // SELL 必须有持仓
		"绝不是卖出指令",                  // DROP ≠ SELL
		"red_flag",                 // 硬雷一票否决
		"confidence_score 为 0-100", // 契约范围
	} {
		if !strings.Contains(ManagerSystemPrompt, want) {
			t.Errorf("manager prompt missing clause %q", want)
		}
	}
}

func TestBuildManagerPromptCarriesPositionsSection(t *testing.T) { // MP.5
	p := BuildManagerPrompt("2026-10-12", `[{"symbol":"600519"}]`, `[{"code":"000001","quantity":1000}]`, `{"600519":"..."}`)
	for _, want := range []string{"当前持仓清单", `"code":"000001"`, "候选股票池", "新闻/公告上下文"} {
		if !strings.Contains(p, want) {
			t.Errorf("manager user prompt missing %q", want)
		}
	}
}

// --- normalizeDecisions：越界分数不再静默裁剪 ----------------------------------

func TestNormalizeConfidenceOutOfRangeIsDirty(t *testing.T) { // MP.6
	in := []DecisionItem{
		{Symbol: "600000", Action: "BUY", ConfidenceScore: 999},
		{Symbol: "600001", Action: "BUY", ConfidenceScore: -5},
		{Symbol: "600002", Action: "buy ", ConfidenceScore: 85},
	}
	out := normalizeDecisions(in)
	if out[0].Action != "DROP" || out[0].ConfidenceScore != 0 {
		t.Errorf("999 must be dirty->DROP/0, got %s/%d", out[0].Action, out[0].ConfidenceScore)
	}
	if out[1].Action != "DROP" || out[1].ConfidenceScore != 0 {
		t.Errorf("-5 must be dirty->DROP/0, got %s/%d", out[1].Action, out[1].ConfidenceScore)
	}
	if out[2].Action != "BUY" || out[2].ConfidenceScore != 85 {
		t.Errorf("valid item altered: %s/%d", out[2].Action, out[2].ConfidenceScore)
	}
}

func TestNormalizeUnknownActionStillDropZero(t *testing.T) { // MP.7
	out := normalizeDecisions([]DecisionItem{{Symbol: "600003", Action: "MOON", ConfidenceScore: 90}})
	if out[0].Action != "DROP" || out[0].ConfidenceScore != 0 {
		t.Errorf("unknown action must DROP/0, got %s/%d", out[0].Action, out[0].ConfidenceScore)
	}
}

// --- ValidateDecisions：服务端兜底，不信任 LLM 自律 ----------------------------

func TestValidateDecisionsDemoteSellWithoutPosition(t *testing.T) { // MP.8
	held := map[string]bool{"000001": true}
	expected := map[string]bool{"000001": true, "600519": true}
	items := []DecisionItem{
		{Symbol: "000001", Action: "SELL", ConfidenceScore: 80},
		{Symbol: "600519", Action: "SELL", ConfidenceScore: 90}, // 无持仓 -> 降级 DROP
		{Symbol: "999999", Action: "BUY", ConfidenceScore: 90},  // 池外 -> 丢弃
		{Symbol: "000001", Action: "BUY", ConfidenceScore: 70},  // 重复 -> 保留首条
	}
	out, violations := ValidateDecisions(items, expected, held)
	if len(out) != 2 {
		t.Fatalf("want 2 kept items, got %d: %+v", len(out), out)
	}
	if out[0].Action != "SELL" {
		t.Errorf("held SELL should survive, got %s", out[0].Action)
	}
	if out[1].Symbol != "600519" || out[1].Action != "DROP" {
		t.Errorf("unheld SELL must demote to DROP, got %s/%s", out[1].Symbol, out[1].Action)
	}
	joined := strings.Join(violations, "|")
	for _, want := range []string{"not in candidate pool", "duplicate", "without position"} {
		if !strings.Contains(joined, want) {
			t.Errorf("violations missing %q: %v", want, violations)
		}
	}
}

func TestValidateDecisionsNilMapsArePermissive(t *testing.T) { // MP.9
	items := []DecisionItem{{Symbol: "600000", Action: "SELL", ConfidenceScore: 60}}
	out, violations := ValidateDecisions(items, nil, nil)
	if len(out) != 1 || len(violations) != 0 {
		t.Errorf("nil expected/held must pass through untouched, got %d/%v", len(out), violations)
	}
}
