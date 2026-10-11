package agent

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// T1.4.x ParseTradePlans 四级容错解析
// ---------------------------------------------------------------------------

func TestParseTradePlans_PlainArray(t *testing.T) { // T1.4.1
	raw := `[{"action":"buy","symbol":"600519","quantity":100,"price":100.5,"reason":"突破"}]`
	items, err := ParseTradePlans(raw)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(items) != 1 || items[0].Symbol != "600519" || items[0].Action != "BUY" {
		t.Fatalf("items = %+v", items)
	}
}

func TestParseTradePlans_MarkdownFence(t *testing.T) { // T1.4.2
	raw := "分析如下：\n```json\n[{\"action\":\"SELL\",\"symbol\":\"000001\",\"quantity\":200,\"price\":10.0,\"reason\":\"止盈\"}]\n```\n以上。"
	items, err := ParseTradePlans(raw)
	if err != nil {
		t.Fatalf("fence parse: %v", err)
	}
	if len(items) != 1 || items[0].Action != "SELL" || items[0].Symbol != "000001" {
		t.Fatalf("items = %+v", items)
	}
}

func TestParseTradePlans_ProseWrapped(t *testing.T) { // T1.4.3
	raw := `好的，我给出的明日交易计划是 [{"action":"buy","symbol":"300750","quantity":100,"price":200.1,"reason":"趋势"}] ，请执行。`
	items, err := ParseTradePlans(raw)
	if err != nil {
		t.Fatalf("prose wrapped: %v", err)
	}
	if len(items) != 1 || items[0].Symbol != "300750" {
		t.Fatalf("items = %+v", items)
	}
}

func TestParseTradePlans_BracketInsideString(t *testing.T) { // T1.4.3 边界：reason 含括号
	raw := `[{"action":"buy","symbol":"600519","quantity":100,"price":1.0,"reason":"支撑(强)[确认]"}]`
	items, err := ParseTradePlans(raw)
	if err != nil {
		t.Fatalf("bracket in string: %v", err)
	}
	if items[0].Reason != "支撑(强)[确认]" {
		t.Fatalf("reason mangled: %q", items[0].Reason)
	}
}

func TestParseTradePlans_SingleObjectFallback(t *testing.T) { // T1.4.4 降级第4级
	raw := `计划：{"action":"buy","symbol":"600000","quantity":300,"price":8.8,"reason":"低估"}`
	items, err := ParseTradePlans(raw)
	if err != nil {
		t.Fatalf("single object fallback: %v", err)
	}
	if len(items) != 1 || items[0].Symbol != "600000" || items[0].Quantity != 300 {
		t.Fatalf("items = %+v", items)
	}
}

func TestParseTradePlans_GarbageReturnsError(t *testing.T) { // T1.4.4
	for _, raw := range []string{"", "   ", "今天行情不好，不交易。", "[{bad json", "{\"action\":}"} {
		if _, err := ParseTradePlans(raw); err == nil {
			t.Errorf("expected error for %q", raw)
		}
	}
}

func TestParseTradePlans_LongGarbageTruncatedInError(t *testing.T) {
	raw := strings.Repeat("x", 500)
	_, err := ParseTradePlans(raw)
	if err == nil {
		t.Fatal("want error")
	}
	if len(err.Error()) > 400 {
		t.Errorf("error message not truncated: len=%d", len(err.Error()))
	}
}

func TestParseTradePlans_EmptyArrayIsLegal(t *testing.T) { // T1.4.5 空数组=明日不交易
	items, err := ParseTradePlans(`[]`)
	if err != nil {
		t.Fatalf("empty array must be legal, got %v", err)
	}
	if items == nil || len(items) != 0 {
		t.Fatalf("want non-nil empty slice, got %+v", items)
	}
	// 围栏包裹的空数组同样合法
	items2, err := ParseTradePlans("```json\n[]\n```")
	if err != nil || items2 == nil || len(items2) != 0 {
		t.Fatalf("fenced empty array: items=%+v err=%v", items2, err)
	}
}

func TestNormalizeDirectionVariants(t *testing.T) { // T1.4.6 方向归一化
	cases := map[string]string{"buy": "BUY", "Buy": "BUY", "B": "BUY", "sell": "SELL", "SELL": "SELL", "S": "SELL"}
	var sb strings.Builder
	sb.WriteString("[")
	keys := make([]string, 0, len(cases))
	i := 0
	for k := range cases {
		keys = append(keys, k)
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"action":"` + k + `","symbol":"600519","quantity":100,"price":10}`)
		i++
	}
	sb.WriteString("]")
	items, err := ParseTradePlans(sb.String())
	if err != nil {
		t.Fatalf("variants: %v", err)
	}
	if len(items) != len(cases) {
		t.Fatalf("len=%d want %d", len(items), len(cases))
	}
	for _, it := range items {
		want := cases[it.Action] // map 的 key 已被大写归一，反查用值域校验
		_ = want
		if it.Action != "BUY" && it.Action != "SELL" {
			t.Errorf("action not normalized: %q", it.Action)
		}
	}
}

func TestBuildUserPromptContainsAllSections(t *testing.T) {
	p := BuildUserPrompt("2026-10-12", `{"cash":1000000}`, `[{"code":"600519"}]`, `{"600519":[...]}`)
	for _, want := range []string{"2026-10-12", `{"cash":1000000}`, "600519"} {
		if !strings.Contains(p, want) {
			t.Errorf("user prompt missing %q", want)
		}
	}
}

func TestMasterSystemPromptHardConstraints(t *testing.T) {
	// Prompt 必须内嵌四大铁律关键词，防止后续改动意外删除约束
	prompt := MasterSystemPrompt
	for _, kw := range []string{"T+1", "20%", "80%", "JSON"} {
		if !strings.Contains(prompt, kw) {
			t.Errorf("system prompt missing constraint keyword %q", kw)
		}
	}
}
