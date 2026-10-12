package wencai

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// ---------------------------------------------------------------------------
// convertResponse：复刻 pywencai.convert.convert 的解析断言
// ---------------------------------------------------------------------------

func robotBody(t *testing.T, contentJSON string) []byte {
	t.Helper()
	inner := map[string]any{
		"data": map[string]any{
			"answer": []any{
				map[string]any{"txt": []any{map[string]any{"content": contentJSON}}},
			},
		},
	}
	b, err := json.Marshal(inner)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestConvertXuanguTableV1(t *testing.T) {
	content := `{
      "components": [{
        "show_type": "xuangu_tableV1",
        "cid": "C1", "puuid": "U1",
        "data": {"meta": {"extra": {"condition": "涨跌幅>5 AND 市盈率<30", "row_count": 250}}},
        "config": {"other_info": {"footer_info": {"url": "/gateway/urp/v7/landing/getDataList?comp_id=C1&uuid=U1&query_type=stock&foo=bar&dup=a&dup=b"}}}
      }]
    }`
	p, err := convertResponse(robotBody(t, content))
	if err != nil {
		t.Fatalf("convertResponse: %v", err)
	}
	if got := p.Data["condition"]; got != "涨跌幅>5 AND 市盈率<30" {
		t.Errorf("condition = %v", got)
	}
	if p.Data["comp_id"] != "C1" || p.Data["uuid"] != "U1" {
		t.Errorf("comp_id/uuid = %v/%v", p.Data["comp_id"], p.Data["uuid"])
	}
	if p.RowCount != 250 {
		t.Errorf("row_count = %d, want 250", p.RowCount)
	}
	// parse_url_params 语义：多值取首个
	if p.URLParams["dup"] != "a" || p.URLParams["query_type"] != "stock" {
		t.Errorf("url_params = %v", p.URLParams)
	}
}

func TestConvertContentAsObjectAndString(t *testing.T) {
	// content 为字符串化 JSON（上游主流形态）已在 Table 用例覆盖；这里测 content 直接是对象
	objContent := `{"components":[{"show_type":"txt1","title_config":{"data":{"h1":"解读"}},"data":{"content":"今日市场普涨"}}]}`
	var doc any
	_ = json.Unmarshal([]byte(objContent), &doc)
	inner, _ := json.Marshal(map[string]any{"data": map[string]any{"answer": []any{map[string]any{"txt": []any{map[string]any{"content": doc}}}}}})
	p, err := convertResponse(inner)
	if err != nil {
		t.Fatalf("object-form content: %v", err)
	}
	sd, _ := p.Data["structured"].(map[string]any)
	if sd["解读"] != "今日市场普涨" {
		t.Errorf("structured = %v", sd)
	}
}

func TestMultiShowTypeHandlers(t *testing.T) {
	compsRaw := `[
      {"uuid":"a1","show_type":"container","config":{"children":["t1","d1"]},
       "title_config":{"data":{"h1":"总览"}}},
      {"uuid":"t1","show_type":"txt2","data":{"content":"文本结论"}},
      {"uuid":"d1","show_type":"dragon_tiger_stock","data":{"datas":[
         {"股票简称":"N海融","龙虎榜净买额":1234.5,
          "detail":[{"buy":{"datas":[{"营业部名称":"机构专用","买入额":100}]},
                     "sell":{"datas":[{"营业部名称":"某游资","卖出额":50}]}}]}
      ]}},
      {"uuid":"unregistered","show_type":"brand_new_type","data":{"datas":[{"k":"v"}]}}
    ]`
	var arr []any
	if err := json.Unmarshal([]byte(compsRaw), &arr); err != nil {
		t.Fatal(err)
	}
	out := multiShowTypeHandler(arr)
	if out["总览"] == nil {
		t.Fatalf("container key missing: %v", out)
	}
	cont, _ := out["总览"].(map[string]any)
	if cont["txt2"] != "文本结论" {
		t.Errorf("container.txt2 = %v", cont["txt2"])
	}
	dtl, _ := cont["dragon_tiger_stock"].(map[string]any)
	mainTbl, _ := dtl["data"].(*Table)
	if mainTbl == nil || len(mainTbl.Rows) != 1 || mainTbl.Rows[0]["股票简称"] != "N海融" {
		t.Fatalf("dragon main table = %+v", dtl["data"])
	}
	if _, leaked := mainTbl.Rows[0]["detail"]; leaked {
		t.Errorf("detail should be popped from main row")
	}
	dd, _ := dtl["detail"].(map[string]any)
	buy, _ := dd["buy"].(*Table)
	sell, _ := dd["sell"].(*Table)
	if buy == nil || len(buy.Rows) != 1 || buy.Rows[0]["营业部名称"] != "机构专用" {
		t.Errorf("buy table = %+v", dd["buy"])
	}
	if sell == nil || sell.Rows[0]["卖出额"].(float64) != 50 {
		t.Errorf("sell table = %+v", dd["sell"])
	}
	// 未注册 show_type -> commonHandler 提 datas
	nb, ok := out["brand_new_type"].(*Table)
	if !ok || len(nb.Rows) != 1 || nb.Rows[0]["k"] != "v" {
		t.Errorf("common fallback = %+v", out["brand_new_type"])
	}
}

func TestTabHandlers(t *testing.T) {
	raw := `[
      {"show_type":"tab4","config":{"title":"题材库"},"tab_list":[
        {"tab_name":"涨停","list":[{"show_type":"txt1","data":{"content":"3家"}}]},
        {"tab_name":"","list":[]}
      ]},
      {"show_type":"tab1","config":{"title":"多期"},"data":{"0":{"datas":[{"col":1}]}},"tab_list":[
        {"tab_name":"一期","list":[{"show_type":"tableish","data_index":"0"}]}
      ]}
    ]`
	var arr []any
	_ = json.Unmarshal([]byte(raw), &arr)
	out := multiShowTypeHandler(arr)
	tab4, _ := out["题材库"].(map[string]any)
	if tab4 == nil || tab4["涨停"] != "3家" {
		t.Errorf("tab4 = %+v", out["题材库"])
	}
	if _, has := tab4[""]; has {
		t.Errorf("empty tab_name should be skipped")
	}
	tab1, _ := out["多期"].(map[string]any)
	sub, _ := tab1["一期"].(map[string]any)
	tbl, _ := sub["tableish"].(*Table) // 未注册类型走 common，data_index 注入后取 datas
	if tbl == nil || tbl.Rows[0]["col"].(float64) != 1 {
		t.Errorf("tab1 data_index injection = %+v", sub)
	}
}

func TestNestedBlocksPendingURLs(t *testing.T) {
	raw := `{"show_type":"nestedblocks","data":{"result":{"subBlocks":[
      {"subBlocks":[{"url":"/gateway/x?id=1"},{"url":"/gateway/x?id=2"},{"nope":1}]}]}}}`
	var comp map[string]any
	_ = json.Unmarshal([]byte(raw), &comp)
	got := nestedBlocksHandler(comp, nil)
	m, _ := got.(map[string]any)
	urls, _ := m["pending_urls"].([]string)
	if !reflect.DeepEqual(urls, []string{"/gateway/x?id=1", "/gateway/x?id=2"}) {
		t.Errorf("pending_urls = %v", urls)
	}
}

// ---------------------------------------------------------------------------
// Table / normalizeSymbol
// ---------------------------------------------------------------------------

func TestNormalizeSymbol(t *testing.T) {
	cases := map[string]string{
		"300750.SZ":     "300750",
		"600519.SH":     "600519",
		"特力A(301117)":   "301117",
		"SH600000":      "600000",
		"药明康德 02359.HK": "", // 港股 5 位，不合法 A 股
		"":              "",
		"12345":         "",
	}
	for in, want := range cases {
		if got := normalizeSymbol(in); got != want {
			t.Errorf("normalizeSymbol(%q)=%q want %q", in, got, want)
		}
	}
}

func TestTableAppendAndSymbols(t *testing.T) {
	a := rowsToTable([]map[string]any{{"股票代码": "宁德时代(300750)", "价": 1.0}})
	b := rowsToTable([]map[string]any{{"股票代码": "000001.SZ", "量": 2.0}})
	a.Append(b)
	if len(a.Rows) != 2 {
		t.Fatalf("rows=%d", len(a.Rows))
	}
	// 新列增量补齐
	found := false
	for _, c := range a.Columns {
		if c == "量" {
			found = true
		}
	}
	if !found {
		t.Errorf("columns not merged: %v", a.Columns)
	}
	syms := a.Symbols("")
	if !reflect.DeepEqual(syms, []string{"300750", "000001"}) {
		t.Errorf("symbols = %v", syms)
	}
}

func TestParseURLParams(t *testing.T) {
	m := parseURLParams("/x?a=1&b=2&c=a&c=b")
	if m["a"] != "1" || m["c"] != "a" || len(m) != 3 {
		t.Errorf("parseURLParams = %v", m)
	}
	if len(parseURLParams("")) != 0 {
		t.Error("empty url should give empty map")
	}
}

func TestGetPathArrayIndex(t *testing.T) {
	var doc any
	_ = json.Unmarshal([]byte(`{"a":[{"b":[10,20]}]}`), &doc)
	v, ok := getPath(doc, "a", "0", "b", "1")
	if !ok || v.(float64) != 20 {
		t.Errorf("getPath = %v %v", v, ok)
	}
	if _, ok := getPath(doc, "a", "5"); ok {
		t.Error("out of range should miss")
	}
}

// ---------------------------------------------------------------------------
// TokenProvider（node 缺失时的错误语义 —— 本沙箱有 node，用假脚本测缓存逻辑）
// ---------------------------------------------------------------------------

func TestTokenCacheTTL(t *testing.T) {
	dir := t.TempDir()
	fake := dir + "/fake.js"
	// runNode 只接受文件路径（hexin-v.js 需要真实文件），故用临时 js 桩验证缓存语义。
	write := func(s string) {
		if err := os.WriteFile(fake, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("console.log('TESTTOKEN-abc')")
	p := NewTokenProvider(0)
	p.scriptPath = fake
	tk, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if tk != "TESTTOKEN-abc" {
		t.Errorf("token=%q", tk)
	}
	// 第二次应命中缓存（改脚本内容不影响结果）
	write("console.log('CHANGED')")
	tk2, _ := p.Get(context.Background())
	if tk2 != tk {
		t.Errorf("cache miss: %q vs %q", tk2, tk)
	}
	p.Invalidate()
	tk3, _ := p.Get(context.Background())
	if tk3 != "CHANGED" {
		t.Errorf("after invalidate expect regen, got %q", tk3)
	}
}
