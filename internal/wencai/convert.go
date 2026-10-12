// 本文件复刻 pywencai/convert.py：show_type 分发处理器与 DataFrame 等价结构。
package wencai

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// ---------------------------------------------------------------------------
// Table：pandas.DataFrame 的轻量等价物（问财行数据 + 稳定列顺序）
// ---------------------------------------------------------------------------

// Table 一次查询的结果表。Rows 保序（接口返回顺序即语义排序），
// Columns 为首次出现的列名顺序，供 LLM/落库时保持稳定的 schema。
type Table struct {
	Columns []string
	Rows    []map[string]any
}

// Append 纵向拼接另一页（等价 pd.concat(ignore_index=True)）。
// 新出现的列追加到 Columns 尾部，保证多页异构列不丢失。
func (t *Table) Append(other *Table) {
	if other == nil {
		return
	}
	seen := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		seen[c] = true
	}
	for _, c := range other.Columns {
		if !seen[c] {
			t.Columns = append(t.Columns, c)
			seen[c] = true
		}
	}
	t.Rows = append(t.Rows, other.Rows...)
}

// SymbolColumn 猜测股票代码列名（问财常见：股票代码/代码/code），找不到返回 ""。
func (t *Table) SymbolColumn() string {
	for _, cand := range []string{"股票代码", "代码", "code", "证券代码"} {
		for _, c := range t.Columns {
			if strings.Contains(c, cand) {
				return c
			}
		}
	}
	return ""
}

// Symbols 提取指定列（或自动探测的代码列）并归一化为 6 位数字代码列表。
func (t *Table) Symbols(column string) []string {
	if column == "" {
		column = t.SymbolColumn()
	}
	if column == "" {
		return nil
	}
	out := make([]string, 0, len(t.Rows))
	seen := map[string]bool{}
	for _, row := range t.Rows {
		raw, _ := row[column].(string)
		sym := normalizeSymbol(raw)
		if sym != "" && !seen[sym] {
			seen[sym] = true
			out = append(out, sym)
		}
	}
	return out
}

// normalizeSymbol "300750.SZ"/"特力A(301117)" -> "301117"；非 6 位数字返回 ""。
func normalizeSymbol(s string) string {
	var digits []rune
	for _, r := range s {
		if r >= '0' && r <= '9' {
			digits = append(digits, r)
		} else if len(digits) > 0 {
			break // 只取第一段连续数字（前缀代码场景）
		}
	}
	// 处理 "301117.SZ"：上面循环在 '.' 处截断已拿到 301117；
	// 处理 "(301117)"：跳过非数字前缀继续收集。
	if len(digits) != 6 {
		// 再试：取字符串中最后一段恰好 6 位的数字
		digits = lastSixDigits(s)
	}
	if len(digits) != 6 {
		return ""
	}
	return string(digits)
}

func lastSixDigits(s string) []rune {
	var cur []rune
	var best []rune
	for _, r := range s {
		if r >= '0' && r <= '9' {
			cur = append(cur, r)
		} else {
			if len(cur) == 6 {
				best = cur
			}
			cur = nil
		}
	}
	if len(cur) == 6 {
		best = cur
	}
	return best
}

// rowsToTable 从 []map[string]any 构建 Table，列顺序按首行 key 优先、其余行增量补齐。
func rowsToTable(rows []map[string]any) *Table {
	t := &Table{Rows: rows}
	seen := map[string]bool{}
	if len(rows) > 0 {
		keys := make([]string, 0, len(rows[0]))
		for k := range rows[0] {
			keys = append(keys, k)
		}
		sort.Strings(keys) // JSON 对象无序，字典序保证跨进程稳定
		for _, k := range keys {
			t.Columns = append(t.Columns, k)
			seen[k] = true
		}
	}
	for _, r := range rows[1:] {
		var newKeys []string
		for k := range r {
			if !seen[k] {
				newKeys = append(newKeys, k)
				seen[k] = true
			}
		}
		sort.Strings(newKeys)
		t.Columns = append(t.Columns, newKeys...)
	}
	return t
}

// ---------------------------------------------------------------------------
// getPath：pydash _.get 的点路径实现（支持数组下标，如 datas.0.buy）
// ---------------------------------------------------------------------------

func getPath(node any, path ...string) (any, bool) {
	cur := node
	for _, seg := range path {
		switch v := cur.(type) {
		case map[string]any:
			next, ok := v[seg]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			var idx int
			if _, err := fmt.Sscanf(seg, "%d", &idx); err != nil || idx < 0 || idx >= len(v) {
				return nil, false
			}
			cur = v[idx]
		default:
			return nil, false
		}
	}
	return cur, true
}

func getPathStr(node any, path ...string) string {
	v, ok := getPath(node, path...)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func toRowList(node any) ([]map[string]any, bool) {
	arr, ok := node.([]any)
	if !ok {
		return nil, false
	}
	rows := make([]map[string]any, 0, len(arr))
	for _, e := range arr {
		if m, ok := e.(map[string]any); ok {
			rows = append(rows, m)
		}
	}
	return rows, len(rows) > 0
}

// ---------------------------------------------------------------------------
// convertResponse：复刻 convert.convert(res)
// ---------------------------------------------------------------------------

// convertResponse 解析 get-robot-data 响应体，产出 RobotParams。
func convertResponse(raw []byte) (*RobotParams, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("convert: invalid robot json: %w", err)
	}
	contentNode, ok := getPath(doc, "data", "answer", "0", "txt", "0", "content")
	if !ok {
		return nil, fmt.Errorf("convert: missing data.answer.0.txt.0.content")
	}
	// content 可能是字符串化 JSON（上游同款双跳）
	var content any
	switch v := contentNode.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &content); err != nil {
			return nil, fmt.Errorf("convert: content not json string: %w", err)
		}
	default:
		content = v
	}
	compsNode, ok := getPath(content, "components")
	if !ok {
		return nil, fmt.Errorf("convert: missing components")
	}
	comps, _ := compsNode.([]any)
	if len(comps) == 0 {
		return nil, fmt.Errorf("convert: empty components")
	}

	params := &RobotParams{URLParams: map[string]string{}}
	first, _ := comps[0].(map[string]any)

	// 单组件且是 xuangu_tableV1：选股链路（condition/comp_id/uuid + row_count + url）
	if len(comps) == 1 && first["show_type"] == "xuangu_tableV1" {
		params.Data = xuanguTableV1Handler(first)
		rc, _ := getPath(first, "data", "meta", "extra", "row_count")
		switch n := rc.(type) {
		case float64:
			params.RowCount = int(n)
		}
		params.URL = getPathStr(first, "config", "other_info", "footer_info", "url")
		params.URLParams = parseURLParams(params.URL)
		return params, nil
	}

	// 多类型混合问答链路
	params.Data = map[string]any{"structured": multiShowTypeHandler(comps)}
	params.URL = getPathStr(first, "config", "other_info", "footer_info", "url")
	params.URLParams = parseURLParams(params.URL)
	return params, nil
}

func xuanguTableV1Handler(comp map[string]any) map[string]any {
	return map[string]any{
		"condition": getPathStr(comp, "data", "meta", "extra", "condition"),
		"comp_id":   comp["cid"],
		"uuid":      comp["puuid"],
	}
}

// showTypeHandlers 复刻 show_type_handler_dict；未注册类型走 commonHandler。
// 用 init() 填充而非字面量：handler 与 showTypeHandler 互相递归，包级变量
// 字面量初始化会触发 initialization loop。
var showTypeHandlers = map[string]func(comp map[string]any, comps []any) any{}

func init() {
	showTypeHandlers["container"] = containerHandler
	showTypeHandlers["txt1"] = txtHandler
	showTypeHandlers["txt2"] = txtHandler
	showTypeHandlers["tab4"] = tab4Handler
	showTypeHandlers["tab1"] = tab1Handler
	showTypeHandlers["dragon_tiger_stock"] = dragonTigerStockHandler
	showTypeHandlers["textblocklinkone"] = textBlockLinkOneHandler
	showTypeHandlers["nestedblocks"] = nestedBlocksHandler
	// wiki1 在上游被注释禁用（依赖额外网络跳转），此处保持一致：不注册 => 走 common
}

func showTypeHandler(comp any, comps []any) any {
	m, ok := comp.(map[string]any)
	if !ok {
		return nil
	}
	st, _ := m["show_type"].(string)
	if h, exists := showTypeHandlers[st]; exists {
		return h(m, comps)
	}
	return commonHandler(m)
}

func commonHandler(comp map[string]any) any {
	datas, _ := getPath(comp, "data", "datas")
	if arr, isArr := datas.([]any); isArr && len(arr) > 0 {
		rows := make([]map[string]any, 0, len(arr))
		for _, e := range arr {
			if r, ok := e.(map[string]any); ok {
				rows = append(rows, r)
			}
		}
		return rowsToTable(rows)
	}
	data, _ := getPath(comp, "data")
	return data
}

func containerHandler(comp map[string]any, comps []any) any {
	result := map[string]any{}
	children, _ := getPath(comp, "config", "children")
	arr, _ := children.([]any)
	for _, u := range arr {
		uuid, _ := u.(string)
		child := findCompByUUID(comps, uuid)
		if child == nil {
			continue
		}
		key := asString(child["show_type"])
		if key == "" {
			continue
		}
		result[key] = showTypeHandler(child, comps)
	}
	return result
}

func findCompByUUID(comps []any, uuid string) map[string]any {
	for _, c := range comps {
		if m, ok := c.(map[string]any); ok && asString(m["uuid"]) == uuid {
			return m
		}
	}
	return nil
}

func txtHandler(comp map[string]any, _ []any) any {
	content, _ := getPath(comp, "data", "content")
	return content
}

func tab4Handler(comp map[string]any, _ []any) any {
	result := map[string]any{}
	tabs, _ := comp["tab_list"].([]any)
	for _, tv := range tabs {
		tab, _ := tv.(map[string]any)
		if tab == nil {
			continue
		}
		name := asString(tab["tab_name"])
		if name == "" {
			continue
		}
		list, _ := tab["list"].([]any)
		sub := map[string]any{}
		for _, tc := range list {
			m, _ := tc.(map[string]any)
			if m == nil {
				continue
			}
			st := asString(m["show_type"])
			sub[st] = showTypeHandler(m, list)
		}
		result[name] = sub
	}
	return result
}

func tab1Handler(comp map[string]any, _ []any) any {
	result := map[string]any{}
	data, _ := comp["data"].(map[string]any)
	tabs, _ := comp["tab_list"].([]any)
	for _, tv := range tabs {
		tab, _ := tv.(map[string]any)
		if tab == nil {
			continue
		}
		name := asString(tab["tab_name"])
		if name == "" {
			continue
		}
		list, _ := tab["list"].([]any)
		sub := map[string]any{}
		for _, tc := range list {
			m, _ := tc.(map[string]any)
			if m == nil {
				continue
			}
			idx := asString(m["data_index"])
			if data != nil && idx != "" {
				m["data"] = data[idx] // 上游 tcomp.set('data', ...) 同款就地注入
			}
			sub[asString(m["show_type"])] = showTypeHandler(m, list)
		}
		result[name] = sub
	}
	return result
}

func dragonTigerStockHandler(comp map[string]any, _ []any) any {
	result := map[string]any{}
	first, ok := getPath(comp, "data", "datas", "0")
	rowMap, _ := first.(map[string]any)
	if !ok || rowMap == nil {
		return result
	}
	cp := map[string]any{}
	for k, v := range rowMap {
		cp[k] = v
	}
	detail := cp["detail"]
	delete(cp, "detail")
	result["data"] = rowsToTable([]map[string]any{cp})
	if dl, isArr := detail.([]any); isArr && len(dl) > 0 {
		d0, _ := dl[0].(map[string]any)
		detailOut := map[string]any{}
		if d0 != nil {
			for _, side := range []string{"buy", "sell"} {
				sideNode, _ := getPath(d0, side, "datas")
				if rows, ok := toRowList(sideNode); ok {
					detailOut[side] = rowsToTable(rows)
				}
			}
		}
		result["detail"] = detailOut
	}
	return result
}

func textBlockLinkOneHandler(comp map[string]any, _ []any) any {
	rows, ok := toRowList(getMust(comp, "data", "result", "data"))
	if !ok {
		return nil
	}
	return rowsToTable(rows)
}

func getMust(comp map[string]any, path ...string) any {
	v, _ := getPath(comp, path...)
	return v
}

// nestedBlocksHandler 股东户数类：subBlocks[].url 需要二次 HTTP GET。
// Go 版不在 convert 层做同步网络跳转（避免不可控放大请求量），改为返回 URL 清单，
// 由上层显式调用 FetchSubURL 拉取——语义等价、行为更可控。
func nestedBlocksHandler(comp map[string]any, _ []any) any {
	subs, _ := getPath(comp, "data", "result", "subBlocks", "0", "subBlocks")
	arr, _ := subs.([]any)
	urls := make([]string, 0, len(arr))
	for _, sv := range arr {
		m, _ := sv.(map[string]any)
		if m == nil {
			continue
		}
		if u := asString(m["url"]); u != "" {
			urls = append(urls, u)
		}
	}
	return map[string]any{"pending_urls": urls}
}

// multiShowTypeHandler 复刻 multi_show_type_handler：key=title.h1/config.title/show_type。
func multiShowTypeHandler(components []any) map[string]any {
	result := map[string]any{}
	for _, cv := range components {
		comp, _ := cv.(map[string]any)
		if comp == nil {
			continue
		}
		key := getCompKey(comp)
		value := showTypeHandler(comp, components)
		if key != "" && value != nil {
			result[key] = value
		}
	}
	return result
}

func getCompKey(comp map[string]any) string {
	if h1 := getPathStr(comp, "title_config", "data", "h1"); h1 != "" {
		return h1
	}
	if t := getPathStr(comp, "config", "title"); t != "" {
		return t
	}
	return asString(comp["show_type"])
}

// parseURLParams 复刻 parse_url_params：query -> map，多值取首个。
func parseURLParams(u string) map[string]string {
	out := map[string]string{}
	if u == "" {
		return out
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return out
	}
	for k, vs := range parsed.Query() {
		if len(vs) > 0 {
			out[k] = vs[0]
		}
	}
	return out
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}
