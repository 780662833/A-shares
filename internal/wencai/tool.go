// 本文件实现问财数据源的 Eino InvokableTool 包装，供 LLM Graph / Agent 直接调用。
// 设计规范与 internal/tool 其他工具一致：失败以 {"ok":false,...} 信封返回，不抛 Go error。
package wencai

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

var (
	_ tool.BaseTool      = (*WencaiQueryTool)(nil)
	_ tool.InvokableTool = (*WencaiQueryTool)(nil)
)

// ToolConfig 工具构造参数。
type ToolConfig struct {
	Client *Client // 必填：底层问财客户端
	// MaxRows table 结果最多回传行数（控制 LLM 上下文体积），默认 30。
	MaxRows int
	// CallTimeout 单次查询超时，默认 30s。
	CallTimeout time.Duration
}

// WencaiQueryTool 同花顺问财自然语言查询工具。零值不可用。
type WencaiQueryTool struct {
	cfg ToolConfig
}

// NewWencaiQueryTool 构造工具。
func NewWencaiQueryTool(cfg ToolConfig) (*WencaiQueryTool, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("wencai_tool: Client is required")
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 30
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = 30 * time.Second
	}
	return &WencaiQueryTool{cfg: cfg}, nil
}

// toolInput LLM 传入的 JSON 参数（对应 pywencai.get 的关键 kwargs）。
type toolInput struct {
	Query     string   `json:"query"`
	QueryType string   `json:"query_type,omitempty"` // stock(默认)/index/fund/bond/company...
	Loop      bool     `json:"loop,omitempty"`       // 是否分页拉全量
	MaxPage   int      `json:"max_page,omitempty"`   // loop 时页数上限（防超大结果），默认 3
	SortKey   string   `json:"sort_key,omitempty"`   // urp_sort_index，如 区间涨跌幅
	SortOrder string   `json:"sort_order,omitempty"` // up / down
	Find      []string `json:"find,omitempty"`       // 按代码列表精确反查
}

// Info 返回工具 schema。Desc 引导 LLM 用自然语言条件句提问。
func (t *WencaiQueryTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "wencai_query",
		Desc: "同花顺问财(iwencai)自然语言选股/资讯查询。输入中文条件句（如「连续3年ROE大于15%且市盈率小于30的股票」" +
			"「今日龙虎榜净买入大于5000万的股票」「近20日北向资金增持排名」），返回结构化表格或问答结构。" +
			"用于候选池增强筛选、题材/资金面证据补充、个股多维信息反查。" +
			"参数 query 必填；loop=true 分页取全量(受 max_page 限制)；find 传6位代码列表做精确反查。",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"query":      {Type: schema.String, Desc: "自然语言查询条件句（中文）", Required: true},
			"query_type": {Type: schema.String, Desc: "意图类型：stock(默认)/index/fund/bond/company", Required: false},
			"loop":       {Type: schema.Boolean, Desc: "是否分页拉取全量，默认 false 只取首页", Required: false},
			"max_page":   {Type: schema.Integer, Desc: "loop 模式最大页数，默认 3", Required: false},
			"sort_key":   {Type: schema.String, Desc: "排序字段(urp_sort_index)，可省略", Required: false},
			"sort_order": {Type: schema.String, Desc: "排序方向 up/down，可省略", Required: false},
			"find":       {Type: schema.Array, Desc: "6位股票代码字符串数组，精确反查模式", Required: false},
		}),
	}, nil
}

// InvokableRun 解析参数 -> Client.Get -> 截断归一化 -> JSON 信封。
func (t *WencaiQueryTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	var in toolInput
	if err := json.Unmarshal([]byte(strings.TrimSpace(argumentsInJSON)), &in); err != nil {
		return errEnvelope(fmt.Sprintf("arguments are not valid JSON: %v", err)), nil
	}
	if strings.TrimSpace(in.Query) == "" {
		return errEnvelope("query is required"), nil
	}

	cctx, cancel := context.WithTimeout(ctx, t.cfg.CallTimeout)
	defer cancel()

	res, err := t.cfg.Client.Get(cctx, Options{
		Query:     in.Query,
		QueryType: in.QueryType,
		Loop:      in.Loop,
		MaxLoop:   in.MaxPage,
		SortKey:   in.SortKey,
		SortOrder: in.SortOrder,
		Find:      in.Find,
	})
	if err != nil {
		log.Printf("wencai_tool: query=%.60q failed: %v", in.Query, err)
		return errEnvelope(fmt.Sprintf("wencai query failed: %v", err)), nil
	}

	out := map[string]any{"ok": true, "kind": res.Kind, "meta": compactMeta(res.Meta)}
	switch res.Kind {
	case "table":
		rows := res.Table.Rows
		truncated := false
		if len(rows) > t.cfg.MaxRows {
			rows = rows[:t.cfg.MaxRows]
			truncated = true
		}
		syms := res.Table.Symbols("")
		out["columns"] = res.Table.Columns
		out["total_rows"] = len(res.Table.Rows)
		out["rows"] = slimRows(rows, res.Table.Columns, t.cfg.MaxRows)
		out["truncated"] = truncated
		if len(syms) > 0 {
			out["symbols_preview"] = headStrings(syms, 20)
		}
	default:
		b, _ := json.Marshal(res.Structured)
		if len(b) > 8<<20 { // 8MB 防御：极端富文本问答直接摘要
			out["structured_digest"] = string(b[:8<<20]) + "...(truncated)"
		} else {
			out["structured"] = json.RawMessage(b)
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return errEnvelope(fmt.Sprintf("marshal result: %v", err)), nil
	}
	return string(b), nil
}

// compactMeta 只保留对 LLM 有用的元字段。
func compactMeta(m map[string]any) map[string]any {
	out := map[string]any{}
	for _, k := range []string{"row_count", "condition"} {
		if v, ok := m[k]; ok && v != "" && v != nil {
			out[k] = v
		}
	}
	if c, ok := m["condition"].(string); ok && len(c) > 300 {
		out["condition"] = c[:300] + "..."
	}
	return out
}

// slimRows 行内长字段截断，避免单条新闻正文挤爆上下文。
func slimRows(rows []map[string]any, cols []string, maxRows int) []map[string]any {
	colSet := map[string]bool{}
	for _, c := range cols {
		colSet[c] = true
	}
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		nr := make(map[string]any, len(r))
		for k, v := range r {
			if s, isStr := v.(string); isStr && len(s) > 120 {
				nr[k] = s[:120] + "..."
			} else {
				nr[k] = v
			}
		}
		out = append(out, nr)
	}
	return out
}

func headStrings(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func errEnvelope(msg string) string {
	b, _ := json.Marshal(map[string]any{"ok": false, "error": msg})
	return string(b)
}
