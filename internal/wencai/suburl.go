// FetchSubURL 补齐 convert.py get_url/nestedblocks/wiki1 的二跳能力：
// 上层拿到 pending_urls 后显式拉取并递归 show_type 解析。
package wencai

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// FetchSubURL GET baseURL+url 的组件数据并走同一套 show_type 解析。
// 对应 Python: get_url(url) -> show_type_handler(wcomp, comps)。
func (c *Client) FetchSubURL(ctx context.Context, subURL string) (any, error) {
	if !strings.HasPrefix(subURL, "http") {
		subURL = c.baseURL + subURL
	}
	var out any
	err := c.whileDo(ctx, "fetch_sub_url", func() error {
		hdrs, herr := c.buildHeaders(ctx)
		if herr != nil {
			return herr
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, subURL, nil)
		for k, vs := range hdrs {
			for _, v := range vs {
				req.Header.Set(k, v)
			}
		}
		resp, rerr := c.http.Do(req)
		if rerr != nil {
			return rerr
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		var doc map[string]any
		if jerr := json.Unmarshal(raw, &doc); jerr != nil {
			return fmt.Errorf("sub_url invalid json: %w", jerr)
		}
		data, ok := doc["data"]
		if !ok {
			return fmt.Errorf("sub_url missing data field")
		}
		comp, _ := data.(map[string]any)
		if comp == nil {
			return fmt.Errorf("sub_url data not an object")
		}
		out = showTypeHandler(comp, []any{data})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ResolvePendingNestedBlocks 把 nestedblocks handler 返回的 pending_urls 逐一拉取，
// 还原 Python nestedblocks_handler 的最终 []结果 形态。输入非该结构时原样返回。
func (c *Client) ResolvePendingNestedBlocks(ctx context.Context, node any) any {
	m, ok := node.(map[string]any)
	if !ok {
		return node
	}
	urlsAny, exists := m["pending_urls"]
	if !exists {
		return node
	}
	urls, _ := urlsAny.([]string)
	result := make([]any, 0, len(urls))
	for _, u := range urls {
		v, err := c.FetchSubURL(ctx, u)
		if err != nil {
			result = append(result, map[string]any{"url": u, "error": err.Error()})
			continue
		}
		result = append(result, v)
	}
	return result
}
