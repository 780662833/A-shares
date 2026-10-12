// Package wencai 是同花顺问财(iwencai)数据源的纯 Go 实现模块。
//
// 复刻上游 pywencai (https://github.com/zsrl/pywencai) 的完整调用链：
//
//	hexin-v token 生成 -> get_robot_data(解析 condition/row_count/url_params)
//	-> getDataList / find(分页取数) -> show_type 结构化转换(container/tab1/tab4/
//	txt/dragon_tiger_stock/textblocklinkone/nestedblocks/wiki1/xuangu_tableV1)
//	-> 统一返回结构 map[string]any。
//
// 与 Python 版的差异（工程化改进）：
//   - hexin-v.js 通过 go:embed 内嵌，运行时用 node 子进程执行（同 pywencai 的
//     subprocess 方案），token 带 TTL 缓存并支持失效自动重生成；
//   - User-Agent 内置确定性轮转表，不依赖 fake_useragent；
//   - while_do 重试语义 1:1 复刻（retry/sleep/log），但错误不再吞栈，聚合上报；
//   - DataFrame 用 []map[string]any + ColumnOrder 表达，保持列顺序稳定。
package wencai

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

//go:embed assets/hexin-v.js
var hexinJS string

// tokenCacheTTL hexin-v token 复用有效期。上游每次请求都重新起 node 生成，
// 实测同一自然日内 token 可复用；这里保守取 10 分钟，兼顾性能与风控。
const defaultTokenTTL = 10 * time.Minute

type cachedToken struct {
	value   string
	expires time.Time
}

// TokenProvider 负责生成并缓存 hexin-v token。零值不可用，请用 NewTokenProvider。
type TokenProvider struct {
	mu     sync.Mutex
	node   string // node 可执行文件路径（空则自动探测）
	ttl    time.Duration
	cached *cachedToken
	// scriptPath 非空时从磁盘脚本执行（测试注入用），否则写临时文件。
	scriptPath string
}

// NewTokenProvider 构造 token 提供者。ttl<=0 时使用默认 10 分钟。
func NewTokenProvider(ttl time.Duration) *TokenProvider {
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	return &TokenProvider{ttl: ttl}
}

// Get 返回当前有效 token；缓存未过期直接命中，否则调用 node 重新生成。
// ctx 用于取消等待中的 node 子进程。
func (p *TokenProvider) Get(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cached != nil && time.Now().Before(p.cached.expires) {
		return p.cached.value, nil
	}

	token, err := p.runNode(ctx)
	if err != nil {
		return "", fmt.Errorf("wencai: generate hexin-v token: %w", err)
	}
	p.cached = &cachedToken{value: token, expires: time.Now().Add(p.ttl)}
	return token, nil
}

// Invalidate 强制丢弃缓存 token（收到问财风控响应时由 Client 调用）。
func (p *TokenProvider) Invalidate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cached = nil
}

// runNode 执行内嵌 JS 并把 stdout 第一行作为 token。
func (p *TokenProvider) runNode(ctx context.Context) (string, error) {
	nodePath, err := p.resolveNode()
	if err != nil {
		return "", err
	}

	script := p.scriptPath
	var cleanup func()
	if script == "" {
		// 写入临时文件执行（hexin-v.js 使用 require("jsdom")，必须是真实文件路径）
		f, cerr := os.CreateTemp("", "hexin-v-*.js")
		if cerr != nil {
			return "", fmt.Errorf("write temp script: %w", cerr)
		}
		if _, werr := f.WriteString(hexinJS); werr != nil {
			f.Close()
			os.Remove(f.Name())
			return "", fmt.Errorf("write temp script: %w", werr)
		}
		f.Close()
		script = f.Name()
		cleanup = func() { os.Remove(script) }
	} else {
		cleanup = func() {}
	}
	defer cleanup()

	cmd := exec.CommandContext(ctx, nodePath, script)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("node exit: %v, stderr=%.200q", err, stderr.String())
	}
	token := strings.TrimSpace(stdout.String())
	// 兼容多行输出：取最后一个非空行（console.log(v()) 为最终输出）
	lines := strings.Split(token, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if s := strings.TrimSpace(lines[i]); s != "" {
			token = s
			break
		}
	}
	if token == "" || strings.Contains(token, "Error") {
		return "", fmt.Errorf("bad token output %.100q stderr=%.100q", token, stderr.String())
	}
	return token, nil
}

// resolveNode 定位 node 可执行文件：显式配置 > PATH 探测。
func (p *TokenProvider) resolveNode() (string, error) {
	if p.node != "" {
		return p.node, nil
	}
	path, err := exec.LookPath("node")
	if err != nil {
		return "", fmt.Errorf("node runtime not found in PATH (required for hexin-v token; " +
			"install node or set ASHARE_WENCAI_NODE_BIN)")
	}
	return path, nil
}
