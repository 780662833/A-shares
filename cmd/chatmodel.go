package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	openai "github.com/sashabaranov/go-openai"
	"gorm.io/gorm"

	"ashare-quant/internal/graph"
)

// buildReviewRunner 组装 Phase 5 复盘 Graph 的执行器。
//
// ChatModel 接入策略（MVP）：
//   - 若配置了 ASHARE_API_KEY，则通过 eino-ext openai chatmodel 连接
//     OpenAI 兼容网关（ASHARE_LLM_BASE_URL 默认 https://api.openai.com/v1，
//     可指向 DeepSeek / 豆包 / Qwen 等兼容端点）；
//   - 未配置时返回「降级 runner」：复盘任务被显式拒绝并记录告警，
//     撮合、查询等其他功能不受影响 —— 系统绝不带着空大脑自动下单。
func buildReviewRunner(cfg appConfig, gdb *gorm.DB) reviewRunner {
	cm, err := newChatModelFromEnv()
	if err != nil {
		log.Printf("WARN: LLM not configured (%v); review task will run in DEGRADED mode (no auto-planning)", err)
		return func(_ context.Context, _ string, _ []string) (map[string]any, error) {
			return nil, errors.New("review disabled: LLM not configured, set ASHARE_API_KEY/ASHARE_MODEL/ASHARE_LLM_BASE_URL to enable daily review")
		}
	}

	runnable, err := graph.BuildReviewGraph(context.Background(), graph.Config{
		ChatModel:  cm,
		DB:         gdb,
		GatewayURL: cfg.GatewayURL,
	})
	if err != nil {
		// Graph 编译失败属致命装配错误：同样降级并打日志，不让进程起不来
		log.Printf("ERROR: build review graph failed: %v; review runs in DEGRADED mode", err)
		return func(_ context.Context, _ string, _ []string) (map[string]any, error) {
			return nil, fmt.Errorf("review graph unavailable: %w", err)
		}
	}

	return func(ctx context.Context, tradeDate string, watchlist []string) (map[string]any, error) {
		state, err := runnable.Invoke(ctx, graph.ReviewInput{TradeDate: tradeDate, Watchlist: watchlist})
		if err != nil {
			return nil, err
		}
		return state, nil
	}
}

// ---------------------------------------------------------------------------
// Eino ChatModel 适配层
// ---------------------------------------------------------------------------

// envOpenAIChatModel 以最小实现满足 model.BaseChatModel 接口，
// 底层走 OpenAI 兼容 /chat/completions。MVP 不引入重量级 provider SDK；
// 生产环境建议替换为 github.com/cloudwego/eino-ext/components/model/openai。
type envOpenAIChatModel struct {
	client *openai.Client
	name   string
}

var _ model.BaseChatModel = (*envOpenAIChatModel)(nil)

func newChatModelFromEnv() (model.BaseChatModel, error) {
	apiKey := strings.TrimSpace(os.Getenv("ASHARE_API_KEY"))
	if apiKey == "" {
		return nil, errors.New("ASHARE_API_KEY is empty")
	}
	name := strings.TrimSpace(os.Getenv("ASHARE_MODEL"))
	if name == "" {
		name = "gpt-4o-mini"
	}

	clientCfg := openai.DefaultConfig(apiKey)
	if baseURL := strings.TrimSpace(os.Getenv("ASHARE_LLM_BASE_URL")); baseURL != "" {
		clientCfg.BaseURL = strings.TrimRight(baseURL, "/") // 兼容 DeepSeek/豆包/Qwen 等 OpenAI 兼容网关
	}
	return &envOpenAIChatModel{client: openai.NewClientWithConfig(clientCfg), name: name}, nil
}

// Generate 把 Eino schema.Message 翻译为 OpenAI 请求并回译结果。
func (m *envOpenAIChatModel) Generate(ctx context.Context, input []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	msgs := make([]openai.ChatCompletionMessage, 0, len(input))
	for _, im := range input {
		role := openai.ChatMessageRoleUser
		switch im.Role {
		case schema.System:
			role = openai.ChatMessageRoleSystem
		case schema.Assistant:
			role = openai.ChatMessageRoleAssistant
		case schema.Tool:
			role = openai.ChatMessageRoleTool
		}
		msgs = append(msgs, openai.ChatCompletionMessage{Role: role, Content: im.Content})
	}

	resp, err := m.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model:       m.name,
		Messages:    msgs,
		Temperature: 0.2, // 决策类任务压低随机性
	})
	if err != nil {
		return nil, fmt.Errorf("chatmodel: openai request failed: %w", err)
	}
	if len(resp.Choices) == 0 {
		return nil, errors.New("chatmodel: empty choices in response")
	}
	out := resp.Choices[0].Message.Content
	// 防御部分兼容网关把推理文本放在 reasoning_content 而 content 为空的情况
	if strings.TrimSpace(out) == "" {
		return nil, fmt.Errorf("chatmodel: empty message content (finish=%s)", resp.Choices[0].FinishReason)
	}
	return schema.AssistantMessage(out, nil), nil
}

// Stream 未实现（本系统全部为同步 Invoke 流），显式报错而非静默。
func (m *envOpenAIChatModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, errors.New("chatmodel: streaming not implemented in MVP adapter")
}
