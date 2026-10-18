// 本文件实现动态自选股池（Layer 0.5）的 Go 侧核心逻辑：
// 调用 Python 网关 POST /api/watchlist/generate，并按生命周期规则落库。
//
// 生命周期规则（与架构设计严格对齐）：
//  1. 生成器命中的股票：不存在则插入（非 MANUAL 来源 ExpireDate = now + 7 天，
//     ACTIVE）；已存在且 SourceType == MANUAL 则跳过不覆盖人工白名单；
//     其余已存在的更新 LastSeenAt / SectorName / Name，并保证 ACTIVE。
//  2. 清理：今日未出现在返回结果中、SourceType != MANUAL、Status == ACTIVE
//     且 ExpireDate < now 的旧记录 -> Status = INACTIVE（保留历史行，不物理删除）。
//  3. BLACKLIST 为人工终态：任何路径都不改写其 Status。
//
// 失败语义：网关不可达/解析失败时返回 error，由调用方（Graph 节点或 cron）
// 决定降级策略——通常降级为"沿用昨日池子"，绝不因池子更新失败而清空 watchlist。
package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"gorm.io/gorm"

	"ashare-quant/internal/model"
)

// WatchlistExpireDuration 非 MANUAL 来源记录的默认过期时长。
// 架构要求"5 个交易日"，自然日近似取 7 天（含周末），到期后若未被再次命中则转 INACTIVE。
var WatchlistExpireDuration = 7 * 24 * time.Hour

// ---------------------------------------------------------------------------
// 网关响应结构（与 data_gateway/watchlist_generator.py 输出字段对齐）
// ---------------------------------------------------------------------------

// WatchEntry 动态池中的一只股票/ETF。
type WatchEntry struct {
	Symbol      string   `json:"symbol"`
	Name        string   `json:"name"`
	SourceType  string   `json:"source_type"` // MANUAL/SECTOR_LEADER/ETF_MOMENTUM/FUND_FLOW
	SectorName  string   `json:"sector_name"`
	Momentum20D *float64 `json:"momentum_20d_pct,omitempty"`
	OrgNetBuy   *float64 `json:"org_net_buy,omitempty"`
}

// WatchlistGenData 网关 /api/watchlist/generate 的 data 字段（导出供 cmd 层日志/统计使用）。
type WatchlistGenData struct {
	TradeDate string         `json:"trade_date"`
	Total     int            `json:"total"`
	BySource  map[string]int `json:"by_source"`
	Pool      []WatchEntry   `json:"pool"`
	Warnings  []string       `json:"warnings"`
}

type watchlistGenData = WatchlistGenData

type watchlistGenEnvelope struct {
	Code    int               `json:"code"`
	Message string            `json:"message"`
	Data    *watchlistGenData `json:"data"`
}

// UpdateWatchlistResult 一次池子更新的统计结果。
type UpdateWatchlistResult struct {
	Generated   int      `json:"generated"`               // 网关返回的池子大小
	Inserted    int      `json:"inserted"`                // 新插入条数
	Updated     int      `json:"updated"`                 // 已存在被刷新 LastSeenAt 的条数
	SkippedMan  int      `json:"skipped_manual_existing"` // 已存在且为 MANUAL, 跳过不覆盖
	Inactivated int      `json:"inactivated"`             // 本轮被置为 INACTIVE 的过期记录数
	Warnings    []string `json:"warnings,omitempty"`
	TradeDate   string   `json:"trade_date"`
}

// ---------------------------------------------------------------------------
// HTTP 调用
// ---------------------------------------------------------------------------

// FetchGeneratedWatchlist 调用网关生成今日动态池。manualSymbols 作为人工白名单透传。
func FetchGeneratedWatchlist(ctx context.Context, baseURL string, manualSymbols []string, timeout time.Duration) (*watchlistGenData, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil, errors.New("watchlist: gateway base url is empty")
	}
	if timeout <= 0 {
		timeout = 3 * time.Minute // ETF 动量精算最多 60 只, 留足余量
	}
	body := map[string]any{"manual_symbols": manualSymbols}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("watchlist: marshal request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+"/api/watchlist/generate", strings.NewReader(string(payload)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("watchlist: gateway unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("watchlist: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("watchlist: gateway HTTP %d: %.256s", resp.StatusCode, string(raw))
	}
	var env watchlistGenEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("watchlist: decode response: %w", err)
	}
	if env.Data == nil {
		return nil, fmt.Errorf("watchlist: empty data envelope (message=%q)", env.Message)
	}
	return env.Data, nil
}

// ---------------------------------------------------------------------------
// 落库（纯函数式，便于单测注入 gdb）
// ---------------------------------------------------------------------------

// ApplyWatchlistPool 按生命周期规则把生成结果写入 SQLite。now 注入以便测试时间边界。
//
// sourceStatuses 为本轮实际来源刷新状态（pool.Result.SourceStatuses 的字符串口径，
// 键为来源名如 SECTOR_LEADER/ETF_MOMENTUM/FUND_FLOW/LIQUIDITY_SCREEN）。
// F-2 冻结合同（第 4 步 E 审计结论）：当前 watchlist_items 没有可信的来源级
// data_as_of 字段，LastSeenAt/运行时刻/旧 ExpireDate 都不能冒充行情有效日期，
// 因此自动过期 sweep 安全降级为逐条保守审计——识别潜在过期候选并记录跳过原因，
// 但不写 INACTIVE。PARTIAL/FAILED/STALE、状态缺失或未知同样不得触发失效；
// MANUAL 行继续受保护。恢复时间驱动的自动失效需要先恢复可信数据日期证据链。
func ApplyWatchlistPool(gdb *gorm.DB, data *watchlistGenData, now time.Time, sourceStatuses map[string]string) (*UpdateWatchlistResult, error) {
	if gdb == nil {
		return nil, errors.New("watchlist: nil db")
	}
	if data == nil {
		return nil, errors.New("watchlist: nil pool data")
	}
	res := &UpdateWatchlistResult{Generated: len(data.Pool), Warnings: data.Warnings, TradeDate: data.TradeDate}

	seen := make(map[string]bool, len(data.Pool))
	for _, e := range data.Pool {
		code := strings.TrimSpace(e.Symbol)
		if !symbolPattern.MatchString(code) || seen[code] {
			continue // 非法代码/重复条目直接丢弃（网关已去重, 双保险）
		}
		seen[code] = true

		var existing model.WatchlistItem
		err := gdb.Where("code = ?", code).First(&existing).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			item := model.WatchlistItem{
				Code:       code,
				Name:       e.Name,
				SourceType: normalizeSourceType(e.SourceType),
				SectorName: e.SectorName,
				Status:     model.WatchStatusActive,
				AddedDate:  now,
				LastSeenAt: &now,
			}
			if item.SourceType != model.WatchSourceManual {
				exp := now.Add(WatchlistExpireDuration)
				item.ExpireDate = &exp
			}
			if err := gdb.Create(&item).Error; err != nil {
				return res, fmt.Errorf("watchlist: insert %s: %w", code, err)
			}
			res.Inserted++
		case err != nil:
			return res, fmt.Errorf("watchlist: query %s: %w", code, err)
		default:
			// 人工白名单/黑名单: 不覆盖来源与状态, 仅刷新存在感(黑名单连 LastSeenAt 也不动, 保持纯人工语义)
			if existing.SourceType == model.WatchSourceManual || existing.Status == model.WatchStatusBlacklist {
				res.SkippedMan++
				continue
			}
			updates := map[string]any{"last_seen_at": now, "status": model.WatchStatusActive}
			if e.Name != "" {
				updates["name"] = e.Name
			}
			if e.SectorName != "" {
				updates["sector_name"] = e.SectorName
			}
			if st := normalizeSourceType(e.SourceType); st != "" && st != existing.SourceType {
				updates["source_type"] = st
			}
			// 重新命中即续期
			exp := now.Add(WatchlistExpireDuration)
			updates["expire_date"] = exp
			if err := gdb.Model(&model.WatchlistItem{}).Where("id = ?", existing.ID).Updates(updates).Error; err != nil {
				return res, fmt.Errorf("watchlist: update %s: %w", code, err)
			}
			res.Updated++
		}
	}

	// —— F-2: 过期候选的保守逐条审计（不再执行基于旧 expire_date 的批量 INACTIVE 更新）——
	// BLACKLIST 因 status<>ACTIVE 天然豁免; MANUAL 行继续受保护。
	// 查询本身失败必须向上返回错误, 不能只记日志后宣称持久化成功。
	var candidates []model.WatchlistItem
	cq := gdb.Where("status = ?", model.WatchStatusActive).
		Where("source_type <> ?", model.WatchSourceManual).
		Where("expire_date IS NOT NULL AND expire_date < ?", now)
	if len(seen) > 0 {
		missed := make([]any, 0, len(seen))
		for c := range seen {
			missed = append(missed, c)
		}
		cq = cq.Where("code NOT IN ?", missed)
	}
	if err := cq.Find(&candidates).Error; err != nil {
		return res, fmt.Errorf("watchlist: expiry audit query: %w", err)
	}
	for _, cand := range candidates {
		log.Printf("watchlist: expiry candidate kept code=%s expire_date=%s reason=%s",
			cand.Code, cand.ExpireDate.Format(time.RFC3339), expirySkipReason(sourceStatuses))
	}
	return res, nil
}

// expirySkipReason 依据本轮来源状态与数据日期证据链给出保守保留原因。
// 无论来源状态如何, 本阶段都不允许时间驱动失效（缺少可信 data_as_of 证据链）;
// 状态维度仅用于审计区分: PARTIAL/FAILED/STALE 与缺失/未知状态在合同上本就
// 禁止依据未命中推进生命周期, 完整成功(OK/SUCCESS_EMPTY)也不足以单独放行失效。
func expirySkipReason(sourceStatuses map[string]string) string {
	if len(sourceStatuses) == 0 {
		return "source statuses missing/unknown (conservative keep)"
	}
	for _, s := range sourceStatuses {
		switch strings.ToUpper(strings.TrimSpace(s)) {
		case "PARTIAL":
			return "source refresh PARTIAL (lifecycle update forbidden)"
		case "FAILED":
			return "source refresh FAILED (lifecycle update forbidden)"
		case "STALE":
			return "source refresh STALE (lifecycle update forbidden)"
		case "OK", "SUCCESS_EMPTY":
			// 完整成功可作审计信号, 但仍无数据日期证据 -> 继续保守保留。
		default:
			return "source status unknown (conservative keep)"
		}
	}
	return "no reliable data_as_of evidence chain (auto-expiry suspended)"
}

func normalizeSourceType(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case model.WatchSourceManual:
		return model.WatchSourceManual
	case model.WatchSourceSector:
		return model.WatchSourceSector
	case model.WatchSourceETF:
		return model.WatchSourceETF
	case model.WatchSourceFund:
		return model.WatchSourceFund
	default:
		return model.WatchSourceFund // 未知来源保守视为最短命来源
	}
}

// ActiveWatchlistCodes 返回当前应参与每日粗筛的股票代码（仅按 status = ACTIVE 过滤）。
// 供 review 流水线在动态池模式下替代环境变量 ASHARE_WATCHLIST 硬编码列表。
//
// F-2.5 语义修复：本函数**不得**再依据 ExpireDate 排除仍为 ACTIVE 的行。
// 3.4.4/F-2 已冻结"自动失效暂停"合同——写入路径（ApplyWatchlistPool）在缺乏
// 可信 data_as_of 证据时不将记录置 INACTIVE；若读取路径继续用旧 ExpireDate
// 过滤，等于通过另一条路径实现相同的过期效果，两条生产环节会对同一条记录给
// 出不同结论。因此这里只认 status 字段；now 参数保留仅为兼容既有调用方签名。
//
// 恢复基于日期的读取过滤的前置条件与自动过期恢复相同：必须先建立可信的
// 来源级数据有效日期证据链并经独立审批，不得用运行时间或 LastSeenAt 推断。
func ActiveWatchlistCodes(gdb *gorm.DB, now time.Time) ([]string, error) {
	_ = now // F-2.5: 读取过滤不再使用运行时刻（签名保持兼容）
	var items []model.WatchlistItem
	if err := gdb.Where("status = ?", model.WatchStatusActive).Find(&items).Error; err != nil {
		return nil, err
	}
	codes := make([]string, 0, len(items))
	for _, it := range items {
		codes = append(codes, it.Code)
	}
	return codes, nil
}
