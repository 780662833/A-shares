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

	// ---- 补丁 A：逐证券生命周期决策（Transition() 输出，JSON round-trip 不再丢弃）----
	// LifecycleAction: TOUCH/KEEP/EXPIRE/MARK_STALE/PROTECT；空串 = 旧版网关载荷
	// （Python 链路无此字段），走显式兼容分支，绝不当作状态机 TOUCH 的隐式别名。
	LifecycleAction string `json:"lifecycle_action,omitempty"`
	// LastSeenAt / ExpireDateP: Transition() 返回的时间锚点，**按第 1 阶段裁定逐条
	// 透传**——Go 原生链路的 TOUCH 必须携带这两个值，写入层直接使用、不得重算
	// （pool 侧 AddDate(0,0,7) 自然日口径与落库侧 Add(7*24h) 定长时在夏令时环境
	// 并不等价，两处独立计算不构成契约）。缺失锚点的非空 TOUCH 属上游矛盾输入，
	// 保守保留并告警；空动作的旧 Python 载荷走独立兼容分支（沿用既有续期规则）。
	// 测试核对的是还原后的时间值与数据库读回值，不要求 JSON 文本逐字节相同。
	LastSeenAt  *time.Time `json:"last_seen_at,omitempty"`
	ExpireDateP *time.Time `json:"expire_date,omitempty"`
}

// WatchlistGenData 网关 /api/watchlist/generate 的 data 字段（导出供 cmd 层日志/统计使用）。
type WatchlistGenData struct {
	TradeDate string         `json:"trade_date"`
	Total     int            `json:"total"`
	BySource  map[string]int `json:"by_source"`
	Pool      []WatchEntry   `json:"pool"`
	Warnings  []string       `json:"warnings"`

	// ExtraDecisions 补丁 A：持久层已知但本轮未出现在 Pool 中的旧候选的状态机决策
	// （键=证券代码，值为 pool 层 AggEntry 同构 JSON 载荷，见 pool.LifecycleDecisionPayload）。
	// Python 网关载荷没有该键 -> nil -> 行为与旧版完全一致。仅供 cmd 原生链路注入。
	ExtraDecisions map[string]json.RawMessage `json:"extra_decisions,omitempty"`
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
	Updated     int      `json:"updated"`                 // 已存在且按状态机 TOUCH 刷新的条数
	Kept        int      `json:"kept"`                    // 补丁 A: KEEP 动作——保留原锚点, 不续期
	AuditOnly   int      `json:"audit_only"`              // 补丁 A: MARK_STALE/EXPIRE/未知动作——保守审计, 不改生命周期
	SkippedMan  int      `json:"skipped_manual_existing"` // 已存在且为 MANUAL, 跳过不覆盖
	Inactivated int      `json:"inactivated"`             // 本轮被置为 INACTIVE 的过期记录数（F-2 冻结下恒为 0）
	Warnings    []string `json:"warnings,omitempty"`
	TradeDate   string   `json:"trade_date"`
}

// ---------------------------------------------------------------------------
// 补丁 A：生命周期动作契约（写入层与 pool.Transition 的唯一映射入口）
// ---------------------------------------------------------------------------

const (
	actionTouch     = "TOUCH"
	actionKeep      = "KEEP"
	actionExpire    = "EXPIRE"
	actionMarkStale = "MARK_STALE"
	actionProtect   = "PROTECT"
)

type applyMode int

const (
	modeInvalid        applyMode = iota // 空动作（旧载荷兼容分支）解析失败后的哨兵，不应到达 switch
	applyModeTouch                      // TOUCH: 唯一允许正常刷新生命周期锚点的动作
	applyModeKeep                       // KEEP: 保留原时间锚点, 不因收到记录而续期
	applyModeAuditOnly                  // MARK_STALE / EXPIRE / 未知动作: 保守保留 + 审计
	applyModeProtect                    // PROTECT: 人工意图/黑名单, 自动逻辑不得改写
)

// lifecycleActionContract 把载荷中的动作字符串映射为写入模式。
// 返回 ok=false 表示非空但不属于合法枚举 -> 调用方必须走保守保留（绝不误续期）。
//   - 空串 = 旧版 Python 网关载荷（无该字段）：识别为兼容契约"收到即命中"，
//     等价 TOUCH，但**仅限**行级非人工/非黑名单记录（护栏在调用点，先于本映射生效；
//     即便旧载荷把 MANUAL 票混进 Pool，也不会被自动续期改写）。
//   - 大小写/空白宽容归一，与 normalizeSourceType 同风格。
func lifecycleActionContract(action string) (applyMode, bool) {
	switch strings.ToUpper(strings.TrimSpace(action)) {
	case "":
		return applyModeTouch, true // 显式兼容分支: 旧载荷"收到即命中", 不是状态机 TOUCH 的别名
	case actionTouch:
		return applyModeTouch, true
	case actionKeep:
		return applyModeKeep, true
	case actionProtect:
		return applyModeProtect, true
	case actionExpire, actionMarkStale:
		return applyModeAuditOnly, true
	default:
		return applyModeAuditOnly, false // 未知动作: 保守保留, 不退化为续期
	}
}

// contractActionLabel 审计日志用的动作标签（未知值原样回显便于排障）。
func contractActionLabel(action string) string {
	if a := strings.ToUpper(strings.TrimSpace(action)); a != "" {
		return a
	}
	return "LEGACY(implicit-touch)"
}

// anchorOrConservative 解析 TOUCH 应落地的 ExpireDate：
//   - 载荷携带 expire_date -> 使用状态机传来的值（第 1 阶段裁定：决策统一口径，
//     写入层不得重算）；
//   - 载荷缺失 -> 返回 nil，调用点必须走"保守保留 + 审计警告"分支——包括库里
//     没有旧值的记录，也**不得**在写入层用 now+WatchlistExpireDuration 补算
//     （那是被撤销的旧设计）。缺锚点的 TOUCH 属上游矛盾输入，宁可少续期。
func anchorOrConservative(e WatchEntry, now time.Time) *time.Time {
	if e.ExpireDateP != nil && !e.ExpireDateP.IsZero() {
		exp := *e.ExpireDateP
		return &exp
	}
	return nil
}

// nonAnchorMeta 提取 KEEP/AUDIT_ONLY 模式下允许更新的非生命周期元数据。
// 绝不包含 last_seen_at / expire_date / status / source_type——这些只能由
// TOUCH（或既有插入路径的人工语义）改写。
func nonAnchorMeta(e WatchEntry) map[string]any {
	meta := map[string]any{}
	if e.Name != "" {
		meta["name"] = e.Name
	}
	if e.SectorName != "" {
		meta["sector_name"] = e.SectorName
	}
	return meta
}

// extraDecision 是 ExtraDecisions 旁路的解码结构（pool.LifecycleDecisionPayload 同构：
// 动作 + 状态机透传的时间锚点；锚点在本旁路仅作审计留证，非 TOUCH 动作不得据其刷新）。
type extraDecision struct {
	LifecycleAction string     `json:"lifecycle_action"`
	LastSeenAt      *time.Time `json:"last_seen_at,omitempty"`
	ExpireDate      *time.Time `json:"expire_date,omitempty"`
}

// LifecycleActionString 供 pool 层回填载荷时复用同一枚举拼写检查（防止两侧漂移）。
func isValidLifecycleAction(a string) bool {
	switch strings.ToUpper(strings.TrimSpace(a)) {
	case actionTouch, actionKeep, actionExpire, actionMarkStale, actionProtect:
		return true
	}
	return false
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
//
// 补丁 A（决策统一）：既有记录的更新不再"收到即续期"，而是执行 Transition()
// 已算出的逐证券动作；动作缺失/非法时按显式兼容规则处理（见 lifecycleActionContract）。
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
			// 新候选入池：不属于五动作迁移（无持久记录可迁移），沿用既有人工/自动
			// 初始化语义。Go 链路中该条目 LifecycleAction 恒为空（Transition 只覆盖
			// 持久层已有记录），旧载荷兼容分支对其同样只影响"存在感"字段。
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
			// —— 补丁 A：按状态机动作决定写入，不再独立推算生命周期 ——
			// P1（护栏）+ 审计顺序（本轮裁定）：先解析动作并记录未知动作警告
			// （lifecycleActionContract 只做分类，不写数据库），再执行 MANUAL /
			// BLACKLIST 行级护栏，最后才进入动作处理分支。这样既保留未知动作的
			// 审计信号，又保证受保护行在**一切**动作（TOUCH/KEEP/MARK_STALE/
			// EXPIRE/PROTECT/未知动作/空动作旧载荷）下零写入——包括 name、
			// sector_name、生命周期锚点（last_seen_at/expire_date）、status 与
			// source_type。旧实现只在 TOUCH 分支内做行级检查，KEEP/AUDIT_ONLY
			// 仍会把元数据写进受保护行，属契约缺口；护栏位于 switch mode 及任何
			// 数据库写入之前后，该路径不复存在。
			mode, ok := lifecycleActionContract(e.LifecycleAction)
			if !ok {
				res.Warnings = append(res.Warnings,
					fmt.Sprintf("watchlist: unknown lifecycle_action %q for %s (conservative keep)", e.LifecycleAction, code))
			}
			if existing.SourceType == model.WatchSourceManual || existing.Status == model.WatchStatusBlacklist {
				res.SkippedMan++
				continue
			}
			switch mode {
			case applyModeProtect:
				res.SkippedMan++
				continue
			case applyModeKeep:
				if meta := nonAnchorMeta(e); len(meta) > 0 {
					if err := gdb.Model(&model.WatchlistItem{}).Where("id = ?", existing.ID).Updates(meta).Error; err != nil {
						return res, fmt.Errorf("watchlist: keep %s metadata: %w", code, err)
					}
				}
				res.Kept++
				continue
			case applyModeTouch:
				// P2（双锚点校验）：Go 原生链路的 TOUCH 必须同时携带有效的 LastSeenAt 与
				// ExpireDateP——任一缺失或为零时间即上游矛盾输入，保守保留原时间字段并
				// 产生明确审计警告，绝不半更新（只刷 last_seen_at 静默保留旧 expire_date
				// 的旧路径已删除），也绝不静默重算续期。
				// （空动作走的是旧版 Python 兼容分支，沿用其"收到即命中 + now 续期"
				// 既有契约，不受本校验约束——见 watchlist_apply_test 的分离测试。）
				if e.LifecycleAction != "" &&
					(e.LastSeenAt == nil ||
						e.LastSeenAt.IsZero() ||
						e.ExpireDateP == nil ||
						e.ExpireDateP.IsZero()) {
					res.Warnings = append(res.Warnings,
						fmt.Sprintf("watchlist: TOUCH without state-machine anchor for %s (conservative keep, no renewal)", code))
					log.Printf("watchlist: lifecycle audit-only code=%s action=TOUCH(missing-anchor) reason=inconsistent input", code)
					res.AuditOnly++
					continue
				}
				updates := map[string]any{"status": model.WatchStatusActive}
				// 执行顺序契约：上方双锚点校验先于本段运行——凡 LifecycleAction 非空
				// 且缺任一有效锚点的 TOUCH 都已整体保守保留（continue），能走到这里的
				// 只有两种形态：(a) Go 原生 TOUCH，双锚点齐备且非零；(b) 空动作旧载荷。
				// 因此下述 else 分支只对旧载荷可达，非空 TOUCH 不存在"只刷一半锚点"
				// 的半更新路径。
				if e.LastSeenAt != nil && !e.LastSeenAt.IsZero() {
					updates["last_seen_at"] = *e.LastSeenAt // 状态机传来的锚点（Go 链路必携带）
				} else {
					updates["last_seen_at"] = now // 仅旧载荷兼容分支可达：沿用既有"收到即命中"续期规则
				}
				if exp := anchorOrConservative(e, now); exp != nil {
					updates["expire_date"] = *exp
				} else {
					// 旧载荷兼容分支（仅空动作可达）：Python 网关无 expire_date 字段，
					// 沿用既有"收到即命中 + now 续期 WatchlistExpireDuration"口径。
					exp := now.Add(WatchlistExpireDuration)
					updates["expire_date"] = exp
				}
				if e.Name != "" {
					updates["name"] = e.Name
				}
				if e.SectorName != "" {
					updates["sector_name"] = e.SectorName
				}
				if st := normalizeSourceType(e.SourceType); st != "" && st != existing.SourceType {
					updates["source_type"] = st
				}
				if err := gdb.Model(&model.WatchlistItem{}).Where("id = ?", existing.ID).Updates(updates).Error; err != nil {
					return res, fmt.Errorf("watchlist: update %s: %w", code, err)
				}
				res.Updated++
			case applyModeAuditOnly:
				// MARK_STALE / EXPIRE / 未知动作：保留原状态与原时间锚点，只允许非生命
				// 周期元数据（名称/板块）更新；绝不写 STALE/EXPIRED 等模型外枚举，绝不
				// 借道 status 恢复自动失效。
				if meta := nonAnchorMeta(e); len(meta) > 0 {
					if err := gdb.Model(&model.WatchlistItem{}).Where("id = ?", existing.ID).Updates(meta).Error; err != nil {
						return res, fmt.Errorf("watchlist: audit-only %s metadata: %w", code, err)
					}
				}
				log.Printf("watchlist: lifecycle audit-only code=%s action=%s reason=%s",
					code, contractActionLabel(e.LifecycleAction), expirySkipReason(sourceStatuses))
				res.AuditOnly++
			}
		}
	}

	// —— 补丁 A：持久层已知但本轮未出现在 Pool 中的旧候选（ExtraDecisions 旁路）——
	// 这些记录由 Generate() 的 Transition 全量遍历产出（persistedHit=false 口径），
	// 使 KEEP/MARK_STALE/EXPIRE/PROTECT 决策不再依赖"是否出现在本轮池子"。
	for code, raw := range data.ExtraDecisions {
		code = strings.TrimSpace(code)
		if !symbolPattern.MatchString(code) || seen[code] {
			continue // 已在主循环处理或非法代码
		}
		var d extraDecision
		if err := json.Unmarshal(raw, &d); err != nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("watchlist: bad extra decision for %s: %v", code, err))
			continue
		}
		mode, ok := lifecycleActionContract(d.LifecycleAction)
		if !ok {
			res.Warnings = append(res.Warnings,
				fmt.Sprintf("watchlist: unknown lifecycle_action %q for %s (conservative keep)", d.LifecycleAction, code))
		}
		var existing model.WatchlistItem
		err := gdb.Where("code = ?", code).First(&existing).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue // 旁路指向库里已不存在的记录: 无事可做（不新建）
		}
		if err != nil {
			return res, fmt.Errorf("watchlist: query extra %s: %w", code, err)
		}
		switch mode {
		case applyModeProtect:
			res.SkippedMan++
		case applyModeKeep:
			res.Kept++
		case applyModeTouch:
			// 旧候选本轮缺席却收到 TOUCH 属上游矛盾输入：保守降级为审计保留。
			log.Printf("watchlist: lifecycle audit-only code=%s action=TOUCH(absent-from-pool) reason=inconsistent input", code)
			res.AuditOnly++
		case applyModeAuditOnly:
			log.Printf("watchlist: lifecycle audit-only code=%s action=%s reason=%s",
				code, contractActionLabel(d.LifecycleAction), expirySkipReason(sourceStatuses))
			res.AuditOnly++
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
