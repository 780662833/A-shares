// freshness_test.go —— 第 0 阶段验收测试：normalizeBusinessDate / shanghaiLoc /
// FreshnessUnknown 的边界语义锁定（对应审查意见"二.1/二.2/二.3"）。
// 全部离线，不触碰网络与数据库。
package pool

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNormalizeBusinessDate_AcceptsValid(t *testing.T) {
	bound := "2026-10-12"
	for _, raw := range []string{"2026-10-12", "2026-02-28", "2024-02-29", "2026-01-01"} {
		got, err := normalizeBusinessDate(raw, bound)
		if err != nil {
			t.Fatalf("合法日期 %q 应通过: %v", raw, err)
		}
		if got != raw {
			t.Fatalf("归一化应保真: in=%q out=%q", raw, got)
		}
	}
}

func TestNormalizeBusinessDate_RejectsInvalid(t *testing.T) {
	bound := "2026-10-12"
	bad := []struct{ raw, why string }{
		{"", "空串≠合法日期"},
		{" 2026-10-12", "前导空白"},
		{"2026-10-12 ", "尾随空白"},
		{"2026-13-01", "非法月份"},
		{"2026-02-30", "历法不存在"},
		{"2025-02-29", "平年无 2/29"},
		{"20261012", "非 YYYY-MM-DD 格式"},
		{"2026/10/12", "分隔符错误"},
		{"2026-10-12T00:00:00", "带时间部分"},
		{"not-a-date", "垃圾输入"},
		{"2026-10-13", "晚于评估日（未来数据）"},
		{"2027-01-01", "远未来"},
	}
	for _, c := range bad {
		if _, err := normalizeBusinessDate(c.raw, bound); err == nil {
			t.Fatalf("应拒绝 %q（%s）", c.raw, c.why)
		}
	}
}

func TestNormalizeBusinessDate_RejectsBadUpperBound(t *testing.T) {
	// 上界本身非法时宁可不判，也不放行可疑日期。
	for _, bound := range []string{"", "garbage", "2026-02-30"} {
		if _, err := normalizeBusinessDate("2026-10-01", bound); err == nil {
			t.Fatalf("上界 %q 非法时应整体拒绝", bound)
		}
	}
}

func TestShanghaiLoc_DaylightSafeBoundary(t *testing.T) {
	// UTC 2026-10-12 20:00 = 上海 2026-10-13 04:00——tradeDateOf 无论传入何种
	// 带时区的时刻，都必须先折算到上海再取日历日（统一业务日期口径）。
	utc := time.Date(2026, 10, 12, 20, 0, 0, 0, time.UTC)
	if got := tradeDateOf(utc.In(shanghaiLoc)); got != "2026-10-13" {
		t.Fatalf("上海口径折算错误: %s", got)
	}
	// 同一时刻的 UTC 表示也必须经 In(shanghaiLoc) 归一 → 同为 10-13。
	// （若有人把 tradeDateOf 改回裸 Format，此断言立即失败。）
	if got := tradeDateOf(utc); got != "2026-10-13" {
		t.Fatalf("tradeDateOf 必须内部折算上海时区, got %s", got)
	}
	// 上海清晨之前的时刻仍属前一交易日。
	sh := time.Date(2026, 10, 12, 4, 0, 0, 0, shanghaiLoc)
	if got := tradeDateOf(sh); got != "2026-10-12" {
		t.Fatalf("上海凌晨应仍为当日: %s", got)
	}
}

func TestFreshnessUnknown_SentinelValue(t *testing.T) {
	// 哨兵字面量被持久化边界与注释合同引用——改动会破坏 UNKNOWN 登记链。
	if FreshnessUnknown != "UNKNOWN" {
		t.Fatalf("FreshnessUnknown 必须恒为 UNKNOWN, got %q", FreshnessUnknown)
	}
}

func TestLiquidityGatePool_LiqDataAsOfRowIntegrity(t *testing.T) {
	// 审查意见二.2：LiqDataAsOf 必须来自 Amount>0 且日期可验证的同一行；
	// 日期缺失/非法/未来的行不得授予日期结论，即便其金额参与过窗口。
	f := newFake()
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return []DailyAmountRow{
			{Date: "2026-10-08", Amount: 2e8},
			{Date: "2026-10-09", Amount: 2e8},
			{Date: "", Amount: 2e8}, // 最后一行金额有效但日期缺失 → 不可证
		}
	}
	g := NewGenerator(f, DefaultConfig())
	pool := []AggEntry{{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector}}
	var warns []string
	out := g.liquidityGatePool(context.Background(), pool, &warns, "2026-10-12", nil, testNow)
	if len(out) != 1 {
		t.Fatalf("样本不足只标 DATA_INSUFFICIENT, 不剔除: %+v", out)
	}
	if out[0].LiqDataAsOf != "" {
		t.Fatalf("末行日期不可证时必须诚实留空, got %q", out[0].LiqDataAsOf)
	}

	// 同一闸门：末行合法 → 取该行日期。
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return []DailyAmountRow{
			{Date: "2026-10-08", Amount: 2e8},
			{Date: "2026-10-09", Amount: 2e8},
		}
	}
	pool = []AggEntry{{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector}}
	out = g.liquidityGatePool(context.Background(), pool, &warns, "2026-10-12", nil, testNow)
	if out[0].LiqDataAsOf != "2026-10-09" {
		t.Fatalf("应取窗口内最后一个有效样本行日期, got %q", out[0].LiqDataAsOf)
	}

	// 未来日期行：金额>0 也绝不授予新鲜度结论。
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return []DailyAmountRow{
			{Date: "2026-10-08", Amount: 2e8},
			{Date: "2026-10-20", Amount: 2e8}, // 晚于评估日
		}
	}
	pool = []AggEntry{{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector}}
	out = g.liquidityGatePool(context.Background(), pool, &warns, "2026-10-12", nil, testNow)
	if out[0].LiqDataAsOf != "" {
		t.Fatalf("未来日期行不得进入新鲜度结论, got %q", out[0].LiqDataAsOf)
	}
}

func TestLiquidityGatePool_TradeDateIsTheBound(t *testing.T) {
	// 审查意见二.1：验证上界必须是调用方传入的 tradeDate，不能悄悄换成系统当前日期。
	// 以 2026-10-12 为评估日，样本行落在"今天真实世界"的未来一侧也必须拒绝——
	// 用固定 tradeDate + 相对 testNow 合法的日期即可复现该口径差异。
	f := newFake()
	f.stockAmounts = func(code string, lookback int) []DailyAmountRow {
		return []DailyAmountRow{{Date: "2026-10-11", Amount: 2e8}}
	}
	g := NewGenerator(f, DefaultConfig())
	pool := []AggEntry{{Symbol: "600519.SH", PrimarySource: sourceSector, SourceType: sourceSector}}
	var warns []string
	// 上界收紧到 2026-10-10：2026-10-11 相对"系统今天"合法、相对评估日是未来 → 必须拒。
	out := g.liquidityGatePool(context.Background(), pool, &warns, "2026-10-10", nil, testNow)
	if out[0].LiqDataAsOf != "" {
		t.Fatalf("上界必须取调用方 tradeDate, got %q", out[0].LiqDataAsOf)
	}
	// 上界为 2026-10-12：同一样本合法放行。
	out = g.liquidityGatePool(context.Background(), pool, &warns, "2026-10-12", nil, testNow)
	if out[0].LiqDataAsOf != "2026-10-11" {
		t.Fatalf("tradeDate 界内样本应放行, got %q", out[0].LiqDataAsOf)
	}
}

func TestDragonTiger_RawDateNotCertified(t *testing.T) {
	// 审查意见二.3：genDragonTiger 的第三返回值是未验证原始日期——Generate 侧
	// 明确 `_ = fundSrcDateRaw` 不入 srcDates；这里锁定函数本身的透传语义：
	// 原始值可审计（非空即最后命中行的 Date），但不做任何归一化/校验。
	f := baseFake()
	f.dragon = append(f.dragon, InstitutionBuy{
		Code: "600519", Name: "贵州茅台", OrgNetAmount: 5e8, Date: "2026/10/09", // 畸形格式
	})
	g := NewGenerator(f, DefaultConfig())
	entries, rawDate, err := g.genDragonTiger(context.Background(), testNow, nil, nil)
	if err != nil {
		t.Fatalf("genDragonTiger 不应报错: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("夹具应产生至少一条命中")
	}
	if !strings.Contains(strings.Join([]string{rawDate, entries[0].SourceDate}, "|"), "2026/10/09") {
		t.Fatalf("原始日期必须原样审计透传: raw=%q entry=%q", rawDate, entries[0].SourceDate)
	}
	// 且该原始值无法通过验证器——证明"入 srcDates 即违约"的判断成立。
	if _, verr := normalizeBusinessDate(rawDate, "2026-10-12"); verr == nil {
		t.Fatal("畸形原始日期不应可通过验证")
	}
}
