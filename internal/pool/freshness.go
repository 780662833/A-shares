// freshness.go —— A.4 数据新鲜度契约的基础设施（第 0 阶段最小重建）。
//
// 本文件补齐 generator.go 调用侧已经引用、但从未落地的三个符号：
//   - shanghaiLoc            A 股业务时区（Asia/Shanghai）；
//   - normalizeBusinessDate  来源级业务日期验证器（格式/真实日历/未来边界三重校验）；
//   - FreshnessUnknown       "无可信日期"的诚实哨兵常量。
//
// 语义严格遵循 generator.go 既有注释中冻结的合同：
//   1. 只有通过验证的日期才能进入 Result.SourceDates / Entry.SourceDate；
//   2. 缺失即未知——持久化边界落 UNKNOWN 哨兵，绝不用 RunID / TradeDate /
//      time.Now() 兜底伪造来源日期；
//   3. 时区口径唯一：A 股所有业务日期折算以 Asia/Shanghai 为准（与 cmd 层
//      shanghaiLocation() 同口径，见 main.go L1045 注释）。
package pool

import (
	"fmt"
	"time"
)

// shanghaiLoc 是包级上海时区。time.LoadLocation 依赖系统 tzdata，生产镜像
// （alpine 等）可能缺失——此时退化为固定 UTC+8（中国自 1991 年起无夏令时，
// +08:00 恒定成立），保证 tradeDateOf 的未来边界上界在任何环境下都可计算，
// 绝不因时区加载失败而 panic 或跳过日期校验。
var shanghaiLoc = func() *time.Location {
	if loc, err := time.LoadLocation("Asia/Shanghai"); err == nil {
		return loc
	}
	return time.FixedZone("CST", 8*3600)
}()

// FreshnessUnknown 来源级业务日期的诚实哨兵：当某来源本轮没有可通过
// normalizeBusinessDate 验证的真实数据截止日期时，持久化边界以此值登记
// source_run_status 的新鲜度结论，而不是伪造一个日期。
const FreshnessUnknown = "UNKNOWN"

// normalizeBusinessDate 校验并归一化一条上游业务日期候选。
//
// raw 为上游原始日期字符串；upperBoundYYYYMMDD 为本次评估日（YYYY-MM-DD，
// 由 tradeDateOf(now) 给出）作为未来边界上界。返回归一化的 "YYYY-MM-DD"。
//
// 拒绝（返回 error，调用方必须把该来源视为"日期不可证"）的情形：
//   - 空串或含前后空白（上游字段缺失 ≠ 某个合法日期）；
//   - 非 YYYY-MM-DD 格式，或历法上不存在的日期（如 2026-02-30、2026-13-01，
//     借助 time.Parse 的严格解析拒绝）；
//   - 晚于 upperBound 的未来日期（供应商回传尚未发生的交易日属于数据事故）；
//   - upperBound 本身非法（无法确立边界时宁可不判，也不放行可疑日期）。
//
// 注意：本函数只证明"这是一个历法上真实存在且不晚于评估日的日期"，不证明
// 它是交易所交易日（交易日历证据链属于来源级资格问题，见 docs 合同文档中的
// 日期证据矩阵；SECTOR/FUND 两级来源正因该证据缺失而被冻结登记资格）。
func normalizeBusinessDate(raw, upperBoundYYYYMMDD string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("business date empty")
	}
	d, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return "", fmt.Errorf("business date %q not a valid YYYY-MM-DD calendar date: %w", raw, err)
	}
	bound, err := time.Parse("2006-01-02", upperBoundYYYYMMDD)
	if err != nil {
		return "", fmt.Errorf("upper bound %q invalid, refusing to certify %q: %w", upperBoundYYYYMMDD, raw, err)
	}
	if d.After(bound) {
		return "", fmt.Errorf("business date %q is after evaluation date %q (future data)", raw, upperBoundYYYYMMDD)
	}
	return d.Format("2006-01-02"), nil
}
