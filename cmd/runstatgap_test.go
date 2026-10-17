// runstatgap_test.go —— 第 3.3 步：运行级状态推导的真实路径表驱动测试。
//
// 历史沿革：
//   - 3.2 阶段本文件曾以 mirrorInlineRunStatus（对 main.go 内联语句的文字化
//     镜像）登记已知缺口 R-A/R-B/R-C，并明确声明其不构成生产路径覆盖。
//   - 3.3.1 已将推导提取为 cmd 包私有函数 deriveRunStatus；
//   - 3.3.2 已按冻结契约编写直接调用生产函数的表驱动用例（当时允许失败）；
//   - 3.3.3 实现冻结契约后，本文件全部用例应转绿。
//
// 冻结契约（可用来源 = OK / SUCCESS_EMPTY；PARTIAL/FAILED/STALE 为降级状态，
// 仅在存在可用来源时把运行级判为 PARTIAL_SUCCESS）：
//
//	空状态集                          -> FAILED
//	全部 OK / SUCCESS_EMPTY           -> SUCCESS
//	全部 FAILED                       -> FAILED
//	仅 FAILED/STALE（无可用来源）     -> FAILED
//	可用 + 任一 PARTIAL               -> PARTIAL_SUCCESS
//	可用 + 任一 FAILED                -> PARTIAL_SUCCESS
//	可用 + 任一 STALE                 -> PARTIAL_SUCCESS
//
// FALLBACK 不由本函数推导（由备用流程显式写入），故不在用例范围内。
package main

import (
	"testing"

	"ashare-quant/internal/pool"
)

func TestDeriveRunStatusContract(t *testing.T) {
	cases := []struct {
		name     string
		statuses map[string]pool.SourceStatus
		want     string // 冻结契约期望值
	}{
		{
			name: "all normal -> SUCCESS",
			statuses: map[string]pool.SourceStatus{
				"SECTOR_LEADER": pool.StatusOK,
				"ETF_MOMENTUM":  pool.StatusSuccessEmpty,
			},
			want: "SUCCESS",
		},
		{
			name: "one PARTIAL among usable -> PARTIAL_SUCCESS",
			statuses: map[string]pool.SourceStatus{
				"SECTOR_LEADER": pool.StatusOK,
				"FUND_FLOW":     pool.StatusPartial,
			},
			want: "PARTIAL_SUCCESS",
		},
		{
			name: "usable + FAILED mix -> PARTIAL_SUCCESS",
			statuses: map[string]pool.SourceStatus{
				"SECTOR_LEADER": pool.StatusOK,
				"FUND_FLOW":     pool.StatusFailed,
			},
			want: "PARTIAL_SUCCESS",
		},
		{
			name: "usable + STALE mix -> PARTIAL_SUCCESS",
			statuses: map[string]pool.SourceStatus{
				"SECTOR_LEADER": pool.StatusOK,
				"FUND_FLOW":     pool.StatusStale,
			},
			want: "PARTIAL_SUCCESS",
		},
		{
			name: "all FAILED -> FAILED",
			statuses: map[string]pool.SourceStatus{
				"SECTOR_LEADER": pool.StatusFailed,
				"ETF_MOMENTUM":  pool.StatusFailed,
			},
			want: "FAILED",
		},
		{
			name: "only FAILED/STALE, no usable source -> FAILED",
			statuses: map[string]pool.SourceStatus{
				"SECTOR_LEADER": pool.StatusFailed,
				"FUND_FLOW":     pool.StatusStale,
			},
			want: "FAILED",
		},
		{
			name:     "empty status set -> FAILED",
			statuses: map[string]pool.SourceStatus{},
			want:     "FAILED",
		},
		{
			name: "only SUCCESS_EMPTY -> SUCCESS",
			statuses: map[string]pool.SourceStatus{
				"FUND_FLOW": pool.StatusSuccessEmpty,
			},
			want: "SUCCESS",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := deriveRunStatus(c.statuses); got != c.want {
				t.Errorf("deriveRunStatus(%v) = %q, want %q (frozen contract)", c.statuses, got, c.want)
			}
		})
	}
}

// TestDeriveRunStatusNilMap 锁定 nil map 与空 map 同语义（无法审计 -> FAILED）。
func TestDeriveRunStatusNilMap(t *testing.T) {
	if got := deriveRunStatus(nil); got != "FAILED" {
		t.Errorf("deriveRunStatus(nil) = %q, want \"FAILED\"", got)
	}
}
