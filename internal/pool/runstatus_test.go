// runstatus_test.go —— 第 3.2 步 P0-3（来源级部分）：SourceStatus 谓词全矩阵测试。
//
// 直接测试 internal/pool/contract.go 中的真实谓词函数（IsKnown /
// IsCompleteSuccess / CanGenerateFreshSignal / CanUpdateLifecycle / Usable），
// 覆盖 5 个合法枚举 + 空串 + 未定义字符串的完整输入矩阵。
// 只锁定现有契约语义，不修改任何判定规则。
//
// 运行级状态推导（SUCCESS / PARTIAL_SUCCESS）位于 cmd/main.go 内联逻辑，
// 不在本文件复刻——按约定复刻副本不构成有效回归测试；其覆盖方式见
// cmd/runstatus_test.go（对提取后的私有函数做表驱动测试）。
package pool

import "testing"

func TestSourceStatus_IsKnown_Matrix(t *testing.T) {
	cases := []struct {
		status SourceStatus
		want   bool
	}{
		{StatusOK, true},
		{StatusSuccessEmpty, true},
		{StatusPartial, true},
		{StatusStale, true},
		{StatusFailed, true},
		{"", false},                // 空串不得当作已知状态
		{"PARTIAL_SUCCESS", false}, // 运行级状态混入来源级枚举必须 false
		{"ok", false},              // 大小写敏感
		{"TIMEOUT", false},         // 未定义字符串
	}
	for _, c := range cases {
		if got := c.status.IsKnown(); got != c.want {
			t.Errorf("IsKnown(%q) = %v, want %v", c.status, got, c.want)
		}
	}
}

func TestSourceStatus_IsCompleteSuccess_Matrix(t *testing.T) {
	cases := []struct {
		status SourceStatus
		want   bool
	}{
		{StatusOK, true},
		{StatusSuccessEmpty, true},
		{StatusPartial, false}, // 部分成功不算完整
		{StatusStale, false},   // 非当前时点数据不算完整
		{StatusFailed, false},
		{"", false},
		{"GARBAGE", false},
	}
	for _, c := range cases {
		if got := c.status.IsCompleteSuccess(); got != c.want {
			t.Errorf("IsCompleteSuccess(%q) = %v, want %v", c.status, got, c.want)
		}
	}
}

func TestSourceStatus_CanGenerateFreshSignal_Matrix(t *testing.T) {
	// 契约红线：仅 OK 可产生新信号；SUCCESS_EMPTY 是"本次没有候选"，
	// 不是"存在合格候选"，不得放行信号。
	for _, s := range []SourceStatus{StatusSuccessEmpty, StatusPartial, StatusStale, StatusFailed, "", "XX"} {
		if s.CanGenerateFreshSignal() {
			t.Errorf("CanGenerateFreshSignal(%q) = true, want false (only OK may emit fresh signals)", s)
		}
	}
	if !StatusOK.CanGenerateFreshSignal() {
		t.Error("CanGenerateFreshSignal(OK) must be true")
	}
}

func TestSourceStatus_CanUpdateLifecycle_Matrix(t *testing.T) {
	// 契约红线：只有完整刷新（OK/SUCCESS_EMPTY）才能依据"本次未命中"推进
	// 自动来源生命周期；FAILED/PARTIAL/STALE 时必须保留历史，防止一次拉取
	// 失败误清有效来源。
	if !StatusOK.CanUpdateLifecycle() || !StatusSuccessEmpty.CanUpdateLifecycle() {
		t.Error("OK and SUCCESS_EMPTY must allow lifecycle update")
	}
	for _, s := range []SourceStatus{StatusPartial, StatusStale, StatusFailed, "", "UNKNOWN"} {
		if s.CanUpdateLifecycle() {
			t.Errorf("CanUpdateLifecycle(%q) = true, want false — non-complete refresh must not expire old records", s)
		}
	}
}

func TestSourceStatus_Usable_EqualsIsCompleteSuccess(t *testing.T) {
	// Usable 是闸门白名单语义（阶段 0 审查 P0-1 修复产物）：与
	// IsCompleteSuccess 完全同集合。旧实现 `s != StatusFailed` 会把
	// PARTIAL/STALE/"" 全部放行——若未来有人改回差集写法，本测试捕获。
	all := []SourceStatus{StatusOK, StatusSuccessEmpty, StatusPartial, StatusStale, StatusFailed, "", "NOPE"}
	for _, s := range all {
		if s.Usable() != s.IsCompleteSuccess() {
			t.Errorf("Usable(%q)=%v diverges from IsCompleteSuccess(%q)=%v", s, s.Usable(), s, s.IsCompleteSuccess())
		}
	}
	// 显式钉死高危状态：PARTIAL 与 STALE 不可被闸门当作通过。
	if StatusPartial.Usable() || StatusStale.Usable() || StatusFailed.Usable() {
		t.Fatal("PARTIAL/STALE/FAILED must never be Usable at gates")
	}
}

func TestSourceStatus_PredicateConsistency(t *testing.T) {
	// 谓词间蕴含关系审计（对全部输入含未知值）：
	//   CanGenerateFreshSignal => IsCompleteSuccess => IsKnown 不成立链？
	//   实际契约: fresh-signal ⊂ complete-success ⊂ known；
	//   lifecycle == complete-success；usable == complete-success。
	all := []SourceStatus{StatusOK, StatusSuccessEmpty, StatusPartial, StatusStale, StatusFailed, "", "JUNK"}
	for _, s := range all {
		if s.CanGenerateFreshSignal() && !s.IsCompleteSuccess() {
			t.Errorf("%q: CanGenerateFreshSignal without IsCompleteSuccess — subset broken", s)
		}
		if s.IsCompleteSuccess() && !s.IsKnown() {
			t.Errorf("%q: complete success but unknown status — subset broken", s)
		}
		if s.CanUpdateLifecycle() != s.IsCompleteSuccess() {
			t.Errorf("%q: lifecycle != complete-success", s)
		}
		if s.Usable() != s.IsCompleteSuccess() {
			t.Errorf("%q: usable != complete-success", s)
		}
	}
}
