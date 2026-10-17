// fingerprint_test.go —— 第 3.2 步 P0-1：配置指纹与策略版本现状锁定测试。
//
// 本文件只验证现有行为，不修改任何业务逻辑。锁定的性质：
//  1. Hash() 对同一配置稳定（确定性）；
//  2. Config 中每个导出字段的变化都会改变指纹（JSON 全量序列化设计使然）；
//  3. StrategyVersion() 当前实现 = "pool-v1-" + Hash()，即版本字符串混入了
//     配置指纹——数据/参数调整会改变"策略版本"，这是已知设计偏差，
//     本测试如实记录现状而不修复；
//  4. StrategyVersion() 长度 20 > screening_runs.strategy_version varchar(16)
//     ——SQLite 不强制截断，落库能读回原值，但该风险在 MySQL 等严格引擎上
//     会导致写入失败或截断。本测试揭示该不一致，不修改字段长度或版本格式。
package pool

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// TestConfigHash_StableForSameConfig 验证配置不变时指纹恒定（P0 验收标准）。
func TestConfigHash_StableForSameConfig(t *testing.T) {
	c1 := DefaultConfig()
	c2 := DefaultConfig()
	if h1, h2 := c1.Hash(), c2.Hash(); h1 != h2 {
		t.Fatalf("hash not stable for identical configs: %q vs %q", h1, h2)
	}
	// 重复调用同样稳定（无随机序 map 参与——Config 全部为标量/字符串字段）。
	if got := c1.Hash(); len(got) != 12 || strings.ContainsAny(got, "GHIJKLMNOPQRSTUVWXYZ") {
		t.Fatalf("hash format unexpected (want 12 hex chars): %q", got)
	}
}

// TestConfigHash_SensitiveToEveryExportedField 逐一翻转 Config 的每个导出字段，
// 确认指纹随之变化——即不存在"影响策略行为却未进入指纹"的配置字段。
// 依赖事实：Hash() 通过 json.Marshal(c) 全量序列化，Go struct 字段顺序确定，
// Config 无 map/slice 字段，序列化顺序天然确定。
func TestConfigHash_SensitiveToEveryExportedField(t *testing.T) {
	base := DefaultConfig()
	baseHash := base.Hash()
	rv := reflect.ValueOf(&base).Elem()
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.PkgPath != "" { // 跳过非导出字段（fullSourceDefault）
			continue
		}
		v := rv.Field(i)
		if !v.CanSet() {
			t.Fatalf("field %s should be exported and settable", f.Name)
		}
		orig := v.Interface()
		var changed bool
		switch v.Kind() {
		case reflect.Int:
			n := v.Int()
			v.SetInt(n + 1)
			changed = true
		case reflect.Float64:
			n := v.Float()
			v.SetFloat(n + 1.5)
			changed = true
		case reflect.Bool:
			v.SetBool(!v.Bool())
			changed = true
		case reflect.String:
			v.SetString(v.String() + "#x")
			changed = true
		default:
			t.Fatalf("unhandled kind %s for field %s (若新增复杂字段需同步扩展本测试并检查序列化确定性)", v.Kind(), f.Name)
		}
		if !changed {
			continue
		}
		mutated := rv.Interface().(Config)
		if mutated.Hash() == baseHash {
			v.Set(reflect.ValueOf(orig))
			t.Fatalf("flipping exported field %s did NOT change hash — field missing from fingerprint", f.Name)
		}
		v.Set(reflect.ValueOf(orig)) // 还原，逐字段独立测试
	}
	// 全部还原后指纹必须回到基线值。
	if rv.Interface().(Config).Hash() != baseHash {
		t.Fatal("hash differs after restoring all fields — mutation leaked")
	}
}

// TestStrategyVersion_CurrentDerivation 锁定 StrategyVersion() 的现状实现：
// 前缀固定 "pool-v1-"，后缀等于配置指纹。即参数级变化（如 MinMarketCap 5e8→5e9）
// 会改变策略版本字符串——这正是第 2.4 项指出的"策略版本与数据/配置版本未分离"
// 的设计偏差，按约定本阶段只揭示、不修改。
func TestStrategyVersion_CurrentDerivation(t *testing.T) {
	c := DefaultConfig()
	sv := c.StrategyVersion()
	want := "pool-v1-" + c.Hash()
	if sv != want {
		t.Fatalf("StrategyVersion derivation changed: got %q want %q", sv, want)
	}
	if !strings.HasPrefix(sv, "pool-v1-") {
		t.Fatalf("strategy version prefix unexpected: %q", sv)
	}
	// 配置变化 -> 版本变化（现状行为如实锁定）。
	c2 := DefaultConfig()
	c2.MinMarketCap = 5e9
	if c2.StrategyVersion() == sv {
		t.Fatal("config-only change failed to alter StrategyVersion — current design derives it from config hash")
	}
}

// TestStrategyVersion_ExceedsColumnWidth 揭示版本字符串长度与 DB 列宽的矛盾：
// "pool-v1-"(8) + sha256 前 12 位 = 20 字符 > screening_runs.strategy_version
// 的 varchar(16) 声明。SQLite（glebarez 驱动）不强制 varchar 长度，因此生产
// 落库目前不会报错——但这是隐性契约违反，迁移到严格引擎即断裂。
// 本测试仅断言长度事实，不改字段定义也不改版本格式。
func TestStrategyVersion_ExceedsColumnWidth(t *testing.T) {
	sv := DefaultConfig().StrategyVersion()
	if len(sv) <= 16 {
		t.Fatalf("expected known over-length (>16), got %d chars: %q — if this fails, the length mismatch was fixed elsewhere; update contract docs accordingly", len(sv), sv)
	}
	if len(sv) != 20 {
		t.Fatalf("unexpected strategy version length %d (%q); fingerprint width assumption (12 hex) broken", len(sv), sv)
	}
	// ConfigHash 自身（12 位）仍在 varchar(16) 之内。
	if len(DefaultConfig().Hash()) > 16 {
		t.Fatal("ConfigHash exceeds declared column width 16")
	}
}

// TestConfigHash_JSONDeterminism 验证 JSON 序列化本身确定（字段声明序、
// 无 map 乱序风险）。附带锁定一个现状发现：DefaultConfig().RSTopK 已经是 0，
// Params() 中的 `c.RSTopK = 0` 是冗余赋值——两个函数当前产出完全相同的配置，
// 指纹一致。若未来两者需要分化，本测试的 hash-equal 断言会失败并提示更新。
func TestConfigHash_JSONDeterminism(t *testing.T) {
	a, err1 := json.Marshal(DefaultConfig())
	b, err2 := json.Marshal(DefaultConfig())
	if err1 != nil || err2 != nil {
		t.Fatalf("marshal error: %v / %v", err1, err2)
	}
	if string(a) != string(b) {
		t.Fatal("Config JSON serialization is not deterministic")
	}
	if DefaultConfig().RSTopK != 0 {
		t.Fatalf("DefaultConfig().RSTopK expected 0 (contract override in Params() is a no-op), got %d", DefaultConfig().RSTopK)
	}
	if DefaultConfig().Hash() != Params().Hash() {
		t.Fatal("Params() and DefaultConfig() currently produce identical configs; hash equality locks that fact")
	}
}
