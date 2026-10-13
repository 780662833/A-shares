package main

import (
	"testing"
	"time"
)

// TestPoolSourceConfig 灰度开关验收：ASHARE_POOL_SOURCE 默认 python，
// 仅显式 "go" 切换 Go 原生池链路；非法值保持默认并告警。
func TestPoolSourceConfig(t *testing.T) {
	t.Setenv("ASHARE_POOL_SOURCE", "")
	cfg := loadConfig()
	if cfg.PoolSource != "python" {
		t.Errorf("default PoolSource want python, got %q", cfg.PoolSource)
	}
	if cfg.PoolTimeout != 6*time.Minute {
		t.Errorf("default PoolTimeout want 6m, got %s", cfg.PoolTimeout)
	}

	t.Setenv("ASHARE_POOL_SOURCE", "go")
	if cfg := loadConfig(); cfg.PoolSource != "go" {
		t.Errorf("override want go, got %q", cfg.PoolSource)
	}

	t.Setenv("ASHARE_POOL_SOURCE", "garbage")
	if cfg := loadConfig(); cfg.PoolSource != "python" {
		t.Errorf("invalid value must keep python, got %q", cfg.PoolSource)
	}

	t.Setenv("ASHARE_POOL_TIMEOUT", "90s")
	if cfg := loadConfig(); cfg.PoolTimeout != 90*time.Second {
		t.Errorf("PoolTimeout override want 90s, got %s", cfg.PoolTimeout)
	}
}
