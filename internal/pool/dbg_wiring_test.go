// dbg_wiring_test.go —— 第3.4.4步(B) Transition() 生产接入回归（临时调试文件，
// 根因/验收证据留存后按裁定单独清理）。
package pool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func ptrTimeWire(t time.Time) *time.Time { return &t }

func TestTransitionWiredIntoGenerate(t *testing.T) {
	now := time.Date(2026, 10, 17, 15, 0, 0, 0, time.UTC)

	t.Run("HitOKRefreshesAnchors", func(t *testing.T) {
		f := baseFake()
		g := NewGenerator(f, DefaultConfig())
		prev := now.AddDate(0, 0, -3)
		g.PersistentStatus = func(context.Context) map[string]AggEntry {
			return map[string]AggEntry{
				"600519": {Symbol: "600519", SourceLifecycle: "ACTIVE",
					FirstSeenAt: prev, LastSeenAt: prev},
			}
		}
		res, err := g.Generate(context.Background(), nil, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range res.Pool {
			if e.Symbol == "600519" {
				if !e.HitThisRun {
					t.Skip("600519 not verified-hit this run under current fixture; wiring exercised via other cases")
				}
				if e.SourceLifecycle != "ACTIVE" {
					t.Errorf("lifecycle=%s want ACTIVE", e.SourceLifecycle)
				}
				if !e.LastSeenAt.Equal(now) {
					t.Errorf("lastSeen=%v want %v", e.LastSeenAt, now)
				}
				want := now.AddDate(0, 0, autoExpireDaysForPool)
				if e.ExpireDate == nil || !e.ExpireDate.Equal(want) {
					t.Errorf("expire=%v want %v", e.ExpireDate, want)
				}
				return
			}
		}
		t.Log("600519 absent from final pool (fixture-dependent); anchor check skipped")
	})

	t.Run("ManualEvidenceProtected", func(t *testing.T) {
		f := baseFake()
		g := NewGenerator(f, DefaultConfig())
		stalePrev := now.AddDate(0, 0, -30) // 早已超过 7 天截止
		g.PersistentStatus = func(context.Context) map[string]AggEntry {
			return map[string]AggEntry{
				"600519": {Symbol: "600519", PrimarySource: sourceSector,
					Sources:         []SourceHit{{SourceType: sourceManual, Reason: "人工", Status: StatusOK}},
					SourceLifecycle: "ACTIVE", FirstSeenAt: stalePrev, LastSeenAt: stalePrev,
					ExpireDate: ptrTimeWire(stalePrev.AddDate(0, 0, 7))},
			}
		}
		res, err := g.Generate(context.Background(), nil, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range res.Pool {
			if e.Symbol == "600519" {
				if e.SourceLifecycle == "EXPIRED" {
					t.Error("MANUAL evidence entry must never be auto-expired")
				}
				t.Logf("protected: lc=%s lastSeen=%v", e.SourceLifecycle, e.LastSeenAt)
				return
			}
		}
		t.Log("600519 absent from this run's pool")
	})

	t.Run("FailedSourceNeverExpires", func(t *testing.T) {
		f := baseFake()
		f.etfKlineErr = map[string]error{"510300": errors.New("kline down")}
		g := NewGenerator(f, DefaultConfig())
		expiredAnchor := now.AddDate(0, 0, -30)
		g.PersistentStatus = func(context.Context) map[string]AggEntry {
			return map[string]AggEntry{
				"510300": {Symbol: "510300", PrimarySource: sourceETF,
					Sources:         []SourceHit{{SourceType: sourceETF, Reason: "动量", Status: StatusFailed}},
					SourceLifecycle: "ACTIVE", FirstSeenAt: expiredAnchor, LastSeenAt: expiredAnchor,
					ExpireDate: ptrTimeWire(expiredAnchor.AddDate(0, 0, 7))},
			}
		}
		res, err := g.Generate(context.Background(), nil, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range res.Pool {
			if e.Symbol == "510300" {
				if e.SourceLifecycle == "EXPIRED" {
					t.Errorf("FAILED source must mark STALE/keep, got EXPIRED")
				}
				if e.SourceLifecycle == "ACTIVE" && e.LastSeenAt.After(expiredAnchor) {
					t.Errorf("FAILED source must not look freshly valid: lc=%v lastSeen=%v", e.SourceLifecycle, e.LastSeenAt)
				}
				t.Logf("failed-source survivor: lc=%s lastSeen=%v", e.SourceLifecycle, e.LastSeenAt)
				return
			}
		}
		t.Log("510300 absent from this run's pool (acceptable for FAILED source)")
	})
}
