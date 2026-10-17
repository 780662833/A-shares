package pool

import (
"context"
"errors"
"testing"
)

func TestDiag3SectorEntries(t *testing.T) {
f := baseFake()
f.sectorRank = append(f.sectorRank, SectorFlow{Code: "BK9999", Name: "失败板块", MainNetInflow: 9e9})
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
g := NewGenerator(f, DefaultConfig())
set, idx, n, err := g.liquidityScreen(context.Background())
t.Logf("screen n=%d err=%v set=%v idxCodes=", n, err, set)
for c := range idx { t.Logf("  idx %s cap=%.0f price=%.0f amt=%.0f", c, idx[c].TotalMarketCap, idx[c].Price, idx[c].AvgAmount20D) }
sectorTopN, leadersK, etfTopM := g.cfg.effectiveSourceLimits()
t.Logf("limits topN=%d leadersK=%d etfTopM=%d tiered=%v", sectorTopN, leadersK, etfTopM, g.cfg.TieredPipeline)
entries, st, aggErr := g.genSectorLeaders(context.Background(), set, idx)
t.Logf("genSector status=%q err=%v", st, aggErr)
for _, e := range entries { t.Logf("SECTORENTRY %s src=%s reason=%s hitstatus=%q", e.Symbol, e.SourceType, e.SectorName, e.HitStatus) }
}
