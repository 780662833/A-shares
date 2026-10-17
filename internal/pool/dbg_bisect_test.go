package pool

import (
"context"
"errors"
"fmt"
"testing"
)

// dbgRunPipeline 复刻 Generate 的池处理链（不含来源发现），在每一段后打印存活符号。
func dbgPrintPool(tag string, pool []AggEntry) {
fmt.Printf("[%s] n=%d:", tag, len(pool))
for _, e := range pool {
fmt.Printf(" %s(%s)", e.Symbol, e.PrimarySource)
}
fmt.Println()
}

func TestDbgBisectSectorMixed(t *testing.T) {
f := baseFake()
f.sectorRank = append(f.sectorRank, SectorFlow{Code: "BK9999", Name: "失败板块", MainNetInflow: 9e9})
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
g := NewGenerator(f, DefaultConfig())
ctx := context.Background()
warns := []string{}

liquidSet, liquidIdx, _, _ := g.liquidityScreen(ctx)
var raw []Entry
etfEntries, etfSt, _ := g.genETFMomentum(ctx, testNow)
raw = append(raw, etfEntries...)
sectorEntries, sectorSt, _ := g.genSectorLeaders(ctx, nil, liquidIdx)
fmt.Println("SECTOR st:", sectorSt)
for _, e := range sectorEntries {
fmt.Printf("  SEC RAW %s hitStatus=%q\n", e.Symbol, e.HitStatus)
}
raw = append(raw, sectorEntries...)
fundEntries, _ := g.genDragonTiger(ctx, testNow, nil, liquidIdx)
raw = append(raw, fundEntries...)
srcStatus := map[string]SourceStatus{"ETF_MOMENTUM": etfSt, "SECTOR_LEADER": sectorSt}
for i := range raw {
if st, ok := srcStatus[raw[i].SourceType]; ok && raw[i].HitStatus == "" {
raw[i].HitStatus = st
}
}
pool := aggregateEntries(raw, "2026-10-12", testNow, nil)
dbgPrintPool("AGG", pool)
_ = liquidSet

pool = g.refinePool(ctx, pool, &warns, testNow)
dbgPrintPool("REFINE", pool)
pool = g.liquidityGatePool(ctx, pool, &warns, testNow)
dbgPrintPool("LIQGATE", pool)
pool = g.RefineScores(pool, &warns)
dbgPrintPool("REFSCORES", pool)
pool = g.ApplyChanLayer(ctx, pool, &warns, testNow)
dbgPrintPool("CHANLAYER", pool)
for _, w := range warns {
fmt.Println("W:", w)
}
}
