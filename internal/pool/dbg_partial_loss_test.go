package pool

import (
"context"
"errors"
"fmt"
"testing"
)

func TestDbgRefineDrop(t *testing.T) {
f := baseFake()
f.sectorRank = append(f.sectorRank, SectorFlow{Code: "BK9999", Name: "失败板块", MainNetInflow: 9e9})
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
g := NewGenerator(f, DefaultConfig())
warns := []string{}

liquidSet, liquidIdx, _, _ := g.liquidityScreen(context.Background())
fmt.Println("liquidSet:", liquidSet)
var raw []Entry
etfEntries, etfSt, err := g.genETFMomentum(context.Background(), testNow)
raw = append(raw, etfEntries...)
sectorEntries, sectorSt, err := g.genSectorLeaders(context.Background(), nil, liquidIdx)
fmt.Println("SECTOR st:", sectorSt, "err:", err)
for _, e := range sectorEntries { fmt.Printf("  SEC RAW %s\n", e.Symbol) }
raw = append(raw, sectorEntries...)
fundEntries, _ := g.genDragonTiger(context.Background(), testNow, nil, liquidIdx)
raw = append(raw, fundEntries...)
srcStatus := map[string]SourceStatus{"ETF_MOMENTUM": etfSt, "SECTOR_LEADER": sectorSt}
for i := range raw {
if st, ok := srcStatus[raw[i].SourceType]; ok && raw[i].HitStatus == "" {
raw[i].HitStatus = st
}
}
pool := aggregateEntries(raw, "2026-10-12", testNow, nil)
out := g.refinePool(context.Background(), pool, &warns, testNow)
syms := map[string]bool{}
for _, e := range out { syms[e.Symbol] = true }
fmt.Println("REFINED SYMS:", syms)
for _, w := range warns { fmt.Println("W:", w) }
}
