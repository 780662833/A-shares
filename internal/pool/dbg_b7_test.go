package pool

import (
"context"
"errors"
"fmt"
"testing"
)

func TestDBG_B7_GenVsManual(t *testing.T) {
f := baseFake()
f.sectorRank = []SectorFlow{
{Code: "BK0488", Name: "食品饮料", MainNetInflow: 9e9},
{Code: "BK0438", Name: "汽车整车", MainNetInflow: 8e9},
{Code: "BK9999", Name: "失败板块", MainNetInflow: 7e9},
{Code: "BK1234", Name: "测试空板块", MainNetInflow: 1e9},
}
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
g := NewGenerator(f, DefaultConfig())
ctx := context.Background()
now := testNow

// what Generate actually passes to genSectorLeaders
liquidSet, liquidIdx, n, err := g.liquidityScreen(ctx)
fmt.Printf("SCREEN n=%d err=%v set=", n, err)
for k := range liquidSet {
fmt.Printf("%s ", k)
}
fmt.Println()
sec, st, serr := g.genSectorLeaders(ctx, liquidSet, liquidIdx)
fmt.Printf("SECTOR(st call) st=%v err=%v entries=", st, serr)
for _, e := range sec {
fmt.Printf("%s ", e.Symbol)
}
fmt.Println()

res, _ := g.Generate(ctx, nil, now)
fmt.Print("GEN FINAL: ")
for _, e := range res.Pool {
fmt.Printf("%s(%s) ", e.Symbol, e.PrimarySource)
}
fmt.Println()
for _, w := range res.Warnings {
fmt.Println("WARN:", w)
}
}
