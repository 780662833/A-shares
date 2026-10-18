package pool

import (
"context"
"errors"
"fmt"
"testing"
)

// 完整 Generate() + B-5.1 重排，但 dragon=nil（FUND SUCCESS_EMPTY）——隔离 FUND 混入的影响。
func TestDbgFullGenRerankNoFund(t *testing.T) {
f := baseFake()
f.sectorRank = []SectorFlow{
{Code: "BK0488", Name: "食品饮料", MainNetInflow: 9e9},
{Code: "BK0438", Name: "汽车整车", MainNetInflow: 8e9},
{Code: "BK9999", Name: "失败板块", MainNetInflow: 7e9},
{Code: "BK1234", Name: "测试空板块", MainNetInflow: 1e9},
}
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
f.dragon = nil
g := NewGenerator(f, DefaultConfig())
res, err := g.Generate(context.Background(), nil, testNow)
if err != nil {
t.Fatal(err)
}
fmt.Println("[no-fund rerank] srcStatus:", res.SourceStatuses)
for _, e := range res.Pool {
hs := ""
for _, h := range e.Sources {
hs += fmt.Sprintf("%s:%s ", h.SourceType, h.Status)
}
fmt.Printf("POOL %s primary=%s strength=%v scored=%v liq=%v srcs=[%s]\n", e.Symbol, e.PrimarySource, e.Strength, e.Scored, e.Liquidity, hs)
}
}

// 复刻链：AGG 后 dump 每条的 Sources/PrimarySource，验证聚合去重是否合并了 SECTOR+FUND。
func TestDbgAggDumpSources(t *testing.T) {
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
_, liquidIdx, _, _ := g.liquidityScreen(ctx)
var raw []Entry
etfEntries, etfSt, _, _ := g.genETFMomentum(ctx, testNow)
raw = append(raw, etfEntries...)
sectorEntries, sectorSt, _ := g.genSectorLeaders(ctx, nil, liquidIdx)
raw = append(raw, sectorEntries...)
fundEntries, _, _ := g.genDragonTiger(ctx, testNow, nil, liquidIdx)
raw = append(raw, fundEntries...)
srcStatus := map[string]SourceStatus{"ETF_MOMENTUM": etfSt, "SECTOR_LEADER": sectorSt}
for i := range raw {
if st, ok := srcStatus[raw[i].SourceType]; ok && raw[i].HitStatus == "" {
raw[i].HitStatus = st
}
}
fmt.Println("RAW entries:")
for _, e := range raw {
fmt.Printf("  %s type=%s hitStatus=%q\n", e.Symbol, e.SourceType, e.HitStatus)
}
pool := aggregateEntries(raw, "2026-10-12", testNow, nil)
fmt.Println("AGG detail:")
for _, e := range pool {
hs := ""
for _, h := range e.Sources {
hs += fmt.Sprintf("%s:%s ", h.SourceType, h.Status)
}
fmt.Printf("  %s primary=%s srcs=[%s]\n", e.Symbol, e.PrimarySource, hs)
}
}
