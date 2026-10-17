package pool

import (
"context"
"errors"
"fmt"
"testing"
)

// 完整 Generate() + B-5.1 重排，但 ETF 来源置空 —— 隔离 SECTOR 股票在混池中的消失点。
func TestDbgFullGenRerankNoEtf(t *testing.T) {
f := baseFake()
f.sectorRank = []SectorFlow{
{Code: "BK0488", Name: "食品饮料", MainNetInflow: 9e9},
{Code: "BK0438", Name: "汽车整车", MainNetInflow: 8e9},
{Code: "BK9999", Name: "失败板块", MainNetInflow: 7e9},
{Code: "BK1234", Name: "测试空板块", MainNetInflow: 1e9},
}
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
f.fundCodes = nil // ETF FAILED -> raw 无 ETF 条目
g := NewGenerator(f, DefaultConfig())
res, err := g.Generate(context.Background(), nil, testNow)
if err != nil {
t.Fatal(err)
}
fmt.Println("[no-etf rerank] srcStatus:", res.SourceStatuses)
for _, e := range res.Pool {
hs := ""
for _, h := range e.Sources {
hs += fmt.Sprintf("%s:%s ", h.SourceType, h.Status)
}
fmt.Printf("POOL %s primary=%s strength=%v scored=%v liq=%v srcs=[%s]\n", e.Symbol, e.PrimarySource, e.Strength, e.Scored, e.Liquidity, hs)
}
}

// 复刻链 + B-5.1 重排：逐段打印（含 REFINE 后 kept 明细）。
func TestDbgBisectRerankStages(t *testing.T) {
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
warns := []string{}

_, liquidIdx, _, _ := g.liquidityScreen(ctx)
var raw []Entry
etfEntries, etfSt, _ := g.genETFMomentum(ctx, testNow)
raw = append(raw, etfEntries...)
sectorEntries, sectorSt, _ := g.genSectorLeaders(ctx, nil, liquidIdx)
fmt.Println("SECTOR st:", sectorSt)
for _, e := range sectorEntries {
fmt.Printf("  SEC RAW %s\n", e.Symbol)
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

pool = g.refinePool(ctx, pool, &warns, testNow)
dbgPrintPool("REFINE", pool)
pool = g.liquidityGatePool(ctx, pool, &warns, testNow)
dbgPrintPool("LIQGATE", pool)
pool = g.RefineScores(pool, &warns)
dbgPrintPool("REFSCORES", pool)
}
