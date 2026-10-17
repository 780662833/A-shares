package pool

import (
"context"
"errors"
"fmt"
"testing"
)

func TestDbgSectorStocksOnly(t *testing.T) {
f := baseFake()
f.sectorRank = append(f.sectorRank, SectorFlow{Code: "BK9999", Name: "失败板块", MainNetInflow: 9e9})
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
f.fundCodes = nil // ETF 来源 FAILED 被跳过 -> raw 无 ETF 条目
f.dragon = nil    // FUND 合法空 -> SUCCESS_EMPTY 不贡献条目
g := NewGenerator(f, DefaultConfig())
res, err := g.Generate(context.Background(), nil, testNow)
if err != nil {
t.Fatal(err)
}
fmt.Println("[stocks-only] srcStatus:", res.SourceStatuses)
for _, e := range res.Pool {
hs := ""
for _, h := range e.Sources {
hs += fmt.Sprintf("%s:%s ", h.SourceType, h.Status)
}
fmt.Printf("  POOL %s primary=%s strength=%v scored=%v srcs=[%s]\n", e.Symbol, e.PrimarySource, e.Strength, e.Scored, hs)
}
}

func TestDbgEtfStocksOnly(t *testing.T) {
f := baseFake()
f.etfKlineErr = map[string]error{"159915": errors.New("kline down")}
f.sectorRank = nil // SECTOR SUCCESS_EMPTY
f.dragon = nil     // FUND SUCCESS_EMPTY
g := NewGenerator(f, DefaultConfig())
res, err := g.Generate(context.Background(), nil, testNow)
if err != nil {
t.Fatal(err)
}
fmt.Println("[etf-only] srcStatus:", res.SourceStatuses)
for _, e := range res.Pool {
hs := ""
for _, h := range e.Sources {
hs += fmt.Sprintf("%s:%s ", h.SourceType, h.Status)
}
fmt.Printf("  POOL %s primary=%s liq=%v srcs=[%s]\n", e.Symbol, e.PrimarySource, e.Liquidity, hs)
}
}
