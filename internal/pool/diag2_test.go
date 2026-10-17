package pool

import (
"context"
"errors"
"testing"
)

func TestDiag2Sector(t *testing.T) {
f := baseFake()
f.sectorRank = append(f.sectorRank, SectorFlow{Code: "BK9999", Name: "失败板块", MainNetInflow: 9e9})
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
g := NewGenerator(f, DefaultConfig())
res, _ := g.Generate(context.Background(), nil, testNow)
for _, e := range res.Pool {
hs := ""
for _, h := range e.Sources { hs += string(h.SourceType) + ":" + string(h.Status) + " " }
t.Logf("POOL %s primary=%s liq=%v hits=[%s]", e.Symbol, e.PrimarySource, e.Liquidity, hs)
}
t.Logf("STATUS SECTOR=%q ETF=%q FUND=%q", res.SourceStatuses["SECTOR_LEADER"], res.SourceStatuses["ETF_MOMENTUM"], res.SourceStatuses["FUND_FLOW"])
}

func TestDiag2ETF(t *testing.T) {
f := baseFake()
f.etfKlineErr = map[string]error{"159915": errors.New("kline down")}
g := NewGenerator(f, DefaultConfig())
res, _ := g.Generate(context.Background(), nil, testNow)
for _, e := range res.Pool {
hs := ""
for _, h := range e.Sources { hs += string(h.SourceType) + ":" + string(h.Status) + " " }
if e.AssetType == "ETF" { t.Logf("ETFPOOL %s primary=%s liq=%v adv=%v hits=[%s]", e.Symbol, e.PrimarySource, e.Liquidity, e.AvgAmount20D, hs) }
}
t.Logf("STATUS ETF=%q", res.SourceStatuses["ETF_MOMENTUM"])
}
