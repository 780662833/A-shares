package pool

import (
"context"
"errors"
"testing"
)

func TestDiagSectorPartial(t *testing.T) {
f := baseFake()
f.sectorRank = append(f.sectorRank, SectorFlow{Code: "BK9999", Name: "失败板块", MainNetInflow: 9e9})
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
g := NewGenerator(f, DefaultConfig())
res, err := g.Generate(context.Background(), nil, testNow)
if err != nil { t.Fatal(err) }
for _, e := range res.Pool {
t.Logf("POOL %s src=%s liq=%v life=%s hits=%d", e.Symbol, e.PrimarySource, e.Liquidity, e.SourceLifecycle, len(e.Sources))
}
t.Logf("STATUS SECTOR=%q ETF=%q", res.SourceStatuses["SECTOR_LEADER"], res.SourceStatuses["ETF_MOMENTUM"])
for _, w := range res.Warnings { t.Logf("WARN %s", w) }
}

func TestDiagETFPartial(t *testing.T) {
f := baseFake()
f.etfKlineErr = map[string]error{"159915": errors.New("kline down")}
g := NewGenerator(f, DefaultConfig())
res, err := g.Generate(context.Background(), nil, testNow)
if err != nil { t.Fatal(err) }
for _, e := range res.Pool {
if e.AssetType == "ETF" || e.PrimarySource == sourceETF {
t.Logf("ETFPOOL %s src=%s liq=%v adv=%v", e.Symbol, e.PrimarySource, e.Liquidity, e.AvgAmount20D)
}
}
t.Logf("STATUS ETF=%q", res.SourceStatuses["ETF_MOMENTUM"])
}
