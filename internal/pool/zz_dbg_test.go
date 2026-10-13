package pool

import (
"context"
"testing"
)

func TestZZDbgDual(t *testing.T) {
f := newFake()
f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
f.indexBars = func(idx string, lookback int) []DailyBar {
t.Logf("IDX lookback=%d", lookback)
return mkFlat(61, 104, 100, 96.296)
}
sb := mkFlat(61, 13, 11.4035, 12.7451)
f.stockBars = func(code string, lookback int) []DailyBar {
t.Logf("STK lookback=%d len=%d", lookback, len(sb))
return sb
}
f.barsOHLC = func(code string, lookback int) []barOHLC { return mkOHLCFromDaily(sb) }
cfg := refineCfg()
g := NewGenerator(f, cfg)
warns := []string{}
out := g.refinePoolWith(context.Background(), []AggEntry{entryOf("600001", sourceSector)}, &warns, testNow, 0, 0)
e := out[0]
t.Logf("score=%v sEx=%v lEx=%v strength=%v scored=%v", e.RankScore, e.RetShortExcess, e.RetLongExcess, e.Strength, e.Scored)
if e.RetShortExcess != nil {
t.Logf("sExVal=%v", *e.RetShortExcess)
}
if e.RetLongExcess != nil {
t.Logf("lExVal=%v", *e.RetLongExcess)
}
for _, w := range warns {
t.Logf("warn: %s", w)
}
}
