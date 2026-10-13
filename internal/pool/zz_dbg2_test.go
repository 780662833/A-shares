package pool

import (
"context"
"testing"
)

func TestZZDbgPath(t *testing.T) {
f := newFake()
f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
f.indexBars = func(idx string, lookback int) []DailyBar { return mkFlat(61, 104, 100, 96.296) }
sb := mkFlat(61, 13, 11.4035, 12.7451)
f.stockBars = func(code string, lookback int) []DailyBar { return sb }
f.barsOHLC = func(code string, lookback int) []barOHLC { return mkOHLCFromDaily(sb) }
cfg := refineCfg()
g := NewGenerator(f, cfg)
warns := []string{}
poolIn := []AggEntry{entryOf("600001", sourceSector)}
out := g.refinePoolWith(context.Background(), poolIn, &warns, testNow, 0, 0)
e := out[0]
t.Logf("A=%v B=%v NONE=%v Unset=%v; got Strength=%v(%q)", StrengthA, StrengthB, StrengthNone, StrengthUnset, e.Strength, e.Strength)
// replicate switch logic manually
sExOK := e.RetShortExcess != nil && *e.RetShortExcess > 0
onlyShort := e.RetLongExcess == nil && e.RetShortExcess != nil
t.Logf("sExOK=%v onlyShort=%v lExNil=%v", sExOK, onlyShort, e.RetLongExcess == nil)
}
