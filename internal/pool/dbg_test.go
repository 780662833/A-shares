package pool

import (
"context"
"testing"
"time"
)

func TestDbgWarn(t *testing.T) {
f := newFake()
ib := mkFlat(61, 100, 100, 100)
sb := mkFlat(61, 110, 110, 110)
f.indexBars = func(string, int) []DailyBar { return ib }
f.stockBars = func(string, int) []DailyBar { return sb }
f.barsOHLC = func(string, int) []barOHLC { return mkOHLCFromDaily(sb) }
g := NewGenerator(f, refineCfgWithFin())
g.SetFinancialProvider(&fakeFin{fn: func(code string, asOf time.Time) (*FinSnapshot, error) {
s := cleanSnapshot()
s.Symbol = code
if code == "600001" {
s.ROE3yMedian = fptr(3.0)
}
return s, nil
}})
warns := []string{}
in := []AggEntry{entryOf("600001", sourceSector), entryOf("600002", sourceSector)}
out := g.refinePoolWith(context.Background(), in, &warns, finAsOf, 0, 0)
for _, e := range out {
t.Logf("DBG1 symbol=%s fin=%s scored=%v rankScore=%v rsscore=%v rankPos=%d strength=%s", e.Symbol, e.Financial, e.Scored, e.RankScore, e.RSScore, e.RankPos, e.Strength)
}
out2 := g.RefineScores(out, &warns)
for _, e := range out2 {
t.Logf("DBG2 symbol=%s fin=%s scored=%v rankScore=%v rsscore=%v rankPos=%d strength=%s demoted=%v", e.Symbol, e.Financial, e.Scored, e.RankScore, e.RSScore, e.RankPos, e.Strength, e.DemotedToWatch)
}
for _, w := range warns {
t.Logf("WARN: %s", w)
}
}
