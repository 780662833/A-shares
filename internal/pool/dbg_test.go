package pool

import (
"fmt"
"testing"
)

func deref(p *float64) interface{} {
if p == nil {
return nil
}
return *p
}

func TestDbgAll(t *testing.T) {
// path semantics
{
f := newFake()
f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
f.indexBars = func(idx string, lookback int) []DailyBar { return mkFlat(61, 100, 100, 100) }
f.sectorIndexBars = func(name string, lookback int) []DailyBar { return mkFlat(61, 99, 82.5, 82.5) }
f.stockBars = func(code string, lookback int) []DailyBar {
switch code {
case "600001":
return mkFlat(61, 110, 100, 100)
case "600002":
return mkFlat(61, 120, 126.316, 100)
case "600003":
return mkFlat(61, 90, 100, 100)
}
return nil
}
out, _ := runRefine(t, f, refineCfg(), []AggEntry{entryOf("600002", sourceSector), entryOf("600003", sourceSector), entryOf("600001", sourceSector)})
for _, e := range out {
fmt.Printf("PATH %v strength=%v score=%v pos=%v rsIdx=%v rsSec=%v\n", e.Symbol, e.Strength, e.RankScore, e.RankPos, deref(e.RS20IndexExcess), deref(e.RS20SectorExcess))
}
}
// single window degeneration
{
f := newFake()
f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
f.indexBars = func(idx string, lookback int) []DailyBar { return mkFlat(21, 105, 100, 100) }
f.stockBars = func(code string, lookback int) []DailyBar { return mkFlat(21, 11.5, 10, 10) }
out, _ := runRefineDual(t, f, refineCfg(), []AggEntry{entryOf("600001", sourceSector)}, 0, 60)
for _, e := range out {
fmt.Printf("SINGLE %v strength=%v score=%v scored=%v longEx=%v reason=%q\n", e.Symbol, e.Strength, e.RankScore, e.Scored, deref(e.RetLongExcess), e.StrengthReason)
}
}
// invalid windows fallback
{
f := newFake()
f.qualify = func(code string) *StockQualification { return qual("2020-01-01", 1, 5) }
f.indexBars = func(idx string, lookback int) []DailyBar { return mkFlat(61, 100, 100, 80) }
f.stockBars = func(code string, lookback int) []DailyBar { return mkFlat(61, 13, 10, 10) }
cfg := refineCfg()
cfg.RSShortWindow = 90
cfg.RSLongWindow = 0
out, _ := runRefine(t, f, cfg, []AggEntry{entryOf("600001", sourceSector)})
for _, e := range out {
fmt.Printf("INVALID %v strength=%v score=%v shortEx=%v longEx=%v reason=%q\n", e.Symbol, e.Strength, e.RankScore, deref(e.RetShortExcess), deref(e.RetLongExcess), e.StrengthReason)
}
}
}
