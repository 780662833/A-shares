package pool

import (
"fmt"
"testing"
)

func TestDbgSoftChain(t *testing.T) {
e := AggEntry{Symbol: "600100", PrimarySource: sourceSector, Sources: []SourceHit{{SourceType: sourceSector}},
Eligibility: EligUnknown, Financial: FinDataInsufficient, Strength: StrengthUnset}
fmt.Printf("DBG Liquidity=%q stockLike=%v gatesApplicable=%v\n", e.Liquidity, e.isStockLike(), e.stockGatesApplicable())
e.ComputeTradability()
fmt.Printf("DBG caseA: %s / %q\n", e.Tradability, e.BlockedBy)
}
