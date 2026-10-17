package pool

import (
"context"
"errors"
"fmt"
"testing"
)

// 完整 Generate() + B-5.1 局部夹具重排，打印最终池与来源状态。
func TestDbgFullGenRerank(t *testing.T) {
f := baseFake()
f.sectorRank = []SectorFlow{
{Code: "BK0488", Name: "食品饮料", MainNetInflow: 9e9},
{Code: "BK0438", Name: "汽车整车", MainNetInflow: 8e9},
{Code: "BK9999", Name: "失败板块", MainNetInflow: 7e9},
{Code: "BK1234", Name: "测试空板块", MainNetInflow: 1e9},
}
f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
g := NewGenerator(f, DefaultConfig())
res, err := g.Generate(context.Background(), nil, testNow)
if err != nil {
t.Fatal(err)
}
fmt.Println("srcStatus:", res.SourceStatuses)
for _, e := range res.Pool {
hs := ""
for _, h := range e.Sources {
hs += fmt.Sprintf("%s:%s ", h.SourceType, h.Status)
}
fmt.Printf("POOL %s primary=%s strength=%v liq=%v srcs=[%s]\n", e.Symbol, e.PrimarySource, e.Strength, e.Liquidity, hs)
}
for _, w := range res.Warnings {
fmt.Println("W:", w)
}
}

// ETF-only 场景：注入 159915 失败，检查 510300 是否存活于完整 Generate()。
func TestDbgFullGenEtfOnly(t *testing.T) {
f := baseFake()
f.etfKlineErr = map[string]error{"159915": errors.New("kline down")}
f.sectorRank = nil
f.dragon = nil
g := NewGenerator(f, DefaultConfig())
res, err := g.Generate(context.Background(), nil, testNow)
if err != nil {
t.Fatal(err)
}
fmt.Println("[etf-only] srcStatus:", res.SourceStatuses)
for _, e := range res.Pool {
fmt.Printf("POOL %s primary=%s liq=%v\n", e.Symbol, e.PrimarySource, e.Liquidity)
}
}
