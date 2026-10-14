package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ceheng-io/stock-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli := stock.New(stock.WithTimeout(15 * time.Second))
	qs, err := cli.Quotes.BatchCN(ctx, []string{"sh600519", "sz300750", "sz000001"}, stock.BatchOptions{})
	if err != nil {
		fmt.Println("err:", err)
		return
	}
	for _, q := range qs {
		fmt.Printf("%s %s price=%.2f vol=%.0f amount=%.0f cap=%.4g\n",
			q.Code, q.Name, q.Price, q.Volume, q.Amount, deref(q.TotalMarketCap))
	}
}
func deref(p *float64) float64 {
	if p == nil {
		return -1
	}
	return *p
}
