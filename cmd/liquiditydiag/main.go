// liquiditydiag —— 生产数据诊断: 统计全市场快照在 liquidityScreen 各道闸门上的漏斗分布。
package main

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"time"

	"ashare-quant/internal/pool"
)

var (
	stRe    = regexp.MustCompile(`(?i)\*?ST`)
	acodeRe = regexp.MustCompile(`^\d{6}$`)
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	prov := pool.NewSDKProvider(15 * time.Second)
	quotes, err := prov.AllStocks(ctx)
	if err != nil {
		fmt.Println("AllStocks error:", err)
		return
	}
	cfg := pool.DefaultConfig()
	var nCode, nST, nPrice, nAmtLE1e8, nCap int
	var pass1e8, pass5kw []pool.StockQuote
	var zero, noCap int
	for _, q := range quotes {
		if q.AvgAmount20D == 0 {
			zero++
		}
		if q.TotalMarketCap == 0 {
			noCap++
		}
		if !acodeRe.MatchString(q.Code) {
			nCode++
			continue
		}
		if cfg.ExcludeST && stRe.MatchString(q.Name) {
			nST++
			continue
		}
		if q.Price < cfg.MinPrice {
			nPrice++
			continue
		}
		if q.AvgAmount20D > 5e7 {
			pass5kw = append(pass5kw, q)
		}
		if q.AvgAmount20D <= cfg.MinAvgAmount {
			nAmtLE1e8++
			continue
		}
		if cfg.MinMarketCap > 0 && q.TotalMarketCap > 0 && q.TotalMarketCap < cfg.MinMarketCap {
			nCap++
			continue
		}
		pass1e8 = append(pass1e8, q)
	}
	fmt.Printf("快照总数=%d  当日成交额=0 的只数=%d (%.1f%%)  市值字段缺失(=0)=%d\n",
		len(quotes), zero, float64(zero)/float64(len(quotes))*100, noCap)
	fmt.Printf("漏斗: 非6位代码剔=%d ST剔=%d 价<%.1f剔=%d 额<=1亿剔=%d 市值<50亿剔=%d\n",
		nCode, nST, cfg.MinPrice, nPrice, nAmtLE1e8, nCap)
	fmt.Printf("通过默认门槛(额>1亿且市值>=50亿)=%d ; 若门槛改为额>5千万=%d\n", len(pass1e8), len(pass5kw))
	sort.Slice(pass5kw, func(i, j int) bool { return pass5kw[i].AvgAmount20D > pass5kw[j].AvgAmount20D })
	n := 10
	if len(pass5kw) < n {
		n = len(pass5kw)
	}
	fmt.Println("--- 额>5千万 按当日成交额降序前", n, "只 ---")
	for _, q := range pass5kw[:n] {
		fmt.Printf("%s %-12s price=%.2f amount=%.3e cap=%.3e\n", q.Code, q.Name, q.Price, q.AvgAmount20D, q.TotalMarketCap)
	}
}
