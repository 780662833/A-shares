package pool

// 第 3.4.1 步：来源级 PARTIAL 状态生产 —— 契约与专项测试。
// 冻结规则（仅 SECTOR_LEADER / ETF_MOMENTUM 两个存在多子请求的来源）:
//   全部子请求成功且有有效候选          -> OK
//   全部子请求成功, 合法地无命中        -> SUCCESS_EMPTY
//   部分子请求失败, 但仍有可用结果      -> PARTIAL
//   部分子请求失败, 且无任何可用结果    -> FAILED（不能只看输出切片为空就判 SUCCESS_EMPTY）
//   全部必要子请求失败                  -> FAILED
// genFundFlow/genDragonTiger 无多子请求部分失败场景，不强行引入计数机制。
// deriveRunStatus、STALE、生命周期转换不在本文件范围。

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 判定函数单元测试（纯逻辑, 表驱动）
// ---------------------------------------------------------------------------

func TestDeriveSourceStatus(t *testing.T) {
	cases := []struct {
		name               string
		total, okCount     int
		hadCoreFailure     bool
		hasUsableResult    bool
		want               SourceStatus
		wantErrContainsAll []string // want==FAILED/PARTIAL 时错误聚合必须包含的子串
	}{
		{"all success with candidates", 5, 5, false, true, StatusOK, nil},
		{"all success no hits is legal empty", 5, 5, false, false, StatusSuccessEmpty, nil},
		// 注意: errs map 由下方代码按 fails=total-okCount 生成 BK001..BK00{fails},
		// 期望片段必须与实际失败键一致（2/5 ok -> 3 个失败键 BK001..BK003）。
		{"partial failure with usable result", 5, 2, false, true, StatusPartial, []string{"BK001", "BK002", "BK003"}},
		{"partial failure without any result", 5, 2, false, false, StatusFailed, []string{"BK001", "BK002", "BK003"}},
		{"core failure even if others ok", 5, 4, true, true, StatusFailed, []string{"core boom"}},
		{"all failed", 5, 0, false, false, StatusFailed, nil},
		{"zero subrequests degenerate to ok-if-usable", 0, 0, false, true, StatusOK, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			errs := map[string]error{}
			if c.total > 0 && c.okCount < c.total {
				fails := c.total - c.okCount
				for i := 0; i < fails; i++ {
					key := "BK00" + string(rune('1'+i)) // BK001..BK003 之类
					errs[key] = errors.New("fetch fail " + key)
				}
			}
			if c.hadCoreFailure {
				errs["__core__"] = errors.New("core boom")
			}
			st, err := deriveSourceStatus(c.total, c.okCount, errs, c.hadCoreFailure, c.hasUsableResult)
			if st != c.want {
				t.Fatalf("status=%q want %q", st, c.want)
			}
			switch c.want {
			case StatusOK, StatusSuccessEmpty:
				if err != nil {
					t.Fatalf("want nil aggregated error, got %v", err)
				}
			default:
				msg := err.Error()
				for _, frag := range c.wantErrContainsAll {
					if !strings.Contains(msg, frag) {
						t.Errorf("aggregated error %q missing fragment %q", msg, frag)
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// fakeProvider 错误注入扩展（SectorRankErr/ConstituentsErr/FundListErr/
// FundBatchQuoteErr/ETFCloseSeriesErr）——此前这些方法恒返回 nil 错误,
// 导致"单板块失败 continue"路径完全不可测。
// ---------------------------------------------------------------------------

func TestGenerateSectorPartialViaEndToEnd(t *testing.T) {
	f := baseFake()
	// 第三个板块取数失败；前两个板块正常出龙头 -> 有可用结果 -> PARTIAL
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
	if got := res.SourceStatuses["SECTOR_LEADER"]; got != StatusPartial {
		t.Fatalf("sector status=%q want PARTIAL", got)
	}
	// 部分结果不丢失: 原两个板块的龙头仍在池内
	syms := map[string]bool{}
	for _, e := range res.Pool {
		syms[e.Symbol] = true
	}
	for _, want := range []string{"600519", "601127", "300750"} {
		if !syms[want] {
			t.Errorf("entry %s lost despite partial success", want)
		}
	}
	// 来源命中记录与来源状态一致：AggEntry 无 HitStatus 字段，
	// 必须通过 Sources[] 中对应 SECTOR 的 SourceHit.Status 验证。
	foundPartialSector := false
	for _, e := range res.Pool {
		for _, hit := range e.Sources {
			if hit.SourceType != sourceSector {
				continue
			}
			foundPartialSector = true
			if hit.Status != StatusPartial {
				t.Fatalf(
					"sector source status = %v, want %v",
					hit.Status,
					StatusPartial,
				)
			}
		}
	}
	if !foundPartialSector {
		t.Fatal("expected a SECTOR SourceHit with PARTIAL status")
	}
	// 错误原因可审计
	if reason := res.SourceErrors["SECTOR_LEADER"]; !strings.Contains(reason, "BK9999") {
		t.Errorf("source_errors missing failing board: %q", reason)
	}
	// warnings 含 PARTIAL 提示
	foundWarn := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "SECTOR_LEADER") && strings.Contains(w, "PARTIAL") {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Errorf("no PARTIAL warning recorded: %v", res.Warnings)
	}
}

func TestGenerateSectorAllFailStillFailed(t *testing.T) {
	f := baseFake()
	f.constituentsErr = map[string]error{}
	for k := range f.constituents {
		f.constituentsErr[k] = errors.New("all boards down")
	}
	g := NewGenerator(f, DefaultConfig())
	res, err := g.Generate(context.Background(), nil, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.SourceStatuses["SECTOR_LEADER"]; got != StatusFailed {
		t.Fatalf("sector status=%q want FAILED", got)
	}
	// FAILED 不得伪装成 SUCCESS_EMPTY
	if got := res.SourceStatuses["SECTOR_LEADER"]; got == StatusSuccessEmpty {
		t.Fatal("FAILED disguised as SUCCESS_EMPTY")
	}
}

func TestGenerateSectorLegalEmptyStillSuccessEmpty(t *testing.T) {
	f := baseFake()
	// 上游正常返回空排行(非错误): 合法无命中 -> SUCCESS_EMPTY
	f.sectorRank = nil
	g := NewGenerator(f, DefaultConfig())
	res, err := g.Generate(context.Background(), nil, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.SourceStatuses["SECTOR_LEADER"]; got != StatusSuccessEmpty {
		t.Fatalf("sector status=%q want SUCCESS_EMPTY (legal empty)", got)
	}
}

func TestGenerateSectorCoreFailureIsFailedNotPartial(t *testing.T) {
	f := baseFake()
	// 核心请求(行业资金流排行)失败, 即使成分股接口完好也必须 FAILED ——
	// 防止"核心失败但辅助成功被误标 PARTIAL"的边界。
	f.sectorRankErr = errors.New("rank api down")
	g := NewGenerator(f, DefaultConfig())
	res, err := g.Generate(context.Background(), nil, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.SourceStatuses["SECTOR_LEADER"]; got != StatusFailed {
		t.Fatalf("sector status=%q want FAILED (core request failure)", got)
	}
}

func TestGenerateETFPartialAndBoundaries(t *testing.T) {
	t.Run("partial with usable result", func(t *testing.T) {
		f := baseFake()
		f.etfKlineErr = map[string]error{"159915": errors.New("kline down")}
		g := NewGenerator(f, DefaultConfig())
		res, err := g.Generate(context.Background(), nil, testNow)
		if err != nil {
			t.Fatal(err)
		}
		if got := res.SourceStatuses["ETF_MOMENTUM"]; got != StatusPartial {
			t.Fatalf("etf status=%q want PARTIAL", got)
		}
		syms := map[string]bool{}
		for _, e := range res.Pool {
			syms[e.Symbol] = true
		}
		if !syms["510300"] {
			t.Error("successful ETF entry lost")
		}
		if syms["159915"] {
			t.Error("failed ETF must not appear as hit")
		}
		if reason := res.SourceErrors["ETF_MOMENTUM"]; !strings.Contains(reason, "159915") {
			t.Errorf("source_errors missing failing etf: %q", reason)
		}
	})
	t.Run("all subrequests failed no result -> FAILED", func(t *testing.T) {
		f := baseFake()
		// 注意: 不能置 f.kline=nil —— fake 的 AllStocks 由 kline(code).Closes[59]
		// 推导价格, 置 nil 会连带破坏流动性初筛(全部拒), SECTOR 来源会被误伤。
		// 只注入 ETF K 线错误, 覆盖全部候选(510300/159915)。
		f.etfKlineErr = map[string]error{
			"510300": errors.New("kline down"),
			"159915": errors.New("kline down"),
		}
		g := NewGenerator(f, DefaultConfig())
		res, err := g.Generate(context.Background(), nil, testNow)
		if err != nil {
			t.Fatal(err)
		}
		if got := res.SourceStatuses["ETF_MOMENTUM"]; got != StatusFailed {
			t.Fatalf("etf status=%q want FAILED", got)
		}
	})
	t.Run("legal empty list -> FAILED per existing contract", func(t *testing.T) {
		// 现状契约: FundList 返回空列表被实现视为取数异常(DeadlineExceeded 哨兵),
		// 维持 FAILED, 本阶段不改语义, 如实锁定。
		f := baseFake()
		f.fundCodes = nil
		g := NewGenerator(f, DefaultConfig())
		res, err := g.Generate(context.Background(), nil, testNow)
		if err != nil {
			t.Fatal(err)
		}
		if got := res.SourceStatuses["ETF_MOMENTUM"]; got != StatusFailed {
			t.Fatalf("etf status=%q want FAILED (existing sentinel semantics)", got)
		}
	})
	t.Run("all ok unchanged", func(t *testing.T) {
		f := baseFake()
		g := NewGenerator(f, DefaultConfig())
		res, err := g.Generate(context.Background(), nil, testNow)
		if err != nil {
			t.Fatal(err)
		}
		if got := res.SourceStatuses["ETF_MOMENTUM"]; got != StatusOK {
			t.Fatalf("etf status=%q want OK", got)
		}
		if got := res.SourceStatuses["SECTOR_LEADER"]; got != StatusOK {
			t.Fatalf("sector status=%q want OK", got)
		}
	})
}
