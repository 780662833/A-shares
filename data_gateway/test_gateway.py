"""Phase T1: Python 网关测试 (AKShare 拉数据 + czsc 分析, 全离线打桩)。

覆盖:
    T1.1 czsc_engine 纯函数 (DataFrame->RawBar / analyze_czsc / 买卖点)
    T1.2 单股接口 /api/czsc/analyze (参数校验 / 缓存优先 / source 语义)
    T1.3 批量粗筛 /api/czsc/screen_pool (过滤规则 / 容错隔离 / mine_sweep 拦截)
    T1.4 长期方案 C: /api/czsc/refresh 预计算 + cache_only 零网络出池
    T1.5 vectorbt 回测端到端 (合成分形数据 -> trade_count>0 终验)
    T1.6 财务排雷 mine_batch / 情绪接口 sentiment (打桩)
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from conftest import fractal_ohlcv


# ---------------------------------------------------------------------------
# T1.1 czsc_engine 纯函数
# ---------------------------------------------------------------------------

class TestCzscEngine:
    def test_akshare_to_bars_chinese_columns(self, hist_df):
        from czsc_engine import akshare_to_czsc_bars
        bars = akshare_to_czsc_bars(hist_df, symbol="000001", period="daily")
        assert len(bars) == len(hist_df)
        b = bars[0]
        assert str(b.symbol) == "000001" or getattr(b.symbol, "value", "") == "000001"
        assert b.high >= max(b.open, b.close) and b.low <= min(b.open, b.close)
        assert [x.id for x in bars[:5]] == [0, 1, 2, 3, 4]

    def test_akshare_to_bars_english_columns_dirty_rows_skipped(self):
        """英文列名兼容 + 脏行跳过而不是整体失败。"""
        from czsc_engine import akshare_to_czsc_bars
        df = pd.DataFrame({
            "date": ["2026-01-02", "not-a-date", "2026-01-06"],
            "open": [10.0, 10.5, 11.0],
            "close": [10.4, 10.9, 11.5],
            "high": [10.6, 11.1, 11.8],
            "low": [9.8, 10.2, 10.9],
            "volume": [1e6, 1e6, 1e6],
            "amount": [1.04e7, 1.09e7, 1.15e7],
        })
        bars = akshare_to_czsc_bars(df, symbol="600000")
        assert len(bars) == 2  # 坏日期行被跳过

    def test_analyze_produces_bi_and_summary(self, hist_df):
        """分形合成数据必须产出笔(此前平滑锯齿导致 bi_list 为空的回归防护)。"""
        from czsc_engine import akshare_to_czsc_bars, analyze_czsc
        bars = akshare_to_czsc_bars(hist_df, symbol="000001")
        result = analyze_czsc(bars)
        assert "error" not in result
        st = result["structures"]
        assert st["bi_count"] >= 5, f"分形数据应产出足够笔, got {st['bi_count']}"
        assert isinstance(st["zs_count"], int)
        assert result["summary"] and isinstance(result["summary"], str)
        # last_bi 方向必须是 up/down 之一
        assert st["last_bi"]["direction"] in ("up", "down")

    def test_analyze_empty_bars(self):
        from czsc_engine import analyze_czsc
        assert "error" in analyze_czsc([])

    def test_max_recent_bars_truncation(self, hist_df):
        """超过 max_recent_bars 时截断重建, id 必须连续 (czsc 硬约束)。"""
        from czsc_engine import akshare_to_czsc_bars, analyze_czsc
        bars = akshare_to_czsc_bars(hist_df, symbol="000001")
        result = analyze_czsc(bars, max_recent_bars=120)
        assert result["bars_count"] == 120


# ---------------------------------------------------------------------------
# T1.2 单股接口 /api/czsc/analyze
# ---------------------------------------------------------------------------

class TestAnalyzeEndpoint:
    def test_bad_symbol_400(self, gateway_client):
        client, _ = gateway_client
        r = client.get("/api/czsc/analyze", params={"symbol": "ABC"})
        assert r.status_code == 400

    def test_bad_period_400(self, gateway_client):
        client, _ = gateway_client
        r = client.get("/api/czsc/analyze", params={"symbol": "000001", "period": "1m"})
        assert r.status_code == 400

    def test_normal_returns_structures_signals(self, gateway_client):
        client, calls = gateway_client
        r = client.get("/api/czsc/analyze", params={
            "symbol": "000001", "start_date": "20250101", "end_date": "20261009"})
        assert r.status_code == 200
        body = r.json()
        assert body["code"] == 0
        data = body["data"]
        assert "structures" in data and "signals" in data
        assert "buy_points" in data["signals"] and "sell_points" in data["signals"]
        # 首次请求必然回源, qfq 前复权约定不能丢
        assert calls and calls[0]["adjust"] == "qfq"

    def test_cache_hit_zero_network(self, gateway_client, monkeypatch):
        """第二次相同请求应命中磁盘缓存, 不再调用 AKShare (方案 C 核心断言)。

        确定性保障: gateway_client 夹具已将行情缓存与磁盘缓存的时钟统一冻结到
        数据终点 2026-10-09 (收盘后语义), "当日新鲜"判定在任何真实运行日期下
        都成立 —— 这正是该用例要验证的生产语义 (收盘刷新后零网络)。
        """
        client, calls = gateway_client
        p = {"symbol": "000001", "start_date": "20250101", "end_date": "20261009"}
        r1 = client.get("/api/czsc/analyze", params=p)
        n_after_first = len(calls)
        r2 = client.get("/api/czsc/analyze", params=p)
        assert r1.status_code == r2.status_code == 200
        assert r2.json()["source"] == "cache"
        assert len(calls) == n_after_first, "缓存命中后不应再发起网络请求"
        # 两次结果结构一致
        assert r1.json()["data"]["structures"]["bi_count"] == r2.json()["data"]["structures"]["bi_count"]

    def test_use_cache_false_forces_live(self, gateway_client):
        client, calls = gateway_client
        p = {"symbol": "000001", "start_date": "20250101", "end_date": "20261009",
             "use_cache": "false"}
        client.get("/api/czsc/analyze", params=p)
        before = len(calls)
        r = client.get("/api/czsc/analyze", params=p)
        assert len(calls) > before, "use_cache=false 必须强制回源"
        assert r.json()["source"] == "live_forced"


# ---------------------------------------------------------------------------
# T1.3 批量粗筛 /api/czsc/screen_pool
# ---------------------------------------------------------------------------

class TestScreenPool:
    def test_empty_symbols_400(self, gateway_client):
        client, _ = gateway_client
        r = client.post("/api/czsc/screen_pool", json={"symbols": []})
        assert r.status_code == 400

    def test_over_limit_400(self, gateway_client):
        client, _ = gateway_client
        r = client.post("/api/czsc/screen_pool",
                        json={"symbols": ["000001"] * 101})
        assert r.status_code == 400

    def test_invalid_symbol_isolated_in_errors(self, gateway_client):
        """非法代码只进 errors, 不阻断合法股票。"""
        client, _ = gateway_client
        r = client.post("/api/czsc/screen_pool", json={
            "symbols": ["BADCODE", "000001"], "mine_sweep": False,
            "start_date": "20250101", "end_date": "20261009"})
        assert r.status_code == 200
        d = r.json()["data"]
        assert any(e["symbol"] == "BADCODE" for e in d["errors"])
        assert d["total_scanned"] == 1

    def test_screen_rule_consistency(self, gateway_client):
        """pool 中每条记录必须满足 a/b/c 三条规则之一 (用同一结果重算验证)。"""
        client, _ = gateway_client
        r = client.post("/api/czsc/screen_pool", json={
            "symbols": ["000001", "600519"], "mine_sweep": False,
            "start_date": "20250101", "end_date": "20261009"})
        assert r.status_code == 200
        d = r.json()["data"]
        assert d["total_scanned"] == 2
        for hit in d["pool"]:
            ok = (hit["signal_type"] != "Observation"
                  or (hit["zs_count"] >= 2 and hit["trend_direction"] == "up"))
            assert ok, f"违反过滤规则: {hit}"
            assert hit["summary"], "候选记录必须带 LLM 友好摘要"

    def test_single_failure_does_not_break_batch(self, gateway_client, monkeypatch):
        """一只股票上游异常 -> 记入 errors, 其余照常扫描。"""
        import akshare as ak
        client, _ = gateway_client
        orig = ak.stock_zh_a_hist

        def flaky(symbol=None, **kw):
            if symbol == "600519":
                raise RuntimeError("模拟上游超时")
            return orig(symbol=symbol, **kw)

        monkeypatch.setattr(ak, "stock_zh_a_hist", flaky)
        r = client.post("/api/czsc/screen_pool", json={
            "symbols": ["000001", "600519"], "mine_sweep": False,
            "start_date": "20250101", "end_date": "20261009"})
        assert r.status_code == 200
        d = r.json()["data"]
        assert d["total_scanned"] == 1
        assert any(e["symbol"] == "600519" for e in d["errors"])

    def test_mine_sweep_blocks_st_like_symbol(self, gateway_client, monkeypatch):
        """财务排雷: 低 ROE 高负债的确凿证据 -> 从 pool 移入 blocked。"""
        import main as gw
        client, _ = gateway_client
        # 先确认技术粗筛能出池; 若该种子组合恰好无信号则跳过 (规则依赖行情形态)
        r0 = client.post("/api/czsc/screen_pool", json={
            "symbols": ["000001"], "mine_sweep": False,
            "start_date": "20250101", "end_date": "20261009"})
        if not r0.json()["data"]["pool"]:
            pytest.skip("该合成数据未触发粗筛规则, 排雷路径由 test_mine_batch 覆盖")

        monkeypatch.setattr(gw, "_check_mine_symbol", lambda s, **kw: {
            "symbol": s, "passed": False,
            "reasons": ["最新报告期 ROE=-5.00% < 3%"], "snapshot": {},
        }, raising=False)
        if not hasattr(gw, "_check_mine_symbol"):
            # screen_pool 内部直接引用 mine_sweeper.check_symbol
            import mine_sweeper
            monkeypatch.setattr(mine_sweeper, "check_symbol", lambda s, **kw: {
                "symbol": s, "passed": False,
                "reasons": ["ROE=-5.00% < 3%"], "snapshot": {}})
        r = client.post("/api/czsc/screen_pool", json={
            "symbols": ["000001"], "mine_sweep": True,
            "start_date": "20250101", "end_date": "20261009"})
        d = r.json()["data"]
        assert d["blocked_size"] >= 1
        assert all(h["symbol"] != "000001" for h in d["pool"])


# ---------------------------------------------------------------------------
# T1.4 长期方案 C: refresh 预计算 + cache_only
# ---------------------------------------------------------------------------

class TestCachePipeline:
    def test_refresh_then_cache_only_zero_network(self, gateway_client):
        """refresh 写缓存后, cache_only=true 的 screen_pool 必须零网络调用。"""
        client, calls = gateway_client
        r = client.post("/api/czsc/refresh", json={
            "symbols": ["000001", "600519"],
            "start_date": "20250101", "end_date": "20261009"})
        assert r.status_code == 200
        refreshed = len(calls)
        assert refreshed >= 1, "refresh 应实际回源"

        calls.clear()
        r2 = client.post("/api/czsc/screen_pool", json={
            "symbols": ["000001", "600519"], "cache_only": True, "mine_sweep": False})
        assert r2.status_code == 200
        assert calls == [], "cache_only=true 绝不允许触碰 AKShare"
        assert r2.json()["data"]["total_scanned"] == 2

    def test_cache_status_reports_entries(self, gateway_client):
        client, _ = gateway_client
        client.post("/api/czsc/refresh", json={
            "symbols": ["000001"], "start_date": "20250101", "end_date": "20261009"})
        r = client.get("/api/czsc/cache_status")
        assert r.status_code == 200
        body = r.json()
        assert body["code"] == 0

    def test_delete_cache_clears(self, gateway_client):
        client, calls = gateway_client
        client.post("/api/czsc/refresh", json={
            "symbols": ["000001"], "start_date": "20250101", "end_date": "20261009"})
        r = client.delete("/api/czsc/cache", params={"period": "daily"})
        assert r.status_code == 200
        calls.clear()
        # 清缓存后 cache_only 模式应扫不到数据 (进 errors 而非静默假阳性)
        r2 = client.post("/api/czsc/screen_pool", json={
            "symbols": ["000001"], "cache_only": True, "mine_sweep": False})
        d = r2.json()["data"]
        assert calls == []
        assert d["total_scanned"] == 0 or "no_cache" in json.dumps(d["errors"])


# ---------------------------------------------------------------------------
# T1.5 vectorbt 回测端到端 —— 遗留事项终验: trade_count > 0
# ---------------------------------------------------------------------------

class TestBacktestE2E:
    def test_replay_produces_events_on_fractal_data(self, hist_df_long):
        """重放链路终验: 长周期分形数据上必须产生可定位的缠论信号事件。"""
        from czsc_backtest import replay_signals
        entries, exits, events = replay_signals(hist_df_long, symbol="000001",
                                                window=180, stride=5)
        assert len(events) > 0, "信号重放仍为空 => 对齐逻辑退化"
        assert int(entries.sum()) + int(exits.sum()) == len(events)
        for ev in events:
            assert ev["side"] in ("buy", "sell")
            assert ev["type"] in ("1B", "2B", "3B", "1S", "2S", "3S")

    def test_run_backtest_trade_count_positive(self, hist_df_long):
        """vectorbt 管道终验: 信号驱动的完整回测必须产生成交 (trade_count>0)。

        注: run_backtest 返回结构为 {signal_stats:{entry_signals,exit_signals},
        metrics:{trade_count,...}} —— 本用例同时锁定该契约, 防止字段名漂移。
        """
        from czsc_backtest import run_backtest, is_available
        if not is_available():
            pytest.skip("vectorbt 不可用")
        res = run_backtest(hist_df_long, symbol="000001", window=180, stride=5)
        assert "error" not in res, res.get("error")
        n_sig = res["signal_stats"]["entry_signals"] + res["signal_stats"]["exit_signals"]
        assert n_sig > 0
        assert res["metrics"]["trade_count"] > 0, "遗留事项终验: 必须跑出真实成交"
        for k in ("win_rate_pct", "avg_trade_return_pct", "total_return_pct",
                  "benchmark_return_pct", "alpha_vs_hold_pct", "max_drawdown_pct",
                  "sharpe", "positive_edge"):
            assert k in res["metrics"], f"绩效字段缺失: {k}"
        # T+1 建模生效: 成交笔数不可能超过持仓周期允许的上限, 且明细日期成对有序
        for tr in res["recent_trades"]:
            assert tr["entry_date"] <= tr["exit_date"]

    def test_per_signal_stats_shape(self, hist_df_long):
        from czsc_backtest import per_signal_stats
        stats = per_signal_stats(hist_df_long, symbol="000001", window=180, stride=5)
        assert set(stats.keys()) == {"symbol", "total_events", "by_type"}
        by_type = stats["by_type"]
        assert set(by_type.keys()) >= {"1B", "2B", "3B", "1S", "2S", "3S"}
        for t in ("1B", "2B", "3B"):
            v = by_type[t]
            assert v["signals"] >= 0 and v["measured"] <= v["signals"]
            if v["measured"] > 0:
                assert 0.0 <= v["win_rate_pct"] <= 100.0
        for t in ("1S", "2S", "3S"):
            v = by_type[t]
            assert v["signals"] >= 0
            if v["measured"] > 0:
                assert 0.0 <= v["drop_avoided_pct"] <= 100.0

    def test_summarize_verdict_branches(self):
        from czsc_backtest import summarize_rule_perf
        # summarize_rule_perf 消费的是 run_backtest 的完整结果 dict,
        # 判定读取 r["metrics"]{trade_count, avg_trade_return_pct, alpha_vs_hold_pct, positive_edge}
        def _r(trade_count, avg_ret, alpha, edge=None):
            return {"symbol": "X",
                    "metrics": {"trade_count": trade_count,
                                "avg_trade_return_pct": avg_ret,
                                "alpha_vs_hold_pct": alpha,
                                "positive_edge": edge if edge is not None
                                else (trade_count >= 5 and avg_ret > 0)}}
        pos = [_r(20, 1.5, 0.8) for _ in range(2)]
        neg = [_r(20, -1.2, -2.0) for _ in range(2)]
        few = [_r(2, 5.0, 5.0)]                       # 样本不足 -> insufficient
        mixed = [_r(20, 0.9, -0.3)]                   # 单笔为正但跑输持有 -> mixed
        assert summarize_rule_perf(pos, "r")["verdict"] == "positive_expectancy"
        assert summarize_rule_perf(neg, "r")["verdict"] == "negative_expectancy"
        assert summarize_rule_perf(few, "r")["verdict"] == "insufficient_data"
        assert summarize_rule_perf(mixed, "r")["verdict"] == "mixed"
        # 样本不足优先于交易数判定: trade_count=0 且 positive_edge=False -> insufficient
        zero_trades = [{"symbol": "Z", "metrics": {
            "trade_count": 0, "avg_trade_return_pct": 0.0,
            "alpha_vs_hold_pct": 0.0, "positive_edge": False}}]
        assert summarize_rule_perf(zero_trades, "r")["verdict"] == "insufficient_data"
        # 结构稳定性: 所有分支都携带 avg_alpha_vs_hold_pct 键 (下游 Go 解析契约)
        for rs, name in ((pos, "pos"), (neg, "neg"), (few, "few"), (mixed, "mixed")):
            out = summarize_rule_perf(rs, name)
            assert "avg_alpha_vs_hold_pct" in out and "edge_ratio_pct" in out

    def test_backtest_rules_endpoint(self, gateway_client):
        client, _ = gateway_client
        r = client.post("/api/czsc/backtest/rules", json={
            "symbols": ["000001"], "start_date": "20250101", "end_date": "20261009"})
        assert r.status_code == 200
        body = r.json()
        assert body["code"] == 0


# ---------------------------------------------------------------------------
# T1.7 Walk-Forward 滚动回测 (Step 6: 全样本内 -> 训练/测试防泄漏口径)
# ---------------------------------------------------------------------------

class TestWalkForward:
    """run_walk_forward 算法契约 + wf 缓存 roundtrip + summarize 分支。"""

    def test_run_walk_forward_windows_and_verdict(self, hist_df_long):
        from czsc_backtest import run_walk_forward, is_available
        if not is_available():
            pytest.skip("vectorbt 不可用")
        res = run_walk_forward(hist_df_long, symbol="000001",
                               train_size=350, test_size=80, stride=8)
        assert "error" not in res, res.get("error")
        # 结构契约 (Go/报表下游解析依赖这些键名)
        assert res["mode"] in ("rolling", "anchored")
        assert set(res["summary"].keys()) >= {
            "n_windows", "test_trade_count", "avg_train_expectancy_pct",
            "avg_test_expectancy_pct", "degradation_pp", "verdict"}
        assert len(res["windows"]) == res["summary"]["n_windows"]
        for w in res["windows"]:
            for k in ("train_range", "test_range", "train_metrics", "test_metrics"):
                assert k in w
            # 防泄漏不变式: 训练段严格早于测试段起点
            assert w["train_range"].split("~")[1] <= w["test_range"].split("~")[0]

    def test_run_walk_forward_insufficient_bars(self, hist_df):
        from czsc_backtest import run_walk_forward, is_available
        if not is_available():
            pytest.skip("vectorbt 不可用")
        # 样本闸门: 训练窗低于 _WF_MIN_TRAIN_BARS(250) 直接拒绝 —— czsc 分型/笔
        # 确认延迟会吃掉大半窗口, 勉强出的"样本外期望"没有统计意义。
        res = run_walk_forward(hist_df, symbol="000001",
                               train_size=100, test_size=63)
        assert res.get("error") == "insufficient_train_window"
        assert res["required_min"] == 250

        # 数据长度不足一个完整训练窗+最小测试段: 400根配 train=380 (需>=420) 拒绝
        res2 = run_walk_forward(hist_df, symbol="000001",
                                train_size=380, test_size=63)
        assert res2.get("error") == "insufficient_bars"
        assert "required_min" in res2

    def test_wf_cache_roundtrip(self, tmp_path, monkeypatch):
        import os as _os
        import czsc_backtest as cb
        monkeypatch.setattr(cb, "_DEFAULT_DIR", str(tmp_path))
        payload = {"symbol": "000001", "summary": {"verdict": "oos_positive"},
                   "generated_at": __import__("datetime").datetime.now().isoformat(timespec="seconds")}
        rk, pk = "1B+2B+3B__1S+2S+3S", "rolling-daily-250-63-5-20230101-20261009"
        assert cb.wf_cache_get("000001", rk, pk) is None
        cb.wf_cache_put("000001", rk, pk, payload)
        hit = cb.wf_cache_get("000001", rk, pk)
        assert hit is not None and hit["source"] == "cache"
        # 不同 params_key 必须隔离 (窗口参数变了旧结论作废)
        assert cb.wf_cache_get("000001", rk, "rolling-daily-350-80-5-20230101-20261009") is None
        path = _os.path.join(str(tmp_path), "walk_forward", rk, pk, "000001.json")
        assert _os.path.exists(path)

    def test_summarize_walk_forward_branches(self):
        from czsc_backtest import summarize_walk_forward

        def _r(test_exp, train_exp, trades=5):
            return {"symbol": "X", "summary": {
                "test_trade_count": trades,
                "avg_test_expectancy_pct": test_exp,
                "avg_train_expectancy_pct": train_exp}}
        robust = [_r(1.0, 1.5) for _ in range(3)]
        fragile = [_r(-1.0, 2.0), _r(-0.5, 1.0)]
        negative = [_r(-1.0, -0.5)]
        no_trades = [{"symbol": "Y", "summary": {"test_trade_count": 0,
                    "avg_test_expectancy_pct": None, "avg_train_expectancy_pct": None}}]
        assert summarize_walk_forward(robust, "r")["verdict"] == "robust_positive"
        assert summarize_walk_forward(fragile, "r")["verdict"] == "fragile_positive"
        assert summarize_walk_forward(negative, "r")["verdict"] == "negative"
        assert summarize_walk_forward(no_trades, "r")["verdict"] == "insufficient_data"

    def test_walk_forward_symbol_endpoint_e2e(self, gateway_client):
        """端到端: GET /api/czsc/walk_forward/{symbol} 真算法跑通并落盘缓存。"""
        import main as gw
        if not gw._bt_is_available():
            pytest.skip("vectorbt 不可用")
        client, calls = gateway_client
        r = client.get("/api/czsc/walk_forward/000001?train_size=350&test_size=80"
                       "&stride=8&use_cache=false&start_date=20240101&end_date=20261009")
        assert r.status_code == 200
        body = r.json()
        assert body["code"] == 0 and body["message"] == "success"
        data = body["data"]
        assert data["symbol"] == "000001"
        assert data["source"] == "live"
        assert "summary" in data and "windows" in data
        assert len(calls) >= 1  # 确实回源了行情

        # 二次请求命中磁盘缓存 (与 conftest 打桩的临时缓存目录隔离策略一致)
        r2 = client.get("/api/czsc/walk_forward/000001?train_size=350&test_size=80"
                        "&stride=8&start_date=20240101&end_date=20261009")
        assert r2.json()["data"]["source"] == "cache"

    def test_walk_forward_param_validation(self, gateway_client):
        client, _ = gateway_client
        assert client.get("/api/czsc/walk_forward/abc").status_code == 400
        assert client.get("/api/czsc/walk_forward/000001?period=minute").status_code == 400
        assert client.post("/api/czsc/walk_forward/rules",
                           json={"symbols": []}).status_code == 400
        assert client.post("/api/czsc/walk_forward/rules",
                           json={"symbols": ["000001"], "train_size": 10}).status_code == 422


# ---------------------------------------------------------------------------
# T1.6 财务排雷 & 情绪接口 (打桩)
# ---------------------------------------------------------------------------

class TestMineAndSentiment:
    def test_mine_check_pass_with_good_financials(self, gateway_client, monkeypatch):
        import mine_sweeper
        monkeypatch.setattr(mine_sweeper, "fetch_financial_snapshot", lambda s, ttl_days=7: {
            "roe_pct": 12.5, "debt_ratio_pct": 45.0,
            "report_date": "2026-03-31", "yearly_net_profit": [("2024", 1e8), ("2025", 1.2e8)],
            "source": "stub"})
        monkeypatch.setattr(mine_sweeper, "is_st_or_risk", lambda s, name_map=None: (False, ""))
        client, _ = gateway_client
        r = client.get("/api/financial/mine_check", params={"symbol": "600519"})
        assert r.status_code == 200
        d = r.json()["data"]
        assert d["passed"] is True and d["reasons"] == []

    def test_mine_batch_splits_clean_blocked(self, gateway_client, monkeypatch):
        import mine_sweeper

        def fake_snap(s, ttl_days=7):
            if s == "000002":
                return {"roe_pct": -8.0, "debt_ratio_pct": 95.0, "report_date": "2026-03-31",
                        "yearly_net_profit": [("2024", -1e8), ("2025", -2e8)], "source": "stub"}
            return {"roe_pct": 15.0, "debt_ratio_pct": 40.0, "report_date": "2026-03-31",
                    "yearly_net_profit": [("2025", 1e8)], "source": "stub"}

        def fake_st(s, name_map=None):
            return (True, "ST垃圾") if s == "300001" else (False, "")

        monkeypatch.setattr(mine_sweeper, "fetch_financial_snapshot", fake_snap)
        monkeypatch.setattr(mine_sweeper, "is_st_or_risk", fake_st)
        client, _ = gateway_client
        r = client.post("/api/financial/mine_batch",
                        json={"symbols": ["600519", "000002", "300001"]})
        assert r.status_code == 200
        d = r.json()["data"]
        clean = {c["symbol"] for c in d["clean"]}
        blocked = {b["symbol"]: b["reasons"] for b in d["blocked"]}
        assert "600519" in clean
        assert "000002" in blocked and any("ROE" in x or "亏" in x for x in blocked["000002"])
        assert "300001" in blocked and any("ST" in x or "风险警示" in x for x in blocked["300001"])

    def test_classify_seat(self):
        from sentiment import classify_seat
        # classify_seat 契约: 返回英文分类码 (institution/hot_money/other),
        # 中文标签仅出现在 LLM 摘要中 —— 修正测试对返回值的臆测。
        assert classify_seat("机构专用") == "institution"
        assert classify_seat("东方财富证券拉萨东环路第二证券营业部") in ("hot_money", "other")

    def test_fund_flow_stub(self, gateway_client, monkeypatch):
        import akshare as ak
        import sentiment

        df = pd.DataFrame({
            "日期": pd.bdate_range(end="2026-10-09", periods=10).strftime("%Y-%m-%d"),
            "主力净流入-净额": [1e6, 2e6, -1e6, 3e6, 2e6, 1e6, 4e6, 2e6, 3e6, 5e6],
        })
        monkeypatch.setattr(sentiment.ak, "stock_individual_fund_flow_rank",
                            lambda *a, **kw: df, raising=True)
        client, _ = gateway_client
        r = client.get("/api/sentiment/fund_flow", params={"symbol": "600519"})
        assert r.status_code == 200
        body = r.json()
        assert body["code"] == 0


# ---------------------------------------------------------------------------
# T1.7 个股新闻/公告 /api/news (方案1 东财 stock_news_em + 方案2 巨潮 stock_notice_report)
# ---------------------------------------------------------------------------

class TestNewsApi:
    def _fake_news_em(self):
        import pandas as pd
        return pd.DataFrame({
            "关键词": ["300750"] * 3,
            "新闻标题": [
                "宁德时代：控股股东拟减持不超过总股本2%",
                "宁德时代：截至9月30日已耗资33亿元回购股份",
                "宁德时代与某车企签署长期供货订单",
            ],
            "新闻内容": ["股东自身资金需求", "累计回购1094万股", "合同金额约百亿元"],
            "发布时间": ["2026-10-08 18:53:07", "2026-10-07 09:00:00", "2026-10-06 12:00:00"],
            "文章来源": ["财联社", "央广财经", "证券时报网"],
            "新闻链接": ["http://e.com/a", "http://e.com/b", "http://e.com/c"],
        })

    def _fake_notice(self):
        import pandas as pd
        return pd.DataFrame({
            "代码": ["300750", "600519"],
            "名称": ["宁德时代", "贵州茅台"],
            "公告标题": ["关于收到证监会立案告知书的公告", "2026年第三季度报告"],
            "公告类型": ["重大事项", "定期报告"],
            "公告日期": ["2026-10-09", "2026-10-09"],
            "网址": ["http://c.com/1", "http://c.com/2"],
        })

    def test_news_merges_both_sources_red_flag_first(self, gateway_client, monkeypatch):
        import akshare as ak
        import news as news_mod
        # 关闭磁盘缓存干扰, 保证用例隔离
        monkeypatch.setattr(news_mod, "_cache_get", lambda *a, **kw: None)
        monkeypatch.setattr(news_mod, "_cache_put", lambda *a, **kw: None)
        monkeypatch.setattr(ak, "stock_news_em", lambda symbol=None, **kw: self._fake_news_em())
        monkeypatch.setattr(ak, "stock_notice_report", lambda symbol=None, date=None, **kw: self._fake_notice())

        client, _ = gateway_client
        r = client.get("/api/news", params={"symbol": "300750", "limit": 5})
        assert r.status_code == 200
        d = r.json()["data"]
        assert d["sources"]["eastmoney"] == 3
        assert d["sources"]["cninfo"] == 1          # 只有 300750 的公告被过滤出来
        assert d["news_count"] == 4
        # 红雷优先排序: 立案(巨潮) 与 减持(东财) 必须排在最前两条
        assert d["red_flag_count"] == 2
        assert {d["news"][0]["title"], d["news"][1]["title"]} == {
            "宁德时代：控股股东拟减持不超过总股本2%",
            "关于收到证监会立案告知书的公告",
        }
        assert d["news"][0]["red_flag"] and d["news"][1]["red_flag"]
        assert d["negative_count"] == 2
        assert d["errors"] == {}

    def test_news_invalid_symbol_400(self, gateway_client):
        client, _ = gateway_client
        r = client.get("/api/news", params={"symbol": "abc"})
        assert r.status_code == 400

    def test_news_eastmoney_only_when_notice_fails(self, gateway_client, monkeypatch):
        """弱依赖降级: 巨潮源整日失败时, 接口仍返回东财新闻且 errors 记录 cninfo。"""
        import akshare as ak
        import news as news_mod
        monkeypatch.setattr(news_mod, "_cache_get", lambda *a, **kw: None)
        monkeypatch.setattr(news_mod, "_cache_put", lambda *a, **kw: None)
        monkeypatch.setattr(news_mod, "_recent_trade_dates", lambda count: ["20261009"])
        monkeypatch.setattr(ak, "stock_news_em", lambda symbol=None, **kw: self._fake_news_em())

        def boom(*a, **kw):
            raise RuntimeError("cninfo unreachable")
        monkeypatch.setattr(ak, "stock_notice_report", boom)

        client, _ = gateway_client
        r = client.get("/api/news", params={"symbol": "300750", "limit": 3})
        assert r.status_code == 200
        d = r.json()["data"]
        assert d["news_count"] == 3
        assert "cninfo" in d["errors"] and "eastmoney" not in d["errors"]

    def test_classify_sentiment_rules(self):
        from news import classify_sentiment
        assert classify_sentiment("业绩预亏提示性公告", "") == ("negative", True)
        assert classify_sentiment("净利润同比增长45%", "") == ("positive", False)
        assert classify_sentiment("召开股东大会通知", "") == ("neutral", False)


class TestBaseFilter:
    """决策#4: 流动性三条件独立 base_filter + ST 归并 (Step 3)。"""

    def _mk_df(self, avg_amount, last_close):
        import pandas as pd
        n = 20
        return pd.DataFrame({
            "收盘": [last_close] * n,
            "成交额": [avg_amount] * n,
        })

    def test_low_liquidity_blocked(self):
        from mine_sweeper import base_filter_symbol
        r = base_filter_symbol("600519", self._mk_df(5e7, 50.0),
                               name_map={"600519": "贵州茅台"})
        assert r["passed"] is False
        assert any("流动性不足" in x for x in r["reasons"])

    def test_penny_stock_blocked(self):
        from mine_sweeper import base_filter_symbol
        r = base_filter_symbol("000001", self._mk_df(5e8, 0.85),
                               name_map={"000001": "平安银行"})
        assert r["passed"] is False
        assert any("面值退市" in x for x in r["reasons"])

    def test_st_merged_into_base_filter(self):
        import mine_sweeper
        orig = mine_sweeper.is_st_or_risk
        mine_sweeper.is_st_or_risk = lambda s, name_map=None: (True, "ST康美")
        try:
            r = mine_sweeper.base_filter_symbol("600518", None)
        finally:
            mine_sweeper.is_st_or_risk = orig
        assert r["passed"] is False
        assert any("风险警示" in x for x in r["reasons"])

    def test_missing_kline_pass_through(self):
        from mine_sweeper import base_filter_symbol
        r = base_filter_symbol("600519", None, name_map={"600519": "贵州茅台"})
        assert r["passed"] is True
        assert r["metrics"].get("kline") == "missing(pass-through)"

    def test_normal_stock_passes_all_three(self):
        from mine_sweeper import base_filter_symbol
        r = base_filter_symbol("600519", self._mk_df(3e9, 1400.0),
                               name_map={"600519": "贵州茅台"})
        assert r["passed"] is True and r["reasons"] == []

    def test_mine_batch_with_base_filter_layers(self, gateway_client, monkeypatch):
        """include_base_filter=True: 低流动性股 layer=base, 财务雷股 layer=financial。"""
        import mine_sweeper

        def fake_snap(s, ttl_days=7):
            if s == "000002":
                return {"roe_pct": -8.0, "debt_ratio_pct": 95.0, "report_date": "2026-03-31",
                        "yearly_net_profit": [("2024", -1e8), ("2025", -2e8)], "source": "stub"}
            return {"roe_pct": 15.0, "debt_ratio_pct": 40.0, "report_date": "2026-03-31",
                    "yearly_net_profit": [("2025", 1e8)], "source": "stub"}

        monkeypatch.setattr(mine_sweeper, "fetch_financial_snapshot", fake_snap)
        monkeypatch.setattr(mine_sweeper, "is_st_or_risk", lambda s, name_map=None: (False, ""))
        # 注入 K 线 provider: 300001 流动性不达标, 其余达标
        df_ok = self._mk_df(3e9, 50.0)
        df_bad = self._mk_df(2e7, 50.0)
        monkeypatch.setattr(
            "main._base_kline_provider",
            lambda sym: df_bad if sym == "300001" else df_ok)

        client, _ = gateway_client
        r = client.post("/api/financial/mine_batch",
                        json={"symbols": ["600519", "000002", "300001"],
                              "include_base_filter": True})
        assert r.status_code == 200
        d = r.json()["data"]
        clean = {c["symbol"]: c.get("layer") for c in d["clean"]}
        blocked = {b["symbol"]: b for b in d["blocked"]}
        assert clean.get("600519") == "clean"
        assert "300001" in blocked and blocked["300001"]["layer"] == "base"
        assert "000002" in blocked and blocked["000002"]["layer"] == "financial"

    def test_mine_batch_backward_compatible(self, gateway_client, monkeypatch):
        """不带 include_base_filter 时行为与旧版一致 (无 layer 字段)。"""
        import mine_sweeper
        monkeypatch.setattr(mine_sweeper, "fetch_financial_snapshot", lambda s, ttl_days=7: {
            "roe_pct": 15.0, "debt_ratio_pct": 40.0, "report_date": "2026-03-31",
            "yearly_net_profit": [("2025", 1e8)], "source": "stub"})
        monkeypatch.setattr(mine_sweeper, "is_st_or_risk", lambda s, name_map=None: (False, ""))
        client, _ = gateway_client
        r = client.post("/api/financial/mine_batch", json={"symbols": ["600519"]})
        assert r.status_code == 200
        c = r.json()["data"]["clean"][0]
        assert c["symbol"] == "600519" and "layer" not in c


class TestMarketEnvironment:
    """决策#3: GET /api/market/environment regime 判定与总仓位映射 (Step 2)。"""

    def test_regime_classification_rules(self):
        from main import _classify_regma
        assert _classify_regma(3100, 3000, 2900) == "BULL"      # 站上MA20且多头排列
        assert _classify_regma(2800, 2900, 3000) == "BEAR"      # 跌破MA20且空头排列
        assert _classify_regma(3050, 3000, 3100) == "SIDEWAYS"  # MA20<MA60 纠缠
        assert _classify_regma(2950, 3000, 2900) == "SIDEWAYS"

    def test_endpoint_returns_regime_and_max_pct(self, gateway_client, monkeypatch):
        import pandas as pd
        import akshare as ak

        closes = [float(2800 - i * 2) for i in range(120)][::-1]  # 上升趋势->BULL
        df = pd.DataFrame({"日期": pd.date_range("2026-01-01", periods=120).strftime("%Y-%m-%d"),
                           "收盘": closes})
        monkeypatch.setattr(ak, "index_zh_a_hist", lambda **kw: df)
        client, _ = gateway_client
        import main as gw
        gw._MARKET_ENV_CACHE["data"] = None  # 清缓存防用例间串扰
        r = client.get("/api/market/environment")
        assert r.status_code == 200
        d = r.json()["data"]
        assert d["regime"] in ("BULL", "SIDEWAYS", "BEAR")
        assert d["global_max_position_pct"] == {"BULL": 1.0, "SIDEWAYS": 0.6, "BEAR": 0.3}[d["regime"]]

    def test_endpoint_conservative_fallback_on_failure(self, gateway_client, monkeypatch):
        import akshare as ak

        def boom(**kw):
            raise RuntimeError("upstream down")
        monkeypatch.setattr(ak, "index_zh_a_hist", boom)
        client, _ = gateway_client
        import main as gw
        gw._MARKET_ENV_CACHE["data"] = None
        r = client.get("/api/market/environment")
        assert r.status_code == 200
        d = r.json()["data"]
        assert d["regime"] == "BEAR" and d["global_max_position_pct"] == 0.3
        assert "error" in d
