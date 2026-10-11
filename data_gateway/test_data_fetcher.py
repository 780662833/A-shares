"""防弹衣数据获取层单元测试: 本地缓存优先 + BaoStock(主)/AKShare(备)降级 + stale兜底。

全部离线运行 (依赖注入桩件), 覆盖架构师 Step 3 的三条验证路径:
    1. 首请求走主源并落库; 2. 二次请求 Cache hit 零网络; 3. 主源挂 -> 降级备源;
    4. 双源全挂 -> 返回旧缓存 is_stale_data=True。
"""

from __future__ import annotations

import pandas as pd
import pytest

from data_fetcher import LocalKlineCache, _bs_daily_df, get_daily_kline_safe


# ---------------------------------------------------------------------------
# 桩件与夹具
# ---------------------------------------------------------------------------

def _mk_df(days: int = 5, end: str = "2026-10-09", price: float = 10.0) -> pd.DataFrame:
    dates = pd.bdate_range(end=end, periods=days)
    return pd.DataFrame({
        "日期": [d.strftime("%Y-%m-%d") for d in dates],
        "开盘": [price] * days,
        "收盘": [price + 0.1] * days,
        "最高": [price + 0.5] * days,
        "最低": [price - 0.5] * days,
        "成交量": [10000.0] * days,
        "成交额": [price * 10000] * days,
    })


class FakeBSRes:
    """模拟 baostock result 对象 (error_code/next/get_row_data)。"""

    def __init__(self, rows, error_code="0", error_msg="success"):
        self._rows, self._i = rows, -1
        self.error_code, self.error_msg = error_code, error_msg

    def next(self):
        self._i += 1
        return self._i < len(self._rows)

    def get_row_data(self):
        return self._rows[self._i]


@pytest.fixture()
def cache(tmp_path):
    return LocalKlineCache(db_path=str(tmp_path / "local_cache.db"))


def _past_window():
    """过去日期窗口: 末端 < 今天, 落库即 fresh (历史不可变)。"""
    return "20260101", "20261009"


# ---------------------------------------------------------------------------
# 用例 1: 首请求走主源 BaoStock -> 写入本地缓存
# ---------------------------------------------------------------------------

def test_first_request_uses_primary_source(cache):
    calls = []

    def bs_query(symbol, start, end):
        calls.append("baostock")
        return _mk_df()

    def ak_hist(symbol, period, start, end):
        calls.append("akshare")
        return _mk_df()

    res = get_daily_kline_safe("600519", *_past_window(), cache=cache,
                               bs_query=bs_query, ak_hist=ak_hist)
    assert res["source"] == "baostock"
    assert calls == ["baostock"], "主源成功时绝不应触碰备用源"
    assert not res["df"].empty
    assert res.get("is_stale_data") is False


# ---------------------------------------------------------------------------
# 用例 2: 第二次同区间请求 -> Cache hit, 零网络
# ---------------------------------------------------------------------------

def test_second_request_hits_cache_zero_network(cache):
    calls = []

    def bs_query(symbol, start, end):
        calls.append("baostock")
        return _mk_df()

    kw = dict(cache=cache, bs_query=bs_query, ak_hist=None)
    r1 = get_daily_kline_safe("600519", *_past_window(), **kw)
    assert r1["source"] == "baostock" and calls == ["baostock"]

    r2 = get_daily_kline_safe("600519", *_past_window(), **kw)
    assert r2["source"] == "cache"
    assert calls == ["baostock"], "缓存命中后不得再发起任何网络请求"
    # 缓存读出的列名与数值必须与原始一致
    assert list(r2["df"].columns) == ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额"]
    assert len(r2["df"]) == len(r1["df"])


# ---------------------------------------------------------------------------
# 用例 3: 主源报错 -> 自动降级 AKShare, 且结果同样落库
# ---------------------------------------------------------------------------

def test_fallback_to_akshare_when_primary_fails(cache):
    calls = []

    def bs_query(symbol, start, end):
        calls.append("baostock")
        raise RuntimeError("baostock login failed")

    def ak_hist(symbol, period, start, end):
        calls.append("akshare")
        return _mk_df(price=20.0)

    res = get_daily_kline_safe("000001", *_past_window(), cache=cache,
                               bs_query=bs_query, ak_hist=ak_hist)
    assert res["source"] == "akshare"
    assert calls == ["baostock", "akshare"], "主源失败应自动切备源"
    assert abs(float(res["df"]["开盘"].iloc[0]) - 20.0) < 1e-9

    # 已落库 -> 第三次请求零网络
    res3 = get_daily_kline_safe("000001", *_past_window(), cache=cache,
                               bs_query=bs_query, ak_hist=ak_hist)
    assert res3["source"] == "cache"
    assert calls == ["baostock", "akshare"]


# ---------------------------------------------------------------------------
# 用例 4: 双源全挂但有旧缓存 -> 返回最近数据并打 is_stale_data
# ---------------------------------------------------------------------------

def test_both_sources_fail_returns_stale_cache(cache):
    # 先只缓存到 2026-10-08 (不含请求末端 10-09)
    df_old = _mk_df(days=4, end="2026-10-08")
    cache.upsert_df("600519", df_old, "baostock")

    def bs_query(symbol, start, end):
        raise RuntimeError("bs down")

    def ak_hist(symbol, period, start, end):
        raise RuntimeError("ak down")

    res = get_daily_kline_safe("600519", *_past_window(), cache=cache,
                               bs_query=bs_query, ak_hist=ak_hist)
    assert res["source"] == "stale_cache"
    assert res["is_stale_data"] is True
    assert not res["df"].empty, "兜底必须给出数据, 下游 czsc 不能崩"
    assert str(res["df"]["日期"].iloc[-1]) == "2026-10-08", "stale 数据末端应为最近已缓存日"


# ---------------------------------------------------------------------------
# 用例 5: 双源全挂且完全无缓存 -> 明确错误, 不抛异常
# ---------------------------------------------------------------------------

def test_all_fail_no_cache_returns_error(cache):
    def bs_query(symbol, start, end):
        raise RuntimeError("bs down")

    res = get_daily_kline_safe("300750", *_past_window(), cache=cache,
                               bs_query=bs_query, ak_hist=None)
    assert res["source"] == "none"
    assert "all_sources_failed_and_no_cache" in res["error"]
    assert res["df"].empty


# ---------------------------------------------------------------------------
# 用例 6: allow_live=False (cache_only) 不回源
# ---------------------------------------------------------------------------

def test_cache_only_mode_never_touches_network(cache):
    calls = []

    def bs_query(symbol, start, end):
        calls.append("baostock")
        return _mk_df()

    # 无缓存 + cache_only -> miss, 零调用
    res = get_daily_kline_safe("600000", *_past_window(), cache=cache,
                               bs_query=bs_query, allow_live=False)
    assert res["source"] == "miss" and calls == []

    # 有缓存(未到期判定按过去窗口=fresh) -> cache, 仍零调用
    get_daily_kline_safe("600000", *_past_window(), cache=cache, bs_query=bs_query)
    calls.clear()
    res2 = get_daily_kline_safe("600000", *_past_window(), cache=cache,
                                bs_query=bs_query, allow_live=False)
    assert res2["source"] == "cache" and calls == []


# ---------------------------------------------------------------------------
# 用例 7: BaoStock 适配器列名归一 + 单位折算 (股->手) + 错误码传播
# ---------------------------------------------------------------------------

def test_bs_adapter_normalizes_columns_and_units():
    rows = [
        ["2026-10-08", "10.0", "10.5", "9.8", "10.2", "123400", "1250000.5"],
        ["2026-10-09", "10.2", "10.8", "10.1", "10.6", "200000", "2100000.0"],
    ]

    class Mod:
        @staticmethod
        def query_history_k_data_plus(code, fields, **kw):
            assert code == "sh.600519" and kw["adjustflag"] == "2"
            return FakeBSRes(rows)

    df = _bs_daily_df(Mod(), "600519", "20260101", "20261009")
    assert list(df.columns) == ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额"]
    assert float(df["成交量"].iloc[0]) == 1234.0, "股应折算为手 (/100)"
    assert float(df["收盘"].iloc[-1]) == 10.6

    class BadMod:
        @staticmethod
        def query_history_k_data_plus(*a, **kw):
            return FakeBSRes([], error_code="10001", error_msg="network error")

    with pytest.raises(RuntimeError, match="baostock 查询失败"):
        _bs_daily_df(BadMod(), "600519", "20260101", "20261009")


# ---------------------------------------------------------------------------
# 用例 8: 当日盘中请求 (末端=今天) 在 15:30 前不算 fresh -> 回源补数
# ---------------------------------------------------------------------------

def test_intraday_request_not_satisfied_by_morning_cache(cache, monkeypatch):
    import datetime as dt
    # 固定"现在"为交易日盘中 10:00, 且缓存里已有"今天早上 09:30 写入的今天数据"
    today = dt.datetime.now()
    if today.weekday() >= 5:  # 周末则顺延到下周一, 保证是工作日
        today = today + dt.timedelta(days=(7 - today.weekday()))
    fake_now = today.replace(hour=10, minute=0, second=0, microsecond=0)
    monkeypatch.setattr("data_fetcher._now", lambda: fake_now)

    df_today = _mk_df(days=3, end=fake_now.strftime("%Y-%m-%d"))
    # 手工写入 update_time=09:30 (早于 15:30 结算线)
    cache.upsert_df("600519", df_today, "baostock")
    con = sqlite3_connect = None
    import sqlite3 as _s3
    con = _s3.connect(cache._db_path)
    con.execute("UPDATE daily_kline_cache SET update_time=? WHERE symbol='600519'",
                (fake_now.replace(hour=9, minute=30).isoformat(timespec="seconds"),))
    con.commit(); con.close()

    calls = []

    def bs_query(symbol, start, end):
        calls.append("baostock")
        return _mk_df(days=3, end=fake_now.strftime("%Y-%m-%d"), price=99.0)

    res = get_daily_kline_safe("600519", "20260101", fake_now.strftime("%Y%m%d"),
                               cache=cache, bs_query=bs_query)
    assert calls == ["baostock"], "盘中(15:30前)今日数据未结算, 应回源刷新"
    assert res["source"] == "baostock"


# ---------------------------------------------------------------------------
# 用例 9: 交易日历感知 — 请求末端落在周末时, 覆盖到上一交易日即 fresh
# (修复真实缺陷: 否则每个周末 screen_pool 会对全池无谓回源打爆限流)
# ---------------------------------------------------------------------------

def test_weekend_end_date_hits_cache_fresh(cache, monkeypatch):
    import datetime as dt
    # "今天"固定为周日, 缓存数据止于周五 -> 应视为最终数据直接命中
    sunday = dt.datetime(2026, 10, 11, 12, 0)
    assert sunday.weekday() == 6
    monkeypatch.setattr("data_fetcher._now", lambda: sunday)

    calls = []

    def bs_query(symbol, start, end):
        calls.append("baostock")
        return _mk_df(days=5, end="2026-10-09")  # 周五收盘数据

    r1 = get_daily_kline_safe("600519", "20260101", "20261011", cache=cache, bs_query=bs_query)
    assert r1["source"] == "baostock" and calls == ["baostock"]

    r2 = get_daily_kline_safe("600519", "20260101", "20261011", cache=cache, bs_query=bs_query)
    assert r2["source"] == "cache", "周末请求应命中周五数据, 不再回源"
    assert calls == ["baostock"], "整个周末流程只允许一次网络请求"


def test_weekend_cache_only_returns_partial_without_full_coverage(cache, monkeypatch):
    """周末但缓存只到周三 -> stale 兜底而非 fresh, 仍能被 cache_only 消费。"""
    import datetime as dt
    sunday = dt.datetime(2026, 10, 11, 12, 0)
    monkeypatch.setattr("data_fetcher._now", lambda: sunday)

    cache.upsert_df("600519", _mk_df(days=3, end="2026-10-07"), "akshare")  # 只到周三
    res = get_daily_kline_safe("600519", "20260101", "20261011", cache=cache,
                               bs_query=None, ak_hist=None, allow_live=False)
    assert res["source"] == "stale_cache"
    assert res["is_stale_data"] is True
