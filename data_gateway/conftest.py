"""pytest 共享夹具: 分形合成行情 + AKShare/Baostock 网络打桩。

沙箱无法访问东财/交易所外网, 所有涉及真实数据源的用例一律通过 monkeypatch
注入本地生成的 OHLCV DataFrame, 保证测试可离线、确定性复现。
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))


# ---------------------------------------------------------------------------
# 合成行情生成器
# ---------------------------------------------------------------------------

def fractal_ohlcv(n: int = 400, seed: int = 7, start_price: float = 10.0) -> pd.DataFrame:
    """生成带多尺度嵌套波动的日线 DataFrame (AKShare stock_zh_a_hist 列名)。

    设计要点: 完全平滑的锯齿会被 czsc 的 K 线包含关系合并后导致 bi_list 为空,
    因此叠加三层不同周期的正弦 (模拟大级别趋势/中级别笔/小级别波动) 与随机
    噪声, 使顶底分型密度接近真实 A 股, 能稳定产出笔与中枢。
    """
    rng = np.random.default_rng(seed)
    t = np.arange(n, dtype=float)
    base = start_price * (
        1.0
        + 0.35 * np.sin(t / 60.0 * 2 * np.pi)          # 大级别趋势 (~120日周期)
        + 0.15 * np.sin(t / 17.0 * 2 * np.pi + 1.1)    # 中级别摆动 (~34日周期)
        + 0.06 * np.sin(t / 5.0 * 2 * np.pi + 0.5)     # 小级别波动
    )
    noise = rng.normal(0, 0.012, n).cumsum() * 0.05 * start_price
    close = np.maximum(base + noise, 0.5)

    spread_hi = np.abs(rng.normal(0.006, 0.004, n))
    spread_lo = np.abs(rng.normal(0.006, 0.004, n))
    open_ = close * (1 + rng.normal(0, 0.004, n))
    high = np.maximum(open_, close) * (1 + spread_hi)
    low = np.minimum(open_, close) * (1 - spread_lo)
    vol = rng.integers(800_000, 5_000_000, n).astype(float)

    # 固定终点而非"今天": 保证 (a) 测试窗口 [20250101, 20261009] 完整覆盖数据,
    # 第二次请求可命中新鲜缓存; (b) 日期序列跨运行确定, 信号重放结果可复现。
    dates = pd.bdate_range(end=datetime(2026, 10, 9), periods=n)  # 工作日序列, 贴近交易日历

    return pd.DataFrame({
        "日期": [d.strftime("%Y-%m-%d") for d in dates],
        "股票代码": ["000001"] * n,
        "开盘": np.round(open_, 2),
        "收盘": np.round(close, 2),
        "最高": np.round(high, 2),
        "最低": np.round(low, 2),
        "成交量": vol,
        "成交额": np.round(close * vol, 2),
    })


@pytest.fixture(scope="session")
def hist_df() -> pd.DataFrame:
    """会话级共享的分形日线数据 (约 400 根 K 线)。"""
    return fractal_ohlcv(400, seed=7)


@pytest.fixture(scope="session")
def hist_df_dense() -> pd.DataFrame:
    """另一组种子, 用于验证多股票批量路径。"""
    return fractal_ohlcv(320, seed=42, start_price=25.0)


@pytest.fixture(scope="session")
def hist_df_long() -> pd.DataFrame:
    """760 根长周期分形数据 (seed=42): 回测终验专用。

    实测该组数据在 window=180/stride=5 重放下产生 7 入场 + 9 出场信号、
    5 笔完整成交 (win_rate 80%), 是 "trade_count > 0" 遗留事项的最小充分样本;
    短序列 (400根) 因买卖点天然稀疏只能出 0-1 笔, 不适合作为成交断言的夹具。
    """
    return fractal_ohlcv(760, seed=42)


# ---------------------------------------------------------------------------
# 网关打桩: 让 main.py 的所有接口在零网络环境下工作
# ---------------------------------------------------------------------------

@pytest.fixture()
def gateway_client(monkeypatch, tmp_path, hist_df):
    """返回 (TestClient, calls) — calls 记录每次 ak.stock_zh_a_hist 的参数。"""
    import akshare as ak
    import main as gw
    from data_fetcher import LocalKlineCache as KlineCache

    # 磁盘缓存指向临时目录, 避免污染仓库且用例间隔离
    cache_dir = str(tmp_path / "czsc_cache")
    monkeypatch.setattr(gw._czsc_cache, "base_dir", cache_dir)
    with gw._czsc_cache._lock:
        gw._czsc_cache._mem.clear()

    calls: list[dict] = []

    def fake_hist(symbol=None, period=None, start_date=None, end_date=None, adjust=None, **kw):
        calls.append({"symbol": symbol, "period": period,
                      "start_date": start_date, "end_date": end_date, "adjust": adjust})
        df = hist_df.copy()
        if start_date:
            s = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
            df = df[df["日期"] >= s]
        if end_date:
            e = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")
            df = df[df["日期"] <= e]
        return df.reset_index(drop=True)

    monkeypatch.setattr(ak, "stock_zh_a_hist", fake_hist)
    # 防弹衣层打桩: BaoStock 主源在沙箱不可用 -> 抛异常验证自动降级到 AKShare;
    # 本地行情缓存指向临时文件, 避免污染仓库且用例间隔离。
    def fake_bs_query(symbol, start_date, end_date):
        raise RuntimeError("baostock unavailable in sandbox")
    monkeypatch.setattr(gw, "_bs_query_daily", fake_bs_query)
    kline_cache = KlineCache(db_path=str(tmp_path / "local_cache.db"))
    monkeypatch.setattr(gw, "_kline_cache", kline_cache)

    # 磁盘缓存 + 行情缓存的"新鲜"判定都依赖 now(); 把整个夹具的运行时刻冻结到
    # 合成数据终点 (2026-10-09 收盘后), 使缓存命中/强制回源的语义在任何真实
    # 运行日期下都确定成立 —— 否则沙箱时钟越过数据终点后, coverage_for_request
    # 永远判 partial, use_cache=false 的"强制回源"会退化成行情缓存命中 (零网络)。
    _frozen_dt = datetime(2026, 10, 9, 16, 0, 0)

    class _MockDatetime:
        """datetime 类替身: now/today 返回固定时刻, 其余属性透传真实 datetime 类。"""

        @classmethod
        def strptime(cls, s, fmt):
            return datetime.strptime(s, fmt)

        @classmethod
        def fromisoformat(cls, s):
            return datetime.fromisoformat(s)

        def __call__(self, *args, **kwargs):
            return datetime(*args, **kwargs)

        def now(self, tz=None):
            return _frozen_dt

        def today(self):
            return _frozen_dt

        def __getattr__(self, name):
            # date.combine / timedelta 等静态成员兜底透传, 避免逐个补桩遗漏
            return getattr(datetime, name)

    mock_dt = _MockDatetime()
    import data_fetcher as dfm

    # 重试等待归零 (沙箱无外网, baostock 桩必然失败 -> 降级路径秒级完成)
    def _no_wait_retry(fn, attempts=3, backoff=0.0):
        last_exc = None
        for _ in range(attempts):
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
        raise last_exc
    monkeypatch.setattr(gw, "_retry_call", _no_wait_retry)
    # 行情缓存新鲜度时钟: data_fetcher 内部统一经 _now() 取当前时间
    monkeypatch.setattr(dfm, "_now", lambda: _frozen_dt)
    # czsc 磁盘缓存新鲜度时钟: main._cache_is_fresh 使用模块级 datetime.now()
    monkeypatch.setattr(gw, "datetime", mock_dt, raising=False)
    # mine_sweeper / czsc_backtest 各自持有独立的 datetime 引用, 同样需要冻结时钟,
    # 否则 _MockDatetime 缺方法或真实时钟越过合成数据终点会导致缓存判定漂移。
    import mine_sweeper as ms_mod
    import czsc_backtest as bt_mod
    monkeypatch.setattr(ms_mod, "datetime", mock_dt, raising=False)
    monkeypatch.setattr(bt_mod, "datetime", mock_dt, raising=False)
    # 限速参数归零, 批量用例秒级完成 (令牌桶 + 逐股 sleep 全部关闭)
    monkeypatch.setattr(gw, "_SCREEN_SLEEP_SECONDS", 0.0)
    monkeypatch.setattr(gw, "_GLOBAL_MIN_INTERVAL", 0.0)

    from fastapi.testclient import TestClient
    client = TestClient(gw.app, raise_server_exceptions=False)
    return client, calls
