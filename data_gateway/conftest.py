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
    # 限速参数归零, 批量用例秒级完成 (令牌桶 + 逐股 sleep 全部关闭)
    monkeypatch.setattr(gw, "_SCREEN_SLEEP_SECONDS", 0.0)
    monkeypatch.setattr(gw, "_GLOBAL_MIN_INTERVAL", 0.0)

    from fastapi.testclient import TestClient
    client = TestClient(gw.app, raise_server_exceptions=False)
    return client, calls
