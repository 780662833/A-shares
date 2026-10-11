"""czsc 粗筛规则历史回测 (vectorbt) —— 漏斗模型的"基石验证层"。

目的:
    在把候选股票池交给 AI (Layer2) 之前, 先用 vectorbt 对 czsc 的筛选信号
    (一买/二买/三买及对应卖点) 做历史回测, 证明这些条件在历史上确实具有
    正期望收益, 避免"用 AI 算命"。

核心思路 (事件驱动 -> 向量化):
    czsc 的买卖点检测器 (_detect_buy_sell_points) 是对 "当前 CZSC 快照" 的
    纯函数。因此可以沿时间轴滚动重放: 对第 i 根 K 线, 用 bars[:i+1] 构建
    CZSC 并判断信号是否恰好落在最后一笔末端 —— 得到布尔序列 entries/exits,
    再喂给 vbt.Portfolio.from_signals 一次算完全部绩效指标。

A 股现实约束建模:
    - T+1: 当日买入不能当日卖出 (min_hold_bars=1);
    - 交易成本: 双边佣金 + 卖出印花税 (fees 参数近似合并)。

性能:
    滚动重放每根 K 线都要重建 CZSC, O(n^2)。默认 stride=5 (每5根评估一次)、
    window=400 (只用近400根重放), 单只 3 年日线约几秒; 结果按 symbol 磁盘
    缓存 (与方案 C 同套路), /api/czsc/backtest/rules 批量接口只算未命中的。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger("data_gateway.czsc_backtest")

# 可选依赖: 未安装时网关其余接口不受影响, 仅回测接口返回 503
try:
    import numpy as np
    import vectorbt as vbt
    from czsc import CZSC

    _BACKTEST_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None          # type: ignore[assignment]
    vbt = None         # type: ignore[assignment]
    CZSC = None        # type: ignore[assignment]
    _BACKTEST_AVAILABLE = False

from czsc_engine import akshare_to_czsc_bars, _detect_buy_sell_points  # noqa: E402

_UP = "向上"
_DOWN = "向下"

# 粗筛规则集合: 买点作为入场信号, 对称卖点作为离场信号
BUY_TYPES = ("1B", "2B", "3B")
SELL_TYPES = ("1S", "2S", "3S")

# 回测默认参数
_DEFAULT_WINDOW = 400      # 每次重放使用的最大 K 线数 (控制 O(n^2) 成本)
_DEFAULT_STRIDE = 5        # 每隔几根 K 线评估一次信号 (降低重放次数)
_DEFAULT_FEES = 0.0013     # 双边成本近似: 佣金万2.5*2 + 卖出印花税千0.5, 摊到双边
_DEFAULT_INIT_CASH = 100_000.0
_MIN_TRADES_FOR_EDGE = 5   # 样本门槛: 少于该成交笔数不判定"有正期望"


def is_available() -> bool:
    return _BACKTEST_AVAILABLE


# ---------------------------------------------------------------------------
# 信号重放: 把 "结构驱动信号" 展开成逐 K 线的 entry/exit 布尔序列
# ---------------------------------------------------------------------------

def _dir_of(bi) -> str:
    return bi.direction.value if hasattr(bi.direction, "value") else str(bi.direction)


def replay_signals(
    df: pd.DataFrame,
    symbol: str,
    period: str = "daily",
    buy_types: Tuple[str, ...] = BUY_TYPES,
    sell_types: Tuple[str, ...] = SELL_TYPES,
    window: int = _DEFAULT_WINDOW,
    stride: int = _DEFAULT_STRIDE,
) -> Tuple[pd.Series, pd.Series, List[Dict[str, Any]]]:
    """沿时间轴滚动重放 czsc 买卖点, 返回 (entries, exits, 事件明细列表)。

    entries/exits 为与 df 等长的布尔 Series (位置索引)。
    事件只在信号笔的末端 K 线触发一次 (dt 对齐), 天然避免同一笔重复入场。
    """
    if not _BACKTEST_AVAILABLE:
        raise RuntimeError("vectorbt/czsc 未安装, 无法执行回测")

    bars_all = akshare_to_czsc_bars(df, symbol=symbol, period=period)
    n = len(bars_all)
    entries = [False] * n
    exits = [False] * n
    events: List[Dict[str, Any]] = []
    if n < 60:
        return pd.Series(entries), pd.Series(exits), events

    # 日期 -> 位置索引 (信号以笔末端 dt 标记, 需映射回对应 K 线位置)
    pos_of_date = {b.dt.strftime("%Y-%m-%d"): i for i, b in enumerate(bars_all)}

    starts = list(range(min(window, n - 1), n, stride))
    if starts and starts[-1] != n - 1:
        starts.append(n - 1)  # 保证最后一根也被评估

    def _mark(sig_dt: str, flag_list: List[bool], side: str, sig_type: str, price: Any):
        """把信号标记在其真实产生位置 (笔末端), 而非重放窗口末端。

        旧实现要求 sig_dt == 窗口末端日期才计数 —— 但一笔要"完成几根K线后"
        才能被确认, 导致信号几乎永远打不上, 回测退化为空仓。现改为对齐到
        笔末端位置; 同一信号跨多个重放窗口重复出现时天然幂等 (布尔置位)。
        注: 笔末端存在轻微前视 (确认滞后), 样本内近似口径, 与文档声明一致。
        """
        i = pos_of_date.get(sig_dt)
        if i is None or i > end:      # 未来信号或无法定位 -> 丢弃
            return False
        if flag_list[i]:              # 已标记过 (重复确认) -> 幂等跳过
            return False
        flag_list[i] = True
        events.append({"dt": sig_dt, "side": side, "type": sig_type,
                       "price": price, "confirmed_at_end": end})
        return True

    for end in starts:
        seg = bars_all[max(0, end - window + 1): end + 1]
        # czsc 要求 id 连续且从 0 开始, 截断段需重建 RawBar
        seg = [
            type(b)(symbol=b.symbol, id=i, freq=b.freq, dt=b.dt, open=b.open,
                    close=b.close, high=b.high, low=b.low, vol=b.vol, amount=b.amount)
            for i, b in enumerate(seg)
        ]
        try:
            c = CZSC(seg)
        except Exception as exc:  # noqa: BLE001 - 个别窗口结构异常直接跳过
            logger.debug("replay window end=%d failed: %s", end, exc)
            continue
        sigs = _detect_buy_sell_points(c)
        for bp in sigs.get("buy_points", []):
            if bp["type"] in buy_types:
                _mark(bp["dt"], entries, "buy", bp["type"], bp["price"])
        for sp in sigs.get("sell_points", []):
            if sp["type"] in sell_types:
                _mark(sp["dt"], exits, "sell", sp["type"], sp["price"])

    idx = pd.RangeIndex(n)
    return (pd.Series(entries, index=idx),
            pd.Series(exits, index=idx),
            events)


# ---------------------------------------------------------------------------
# vectorbt 组合计算
# ---------------------------------------------------------------------------

def run_backtest(
    df: pd.DataFrame,
    symbol: str,
    period: str = "daily",
    buy_types: Tuple[str, ...] = BUY_TYPES,
    sell_types: Tuple[str, ...] = SELL_TYPES,
    window: int = _DEFAULT_WINDOW,
    stride: int = _DEFAULT_STRIDE,
    fees: float = _DEFAULT_FEES,
    init_cash: float = _DEFAULT_INIT_CASH,
) -> Dict[str, Any]:
    """对单只股票执行 "czsc 信号 -> vectorbt 绩效" 全流程, 输出 JSON 友好 dict。"""
    t0 = time.monotonic()
    close = pd.to_numeric(
        df["收盘"] if "收盘" in df.columns else df["close"], errors="coerce"
    ).astype(float).reset_index(drop=True)
    if close.isna().all():
        return {"symbol": symbol, "error": "no_valid_close"}

    entries, exits, events = replay_signals(
        df, symbol, period, buy_types, sell_types, window, stride
    )
    n_entries, n_exits = int(entries.sum()), int(exits.sum())

    # A股 T+1 建模: 本环境 vectorbt 无 min_hold_bars 参数 (新版才有),
    # 用信号序列平移实现 —— 当日买入信号最早次日才能卖出, 等价于最少持仓 1 根K线。
    import inspect as _inspect
    _supports_mhb = "min_hold_bars" in _inspect.signature(
        vbt.Portfolio.from_signals).parameters
    if _supports_mhb:
        exits_effective = exits
    else:
        exits_effective = exits.shift(1, fill_value=False) & ~entries  # 同日进出冲突以买为准顺延

    kwargs: Dict[str, Any] = dict(init_cash=init_cash, fees=fees, freq="D")
    if _supports_mhb:
        kwargs["min_hold_bars"] = 1
    pf = vbt.Portfolio.from_signals(close, entries, exits_effective, **kwargs)

    trades = pf.trades
    recs = trades.records if trades.count() > 0 else pd.DataFrame(
        columns=["entry_idx", "exit_idx", "pnl", "return", "status"])
    # 兼容 vectorbt 版本差异: status 列可能是字符串枚举 ("closed"/"open"),
    # 也可能是整数编码 (Trade.Status.Closed == 1), 直接 == "closed" 会静默全滤空。
    def _is_closed_row(v: Any) -> bool:
        s = str(v).lower()
        return s in ("closed", "1") or "closed" in s and "open" not in s
    closed = recs[recs["status"].map(_is_closed_row)] if len(recs) else recs
    wins = int((closed["pnl"] > 0).sum()) if len(closed) else 0
    total_ret = float(pf.total_return())
    bench_ret = float(close.iloc[-1] / close.iloc[0] - 1) if close.iloc[0] else 0.0

    trade_rows: List[Dict[str, Any]] = []
    date_col = df["日期"] if "日期" in df.columns else df.iloc[:, 0]
    def _d(i: Any) -> str:
        try:
            return str(date_col.iloc[int(i)])[:10]
        except Exception:  # noqa: BLE001
            return ""
    for _, r in closed.tail(12).iterrows():
        trade_rows.append({
            "entry_date": _d(r["entry_idx"]),
            "exit_date": _d(r["exit_idx"]),
            "ret_pct": round(float(r["return"]) * 100, 2),
            "pnl": round(float(r["pnl"]), 2),
        })

    n_closed = int(len(closed))
    expectancy = float(closed["return"].mean()) if n_closed else 0.0
    positive_edge = bool(n_closed >= _MIN_TRADES_FOR_EDGE and expectancy > 0)

    result = {
        "symbol": symbol,
        "period": period,
        "bars": int(len(close)),
        "date_range": f"{str(date_col.iloc[0])[:10]}~{str(date_col.iloc[-1])[:10]}",
        "rules": {"buy": list(buy_types), "sell": list(sell_types)},
        "signal_stats": {
            "entry_signals": n_entries,
            "exit_signals": n_exits,
            "events": events[-20:],           # 最近 20 个信号事件明细
        },
        "metrics": {
            "total_return_pct": round(total_ret * 100, 2),
            "benchmark_return_pct": round(bench_ret * 100, 2),
            "alpha_vs_hold_pct": round((total_ret - bench_ret) * 100, 2),
            "max_drawdown_pct": round(float(pf.max_drawdown()) * 100, 2),
            "sharpe": round(float(pf.sharpe_ratio()), 3),
            "trade_count": n_closed,
            "win_rate_pct": round(wins / n_closed * 100, 1) if n_closed else None,
            "avg_trade_return_pct": round(expectancy * 100, 2),
            "positive_edge": positive_edge,
        },
        "recent_trades": trade_rows,
        "elapsed_seconds": round(time.monotonic() - t0, 2),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    return result


# ---------------------------------------------------------------------------
# 磁盘缓存 (复用方案 C 思想: 回测结果按 symbol/规则集 落盘, 批量接口只算增量)
# ---------------------------------------------------------------------------

_DEFAULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
_lock = threading.Lock()


def _rule_key(buy_types: Tuple[str, ...], sell_types: Tuple[str, ...]) -> str:
    return f"{'-'.join(sorted(buy_types))}__{'-'.join(sorted(sell_types))}"


def _bt_path(symbol: str, rule_k: str) -> str:
    d = os.path.join(_DEFAULT_DIR, "backtest", rule_k)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{symbol}.json")


def cache_get(symbol: str, buy_types: Tuple[str, ...], sell_types: Tuple[str, ...],
              max_age_days: int = 7) -> Optional[Dict[str, Any]]:
    """读取回测缓存; 超过 max_age_days 视为过期 (行情变了结论可能翻转)。"""
    path = _bt_path(symbol, _rule_key(buy_types, sell_types))
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        gen = datetime.fromisoformat(obj.get("generated_at", "2000-01-01"))
        if (datetime.now() - gen).days > max_age_days:
            return None
        obj["source"] = "cache"
        return obj
    except Exception:  # noqa: BLE001
        return None


def cache_put(symbol: str, buy_types: Tuple[str, ...], sell_types: Tuple[str, ...],
              result: Dict[str, Any]) -> None:
    path = _bt_path(symbol, _rule_key(buy_types, sell_types))
    tmp = f"{path}.tmp.{os.getpid()}.{int(time.time()*1000)}"
    try:
        with _lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False)
            os.replace(tmp, path)
    except Exception as exc:  # noqa: BLE001 - 写失败仅降级
        logger.warning("backtest cache write failed %s: %s", path, exc)


def _extract_side_events(events: List[Dict[str, Any]], side: str,
                         types: Tuple[str, ...]) -> List[Dict[str, Any]]:
    """从重放事件流中取出指定方向/类型的事件, 并标注其后最近的反向信号。

    用于分信号类型统计: 每个买点事件后, 找到时间上紧随其后的第一个卖点
    (可能来自其他类型), 作为近似离场点 —— 不重复持仓, 只度量"该信号触发后
    到下一次反向信号"的单笔期望。
    """
    out: List[Dict[str, Any]] = []
    all_side = [e for e in events if e["side"] == ("sell" if side == "buy" else "buy")]
    for e in events:
        if e["side"] != side or e["type"] not in types:
            continue
        nxt = next((x for x in all_side if x["dt"] > e["dt"]), None)
        item = dict(e)
        item["next_opposite_dt"] = nxt["dt"] if nxt else None
        item["next_opposite_type"] = nxt["type"] if nxt else None
        out.append(item)
    return out


def _event_ret(event: Dict[str, Any], close: pd.Series, date_map: Dict[str, int]) -> Optional[float]:
    """按日期对齐收盘价, 计算事件到下一反向信号的近似单笔收益率。"""
    try:
        i0 = date_map.get(event["dt"])
        d1 = event.get("next_opposite_dt")
        if i0 is None or d1 is None:
            return None
        i1 = date_map.get(d1)
        if i1 is None or i1 <= i0:
            return None
        p0, p1 = float(close.iloc[i0]), float(close.iloc[i1])
        if p0 <= 0:
            return None
        return p1 / p0 - 1.0
    except Exception:  # noqa: BLE001
        return None


def per_signal_stats(
    df: pd.DataFrame,
    symbol: str,
    period: str = "daily",
    buy_types: Tuple[str, ...] = BUY_TYPES,
    sell_types: Tuple[str, ...] = SELL_TYPES,
    window: int = _DEFAULT_WINDOW,
    stride: int = _DEFAULT_STRIDE,
) -> Dict[str, Any]:
    """按信号类型分别统计触发次数与近似期望收益 (回答"三买 vs 一买谁更准")。

    复用同一次滚动重放的事件流, 无额外 CZSC 开销; 每类买点用"事件价 ->
    下一反向信号价"近似单笔收益 (未含成本, 只做相对排序参考)。
    """
    _, _, events = replay_signals(df, symbol, period, buy_types, sell_types, window, stride)
    date_col = df["日期"] if "日期" in df.columns else df.iloc[:, 0]
    close = pd.to_numeric(df["收盘"] if "收盘" in df.columns else df["close"],
                          errors="coerce").reset_index(drop=True)
    date_map = {str(d)[:10]: i for i, d in enumerate(date_col)}

    by_type: Dict[str, Any] = {}
    for t in buy_types:
        evs = _extract_side_events(events, "buy", (t,))
        rets = [r for e in evs if (r := _event_ret(e, close, date_map)) is not None]
        wins = sum(1 for r in rets if r > 0)
        by_type[t] = {
            "signals": len(evs),
            "measured": len(rets),
            "win_rate_pct": round(wins / len(rets) * 100, 1) if rets else None,
            "avg_return_pct": round(sum(rets) / len(rets) * 100, 2) if rets else None,
            "positive_expectancy": bool(len(rets) >= 3 and sum(rets) / len(rets) > 0),
        }
    for t in sell_types:
        evs = _extract_side_events(events, "sell", (t,))
        rets = [r for e in evs if (r := _event_ret(e, close, date_map)) is not None]
        avoid = sum(1 for r in rets if r < 0)   # 卖点后下跌 = 成功规避
        by_type[t] = {
            "signals": len(evs),
            "measured": len(rets),
            "drop_avoided_pct": round(avoid / len(rets) * 100, 1) if rets else None,
            "avg_return_after_pct": round(sum(rets) / len(rets) * 100, 2) if rets else None,
        }
    return {"symbol": symbol, "total_events": len(events), "by_type": by_type}


def summarize_rule_perf(results: List[Dict[str, Any]], rule_name: str) -> Dict[str, Any]:
    """跨股票聚合某一规则集的期望收益 —— 回答"三买到底有没有正期望"。

    判定口径升级为"双重正期望": 平均单笔收益为正 **且** 组合跑赢买入持有
    (alpha>0) 才给出 positive_expectancy, 避免牛市贝塔掩盖信号本身无效。
    样本不足时返回 insufficient_data, 但所有分支均包含 avg_alpha_vs_hold_pct
    字段 (可能为 None), 保证下游 Go/LLM 解析结构稳定。
    """
    valid = [r for r in results if "metrics" in r and r["metrics"]["trade_count"] > 0]
    if not valid:
        return {"rule": rule_name, "symbols_tested": len(results),
                "symbols_with_trades": 0, "verdict": "insufficient_data",
                "avg_expectancy_pct": None, "median_expectancy_pct": None,
                "avg_alpha_vs_hold_pct": None, "stocks_with_positive_edge": 0,
                "edge_ratio_pct": None}
    exps = [r["metrics"]["avg_trade_return_pct"] for r in valid]
    alphas = [r["metrics"]["alpha_vs_hold_pct"] for r in valid]
    edges = sum(1 for r in valid if r["metrics"]["positive_edge"])
    avg_exp = sum(exps) / len(exps)
    avg_alpha = sum(alphas) / len(alphas)
    if avg_exp > 0 and avg_alpha > 0:
        verdict = "positive_expectancy"
    elif avg_exp <= 0 and avg_alpha <= 0:
        verdict = "negative_expectancy"
    else:
        verdict = "mixed"   # 单笔为正但跑输持有(或反之), 需人工复核
    return {
        "rule": rule_name,
        "symbols_tested": len(results),
        "symbols_with_trades": len(valid),
        "avg_expectancy_pct": round(avg_exp, 2),
        "median_expectancy_pct": round(sorted(exps)[len(exps) // 2], 2),
        "avg_alpha_vs_hold_pct": round(avg_alpha, 2),
        "stocks_with_positive_edge": edges,
        "edge_ratio_pct": round(edges / len(valid) * 100, 1),
        "verdict": verdict,
    }
