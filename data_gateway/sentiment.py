"""A股特色数据模块: 龙虎榜席位分类 + 主力资金流向趋势 (优先级2)。

职责:
    1. 龙虎榜 (ak.stock_lhb_stock_detail_date_em / ak.stock_lhb_jgmmtj_em 等):
       对指定股票拉取最近 N 天上榜记录, 按"机构专用 / 游资(知名营业部) / 散户"
       三类席位做买卖净额分类 —— 机构净买入通常代表中线资金认可。
    2. 资金流向 (ak.stock_individual_fund_flow):
       拉取近 10 交易日主力/超大单/大单净流入序列, 计算简单趋势指标:
         - main_net_inflow_5d  : 近5日主力净流入合计 (万元)
         - main_net_inflow_trend: "increasing"|"decreasing"|"flat" (前后5日均值比较)
         - consecutive_days    : 主力连续净流入天数 (情绪强度)

设计约定 (与 czsc_engine/mine_sweeper 一致):
    - AKShare 接口列名可能随版本漂移, 统一用 _pick_col 关键词匹配抗变化;
      本环境无法直连上游时, 以"合成 DataFrame 注入"方式完成单元验证;
    - 所有对外函数返回 JSON 友好 dict, 失败返回 {"error": ...} 不抛异常;
    - 结果磁盘缓存 TTL 默认 1 天 (日频情绪数据当日有效), 供 Eino Tool 高频读。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("data_gateway.sentiment")

try:
    import akshare as ak
    import pandas as pd
    _AK_AVAILABLE = True
except ImportError:  # pragma: no cover
    ak = None      # type: ignore[assignment]
    pd = None      # type: ignore[assignment]
    _AK_AVAILABLE = False


# ---------------------------------------------------------------------------
# 缓存 (与 mine_sweeper 同套路, TTL 短因为情绪数据日异)
# ---------------------------------------------------------------------------

_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache", "sentiment")
_lock = threading.Lock()


def _cache_path(kind: str, symbol: str) -> str:
    d = os.path.join(_CACHE_DIR, kind)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{symbol}.json")


def _cache_get(kind: str, symbol: str, ttl_days: int = 1) -> Optional[Dict[str, Any]]:
    path = _cache_path(kind, symbol)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        gen = datetime.fromisoformat(obj.get("generated_at", "2000-01-01"))
        if (datetime.now() - gen).days >= ttl_days:
            return None
        obj["source"] = "cache"
        return obj
    except Exception:  # noqa: BLE001
        return None


def _cache_put(kind: str, symbol: str, obj: Dict[str, Any]) -> None:
    path = _cache_path(kind, symbol)
    tmp = f"{path}.tmp.{os.getpid()}.{int(time.time()*1000)}"
    obj["generated_at"] = datetime.now().isoformat(timespec="seconds")
    try:
        with _lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False)
            os.replace(tmp, path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("sentiment cache write failed %s: %s", path, exc)


def _pick_col(cols: List[str], *keywords: str) -> Optional[str]:
    for kw in keywords:
        for c in cols:
            if kw in c:
                return c
    return None


def _market_of(symbol: str) -> str:
    """6位代码 -> AKShare 资金流接口的市场后缀 sh/sz/bj。"""
    if symbol.startswith(("6", "9", "5")):
        return "sh"
    if symbol.startswith(("4", "8")):
        return "bj"
    return "sz"


# ---------------------------------------------------------------------------
# 1) 主力资金流向趋势
# ---------------------------------------------------------------------------

def fund_flow_trend(symbol: str, days: int = 10, use_cache: bool = True) -> Dict[str, Any]:
    """近 N 个交易日主力资金净流入趋势分析。

    数据源: ak.stock_individual_fund_flow(stock, market) —— 东财个股资金流,
    列含 日期/主力净流入-净额/主力净流入-净占比 等 (单位: 元)。
    """
    if use_cache:
        hit = _cache_get("fund_flow", symbol)
        if hit is not None and hit.get("days", 0) >= min(days, 10):
            return hit
    if not _AK_AVAILABLE:
        return {"symbol": symbol, "error": "akshare_unavailable"}

    try:
        df = ak.stock_individual_fund_flow(stock=symbol, market=_market_of(symbol))
    except Exception as exc:  # noqa: BLE001
        logger.warning("fund_flow %s fetch failed: %s", symbol, exc)
        return {"symbol": symbol, "error": f"fetch_failed: {str(exc)[:120]}"}
    if df is None or df.empty:
        return {"symbol": symbol, "error": "no_data"}

    cols = list(df.columns)
    date_col = _pick_col(cols, "日期") or cols[0]
    main_col = _pick_col(cols, "主力净流入-净额", "主力净流入")
    super_col = _pick_col(cols, "超大单净流入-净额")
    big_col = _pick_col(cols, "大单净流入-净额")
    if main_col is None:
        return {"symbol": symbol, "error": "column_moved: 主力净流入"}

    df = df.copy()
    df["_main"] = pd.to_numeric(df[main_col], errors="coerce")
    df["_dt"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.sort_values("_dt").tail(days).reset_index(drop=True)

    series = [round(float(v) / 1e4, 1) if pd.notna(v) else None  # 元 -> 万元
              for v in df["_main"]]
    valid = [v for v in series if v is not None]
    if not valid:
        return {"symbol": symbol, "error": "all_nan"}

    last5 = valid[-5:]
    prev5 = valid[-10:-5]
    mean_last = sum(last5) / len(last5)
    trend = "flat"
    if prev5:
        mean_prev = sum(prev5) / len(prev5)
        if mean_last > mean_prev * 1.2 and mean_last > 0:
            trend = "increasing"
        elif mean_last < mean_prev * 0.8 and mean_last < 0:
            trend = "decreasing"

    consec = 0
    for v in reversed(valid):
        if v > 0:
            consec += 1
        else:
            break

    out = {
        "symbol": symbol,
        "days": len(valid),
        "window": f"{str(df['_dt'].iloc[0])[:10]}~{str(df['_dt'].iloc[-1])[:10]}",
        "main_net_inflow_daily_wan": series,
        "main_net_inflow_5d_wan": round(sum(last5), 1),
        "main_net_inflow_nd_wan": round(sum(valid), 1),
        "main_net_inflow_trend": trend,
        "consecutive_inflow_days": consec,
        "super_large_5d_wan": (
            round(pd.to_numeric(df[super_col], errors="coerce").tail(5).sum() / 1e4, 1)
            if super_col else None),
        "source": "live",
    }
    _cache_put("fund_flow", symbol, out)
    return out


# ---------------------------------------------------------------------------
# 2) 龙虎榜席位分类
# ---------------------------------------------------------------------------

# 知名游资营业部关键词表 (MVP 精选, 命中即归类"游资"; 未命中且非机构归为"其他席位")
_HOT_MONEY_KEYWORDS = (
    "东方财富证券拉萨", "华泰证券深圳益田路", "中国银河证券北京阜成路",
    "国泰君安证券上海江苏路", "华鑫证券有限责任公司上海分公司", "东方财富昌都",
    "招商证券交易单元", "中信证券杭州延安路", "国金证券上海互联网",
)


def classify_seat(name: str) -> str:
    """营业部名称三分法: 机构专用 / 游资 / 其他。"""
    n = str(name)
    if "机构专用" in n:
        return "institution"
    if any(k in n for k in _HOT_MONEY_KEYWORDS):
        return "hot_money"
    return "other"


def lhb_analysis(symbol: str, lookback_days: int = 30, use_cache: bool = True) -> Dict[str, Any]:
    """最近上榜记录的席位分类与净额聚合。

    数据源: ak.stock_lhb_stock_statistic_em 系列不稳定, 这里选用覆盖面较好的
    ak.stock_lhb_detail_em(start_date, end_date) 全市场榜单再按代码过滤 ——
    一次请求即可服务多只股票 (调用方应缓存全市场结果, 本函数内部做了单日缓存)。
    """
    if use_cache:
        hit = _cache_get("lhb", symbol)
        if hit is not None:
            return hit
    if not _AK_AVAILABLE:
        return {"symbol": symbol, "error": "akshare_unavailable"}

    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    try:
        df = ak.stock_lhb_detail_em(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("lhb %s fetch failed: %s", symbol, exc)
        return {"symbol": symbol, "error": f"fetch_failed: {str(exc)[:120]}"}
    if df is None or df.empty:
        return {"symbol": symbol, "error": "no_lhb_records"}

    cols = list(df.columns)
    code_col = _pick_col(cols, "代码")
    date_col = _pick_col(cols, "上榜日期", "日期")
    reason_col = _pick_col(cols, "解读", "上榜原因")
    net_col = _pick_col(cols, "龙虎榜净买额", "净买额")
    buy_col = _pick_col(cols, "龙虎榜买入额", "买入额")
    sell_col = _pick_col(cols, "龙虎榜卖出额", "卖出额")
    if code_col is None:
        return {"symbol": symbol, "error": "column_moved: 代码"}

    rows = df[df[code_col].astype(str).str.zfill(6) == symbol]
    if rows.empty:
        out = {"symbol": symbol, "on_list_count": 0,
               "message": f"近{lookback_days}日未上龙虎榜", "source": "live"}
        _cache_put("lhb", symbol, out)
        return out

    records: List[Dict[str, Any]] = []
    inst_net = hot_net = other_net = 0.0
    for _, r in rows.iterrows():
        def _num(colname: Optional[str]) -> float:
            if colname is None:
                return 0.0
            try:
                return float(str(r[colname]).replace(",", "").replace("%", ""))
            except (TypeError, ValueError, KeyError):
                return 0.0
        net = _num(net_col) / 1e8   # 元 -> 亿元
        rec = {
            "date": str(r[date_col])[:10] if date_col else "",
            "reason": str(r[reason_col]) if reason_col else "",
            "net_buy_yi": round(net, 3),
            "buy_yi": round(_num(buy_col) / 1e8, 3),
            "sell_yi": round(_num(sell_col) / 1e8, 3),
        }
        # 席位侧信息在全市场榜单里是聚合字段, 细分席位需逐日明细接口;
        # MVP 用"解读"文本里的机构信号做粗分类, 保留扩展位。
        reason_txt = rec["reason"]
        if "机构" in reason_txt:
            inst_net += net
            rec["seat_class"] = "institution"
        elif any(k in reason_txt for k in _HOT_MONEY_KEYWORDS):
            hot_net += net
            rec["seat_class"] = "hot_money"
        else:
            other_net += net
            rec["seat_class"] = "other"
        records.append(rec)

    dominant = max([("institution", inst_net), ("hot_money", hot_net), ("other", other_net)],
                   key=lambda kv: abs(kv[1]))[0]
    out = {
        "symbol": symbol,
        "lookback_days": lookback_days,
        "on_list_count": len(records),
        "records": records[:10],
        "seat_net_buy_yi": {
            "institution": round(inst_net, 3),
            "hot_money": round(hot_net, 3),
            "other": round(other_net, 3),
        },
        "dominant_seat": dominant,
        "signal": (
            "机构净买入" if dominant == "institution" and inst_net > 0 else
            "游资主导(情绪驱动,注意退潮风险)" if dominant == "hot_money" and hot_net > 0 else
            "中性"
        ),
        "source": "live",
    }
    _cache_put("lhb", symbol, out)
    return out


# ---------------------------------------------------------------------------
# 3) 合并输出 (供 Eino Tool / screen_pool 直接消费的一句话摘要)
# ---------------------------------------------------------------------------

def sentiment_summary(symbol: str, include_lhb: bool = True) -> Dict[str, Any]:
    """资金流 + 龙虎榜合并的 LLM 友好情绪摘要。任一子项失败不影响另一项。"""
    flow = fund_flow_trend(symbol)
    lhb = lhb_analysis(symbol) if include_lhb else {"error": "skipped"}

    parts: List[str] = []
    if "error" not in flow:
        arrow = {"increasing": "增强", "decreasing": "减弱", "flat": "平稳"}[
            flow.get("main_net_inflow_trend", "flat")]
        parts.append(
            f"近5日主力净流入 {flow['main_net_inflow_5d_wan']} 万元, 趋势{arrow}"
            f"(连续流入{flow['consecutive_inflow_days']}日)")
    if include_lhb and "error" not in lhb and lhb.get("on_list_count", 0) > 0:
        parts.append(f"近{lhb['lookback_days']}日上榜{lhb['on_list_count']}次, {lhb['signal']}")

    return {
        "symbol": symbol,
        "fund_flow": flow,
        "lhb": lhb,
        "summary": "。".join(parts) + ("。" if parts else ""),
        "has_data": bool(parts),
    }
