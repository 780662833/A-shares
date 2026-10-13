"""
czsc 缠论分析引擎 (集成 https://github.com/zengbin93/czsc 核心库 v1.x)

职责:
    1. 将 AKShare 返回的中文列名 DataFrame 转换为 czsc 需要的 RawBar 列表;
    2. 运行 CZSC 分析器提取缠论结构 (分型 / 笔 / 中枢);
    3. 基于结构演化显式生成三类买卖点信号 (一买/二买/三买及对应卖点);
    4. 输出 LLM 友好的自然语言 summary (避免让 LLM 直接啃深层 JSON)。

注意 (与旧版教程代码的关键差异, 均已在 czsc 1.0.1 上实测):
    - Freq 枚举成员是 D/W/M/Y, 不存在 Freq.DAY / Freq.FREQ_MAP;
    - CZSC 对象的中枢在属性 zs_list 上, 不存在 get_zs_seq() / is_divergence();
    - Bi 上没有 .low/.high 之外的 fx.price, 分型极值取 fx.high / fx.low;
    - czsc_skills 仓库的信号工厂 (czsc.signals.*) 需配套 events/weights 数据表,
      个人网关按需计算时采用"结构驱动信号"的内置逻辑即可满足买卖点需求。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from czsc import CZSC, Freq, RawBar

logger = logging.getLogger("data_gateway.czsc")

# AKShare period -> czsc Freq 映射 (彻底替代 Tushare 后的唯一数据源约定)
_PERIOD_TO_FREQ: Dict[str, Freq] = {
    "daily": Freq.D,
    "weekly": Freq.W,
    "monthly": Freq.M,
}

# 中文方向值 (czsc Direction 枚举的 value 为 '向上'/'向下', name 为 Up/Down)
_UP = "向上"
_DOWN = "向下"


def _first_present(row: pd.Series, *keys: str) -> Any:
    """按优先级返回第一个存在且非空的列值, 兼容中英文列名。"""
    for k in keys:
        if k in row.index and pd.notna(row[k]):
            return row[k]
    return None


def akshare_to_czsc_bars(df: pd.DataFrame, symbol: str, period: str = "daily") -> List[RawBar]:
    """把 AKShare stock_zh_a_hist 的 DataFrame 转成 czsc.RawBar 列表。

    AKShare 日线列名为中文: 日期/开盘/收盘/最高/最低/成交量/成交额;
    同时兼容英文列名 (date/open/close/high/low/volume/amount)。
    """
    freq = _PERIOD_TO_FREQ.get(period, Freq.D)
    bars: List[RawBar] = []
    for _, row in df.iterrows():
        dt_raw = _first_present(row, "日期", "date", "time", "datetime")
        if dt_raw is None:
            continue
        try:
            dt = pd.to_datetime(str(dt_raw))
            bar = RawBar(
                symbol=symbol,
                id=len(bars),
                freq=freq,
                dt=dt,
                open=float(_first_present(row, "开盘", "open")),
                close=float(_first_present(row, "收盘", "close")),
                high=float(_first_present(row, "最高", "high")),
                low=float(_first_present(row, "最低", "low")),
                vol=float(_first_present(row, "成交量", "volume", "vol") or 0),
                amount=float(_first_present(row, "成交额", "amount") or 0),
            )
        except (TypeError, ValueError) as exc:  # 脏行跳过而不是整体失败
            logger.warning("跳过无法解析的K线行 %s: %s", dt_raw, exc)
            continue
        bars.append(bar)
    return bars


def _bi_dict(bi) -> Dict[str, Any]:
    """序列化一笔 (Bi), 只暴露 LLM 决策需要的字段。"""
    direction_cn = bi.direction.value if hasattr(bi.direction, "value") else str(bi.direction)
    return {
        "direction": "up" if direction_cn == _UP else "down",
        "direction_cn": direction_cn,
        "start_dt": bi.sdt.strftime("%Y-%m-%d"),
        "end_dt": bi.edt.strftime("%Y-%m-%d"),
        "high": round(float(bi.high), 3),
        "low": round(float(bi.low), 3),
        "length": int(bi.length),          # 笔包含的合并K线数量
        "power_price": round(float(bi.power_price), 3),  # 笔的价格力度 (背驰比较用)
    }


def _zs_dict(zs) -> Dict[str, Any]:
    """序列化一个中枢 (ZS)。zg=中枢上沿, zd=中枢下沿, gg/dd=中枢区间极值。"""
    return {
        "zg": round(float(zs.zg), 3),
        "zd": round(float(zs.zd), 3),
        "zz": round(float(zs.zz), 3),
        "start_dt": zs.sdt.strftime("%Y-%m-%d"),
        "end_dt": zs.edt.strftime("%Y-%m-%d"),
        "bi_count": len(zs.bis),
    }


def _risk_fields(bi, zs) -> Dict[str, Any]:
    """Phase A 止损价契约 (决策#1: invalidation 与 stop_loss 并存, 取较高者为实际风控线):

    - invalidation (结构失效线): 最近中枢上沿 ZG —— 价格跌回 ZG 之下意味着
      "向上离开中枢"的判定逻辑死亡 (V2.0 用 30 分钟中枢 ZG, Phase A 沿用日线 ZG)。
    - stop_loss (价格止损线): 回抽低点 × 0.99 —— 以买点确认笔(回抽笔)的低点为基准,
      留 1% 缓冲防插针。
    - effective_stop: max(stop_loss, invalidation), 二者取较高者作为实际风控线。
    """
    low = round(float(bi.low), 3)
    stop_loss = round(low * 0.99, 3)
    invalidation = round(float(zs.zg), 3) if zs is not None else None
    effective = stop_loss
    if invalidation is not None and invalidation > effective:
        effective = invalidation
    return {
        "pullback_low": low,        # 回抽低点 (买点确认笔的极值)
        "stop_loss": stop_loss,     # 价格止损 = 回抽低点 × 0.99
        "invalidation": invalidation,  # 结构失效线 = 最近中枢 ZG (无中枢时 None)
        "effective_stop": effective,   # 实际风控线 = max(stop_loss, invalidation)
    }


def _detect_buy_sell_points(c: CZSC) -> Dict[str, List[Dict[str, Any]]]:
    """结构驱动的三类买卖点识别 (简化但符合缠论定义):

    一买 (1B): 下跌趋势中, 最后一笔向下且相对前一同向笔出现背驰 (力度衰减);
    二买 (2B): 一买确认后回调笔不创新低;
    三买 (3B): 向上离开中枢后, 向下回调笔的低点仍高于中枢上沿 ZG;
    对称地给出 1S/2S/3S 卖点。

    Phase A 扩展字段: 每个买卖点附带 pullback_low / stop_loss / invalidation /
    effective_stop (见 _risk_fields), 供 Go 端仓位计算与持仓止损监控使用。
    """
    buys: List[Dict[str, Any]] = []
    sells: List[Dict[str, Any]] = []
    bis = c.bi_list
    zss = c.zs_list
    if len(bis) < 3:
        return {"buy_points": buys, "sell_points": sells}

    last, prev, prev2 = bis[-1], bis[-2], bis[-3]
    last_dir = last.direction.value if hasattr(last.direction, "value") else str(last.direction)

    # --- 三买 / 三卖: 回调(反弹)笔不重新进入最近中枢 ---
    if zss:
        zs = zss[-1]
        # 只有当该笔发生在中枢结束之后才有意义
        if last_dir == _DOWN and last.low > zs.zg:
            buys.append({
                "type": "3B",
                "name_cn": "第三类买点",
                "price": round(float(last.low), 3),
                "dt": last.edt.strftime("%Y-%m-%d"),
                "reason": f"向下回调笔低点 {last.low:.2f} 高于最近中枢上沿 ZG={zs.zg:.2f}, 上涨动能确认",
                **_risk_fields(last, zs),
            })
        if last_dir == _UP and last.high < zs.zd:
            sells.append({
                "type": "3S",
                "name_cn": "第三类卖点",
                "price": round(float(last.high), 3),
                "dt": last.edt.strftime("%Y-%m-%d"),
                "reason": f"向上反弹笔高点 {last.high:.2f} 低于最近中枢下沿 ZD={zs.zd:.2f}, 下跌动能确认",
                **_risk_fields(last, zs),
            })

    # --- 一买 / 一卖: 相邻同向笔的力度背驰 (power_price 衰减近似 MACD 面积背驰) ---
    if prev2 is not None:
        p_last = float(last.power_price)
        p_prev2 = float(prev2.power_price)
        if last_dir == _DOWN and prev2.direction.value == _DOWN and p_last < p_prev2 * 0.8 \
                and last.low <= prev2.low:
            buys.append({
                "type": "1B",
                "name_cn": "第一类买点(疑似底背驰)",
                "price": round(float(last.low), 3),
                "dt": last.edt.strftime("%Y-%m-%d"),
                "reason": f"下跌笔力度 {p_last:.2f} 不足前一下跌笔 {p_prev2:.2f} 的80%, 且创出新低, 存在底背驰",
                **_risk_fields(last, zss[-1] if zss else None),
            })
        if last_dir == _UP and prev2.direction.value == _UP and p_last < p_prev2 * 0.8 \
                and last.high >= prev2.high:
            sells.append({
                "type": "1S",
                "name_cn": "第一类卖点(疑似顶背驰)",
                "price": round(float(last.high), 3),
                "dt": last.edt.strftime("%Y-%m-%d"),
                "reason": f"上涨笔力度 {p_last:.2f} 不足前一上涨笔 {p_prev2:.2f} 的80%, 且创出新高, 存在顶背驰",
                **_risk_fields(last, zss[-1] if zss else None),
            })

    # --- 二买 / 二卖: 一买/一卖确认后, 首次回调(反弹)笔不创新低(高) ---
    prev_dir = prev.direction.value if hasattr(prev.direction, "value") else str(prev.direction)
    if any(b["type"] == "1B" for b in buys) and prev_dir == _UP and last_dir == _DOWN \
            and last.low > prev2.low:
        buys.append({
            "type": "2B",
            "name_cn": "第二类买点",
            "price": round(float(last.low), 3),
            "dt": last.edt.strftime("%Y-%m-%d"),
            "reason": "一买后的首次回调未创新低, 次级别确认支撑",
            **_risk_fields(last, zss[-1] if zss else None),
        })
    if any(s["type"] == "1S" for s in sells) and prev_dir == _DOWN and last_dir == _UP \
            and last.high < prev2.high:
        sells.append({
            "type": "2S",
            "name_cn": "第二类卖点",
            "price": round(float(last.high), 3),
            "dt": last.edt.strftime("%Y-%m-%d"),
            "reason": "一卖后的首次反弹未创新高, 次级别确认压力",
            **_risk_fields(last, zss[-1] if zss else None),
        })

    return {"buy_points": buys, "sell_points": sells}


def _build_summary(symbol: str, structures: Dict[str, Any], signals: Dict[str, Any]) -> str:
    """把结构化结果压成一句 LLM 易读的自然语言 (专家建议第 3 条)。"""
    parts = [f"{symbol}: 共识别 {structures['bi_count']} 笔、{structures['zs_count']} 个中枢"]
    lb = structures.get("last_bi")
    if lb:
        parts.append(f"当前处于{'向上' if lb['direction'] == 'up' else '向下'}笔 (力度 {lb['power_price']})")
    if structures.get("last_zs"):
        z = structures["last_zs"]
        parts.append(f"最近中枢区间 [{z['zd']}, {z['zg']}]")
    bp = signals.get("buy_points") or []
    sp = signals.get("sell_points") or []
    if bp:
        parts.append("出现买点信号: " + ", ".join(x["name_cn"] for x in bp))
    if sp:
        parts.append("出现卖点信号: " + ", ".join(x["name_cn"] for x in sp))
    if not bp and not sp:
        parts.append("当前无标准三类买卖点信号, 处于结构延伸阶段")
    return "。".join(parts) + "。"


def analyze_czsc(bars: List[RawBar], max_recent_bars: int = 500) -> Dict[str, Any]:
    """执行 czsc 核心分析: 结构提取 + 买卖点信号生成 + 自然语言摘要。

    专家建议第 2 条: 只喂最近 max_recent_bars 根 K 线 (日线 500 根足够定位
    当前中枢与买卖点), 避免长序列拖慢响应。
    """
    if not bars:
        return {"error": "No bars provided"}

    if len(bars) > max_recent_bars:
        # czsc 要求 RawBar.id 连续, 且 native 实现的 id 只读, 故截断后重建对象
        bars = [
            RawBar(symbol=b.symbol, id=i, freq=b.freq, dt=b.dt, open=b.open,
                   close=b.close, high=b.high, low=b.low, vol=b.vol, amount=b.amount)
            for i, b in enumerate(bars[-max_recent_bars:])
        ]

    c = CZSC(bars)
    bi_list, zs_list = c.bi_list, c.zs_list
    if not bi_list:
        return {
            "symbol": bars[0].symbol,
            "bars_count": len(bars),
            "structures": {"bi_count": 0, "zs_count": 0, "last_bi": None, "last_zs": None},
            "signals": {"buy_points": [], "sell_points": []},
            "summary": "K线数量不足以形成有效笔, 无法进行缠论分析。",
        }

    last_bi, last_zs = bi_list[-1], (zs_list[-1] if zs_list else None)
    structures = {
        "bi_count": len(bi_list),
        "fx_count": len(c.fx_list),
        "zs_count": len(zs_list),
        "last_bi": _bi_dict(last_bi),
        "recent_bis": [_bi_dict(b) for b in bi_list[-6:]],
        "last_zs": _zs_dict(last_zs) if last_zs else None,
        "ubi_fenxing": str(c.ubi_fxs[-1].mark) if c.ubi_fxs else None,  # 未完成笔的最新分型 (顶/底分型)
    }
    signals = _detect_buy_sell_points(c)
    result: Dict[str, Any] = {
        "symbol": bars[0].symbol,
        "freq": c.freq.value if hasattr(c.freq, "value") else str(c.freq),
        "bars_count": len(bars),
        "period_range": f"{bars[0].dt:%Y-%m-%d} ~ {bars[-1].dt:%Y-%m-%d}",
        "structures": structures,
        "signals": signals,
    }
    result["summary"] = _build_summary(result["symbol"], structures, signals)
    return result
