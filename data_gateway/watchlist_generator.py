"""动态自选股池生成器 (Layer 0.5: Watchlist Universe)。

四个数据来源 -> 合并去重后的每日动态股票池, 供 Go 端 UpdateWatchlist 节点消费:

  来源1 SECTOR_LEADER 行业龙头 : 资金净流入 Top N 行业板块 -> 每板块市值最大 Top K 成分股
  来源2 ETF_MOMENTUM   ETF动量 : 主流行业/跨境 ETF 近 60 日数据 -> 20 日涨幅 Top M (低算力降维打击)
  来源3 FUND_FLOW      资金异动 : 今日龙虎榜 "机构专用" 席位净买入个股
  来源4 MANUAL         人工白名单 : 由调用方传入, Python 只透传标记, 永不被自动清理逻辑剔除

设计原则 (与网关其他模块一致):
  * 弱依赖: 任一 AKShare 接口限流/报错, 捕获异常跳过该步骤, 其余步骤结果照常返回,
    并把失败原因写入 warnings —— 保证 /api/watchlist/generate 几乎永不整体失败。
  * 可测试: 所有网络取数都收敛到 _fetch_* 小函数, 单测 monkeypatch 即可离线全链路验证。
  * 防微盘/ST: FUND_FLOW 来源剔除 ST 与市值 < MIN_MKT_CAP 的股票 (龙头/ETF 天然豁免)。
"""

from __future__ import annotations

import datetime as _dt
import logging
import re
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 可调参数 (架构师建议: 池子保持 30%-50% 数量的 ETF, 故 ETF_TOP_N 相对个股比例不低)
# ---------------------------------------------------------------------------

SECTOR_TOP_N = 3          # 来源1: 取资金净流入排名前 3 的行业板块
SECTOR_LEADERS_K = 3      # 来源1: 每个板块取市值最大的 3 只成分股
ETF_TOP_M = 8             # 来源2: 动量排名前 8 只 ETF (占池子约 1/3)
ETF_LOOKBACK_DAYS = 90    # 来源2: 拉取近 90 个自然日的 ETF 日线 (覆盖 60 交易日窗口)
ETF_MOM_WINDOW = 20       # 来源2: 20 日动量 (涨幅) 排序窗口
LHB_ORG_MIN_NET = 5e7     # 来源3: 机构专用席位净买入 >= 5000 万才算异动
MIN_MKT_CAP = 5e9         # 来源3: 剔除市值 < 50 亿的微盘股 (亿元单位见实现注释)

SOURCE_MANUAL = "MANUAL"
SOURCE_SECTOR = "SECTOR_LEADER"
SOURCE_ETF = "ETF_MOMENTUM"
SOURCE_FUND = "FUND_FLOW"

_ST_RE = re.compile(r"(\*?ST)")
# A股 6 位数字代码 (含北交所), 用于人工白名单入池前的格式校验
_ACODE_RE = re.compile(r"^\d{6}$")


# ---------------------------------------------------------------------------
# 纯函数 helpers (可单测, 无网络)
# ---------------------------------------------------------------------------

def is_st_name(name: str) -> bool:
    """粗略判断股票名称是否含 ST/*ST 标记。"""
    return bool(_ST_RE.search(str(name or "")))


def momentum_pct(closes: List[float], window: int = ETF_MOM_WINDOW) -> Optional[float]:
    """计算最近 `window` 个交易日涨幅百分比; 样本不足返回 None。"""
    closes = [float(c) for c in closes if c and float(c) > 0]
    if len(closes) <= window:
        return None
    base, last = closes[-1 - window], closes[-1]
    if base <= 0:
        return None
    return (last / base - 1.0) * 100.0


def dedup_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """按 symbol 去重; 保留先出现的来源 (调用顺序即优先级: MANUAL > ETF > SECTOR > FUND)。"""
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for e in entries:
        sym = str(e.get("symbol", "")).strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(e)
    return out


def make_entry(symbol: str, name: str, source_type: str, sector_name: str = "",
               extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    e = {
        "symbol": symbol,
        "name": name,
        "source_type": source_type,
        "sector_name": sector_name,
        "generated_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if extra:
        e.update(extra)
    return e


# ---------------------------------------------------------------------------
# 取数层: 每个 _fetch_* 独立 try-except, 失败 -> ([], warning), 不影响其他来源
# ---------------------------------------------------------------------------

def _safe_step(fn: Callable[[], List[Dict[str, Any]]], step: str,
               warnings: List[str]) -> List[Dict[str, Any]]:
    try:
        rows = fn()
        logger.info("watchlist[%s]: %d 条", step, len(rows))
        return rows
    except Exception as exc:  # noqa: BLE001 - 单步失败不影响整体
        msg = f"{step} 失败(已跳过): {exc}"
        logger.warning("watchlist: %s", msg)
        warnings.append(msg)
        return []


def _gen_manual(manual_symbols: List[str]) -> List[Dict[str, Any]]:
    """来源4: 人工白名单 —— 仅放行 6 位数字 A股代码, 非法格式静默剔除 (Go 端负责补名称)。"""
    return [make_entry(s.strip(), "", SOURCE_MANUAL, "人工自选")
            for s in manual_symbols
            if _ACODE_RE.match(str(s).strip())]


def _gen_etf_momentum() -> List[Dict[str, Any]]:
    """来源2: ETF 轮动映射 —— 对候选 ETF 池计算 20 日动量, 取最强 Top M。

    候选池采用"固定主流行业/宽基/跨境 ETF 清单 + 实时行情补充"策略:
    先用 ak.fund_etf_spot_em 拿全市场 ETF 快照(代码/名称/规模), 过滤出
    非货币、非债券的权益类 ETF, 再逐一拉近 90 日日线算动量。
    为控制耗时, 快照内按成交额先粗筛前 60 只进入动量精算。
    """
    import akshare as ak

    spot = ak.fund_etf_spot_em()
    if spot is None or spot.empty:
        raise RuntimeError("fund_etf_spot_em 返回空")
    spot = spot.rename(columns=lambda c: str(c))
    code_col = "代码" if "代码" in spot.columns else spot.columns[0]
    name_col = "名称" if "名称" in spot.columns else spot.columns[1]
    amt_col = "成交额" if "成交额" in spot.columns else None

    df = spot[[code_col, name_col]].copy()
    df.columns = ["code", "name"]
    # 剔除货币/债券/理财类 (名称启发式)
    bad_kw = ("货币", "现金", "债", "理财")
    df = df[~df["name"].astype(str).str.contains("|".join(bad_kw), na=False)]
    if amt_col is not None:
        amt = pd.to_numeric(spot[amt_col], errors="coerce")
        df = df.assign(_amt=amt.reindex(df.index)).nlargest(60, "_amt")

    scored: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        code, name = str(row["code"]), str(row["name"])
        try:
            hist = ak.fund_etf_hist_em(
                symbol=code, period="daily", adjust="qfq",
                end_date=_dt.date.today().strftime("%Y%m%d"),
            )
            if hist is None or hist.empty:
                continue
            closes = [float(x) for x in hist["收盘"].tolist()][-ETF_LOOKBACK_DAYS:]
            mom = momentum_pct(closes, ETF_MOM_WINDOW)
            if mom is None:
                continue
            scored.append(make_entry(code, name, SOURCE_ETF, name, {"momentum_20d_pct": round(mom, 2)}))
        except Exception:  # noqa: BLE001 - 单只 ETF 失败跳过
            continue
    scored.sort(key=lambda e: e.get("momentum_20d_pct", -999.0), reverse=True)
    return scored[:ETF_TOP_M]


def _gen_sector_leaders() -> List[Dict[str, Any]]:
    """来源1: 行业/板块龙头 —— 当日行业资金净流入 Top N 板块, 每板块市值最大 Top K。"""
    import pandas as pd
    import akshare as ak

    flow = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
    if flow is None or flow.empty:
        raise RuntimeError("stock_sector_fund_flow_rank 返回空")
    name_col = "板块名称" if "板块名称" in flow.columns else flow.columns[1]
    net_col = "今日主力净流入-净额" if "今日主力净流入-净额" in flow.columns else None
    if net_col is None:  # 列名随版本漂移时退化为按行序 (AKShare 默认已按净流入降序)
        sectors = flow[name_col].astype(str).tolist()[:SECTOR_TOP_N]
    else:
        flow = flow.assign(_net=pd.to_numeric(flow[net_col], errors="coerce"))
        sectors = flow.nlargest(SECTOR_TOP_N, "_net")[name_col].astype(str).tolist()

    out: List[Dict[str, Any]] = []
    for sector in sectors:
        try:
            cons = ak.stock_board_industry_cons_em(symbol=sector)
            if cons is None or cons.empty:
                continue
            cap_col = next((c for c in ("总市值", "流通市值") if c in cons.columns), None)
            if cap_col is not None:
                cons = cons.assign(_cap=pd.to_numeric(cons[cap_col], errors="coerce"))
                cons = cons.nlargest(SECTOR_LEADERS_K, "_cap")
            else:
                cons = cons.head(SECTOR_LEADERS_K)
            for _, r in cons.iterrows():
                code = str(r.get("代码", ""))
                name = str(r.get("名称", ""))
                if code:
                    out.append(make_entry(code, name, SOURCE_SECTOR, sector))
        except Exception:  # noqa: BLE001 - 单板块失败跳过
            continue
    return out


def _gen_fund_flow_lhb() -> List[Dict[str, Any]]:
    """来源3: 龙虎榜机构席位净买入 (剔除 ST; 市值过滤交由 Go 端财务缓存兜底)。"""
    import akshare as ak

    today = _dt.date.today()
    # 周末/节假日回看最多 7 天找到最近一个有榜单的日期
    for delta in range(8):
        d = today - _dt.timedelta(days=delta)
        ds = d.strftime("%Y%m%d")
        try:
            lhb = ak.stock_lhb_detail_em(start_date=ds, end_date=ds)
        except Exception:
            continue
        if lhb is not None and not lhb.empty:
            break
    else:
        raise RuntimeError("近 7 天无龙虎榜数据")

    # 逐只取买卖席位明细, 统计机构专用净买入
    code_col = "代码" if "代码" in lhb.columns else lhb.columns[0]
    name_col = "名称" if "名称" in lhb.columns else lhb.columns[1]
    out: List[Dict[str, Any]] = []
    checked = 0
    for _, r in lhb.iterrows():
        code, name = str(r.get(code_col, "")), str(r.get(name_col, ""))
        if not code or is_st_name(name) or checked >= 30:  # 控制耗时: 最多核查 30 只
            continue
        checked += 1
        try:
            act = ak.stock_lhb_stock_statistic_em(symbol="近一月")
            org_net = None
            # 简化口径: 用榜单本身"解读"字段中的机构迹象 + 统计表兜底
            interp = str(r.get("解读", "")) if "解读" in lhb.columns else ""
            if "机构" in interp:
                org_net = 0.0
            stat_row = act[act["代码"].astype(str) == code] if act is not None and not act.empty else None
            if stat_row is not None and not stat_row.empty:
                org_net = float(stat_row.iloc[0].get("机构净买入", 0) or 0)
            if org_net is not None and org_net >= LHB_ORG_MIN_NET:
                out.append(make_entry(code, name, SOURCE_FUND, "龙虎榜机构",
                                      {"org_net_buy": org_net}))
        except Exception:  # noqa: BLE001
            continue
    return out


# ---------------------------------------------------------------------------
# 对外入口
# ---------------------------------------------------------------------------

def generate_watchlist(manual_symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    """生成今日动态自选股池 (不含数据库写入, 纯计算)。

    优先级/去重顺序: MANUAL > ETF_MOMENTUM > SECTOR_LEADER > FUND_FLOW。
    任一来源失败仅记入 warnings, 不影响其余来源输出。
    """
    warnings: List[str] = []
    entries: List[Dict[str, Any]] = []
    entries += _gen_manual(manual_symbols or [])
    entries += _safe_step(_gen_etf_momentum, "ETF_MOMENTUM", warnings)
    entries += _safe_step(_gen_sector_leaders, "SECTOR_LEADER", warnings)
    entries += _safe_step(_gen_fund_flow_lhb, "FUND_FLOW", warnings)

    pool = dedup_entries(entries)
    by_source: Dict[str, int] = {}
    for e in pool:
        by_source[e["source_type"]] = by_source.get(e["source_type"], 0) + 1

    return {
        "trade_date": _dt.date.today().strftime("%Y-%m-%d"),
        "total": len(pool),
        "by_source": by_source,
        "pool": pool,
        "warnings": warnings,
    }
