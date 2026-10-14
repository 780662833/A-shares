"""财务排雷规则引擎 (Layer1 粗筛的"垃圾股过滤器")。

目的:
    在把候选股票池交给 AI (Layer2) 之前, 用纯代码规则直接过滤掉
    ST / 连亏 / 高负债 / 低盈利的垃圾股, 防止 AI 踩雷, 同时节省 LLM Token。

实现 (全部基于 AKShare 免费接口, 彻底告别 Tushare):
    1. ST 检查   : stock_list (AKShare 实时列表已剔除风险警示股) + 名称兜底;
    2. ROE/负债率: ak.stock_financial_analysis_indicator (新浪财务指标,
                   含 净资产收益率(%) / 资产负债率(%), 本模块已在 1.19.x 实测列名);
    3. 连亏检查  : 同接口"净利润"字段为负的最新报告期数量 >= 2 即淘汰;
    4. M-Score   : Beneish 造假概率模型需要 应收账款/存货/营业成本/总资产等
                   多科目明细, AKShare 免费日线级接口无法稳定凑齐 8 因子,
                   MVP 阶段以 "ROE + 负债率 + 连亏 + ST" 四板斧替代,
                   并保留扩展位 (score_fn 参数), 后续可接 Baostock 明细补全。

缓存:
    季频财务数据变化极慢, 按 symbol 落盘 JSON, TTL 默认 7 天,
    screen_pool 批量调用时零额外网络开销。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("data_gateway.mine_sweeper")

try:
    import akshare as ak
    import pandas as pd
    _AK_AVAILABLE = True
except ImportError:  # pragma: no cover
    ak = None      # type: ignore[assignment]
    pd = None      # type: ignore[assignment]
    _AK_AVAILABLE = False


# ---------------------------------------------------------------------------
# 排雷阈值 (可按风格微调; 数值参考 A 股公募常用的保守口径)
# ---------------------------------------------------------------------------

class MineConfig:
    """财务排雷规则阈值集合。"""

    def __init__(
        self,
        min_roe_pct: float = 3.0,          # 最新报告期 ROE 低于该值 → 淘汰
        max_debt_ratio_pct: float = 75.0,  # 资产负债率高于该值 → 淘汰 (金融地产除外见豁免)
        max_loss_periods: int = 1,         # 最近年度净利润亏损期数 > 该值 → 淘汰 (即连亏2期)
        exclude_st: bool = True,           # ST/*ST 直接淘汰
        cache_ttl_days: int = 7,           # 财务缓存有效期
    ) -> None:
        self.min_roe_pct = min_roe_pct
        self.max_debt_ratio_pct = max_debt_ratio_pct
        self.max_loss_periods = max_loss_periods
        self.exclude_st = exclude_st
        self.cache_ttl_days = cache_ttl_days


DEFAULT_CFG = MineConfig()

# 行业性高杠杆豁免关键词 (银行/证券/保险/地产 资产负债率天然 >85%, 不适用通用阈值)
_DEBT_EXEMPT_KEYWORDS = ("银行", "证券", "保险", "信托", "房地产")

# 缓存目录 (与 czsc_cache 同根, 便于统一清理)
_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache", "financial")
_lock = threading.Lock()


def _cache_path(symbol: str) -> str:
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, f"{symbol}.json")


def _cache_get(symbol: str, ttl_days: int) -> Optional[Dict[str, Any]]:
    path = _cache_path(symbol)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        gen = datetime.fromisoformat(obj.get("generated_at", "2000-01-01"))
        if (datetime.now() - gen).days > ttl_days:
            return None
        return obj
    except Exception:  # noqa: BLE001
        return None


def _cache_put(symbol: str, obj: Dict[str, Any]) -> None:
    path = _cache_path(symbol)
    tmp = f"{path}.tmp.{os.getpid()}.{int(time.time()*1000)}"
    try:
        with _lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False)
            os.replace(tmp, path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("financial cache write failed %s: %s", path, exc)


# ---------------------------------------------------------------------------
# 原始数据采集 (带缓存)
# ---------------------------------------------------------------------------

def _pick_col(df_cols: List[str], *keywords: str) -> Optional[str]:
    """在中文列名里按关键词优先级挑一列, 抗 AKShare 列名微调。"""
    for kw in keywords:
        for c in df_cols:
            if kw in c:
                return c
    return None


def fetch_financial_snapshot(symbol: str, ttl_days: int = 7) -> Dict[str, Any]:
    """拉取单只股票的关键财务快照: 名称/最新ROE/负债率/近N年净利润序列。

    数据源: ak.stock_financial_analysis_indicator (新浪, 年/季频)。
    失败返回 {"error": ...}, 调用方决定降级策略 (排雷失败默认放行不误杀)。
    """
    cached = _cache_get(symbol, ttl_days)
    if cached is not None:
        cached["source"] = "cache"
        return cached

    if not _AK_AVAILABLE:
        return {"symbol": symbol, "error": "akshare_unavailable"}

    snap: Dict[str, Any] = {"symbol": symbol, "source": "live"}
    try:
        start_year = str(datetime.now().year - 3)
        df = ak.stock_financial_analysis_indicator(symbol=symbol, start_year=start_year)
        if df is None or df.empty:
            snap["error"] = "no_financial_data"
            return snap
        cols = list(df.columns)
        date_col = _pick_col(cols, "日期") or cols[0]
        roe_col = _pick_col(cols, "净资产收益率(%)", "加权净资产收益率")
        debt_col = _pick_col(cols, "资产负债率")
        profit_col = _pick_col(cols, "净利润")

        # 按报告期升序排列, 取最新一期
        df = df.copy()
        df["_dt"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values("_dt")
        latest = df.iloc[-1]

        def _f(v: Any) -> Optional[float]:
            try:
                x = float(str(v).replace(",", "").strip())
                return x
            except (TypeError, ValueError):
                return None

        snap["report_date"] = str(latest[date_col])[:10]
        snap["roe_pct"] = _f(latest[roe_col]) if roe_col else None
        snap["debt_ratio_pct"] = _f(latest[debt_col]) if debt_col else None

        # 近 3 年年报净利润序列 (用于连亏判断): 只取每年最后一个报告期
        profits: List[Tuple[str, Optional[float]]] = []
        if profit_col:
            yearly = df[df["_dt"].dt.strftime("%m-%d") == "12-31"]
            for _, r in yearly.iterrows():
                profits.append((str(r[date_col])[:10], _f(r[profit_col])))
        snap["yearly_net_profit"] = profits
    except Exception as exc:  # noqa: BLE001 - 上游抖动不炸整个网关
        logger.warning("fetch_financial_snapshot %s failed: %s", symbol, exc)
        snap["error"] = str(exc)[:200]
        return snap

    snap["generated_at"] = datetime.now().isoformat(timespec="seconds")
    _cache_put(symbol, snap)
    return snap


def is_st_or_risk(symbol: str, name_map: Optional[Dict[str, str]] = None) -> Tuple[bool, str]:
    """ST/*ST/退市风险检查。

    优先用调用方传入的名称映射 (screen_pool 已有 stock_list 缓存, 零成本);
    无映射时回退到实时列表查询。AKShare stock_list 本身包含风险警示股名称,
    因此名称含 "ST" 即命中。
    """
    name = ""
    if name_map is not None:
        name = name_map.get(symbol, "")
    if not name:
        try:
            df = ak.stock_zh_a_spot_em()
            row = df[df["代码"] == symbol]
            name = str(row.iloc[0]["名称"]) if not row.empty else ""
        except Exception as exc:  # noqa: BLE001
            logger.warning("st_check %s name lookup failed: %s", symbol, exc)
            return False, ""   # 查不到名称 → 不误杀
    upper = name.upper().replace(" ", "")
    if "ST" in upper:
        return True, name
    return False, name


# ---------------------------------------------------------------------------
# 排雷主入口
# ---------------------------------------------------------------------------

def check_symbol(
    symbol: str,
    name_map: Optional[Dict[str, str]] = None,
    industry: str = "",
    cfg: MineConfig = DEFAULT_CFG,
) -> Dict[str, Any]:
    """对单只股票执行财务排雷, 返回 {passed, reasons[], snapshot} 结构。

    设计原则: **宁可漏杀, 不可误杀** —— 任何数据缺失都不作为淘汰理由,
    只有拿到确凿的负面证据才拦截 (防止上游接口抖动把好股票挡在池外)。
    """
    reasons: List[str] = []

    # 规则 1: ST / 风险警示
    if cfg.exclude_st:
        st, name = is_st_or_risk(symbol, name_map)
        if st:
            reasons.append(f"风险警示股: {name}")

    # 规则 2/3/4: 财务指标
    snap = fetch_financial_snapshot(symbol, cfg.cache_ttl_days)
    if "error" not in snap:
        roe = snap.get("roe_pct")
        debt = snap.get("debt_ratio_pct")
        exempt = any(k in industry for k in _DEBT_EXEMPT_KEYWORDS)

        if roe is not None and roe < cfg.min_roe_pct:
            reasons.append(f"最新报告期({snap.get('report_date')}) ROE={roe:.2f}% < {cfg.min_roe_pct}%")
        if debt is not None and not exempt and debt > cfg.max_debt_ratio_pct:
            reasons.append(f"资产负债率 {debt:.1f}% > {cfg.max_debt_ratio_pct}%")

        losses = [p for (_, p) in (snap.get("yearly_net_profit") or []) if p is not None and p < 0]
        if len(losses) > cfg.max_loss_periods:
            years = [d for (d, p) in snap["yearly_net_profit"] if p is not None and p < 0]
            reasons.append(f"近年净利润连亏 {len(losses)} 期 ({','.join(years)})")

    return {
        "symbol": symbol,
        "passed": len(reasons) == 0,
        "reasons": reasons,
        "snapshot": {k: snap.get(k) for k in ("report_date", "roe_pct", "debt_ratio_pct")} | {
            "source": snap.get("source", "error"),
        },
    }


# ---------------------------------------------------------------------------
# 基础过滤 base_filter (决策#4: 流动性三条件独立成层, ST 检查归并到此避免两处维护)
#   Layer 0 技术粗筛前置闸门, 与财务四规则解耦:
#   L1 日均成交额 >= min_avg_amount_yuan (默认 1 亿, 防滑点/出货难);
#   L2 最新收盘价 >= min_price (默认 1.0 元, 防面值退市风险股);
#   L3 非 ST/风险警示 (原 mine_sweeper 规则1 归并至此, 单一维护点)。
#   数据源复用 /api/daily_kline 同款链路 (_ak_hist_throttled) 与 stock_list 缓存,
#   任何数据缺失一律放行 (宁可漏杀不可误杀)。
# ---------------------------------------------------------------------------

class BaseFilterConfig:
    def __init__(
        self,
        min_avg_amount_yuan: float = 1e8,  # 近 N 日日均成交额下限 (元)
        min_price: float = 1.0,            # 最新收盘价下限 (元)
        avg_window_days: int = 20,         # 成交额均值窗口 (交易日)
        exclude_st: bool = True,           # ST 归并点: base_filter 统一执行
    ) -> None:
        self.min_avg_amount_yuan = min_avg_amount_yuan
        self.min_price = min_price
        self.avg_window_days = avg_window_days
        self.exclude_st = exclude_st


BASE_FILTER_CFG = BaseFilterConfig()


def base_filter_symbol(
    symbol: str,
    kline_df: Any,
    name_map: Optional[Dict[str, str]] = None,
    cfg: BaseFilterConfig = BASE_FILTER_CFG,
) -> Dict[str, Any]:
    """对单只股票执行基础过滤, 返回 {passed, reasons[], metrics}。

    kline_df: AKShare 日线 DataFrame (含 收盘/成交额 列, 可为 None=缺数据放行)。
    """
    reasons: List[str] = []
    metrics: Dict[str, Any] = {}

    # 条件 3 (归并): ST / 风险警示 —— 从 check_symbol 规则1 迁移到 base_filter
    if cfg.exclude_st:
        st, name = is_st_or_risk(symbol, name_map)
        if st:
            reasons.append(f"风险警示股: {name}")

    if kline_df is not None and len(kline_df) > 0:
        try:
            closes = pd.to_numeric(kline_df["收盘"], errors="coerce").dropna()
            amounts = pd.to_numeric(kline_df["成交额"], errors="coerce").dropna()
            if not closes.empty:
                last_close = float(closes.iloc[-1])
                metrics["last_close"] = round(last_close, 3)
                if last_close < cfg.min_price:
                    reasons.append(
                        f"最新收盘价 {last_close:.2f} < {cfg.min_price} 元 (面值退市风险)")
            if not amounts.empty:
                avg_amt = float(amounts.tail(cfg.avg_window_days).mean())
                metrics["avg_amount_yuan_20d"] = round(avg_amt, 0)
                if avg_amt < cfg.min_avg_amount_yuan:
                    reasons.append(
                        f"{cfg.avg_window_days}日均成交额 {avg_amt/1e8:.2f}亿 "
                        f"< {cfg.min_avg_amount_yuan/1e8:.0f}亿 (流动性不足)")
        except Exception as exc:  # noqa: BLE001 - 解析失败不误杀
            logger.warning("base_filter %s parse failed, pass: %s", symbol, exc)
    else:
        metrics["kline"] = "missing(pass-through)"

    return {"symbol": symbol, "passed": len(reasons) == 0,
            "reasons": reasons, "metrics": metrics}


def sweep_symbols(
    symbols: List[str],
    name_map: Optional[Dict[str, str]] = None,
    cfg: MineConfig = DEFAULT_CFG,
    kline_provider: Optional[Any] = None,
    base_cfg: Optional[BaseFilterConfig] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """批量排雷: 返回 {"clean": [...], "blocked": [...]}。单只异常默认放行。

    决策#4 集成点: 若提供 kline_provider(symbol)->日线 DataFrame, 则先跑
    base_filter (流动性三条件+ST), 未过关者直接进入 blocked (layer="base"),
    不再消耗财务接口配额; 通过者再走财务四规则 (layer="financial")。
    kline_provider 为 None 时行为与旧版完全一致 (向后兼容)。
    """
    clean: List[Dict[str, Any]] = []
    blocked: List[Dict[str, Any]] = []
    for s in symbols:
        # ---- Layer 0: 基础过滤 (流动性 + ST 归并点) ----
        if kline_provider is not None:
            try:
                kdf = kline_provider(s)
            except Exception as exc:  # noqa: BLE001 - K线获取失败不误杀
                logger.warning("base_filter %s kline fetch failed, pass: %s", s, exc)
                kdf = None
            try:
                bf = base_filter_symbol(s, kdf, name_map=name_map,
                                        cfg=base_cfg or BASE_FILTER_CFG)
            except Exception as exc:  # noqa: BLE001
                logger.warning("base_filter %s error, pass: %s", s, exc)
                bf = {"passed": True, "reasons": []}
            if not bf.get("passed"):
                blocked.append({"symbol": s, "layer": "base",
                                "reasons": bf.get("reasons", []),
                                "metrics": bf.get("metrics", {})})
                continue
        # ---- Layer 1: 财务四规则 ----
        try:
            r = check_symbol(s, name_map=name_map, cfg=cfg)
        except Exception as exc:  # noqa: BLE001 - 排雷自身失败不误杀
            logger.warning("mine_sweep %s error, pass-through: %s", s, exc)
            r = {"symbol": s, "passed": True, "reasons": [],
                 "snapshot": {"source": "error"}}
        if r["passed"]:
            if kline_provider is not None:
                r["layer"] = "clean"
            clean.append(r)
        else:
            r["layer"] = "financial"
            blocked.append(r)
    return {"clean": clean, "blocked": blocked}
