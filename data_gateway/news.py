"""个股新闻/公告模块 (漏斗模型 Layer2 排雷数据源)。

方案1: 东方财富个股新闻  ak.stock_news_em(symbol="300750")
    覆盖面广、更新快; 列名: 关键词/新闻标题/新闻内容/发布时间/文章来源/新闻链接。
方案2: 巨潮资讯网公告    ak.stock_notice_report(symbol="全部", date=YYYYMMDD)
    官方指定披露平台, 数据 100% 准确; 该接口按"交易日全市场"返回, 无法直接按
    股票代码查询, 因此本模块实现为: 逐日回溯最近 N 个交易日的全量公告表,
    再按代码过滤 —— 单日全量约数千行, 回看窗口默认 5 个交易日, 配合磁盘缓存
    TTL 1 天, 每股每天最多回源一次, 不会造成上游压力。

设计约定 (与 sentiment/mine_sweeper 一致):
    - AKShare 接口列名可能随版本漂移, 统一用 _pick_col 关键词匹配抗变化;
    - 所有对外函数返回 JSON 友好 dict, 单源失败只记 error, 不抛异常;
    - 结果磁盘缓存 .cache/news/<kind>/<symbol>.json, TTL 默认 1 天;
    - 上游调用带指数退避重试 (_retry_call), 应对东财偶发限流/连接重置;
    - 输出条目附带 sentiment / red_flag 粗标签, Go 端 NewsSearchTool 直接消费。
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("data_gateway.news")

try:
    import akshare as ak
    import pandas as pd
    _AK_AVAILABLE = True
except ImportError:  # pragma: no cover
    ak = None      # type: ignore[assignment]
    pd = None      # type: ignore[assignment]
    _AK_AVAILABLE = False

_SYMBOL_RE = re.compile(r"^\d{6}$")

# ---------------------------------------------------------------------------
# 情绪/硬雷关键词 (与 Go 端 internal/tool/news_search.go 的表保持同步)
# ---------------------------------------------------------------------------
RED_FLAG_KEYWORDS = [
    "预亏", "亏损", "减持", "立案", "调查", "处罚", "警示函", "退市",
    "商誉减值", "质押爆仓", "违规", "诉讼", "暂停上市", "实施ST",
]
POSITIVE_KEYWORDS = [
    "增长", "大增", "扭亏", "回购", "中标", "涨停", "获批", "分红", "订单", "突破",
]


def classify_sentiment(title: str, summary: str) -> tuple[str, bool]:
    """返回 (sentiment, red_flag)。负面优先于正面 —— 排雷场景宁严勿松。"""
    text = f"{title}{summary}"
    for k in RED_FLAG_KEYWORDS:
        if k in text:
            return "negative", True
    for k in POSITIVE_KEYWORDS:
        if k in text:
            return "positive", False
    return "neutral", False


# ---------------------------------------------------------------------------
# 磁盘缓存 (TTL 1 天: 新闻/公告日频更新, 当日命中零网络开销)
# ---------------------------------------------------------------------------
_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache", "news")
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
    tmp = f"{path}.tmp.{os.getpid()}.{int(time.time() * 1000)}"
    obj["generated_at"] = datetime.now().isoformat(timespec="seconds")
    try:
        with _lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False)
            os.replace(tmp, path)
    except Exception as exc:  # noqa: BLE001 - 写缓存失败不影响主链路
        logger.warning("news 缓存写入失败 %s/%s: %s", kind, symbol, exc)


# ---------------------------------------------------------------------------
# 通用小工具
# ---------------------------------------------------------------------------

# 确定性异常特征: 重试不可能成功, 必须快速失败。
# 已知案例: akshare<=1.19.x stock_news_em 内部对 pyarrow 字符串列执行
#   .str.replace(r"\u3000", "", regex=True), RE2 引擎不支持 \u 转义,
#   抛 pyarrow.lib.ArrowInvalid("invalid escape sequence: \u")。
# 该错误在函数内部数据清洗阶段触发 (网络请求已成功), 与网络抖动无关,
# 走指数退避重试只会白白多等数秒并放大日志噪音。
_DETERMINISTIC_ERR_HINTS = ("invalid escape sequence", "invalid regular expression")


def _is_deterministic_error(exc: BaseException) -> bool:
    """判断异常是否为"重试必然复现"的确定性错误 (正则/解析类)。"""
    if type(exc).__name__ == "ArrowInvalid":
        return True
    msg = str(exc).lower()
    return any(h in msg for h in _DETERMINISTIC_ERR_HINTS)


def _retry_call(fn: Callable[[], Any], attempts: int = 3, backoff: float = 1.5) -> Any:
    """指数退避重试, 应对东财接口偶发限流 (与 main._retry_call 同语义, 独立实现避免循环导入)。

    确定性错误 (如 ArrowInvalid 正则编译失败) 首次即抛出, 不浪费退避时间。
    """
    last_exc: Optional[Exception] = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if _is_deterministic_error(exc):
                logger.error("news 上游返回确定性错误, 放弃重试: %s: %s", type(exc).__name__, exc)
                raise
            wait = backoff * (2 ** i)
            logger.warning("news 上游调用失败(第 %d/%d 次): %s, %.1fs 后重试", i + 1, attempts, exc, wait)
            time.sleep(wait)
    assert last_exc is not None
    raise last_exc


def _pick_col(df, keyword: str) -> Optional[str]:
    """按关键词模糊匹配列名, 抗 AKShare 版本列名漂移。"""
    for c in df.columns:
        if keyword in str(c):
            return c
    return None


def _clean(value: Any, limit: int = 300) -> str:
    """NaN/None -> 空串, 压缩空白并截断, 保证 LLM 上下文不被长文撑爆。"""
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
    except Exception:  # noqa: BLE001
        pass
    s = re.sub(r"\s+", " ", str(value)).strip()
    return s[:limit]


def _norm_date(value: Any) -> str:
    """任意时间表达 -> 'YYYY-MM-DD' (Go 端 NewsItem.Published 契约格式)。"""
    s = str(value or "").strip()
    m = re.match(r"^(\d{4})[-/.]?(\d{2})[-/.]?(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


# ---------------------------------------------------------------------------
# 方案1: 东方财富个股新闻 ak.stock_news_em
# ---------------------------------------------------------------------------

def _strip_html_tags(s):
    """去掉 <...> 标签 (上游 akshare 只清 <em>, 实测还有 <br> 等残留)。"""
    return s.str.replace(r"<[^><]*>", "", regex=True)


def _patched_stock_news_em(symbol: str):
    """猴补丁通道: 修复 akshare stock_news_em 的 r"\\u3000" 正则 bug 后原样执行。

    旧版 akshare (含 1.19.x) 源码用 str.replace(r"\\u3000", "", regex=True),
    在 pandas>=3 + pyarrow ArrowStringArray (RE2 引擎) 下抛 ArrowInvalid。
    这里把该行替换为字面量全角空格 (regex=False) 后 exec 出修正版函数,
    保持上游 URL/参数/列名/排序完全一致; 失败返回 None (由调用方走直连兜底)。
    """
    import inspect
    import io
    import contextlib

    try:
        src = inspect.getsource(ak.stock_news_em)
        fixed = (src
                 .replace('r"\\u3000"', '"\\u3000"')
                 .replace("r'\\u3000'", "'\\u3000'"))
        # 防御性: 若未来版本改写清洗语句导致找不到目标行, 放弃补丁 (避免静默错替)
        if "\\u3000" not in fixed or fixed == src:
            logger.warning("stock_news_em 源码结构已变化, 跳过猴补丁")
            return None
        ns = dict(vars(ak.news.news_stock))
        ns["__name__"] = "akshare_patch_em_news"
        exec(compile(fixed, "<akshare-patched-stock_news_em>", "exec"), ns)
        fn = ns.get("stock_news_em")
        if fn is None:
            return None
        with contextlib.redirect_stdout(io.StringIO()):  # 屏蔽上游 print 提示
            return fn(symbol=symbol)
    except Exception as exc:  # noqa: BLE001
        logger.warning("stock_news_em 猴补丁通道失败(%s): %s", symbol, exc)
        return None


def _clean_em_fallback(symbol: str):
    """ak.stock_news_em 崩溃时的兜底: 直连东财搜索接口自建 DataFrame。

    复刻上游请求参数, 但清洗逻辑全部用 Python re 在 object 列上完成,
    彻底绕开 pyarrow RE2 不支持 \\u 转义的问题。失败返回 None。
    """
    import requests

    url = "https://search-api-web.eastmoney.com/search/jsonp"
    inner_param = {
        "uid": "",
        "keyword": symbol,
        "type": ["cmsArticleWebOld"],
        "client": "web",
        "clientType": "web",
        "clientVersion": "curr",
        "param": {"cmsArticleWebOld": {
            "searchScope": "default", "sort": "default",
            "pageIndex": 1, "pageSize": 100,
            "preTag": "<em>", "postTag": "</em>",
        }},
    }
    headers = {
        "accept": "*/*",
        "referer": f"https://so.eastmoney.com/news/s?keyword={symbol}",
        "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
    }
    try:
        r = requests.get(url, params={"cb": "callback", "param": json.dumps(inner_param, ensure_ascii=False)},
                         headers=headers, timeout=15)
        text = r.text
        payload = text[text.index("(") + 1:text.rindex(")")]
        arts = json.loads(payload)["result"]["cmsArticleWebOld"]
    except Exception as exc:  # noqa: BLE001
        logger.warning("stock_news_em 兜底直连失败(%s): %s", symbol, exc)
        return None
    if not arts:
        return pd.DataFrame(columns=["关键词", "新闻标题", "新闻内容", "发布时间", "文章来源", "新闻链接"])
    df = pd.DataFrame(arts, dtype="object")
    df["新闻链接"] = "http://finance.eastmoney.com/a/" + df["code"].astype(str) + ".html"
    out = pd.DataFrame({
        "关键词": symbol,
        "新闻标题": df["title"],
        "新闻内容": df["content"],
        "发布时间": df["date"],
        "文章来源": df["mediaName"],
        "新闻链接": df["新闻链接"],
    })
    for col in ("新闻标题", "新闻内容"):
        out[col] = (out[col].fillna("").astype(str)
                    .str.replace("\u3000", "", regex=False)      # 全角空格
                    .str.replace("\r\n", " ", regex=False)
                    .pipe(_strip_html_tags))
    return out


def fetch_eastmoney_news(symbol: str, limit: int = 10) -> Dict[str, Any]:
    """拉取个股新闻 (东方财富)。返回 {items:[...], source:"eastmoney"} 或 {error:...}。"""
    if not _AK_AVAILABLE:
        return {"error": "akshare 未安装"}
    cached = _cache_get("em", symbol)
    if cached is not None:
        return cached
    try:
        df = _retry_call(lambda: ak.stock_news_em(symbol=symbol))
    except Exception as exc:  # noqa: BLE001
        logger.error("stock_news_em(%s) 失败: %s", symbol, exc)
        # 兜底链: 猴补丁(修复上游正则bug后原样执行) -> 直连东财自建DataFrame
        patched = _patched_stock_news_em(symbol)
        if patched is not None and len(patched) > 0:
            df = patched
            logger.info("stock_news_em(%s) 已走猴补丁通道恢复数据", symbol)
        else:
            df = _clean_em_fallback(symbol)
            if df is None:
                return {"error": f"stock_news_em: {type(exc).__name__}: {exc}"}
            logger.info("stock_news_em(%s) 已走直连兜底通道恢复数据", symbol)

    items: List[Dict[str, Any]] = []
    if df is not None and len(df) > 0:
        col_title = _pick_col(df, "标题")
        col_content = _pick_col(df, "内容")
        col_time = _pick_col(df, "时间")
        col_src = _pick_col(df, "来源")
        col_url = _pick_col(df, "链接")
        for _, row in df.iterrows():
            title = _clean(row[col_title], 200) if col_title else ""
            if not title:
                continue
            summary = _clean(row[col_content], 300) if col_content else ""
            sent, flag = classify_sentiment(title, summary)
            items.append({
                "title": title,
                "summary": summary,
                "published": _norm_date(row[col_time]) if col_time else "",
                "source": _clean(row[col_src], 50) if col_src else "东方财富",
                "url": _clean(row[col_url], 300) if col_url else "",
                "sentiment": sent,
                "red_flag": flag,
            })
    result = {"items": items, "source": "eastmoney", "count": len(items)}
    _cache_put("em", symbol, result)
    return result


# ---------------------------------------------------------------------------
# 方案2: 巨潮资讯网公告 ak.stock_notice_report (按交易日全量 -> 本地过滤)
# ---------------------------------------------------------------------------

def _recent_trade_dates(count: int) -> List[str]:
    """从昨天起向前生成工作日 YYYYMMDD 序列 (近似交易日历, 节假日仅多拉一天空表)。"""
    dates: List[str] = []
    d = datetime.now() - timedelta(days=1)
    while len(dates) < count:
        if d.weekday() < 5:
            dates.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return dates


def fetch_cninfo_notices(symbol: str, lookback_days: int = 5) -> Dict[str, Any]:
    """回溯最近 lookback_days 个交易日的巨潮全量公告, 过滤出该股公告。

    弱依赖: 单日拉取失败只记 warning, 已拿到的部分照常返回; 全部失败才返回 error。
    """
    if not _AK_AVAILABLE:
        return {"error": "akshare 未安装"}
    cached = _cache_get("cninfo", symbol)
    if cached is not None:
        return cached

    items: List[Dict[str, Any]] = []
    warnings: List[str] = []
    ok_days = 0
    for day in _recent_trade_dates(max(1, min(lookback_days, 10))):
        try:
            df = _retry_call(lambda: ak.stock_notice_report(symbol="全部", date=day), attempts=2)
            ok_days += 1
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"{day}: {type(exc).__name__}")
            logger.warning("stock_notice_report(%s) 失败: %s", day, exc)
            continue
        if df is None or len(df) == 0:
            continue
        col_code = _pick_col(df, "代码")
        col_title = _pick_col(df, "标题")
        col_type = _pick_col(df, "类型")
        col_date = _pick_col(df, "日期")
        col_url = _pick_col(df, "网址") or _pick_col(df, "链接")
        if not col_code or not col_title:
            warnings.append(f"{day}: 列名漂移, 跳过")
            continue
        sub = df[df[col_code].astype(str).str.zfill(6) == symbol]
        for _, row in sub.iterrows():
            title = _clean(row[col_title], 200)
            ntype = _clean(row[col_type], 60) if col_type else ""
            sent, flag = classify_sentiment(title, ntype)
            items.append({
                "title": title,
                "summary": ntype,
                "published": _norm_date(row[col_date]) if col_date else _norm_date(day),
                "source": "巨潮公告",
                "url": _clean(row[col_url], 300) if col_url else "",
                "sentiment": sent,
                "red_flag": flag,
            })

    if ok_days == 0:
        return {"error": "stock_notice_report 全部交易日拉取失败: " + "; ".join(warnings[:3])}

    # 去重 (同一公告可能被多类型收录) + 按日期倒序
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for it in sorted(items, key=lambda x: x["published"], reverse=True):
        key = (it["title"], it["published"])
        if key not in seen:
            seen.add(key)
            uniq.append(it)

    result = {
        "items": uniq,
        "source": "cninfo",
        "count": len(uniq),
        "scanned_days": ok_days,
        "warnings": warnings,
    }
    _cache_put("cninfo", symbol, result)
    return result


# ---------------------------------------------------------------------------
# 聚合入口: 供 main.py /api/news 路由消费
# ---------------------------------------------------------------------------

def get_stock_news(symbol: str, limit: int = 5, include_notice: bool = True) -> Dict[str, Any]:
    """合并新闻(方案1) + 公告(方案2), 红雷优先排序, 截断到 limit 条。

    排序策略: 命中硬雷关键词的条目永远排在最前 (LLM 注意力有限, 先看到风险),
    其余按发布时间倒序。两个来源各自失败不影响另一个 (弱依赖降级)。
    """
    if not _SYMBOL_RE.match(symbol or ""):
        return {"error": f"非法股票代码: {symbol!r}"}
    limit = max(1, min(int(limit or 5), 20))

    merged: List[Dict[str, Any]] = []
    errors: Dict[str, str] = {}
    src_stats: Dict[str, int] = {}

    em = fetch_eastmoney_news(symbol, limit=limit)
    if "error" in em:
        errors["eastmoney"] = em["error"]
    else:
        merged.extend(em["items"])
        src_stats["eastmoney"] = len(em["items"])

    if include_notice:
        cn = fetch_cninfo_notices(symbol)
        if "error" in cn:
            errors["cninfo"] = cn["error"]
        else:
            merged.extend(cn["items"])
            src_stats["cninfo"] = len(cn["items"])

    # 跨来源去重 (标题前 40 字相同视为同一事件)
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for it in merged:
        key = re.sub(r"\s+", "", it["title"])[:40]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)

    deduped.sort(key=lambda x: (not x["red_flag"], x["published"]), reverse=False)
    news = deduped[:limit]

    neg = sum(1 for x in news if x["sentiment"] == "negative")
    flags = sum(1 for x in news if x["red_flag"])
    return {
        "symbol": symbol,
        "news": news,
        "news_count": len(news),
        "negative_count": neg,
        "red_flag_count": flags,
        "sources": src_stats,
        "errors": errors,
        "summary": (
            f"{symbol} 近期共 {len(news)} 条新闻/公告, "
            f"其中负面 {neg} 条, 命中硬雷关键词 {flags} 条。"
        ),
    }
