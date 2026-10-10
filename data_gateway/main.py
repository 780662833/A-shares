"""
A股日线级别 AI 模拟交易系统 - Python 数据网关服务

功能说明:
    Go 侧 (Eino Agent) 无法直接调用 AKShare/Baostock, 本服务以 RESTful API
    的形式封装行情与财务数据源, 供 Go 端 HTTP Client 消费。

接口列表:
    GET /health                        健康检查
    GET /api/daily_kline               日K线 (前复权, AKShare)
    GET /api/stock_list                A股正常上市股票列表 (AKShare)
    GET /api/financial_indicator       最新一期 ROE / 净利润率 (Baostock)
    GET /api/czsc/analyze             缠论结构+三类买卖点信号 (缓存优先, 未命中回源)
    POST /api/czsc/screen_pool        漏斗模型 Layer1: 批量粗筛候选股票池 (缓存优先)
    POST /api/czsc/refresh            收盘后定时预计算入口 (Go cron 每日 15:10 调用)
    GET  /api/czsc/cache_status       查看磁盘缓存覆盖情况
    DELETE /api/czsc/cache            清空缓存 (调试用)

长期方案 C (缓存化):
    czsc 分析结果按 <period>/<symbol>.json 落盘 (czsc_cache.py)。
    Go 端 cron 每个交易日 15:10 POST /api/czsc/refresh 全量刷新自选股缓存;
    analyze / screen_pool 请求先查缓存, 当日命中则零网络开销毫秒返回;
    未命中且允许回源时走 "旧缓存增量拼接" 或全量拉取, 并写回缓存。

启动方式:
    uvicorn main:app --host 0.0.0.0 --port 8000
"""

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional

import akshare as ak
import baostock as bs
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# czsc 缠论分析引擎 (可选依赖: 未安装 czsc 时网关其余接口仍可用, 仅禁用 /api/czsc/analyze)
try:
    from czsc_engine import akshare_to_czsc_bars, analyze_czsc
    _CZSC_AVAILABLE = True
except ImportError:  # pragma: no cover
    akshare_to_czsc_bars = None  # type: ignore[assignment]
    analyze_czsc = None          # type: ignore[assignment]
    _CZSC_AVAILABLE = False

# czsc 结果磁盘缓存 (长期方案 C)。纯标准库实现, 恒可导入。
from czsc_cache import cache as _czsc_cache, _compact as _date_compact  # noqa: E402

# vectorbt 回测层 (基石验证): 粗筛规则历史正期望检验, 可选依赖
try:
    from czsc_backtest import (
        is_available as _bt_is_available,
        run_backtest as _run_backtest,
        replay_signals as _replay_signals,
        per_signal_stats as _per_signal_stats,
        summarize_rule_perf as _summarize_rule_perf,
        cache_get as _bt_cache_get,
        cache_put as _bt_cache_put,
        BUY_TYPES as _BT_BUY_TYPES,
        SELL_TYPES as _BT_SELL_TYPES,
    )
except ImportError:  # pragma: no cover
    _bt_is_available = lambda: False  # type: ignore[assignment]
    _run_backtest = _replay_signals = _per_signal_stats = _summarize_rule_perf = None  # type: ignore[assignment]
    _bt_cache_get = _bt_cache_put = None  # type: ignore[assignment]
    _BT_BUY_TYPES, _BT_SELL_TYPES = ("1B", "2B", "3B"), ("1S", "2S", "3S")

# 财务排雷规则引擎 (垃圾股过滤器), 可选依赖 (需要 akshare)
try:
    from mine_sweeper import (
        check_symbol as _mine_check,
        sweep_symbols as _mine_sweep,
        MineConfig as _MineConfig,
    )
    _MINE_AVAILABLE = True
except ImportError:  # pragma: no cover
    _mine_check = _mine_sweep = None  # type: ignore[assignment]
    _MineConfig = None  # type: ignore[assignment]
    _MINE_AVAILABLE = False

# A股特色情绪数据: 龙虎榜席位分类 + 主力资金流向 (Eino Tool 消费)
try:
    from sentiment import (
        fund_flow_trend as _fund_flow_trend,
        lhb_analysis as _lhb_analysis,
        sentiment_summary as _sentiment_summary,
    )
    _SENTIMENT_AVAILABLE = True
except ImportError:  # pragma: no cover
    _fund_flow_trend = _lhb_analysis = _sentiment_summary = None  # type: ignore[assignment]
    _SENTIMENT_AVAILABLE = False

# ---------------------------------------------------------------------------
# 日志配置 (生产级: 带时间戳/级别/模块名)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("data_gateway")

# ---------------------------------------------------------------------------
# FastAPI 应用初始化 + CORS 中间件
# ---------------------------------------------------------------------------
app = FastAPI(
    title="A股数据网关",
    description="为 Go/Eino 交易引擎提供 AKShare / Baostock 数据代理服务",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # MVP 阶段放开跨域, 生产环境应收敛白名单
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
_SYMBOL_RE = re.compile(r"^\d{6}$")
_DATE_FMT = "%Y%m%d"


def _validate_symbol(symbol: str) -> str:
    """校验 6 位纯数字股票代码, 非法则抛 400。"""
    symbol = (symbol or "").strip()
    if not _SYMBOL_RE.match(symbol):
        raise HTTPException(
            status_code=400,
            detail=f"非法的股票代码: '{symbol}', 需为 6 位数字, 例如 '000001'",
        )
    return symbol


def _validate_date(date_str: Optional[str], param_name: str) -> Optional[str]:
    """校验日期格式 YYYYMMDD, 为空返回 None。"""
    if date_str is None or date_str.strip() == "":
        return None
    date_str = date_str.strip()
    try:
        datetime.strptime(date_str, _DATE_FMT)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"参数 '{param_name}' 日期格式错误: '{date_str}', 应为 YYYYMMDD",
        )
    return date_str


def _safe_float(value: Any) -> Optional[float]:
    """安全转换为 float, NaN/None/异常统一返回 None (JSON null)。"""
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        result = float(value)
        if pd.isna(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


def _to_baostock_code(symbol: str) -> str:
    """将 6 位代码转为 Baostock 格式: sh.600000 / sz.000001 / bj.830799。"""
    if symbol.startswith(("6", "9")):
        return f"sh.{symbol}"
    if symbol.startswith(("0", "2", "3")):
        return f"sz.{symbol}"
    if symbol.startswith(("4", "8")):
        return f"bj.{symbol}"
    # 兜底按深市处理
    return f"sz.{symbol}"


# ---------------------------------------------------------------------------
# Baostock 会话管理
#   baostock 的 query 接口依赖全局登录会话, 且该库并非线程安全。
#   这里用 "锁 + 惰性登录" 保证每个请求都能拿到有效会话, 失败时自动重登。
# ---------------------------------------------------------------------------
_bs_lock = threading.Lock()
_bs_logged_in = False


def _bs_ensure_login() -> None:
    """确保 Baostock 处于已登录状态 (调用方必须持有 _bs_lock)。"""
    global _bs_logged_in
    if _bs_logged_in:
        return
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: code={lg.error_code}, msg={lg.error_msg}")
    _bs_logged_in = True
    logger.info("baostock 登录成功")


def _bs_logout_quietly() -> None:
    """静默注销 (用于异常恢复), 调用方必须持有 _bs_lock。"""
    global _bs_logged_in
    try:
        bs.logout()
    except Exception:  # noqa: BLE001 - 清理动作, 忽略一切异常
        pass
    _bs_logged_in = False


# ---------------------------------------------------------------------------
# 通用异常处理: 数据源报错 -> HTTP 500, 绝不让服务崩溃
# ---------------------------------------------------------------------------
@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc: Exception):  # noqa: ANN001
    logger.exception("未捕获异常 [%s %s]: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=500,
        content={"code": 500, "message": f"服务内部错误: {exc}", "data": None},
    )


def _gateway_error(status_code: int, source: str, exc: Exception) -> HTTPException:
    """构造统一错误响应。"""
    logger.error("%s 调用失败: %s", source, exc, exc_info=True)
    return HTTPException(
        status_code=status_code,
        detail=f"{source} 数据获取失败: {type(exc).__name__}: {exc}",
    )


def _retry_call(fn: Callable[[], Any], attempts: int = 3, backoff: float = 1.5) -> Any:
    """
    对上游数据源调用做指数退避重试 (应对东财等接口的偶发限流/连接重置)。
    全部尝试失败时抛出最后一次的异常, 由调用方统一转 HTTP 500。
    """
    last_exc: Optional[Exception] = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = backoff * (2 ** i)
            logger.warning("上游调用失败(第 %d/%d 次): %s, %.1fs 后重试", i + 1, attempts, exc, wait)
            time.sleep(wait)
    assert last_exc is not None
    raise last_exc


# ---------------------------------------------------------------------------
# 接口: 健康检查
# ---------------------------------------------------------------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "data_gateway", "time": datetime.now().isoformat()}


# ---------------------------------------------------------------------------
# 接口 1: 日K线 (前复权)
# ---------------------------------------------------------------------------
@app.get("/api/daily_kline")
def get_daily_kline(
    symbol: str = Query(..., description="6位股票代码, 如 000001"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD, 默认近一年"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD, 默认今天"),
) -> Dict[str, Any]:
    """
    获取指定股票的日 K 线 (前复权)。

    返回字段: date, open, close, high, low, volume, amount
    """
    symbol = _validate_symbol(symbol)
    start_date = _validate_date(start_date, "start_date")
    end_date = _validate_date(end_date, "end_date")

    # 默认取最近一年数据
    if not end_date:
        end_date = datetime.now().strftime(_DATE_FMT)
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime(_DATE_FMT)

    try:
        # AKShare 东财接口, adjust="qfq" 即前复权 (带指数退避重试)
        df: pd.DataFrame = _retry_call(
            lambda: ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
        )
    except Exception as exc:  # noqa: BLE001 - 数据源异常统一转 500
        raise _gateway_error(500, "akshare(stock_zh_a_hist)", exc)

    if df is None or df.empty:
        logger.warning("daily_kline: symbol=%s 区间[%s,%s]无数据", symbol, start_date, end_date)
        return {
            "code": 0,
            "message": "success",
            "symbol": symbol,
            "adjust": "qfq",
            "count": 0,
            "data": [],
        }

    records: List[Dict[str, Any]] = []
    try:
        for _, row in df.iterrows():
            records.append(
                {
                    "date": str(row["日期"]),
                    "open": _safe_float(row["开盘"]),
                    "close": _safe_float(row["收盘"]),
                    "high": _safe_float(row["最高"]),
                    "low": _safe_float(row["最低"]),
                    "volume": _safe_float(row["成交量"]),   # 单位: 手
                    "amount": _safe_float(row["成交额"]),   # 单位: 元
                }
            )
    except KeyError as exc:
        # AKShare 上游改版导致列名变化时, 给出明确错误而非静默错值
        raise _gateway_error(500, "akshare(daily_kline 列解析)", exc)

    logger.info("daily_kline: symbol=%s rows=%d range=[%s,%s]", symbol, len(records), start_date, end_date)
    return {
        "code": 0,
        "message": "success",
        "symbol": symbol,
        "adjust": "qfq",
        "count": len(records),
        "data": records,
    }


# ---------------------------------------------------------------------------
# 接口 2: A 股股票列表 (正常上市交易)
# ---------------------------------------------------------------------------
def _fetch_spot_df() -> pd.DataFrame:
    """主数据源: 东财实时行情快照 (含停牌等状态字段)。"""
    return ak.stock_zh_a_spot_em()


def _fetch_code_name_df() -> pd.DataFrame:
    """备用数据源: A股代码名称静态表 (交易所基本资料, 压力更小)。"""
    df = ak.stock_info_a_code_name()
    # 统一列名为 code/name, 与主源解析逻辑对齐
    return df.rename(columns={"code": "代码", "name": "名称"})


@lru_cache(maxsize=1)
def _stock_list_cached() -> List[Dict[str, Any]]:
    """
    股票列表带进程内缓存 (TTL 由时间桶实现, 约 1 小时刷新一次),
    避免每次请求都打爆上游接口。返回解析后的记录列表。
    """
    bucket = int(time.time() // 3600)  # 缓存桶: 同一小时内命中缓存
    logger.debug("stock_list cache bucket=%d", bucket)

    df: Optional[pd.DataFrame] = None
    source_used = "stock_zh_a_spot_em"
    try:
        df = _retry_call(_fetch_spot_df)
    except Exception as exc:  # noqa: BLE001 - 主源失败, 尝试备源
        logger.warning("stock_zh_a_spot_em 获取失败, 回退到 stock_info_a_code_name: %s", exc)
        source_used = "stock_info_a_code_name"

    if df is None:
        df = _retry_call(_fetch_code_name_df)

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        code = str(row["代码"]).strip()
        name = str(row["名称"]).strip()
        if not _SYMBOL_RE.match(code):
            continue
        # 正常上市交易过滤: 剔除停牌、退市整理期 (名称含 ST 仍保留, 由策略层决定)
        status = str(row.get("停牌标志", "") or "")
        if status == "停牌":
            continue
        if "退" in name:  # 例: "退市海润"
            continue
        records.append({"symbol": code, "name": name})

    logger.info("stock_list: source=%s 共 %d 只正常上市股票", source_used, len(records))
    return records


@app.get("/api/stock_list")
def get_stock_list() -> Dict[str, Any]:
    """
    获取 A 股所有正常上市交易的股票列表 (代码 + 名称)。

    主数据源: AKShare 东财实时行情快照 (剔除停牌/退市整理等异常状态);
    主源不可用时自动回退到交易所基本资料接口, 并带 1 小时进程内缓存。
    """
    try:
        records = _stock_list_cached()
    except Exception as exc:  # noqa: BLE001
        raise _gateway_error(500, "akshare(stock_list)", exc)

    if not records:
        raise _gateway_error(
            500, "akshare(stock_list)", RuntimeError("返回空数据, 可能上游接口限流或改版")
        )

    return {
        "code": 0,
        "message": "success",
        "count": len(records),
        "data": records,
    }


# ---------------------------------------------------------------------------
# 接口 3: 财务指标 (最新一期 ROE / 净利润率, Baostock)
# ---------------------------------------------------------------------------
def _query_profit_by_period(bs_code: str, period: str) -> Optional[pd.DataFrame]:
    """查询指定季度的盈利数据, 返回 DataFrame (可能为空)。调用方持有 _bs_lock。"""
    rs = bs.query_profit_data(code=bs_code, year=int(period[:4]), quarter=int(period[4:]))
    if rs.error_code != "0":
        raise RuntimeError(f"baostock query_profit_data 失败: {rs.error_code} {rs.error_msg}")
    rows: List[List[str]] = []
    while rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return None
    return pd.DataFrame(rows, columns=rs.fields)


def _pick_latest_period_with_data(bs_code: str, lookback_quarters: int = 8) -> Optional[Dict[str, Any]]:
    """
    从当前日期向前回溯最多 lookback_quarters 个季度,
    找到第一个有盈利数据的报告期。调用方持有 _bs_lock。
    """
    now = datetime.now()
    # 生成候选报告期 (YYYYQQ), 从最近的已结束季度开始
    year, q = now.year, (now.month - 1) // 3  # 0-based 上一季度索引
    year_for_period = now.year
    current_q = (now.month - 1) // 3 + 1      # 当前所处季度 (1-based)
    # 从上一个完整季度开始回溯
    y, p_q = year_for_period, current_q - 1
    if p_q == 0:
        p_q = 4
        y -= 1

    candidates: List[str] = []
    for _ in range(lookback_quarters):
        candidates.append(f"{y}{p_q:02d}")
        p_q -= 1
        if p_q == 0:
            p_q = 4
            y -= 1
    del year, q  # 上方两行为兼容保留, 实际使用 y/p_q 序列

    for period in candidates:
        df = _query_profit_by_period(bs_code, period)
        if df is not None and not df.empty:
            row = df.iloc[-1]
            roe = _safe_float(row.get("roeAvg"))           # 净资产收益率(平均)
            net_margin = _safe_float(row.get("npMargin"))  # 销售净利润率
            return {
                "period": period,
                "roe": roe,               # 小数形式, 如 0.1234 = 12.34%
                "net_profit_margin": net_margin,
                "eps": _safe_float(row.get("epsTTM")),
                "net_profit": _safe_float(row.get("netProfit")),
            }
    return None


@app.get("/api/financial_indicator")
def get_financial_indicator(
    symbol: str = Query(..., description="6位股票代码, 如 600519"),
) -> Dict[str, Any]:
    """
    获取指定股票最新一期的 ROE 与净利润率 (Baostock 季频财务数据)。

    返回字段:
        roe              : 净资产收益率 (平均值, 小数)
        net_profit_margin: 销售净利润率 (小数)
    """
    symbol = _validate_symbol(symbol)
    bs_code = _to_baostock_code(symbol)

    result: Optional[Dict[str, Any]] = None
    try:
        with _bs_lock:
            _bs_ensure_login()
            try:
                result = _pick_latest_period_with_data(bs_code)
            except Exception as inner_exc:  # noqa: BLE001
                # 会话可能失效: 强制重登后重试一次
                logger.warning("baostock 查询异常, 尝试重新登录后重试: %s", inner_exc)
                _bs_logout_quietly()
                _bs_ensure_login()
                result = _pick_latest_period_with_data(bs_code)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise _gateway_error(500, "baostock(financial_indicator)", exc)

    if result is None:
        logger.warning("financial_indicator: symbol=%s 近 8 个季度无财务数据", symbol)
        return {
            "code": 0,
            "message": "no_data",
            "symbol": symbol,
            "data": None,
        }

    logger.info(
        "financial_indicator: symbol=%s period=%s roe=%s margin=%s",
        symbol, result["period"], result["roe"], result["net_profit_margin"],
    )
    return {
        "code": 0,
        "message": "success",
        "symbol": symbol,
        "data": result,
    }


# ---------------------------------------------------------------------------
# 接口 4: czsc 缠论结构 + 买卖点信号分析 (数据源: AKShare, 已彻底替代 Tushare)
# ---------------------------------------------------------------------------
_CZSC_PERIODS = {"daily", "weekly", "monthly"}

# 注: /api/czsc/analyze 的缓存优先实现见下方 "长期方案 C" 区块
# (同名函数覆盖注册, FastAPI 以最后注册的版本为准), 此处不再保留旧版实时实现。


# ---------------------------------------------------------------------------
# 漏斗模型 Layer1: 批量粗筛 (screen_pool)
# ---------------------------------------------------------------------------

# 单次批量扫描的股票数量上限 (AKShare 免费接口限流保护)
_SCREEN_MAX_SYMBOLS = 100
# 每只股票之间的请求间隔 (秒), 防止被封 IP
_SCREEN_SLEEP_SECONDS = 0.5


class ScreenPoolRequest(BaseModel):
    """POST /api/czsc/screen_pool 的请求体。"""

    symbols: List[str] = Field(..., description="股票代码列表, 如 ['603236', '000001']")
    start_date: Optional[str] = Field(None, description="开始日期 YYYYMMDD, 默认近一年半")
    end_date: Optional[str] = Field(None, description="结束日期 YYYYMMDD, 默认今天")
    period: str = Field("daily", description="周期: daily/weekly/monthly")
    cache_only: bool = Field(
        False,
        description="true=只读磁盘缓存不回源AKShare(配合每日refresh秒级出池); "
                    "false=缓存未命中时增量/全量回源(首次或补数据时用)",
    )
    mine_sweep: bool = Field(
        True,
        description="true=对通过技术粗筛的股票追加财务排雷(ST/连亏/高负债/低ROE), "
                    "被拦截者移入 blocked 列表并给出理由; false=跳过排雷",
    )
    edge_check: bool = Field(
        False,
        description="true=对候选股追加 vectorbt 历史正期望检验 (读回测缓存优先, "
                    "未命中才现算, 单只约数秒; 默认关闭以保响应速度)",
    )
    drop_negative_edge: bool = Field(
        False,
        description="true 且 edge_check=true 时, 把样本充足但期望收益为负的股票"
                    "从 pool 移入 blocked (硬拦截); 默认 false=仅标注 edge 供 LLM 参考",
    )


def _fetch_czsc_result(symbol: str, period: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """全量拉取单只股票的前复权K线并执行 czsc 分析 (缓存未命中时的回源路径)。"""
    df: pd.DataFrame = _retry_call(
        lambda: ak.stock_zh_a_hist(
            symbol=symbol,
            period=period,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",   # 前复权, 保证跨除权日的笔/中枢不被价格跳空打断
        )
    )
    if df is None or df.empty:
        return {"error": "no_data"}
    bars = akshare_to_czsc_bars(df, symbol=symbol, period=period)
    result = analyze_czsc(bars)
    if "error" not in result:
        last_dt = str(df.iloc[-1]["日期"] if "日期" in df.columns else df.iloc[-1, 0])[:10]
        result["window_end"] = last_dt.replace("-", "")
    return result


# ---------------------------------------------------------------------------
# 长期方案 C: 缓存读取 / 增量拼接 / 预计算调度
# ---------------------------------------------------------------------------

# 增量窗口上限: 旧缓存距今超过该天数则放弃增量, 直接全量重拉 (防脏数据累积)
_INCREMENTAL_MAX_GAP_DAYS = 30
# refresh 并发度: 瓶颈是 AKShare 网络 I/O, 4~8 线程 + 全局限速器即可稳定跑满
_REFRESH_WORKERS = int(os.environ.get("CZSC_REFRESH_WORKERS", "6"))
# 全局请求最小间隔 (秒): 所有线程共享的令牌桶, 替代原先每只股票 sleep(0.5)
_GLOBAL_MIN_INTERVAL = float(os.environ.get("CZSC_MIN_INTERVAL", "0.35"))

_rate_lock = threading.Lock()
_last_request_ts = [0.0]


def _throttle() -> None:
    """全局令牌桶: 保证任意两次 AKShare 行情请求间隔 >= _GLOBAL_MIN_INTERVAL。

    与逐股 sleep 不同, 该限速对并发线程统一生效 —— 多线程只是把"等待上游
    响应"的时间重叠起来, 总请求速率仍受控, 不会触发封 IP。
    """
    with _rate_lock:
        now = time.monotonic()
        wait = _GLOBAL_MIN_INTERVAL - (now - _last_request_ts[0])
        if wait > 0:
            time.sleep(wait)
        _last_request_ts[0] = time.monotonic()


def _ak_hist_throttled(symbol: str, period: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """带全局限速的 stock_zh_a_hist 调用 (重试逻辑仍由外层 _retry_call 负责)。"""
    _throttle()
    return ak.stock_zh_a_hist(
        symbol=symbol, period=period,
        start_date=start_date, end_date=end_date, adjust="qfq",
    )


def _cache_is_fresh(entry: Dict[str, Any], end_date: str) -> bool:
    """缓存是否覆盖到请求窗口末端: 缓存最后交易日 >= min(end_date, 今天)。

    非交易日请求时 end_date 可能落在周末, 此时只要缓存是最近一个交易日即算新鲜。
    """
    cached_end = str(entry.get("window_end", ""))
    if not cached_end:
        return False
    effective_end = min(end_date, datetime.now().strftime(_DATE_FMT))
    return cached_end >= effective_end


def _incremental_fetch(symbol: str, period: str, entry: Dict[str, Any], end_date: str) -> Optional[Dict[str, Any]]:
    """基于旧缓存做增量: 只拉取缓存最后一根K线之后的新数据, 拼接后重新分析。

    返回新的完整 result; 增量失败 (无新数据以外的异常) 返回 None 交由调用方全量回源。
    注意 qfq 近似: 若期间发生除权, 历史与新段存在平移误差, 每日收盘后的全量
    refresh 会消除该误差, MVP 阶段接受此近似。
    """
    cached_start = _date_compact(str(entry.get("window_start", "")))
    cached_end = _date_compact(str(entry.get("window_end", "")))
    if not cached_start or not cached_end:
        return None
    gap_days = (datetime.strptime(end_date, _DATE_FMT) - datetime.strptime(cached_end, _DATE_FMT)).days
    if gap_days < 0 or gap_days > _INCREMENTAL_MAX_GAP_DAYS:
        return None  # 缺口过大或时间倒挂, 走全量

    try:
        new_df = _retry_call(lambda: _ak_hist_throttled(symbol, period, cached_end, end_date))
    except Exception as exc:  # noqa: BLE001 - 增量失败降级为全量回源
        logger.warning("incremental fetch %s failed, fallback to full: %s", symbol, exc)
        return None

    old_result: Dict[str, Any] = entry["result"]
    if new_df is None or new_df.empty:
        # 区间内无新交易日 (周末/节假日): 缓存即最新结果
        return old_result

    # 去掉新数据中与缓存末端重叠的那一根, 避免重复K线破坏 czsc 结构
    last_day = str(entry.get("trade_date", ""))
    if last_day:
        date_col = "日期" if "日期" in new_df.columns else new_df.columns[0]
        new_df = new_df[new_df[date_col].astype(str).str[:10] > last_day]
    if new_df.empty:
        return old_result

    # 简化策略: czsc 需要连续原始K线才能重算结构, 而缓存里只有分析结果没有 bars,
    # 因此采用"尾部重拉": 从 (cached_end - 90天) 起重拉拼接段并整体重算,
    # 传输量 (~60根) 仍远小于全量 (~500根), 且保证近端结构绝对正确。
    tail_start = (datetime.strptime(cached_end, _DATE_FMT) - timedelta(days=90)).strftime(_DATE_FMT)
    try:
        tail_df = _retry_call(lambda: _ak_hist_throttled(symbol, period, tail_start, end_date))
    except Exception as exc:  # noqa: BLE001
        logger.warning("tail refetch %s failed: %s", symbol, exc)
        return None
    if tail_df is None or tail_df.empty:
        return old_result
    bars = akshare_to_czsc_bars(tail_df, symbol=symbol, period=period)
    result = analyze_czsc(bars)
    if "error" in result:
        return None
    result["window_end"] = str(tail_df.iloc[-1, 0])[:10].replace("-", "")
    return result


def _get_or_compute(
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
    allow_live: bool = True,
) -> tuple:
    """缓存优先地获取 czsc 分析结果。返回 (result, meta)。

    meta: {"source": "cache"|"incremental"|"live"|"stale_cache"|"miss", ...}
    - 当日新鲜缓存 -> 直接返回 (零网络, 毫秒级);
    - 过期缓存且 allow_live -> 尝试增量, 失败则全量回源, 均写回缓存;
    - cache_only 模式 (refresh 关闭时) 且无新鲜缓存 -> 返回过期缓存并标记 stale。
    """
    entry = _czsc_cache.get(symbol, period)
    if entry is not None and _cache_is_fresh(entry, end_date):
        return entry["result"], {"source": "cache", "generated_at": entry.get("generated_at", "")}

    if not allow_live:
        if entry is not None:
            return entry["result"], {"source": "stale_cache", "trade_date": entry.get("trade_date", "")}
        return {"error": "cache_miss"}, {"source": "miss"}

    # 回源路径 1: 有旧缓存 -> 增量拼接
    if entry is not None:
        inc = _incremental_fetch(symbol, period, entry, end_date)
        if inc is not None and "error" not in inc:
            _czsc_cache.put(symbol, period, inc,
                            _date_compact(str(entry.get("window_start", ""))) or start_date,
                            inc.get("window_end", end_date))
            return inc, {"source": "incremental"}

    # 回源路径 2: 全量拉取 (带全局限速)
    result = _fetch_czsc_result(symbol, period, start_date, end_date)
    if "error" not in result:
        _czsc_cache.put(symbol, period, result, start_date, result.get("window_end", end_date))
        return result, {"source": "live"}
    # 全量失败但有旧缓存 -> 降级用旧数据
    if entry is not None:
        return entry["result"], {"source": "stale_cache", "trade_date": entry.get("trade_date", "")}
    return result, {"source": "live"}


class RefreshRequest(BaseModel):
    """POST /api/czsc/refresh 的请求体 (Go cron 每日收盘后调用)。"""

    symbols: List[str] = Field(..., description="需要预计算/刷新缓存的股票代码列表")
    period: str = Field("daily", description="周期: daily/weekly/monthly")
    start_date: Optional[str] = Field(None, description="开始日期 YYYYMMDD, 默认近一年半")
    end_date: Optional[str] = Field(None, description="结束日期 YYYYMMDD, 默认今天")
    force: bool = Field(False, description="true=无视新鲜度强制全量重拉; false=只补缺失/过期")


def _refresh_one(symbol: str, period: str, start_date: str, end_date: str, force: bool) -> Dict[str, Any]:
    """刷新单只股票缓存 (线程池 worker), 永不抛异常, 以 dict 汇报状态。"""
    try:
        if force:
            result = _fetch_czsc_result(symbol, period, start_date, end_date)
            if "error" in result:
                return {"symbol": symbol, "status": "failed", "error": result["error"]}
            _czsc_cache.put(symbol, period, result, start_date, result.get("window_end", end_date))
            return {"symbol": symbol, "status": "refreshed", "trade_date": result.get("window_end", "")}
        result, meta = _get_or_compute(symbol, period, start_date, end_date)
        if "error" in result:
            return {"symbol": symbol, "status": "failed", "error": result["error"]}
        return {"symbol": symbol, "status": meta["source"], "trade_date": result.get("window_end", "")}
    except Exception as exc:  # noqa: BLE001 - 单只失败不影响整体
        logger.warning("refresh symbol=%s failed: %s", symbol, exc)
        return {"symbol": symbol, "status": "failed", "error": str(exc)}


@app.post("/api/czsc/refresh")
def post_czsc_refresh(req: RefreshRequest) -> Dict[str, Any]:
    """收盘后定时预计算入口: 并发刷新一批股票的 czsc 结果磁盘缓存。

    Go 端 cron 每个交易日 15:10 调用本接口 (force=true 全量刷新自选股+持仓股),
    之后的 screen_pool / analyze 全部走缓存, 毫秒级返回且不再实时打 AKShare。
    全量刷新时因除权重算 qfq 序列, 顺带消除增量拼接的复权漂移近似。
    """
    if not _CZSC_AVAILABLE:
        raise HTTPException(status_code=503, detail="czsc 未安装, 请执行 pip install czsc 后重启网关")
    if req.period not in _CZSC_PERIODS:
        raise HTTPException(status_code=400, detail=f"非法周期: '{req.period}', 需为 daily/weekly/monthly")
    if not req.symbols:
        raise HTTPException(status_code=400, detail="symbols 不能为空")

    valid: List[str] = []
    errors: List[Dict[str, str]] = []
    for s in req.symbols:
        try:
            sym = _validate_symbol(str(s))
            if sym not in valid:
                valid.append(sym)
        except HTTPException as exc:
            errors.append({"symbol": str(s), "error": str(exc.detail)})

    end_date = _validate_date(req.end_date, "end_date") or datetime.now().strftime(_DATE_FMT)
    start_date = _validate_date(req.start_date, "start_date") or (
        datetime.now() - timedelta(days=550)
    ).strftime(_DATE_FMT)

    started = time.monotonic()
    results: List[Dict[str, Any]] = []
    if valid:
        with ThreadPoolExecutor(max_workers=min(_REFRESH_WORKERS, len(valid))) as pool:
            futures = [
                pool.submit(_refresh_one, sym, req.period, start_date, end_date, req.force)
                for sym in valid
            ]
            for fut in futures:
                results.append(fut.result())

    counter: Dict[str, int] = {}
    for r in results:
        counter[r["status"]] = counter.get(r["status"], 0) + 1
    elapsed = round(time.monotonic() - started, 2)
    logger.info(
        "czsc refresh done: period=%s force=%s total=%d stats=%s elapsed=%.2fs",
        req.period, req.force, len(results), counter, elapsed,
    )
    return {
        "code": 0,
        "message": "success",
        "data": {
            "period": req.period,
            "force": req.force,
            "window": f"{start_date}~{end_date}",
            "total": len(results),
            "stats": counter,
            "elapsed_seconds": elapsed,
            "results": results,
            "errors": errors,
        },
    }


@app.get("/api/czsc/cache_status")
def get_cache_status() -> Dict[str, Any]:
    """查看 czsc 磁盘缓存覆盖情况 (条目数 / 交易日分布), 供运维与调试。"""
    return {"code": 0, "message": "success", "data": _czsc_cache.stats()}


@app.delete("/api/czsc/cache")
def delete_czsc_cache(
    period: Optional[str] = Query(None, description="仅清指定周期 daily/weekly/monthly, 缺省清全部"),
) -> Dict[str, Any]:
    """清空 czsc 结果缓存 (调试用)。"""
    if period is not None and period not in _CZSC_PERIODS:
        raise HTTPException(status_code=400, detail=f"非法周期: '{period}'")
    removed = _czsc_cache.clear(period)
    return {"code": 0, "message": "success", "data": {"removed": removed}}


@app.get("/api/czsc/analyze")
def get_czsc_analysis(
    symbol: str = Query(..., description="6位股票代码, 如 000001"),
    period: str = Query("daily", description="周期: daily/weekly/monthly"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD, 默认近一年半"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD, 默认今天"),
    use_cache: bool = Query(True, description="true=缓存优先(默认); false=强制实时回源(调试)"),
) -> Dict[str, Any]:
    """
    获取指定股票的缠论结构与三类买卖点信号 (AKShare 行情 + czsc 核心库)。

    长期方案 C: 默认命中当日磁盘缓存直接返回; 未命中时增量/全量回源并写回缓存。
    返回 data.summary 为自然语言摘要, Go/Eino 侧可直接喂给 LLM;
    data.structures 含笔/分型/中枢, data.signals 含 1B/2B/3B 与 1S/2S/3S。
    """
    symbol = _validate_symbol(symbol)
    if not _CZSC_AVAILABLE:
        raise HTTPException(status_code=503, detail="czsc 未安装, 请执行 pip install czsc 后重启网关")
    if period not in _CZSC_PERIODS:
        raise HTTPException(status_code=400, detail=f"非法周期: '{period}', 需为 daily/weekly/monthly")
    start_date = _validate_date(start_date, "start_date")
    end_date = _validate_date(end_date, "end_date")

    # 默认取最近约 500 个交易日 (专家建议: 日线 500 根足够定位当前中枢与买卖点)
    if not end_date:
        end_date = datetime.now().strftime(_DATE_FMT)
    if not start_date:
        start_date = (datetime.now() - timedelta(days=550)).strftime(_DATE_FMT)

    try:
        if use_cache:
            result, meta = _get_or_compute(symbol, period, start_date, end_date)
        else:
            result, meta = _fetch_czsc_result_cached(symbol, period, start_date, end_date)
    except Exception as exc:  # noqa: BLE001 - czsc 计算/上游异常统一转 500
        raise _gateway_error(500, "czsc(analyze)", exc)

    if "error" in result:
        return {"code": 0, "message": "insufficient_data", "symbol": symbol, "data": result}

    logger.info(
        "czsc_analyze: symbol=%s period=%s source=%s bi=%d zs=%d buys=%d sells=%d",
        symbol, period, meta.get("source", "?"),
        result["structures"]["bi_count"], result["structures"]["zs_count"],
        len(result["signals"]["buy_points"]), len(result["signals"]["sell_points"]),
    )
    return {
        "code": 0,
        "message": "success",
        "symbol": symbol,
        "period": period,
        "adjust": "qfq",
        "source": meta.get("source", "live"),
        "data": result,
    }


def _fetch_czsc_result_cached(symbol: str, period: str, start_date: str, end_date: str) -> tuple:
    """use_cache=false 分支: 强制实时全量拉取, 但仍顺手刷新缓存保持一致。"""
    result = _fetch_czsc_result(symbol, period, start_date, end_date)
    if "error" not in result:
        _czsc_cache.put(symbol, period, result, start_date, result.get("window_end", end_date))
    return result, {"source": "live_forced"}


def _match_screen_rule(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """核心过滤规则: 返回命中的候选记录, 未命中返回 None。

    a) signals.buy_points 非空  -> 缠论买点 (signal_type 取第一个买点的 type)
    b) signals.sell_points 非空  -> 缠论卖点 (signal_type 取第一个卖点的 type)
    c) structures.zs_count >= 2 且最后一笔向上 -> 趋势观察池 (Observation)
    """
    signals = result.get("signals") or {}
    structures = result.get("structures") or {}
    buys = signals.get("buy_points") or []
    sells = signals.get("sell_points") or []

    signal_type: Optional[str] = None
    if buys:
        signal_type = buys[0]["type"]
    elif sells:
        signal_type = sells[0]["type"]
    elif structures.get("zs_count", 0) >= 2 and (structures.get("last_bi") or {}).get("direction") == "up":
        signal_type = "Observation"

    if signal_type is None:
        return None
    return {
        "symbol": result["symbol"],
        "signal_type": signal_type,
        "buy_points": buys,
        "sell_points": sells,
        "zs_count": structures.get("zs_count", 0),
        "bi_count": structures.get("bi_count", 0),
        "trend_direction": (structures.get("last_bi") or {}).get("direction", "unknown"),
        "summary": result.get("summary", ""),
    }


@app.post("/api/czsc/screen_pool")
def post_screen_pool(req: ScreenPoolRequest) -> Dict[str, Any]:
    """批量粗筛: 遍历自选股, 只保留出现缠论买卖点或趋势向上的候选股票池。

    输出 pool 直接供 Go/Eino Layer2 (AI 排雷精选) 消费;
    单只股票失败仅记入 errors, 不影响整体扫描。

    长期方案 C: 默认缓存优先 (_get_or_compute), 命中当日磁盘缓存的股票零网络开销;
    cache_only=true 时完全不回源 AKShare (要求先跑过 /api/czsc/refresh 预计算)。
    需要回源的少量股票通过线程池并发补齐 (全局令牌桶限速, 防封不叠加耗时)。
    """
    if not _CZSC_AVAILABLE:
        raise HTTPException(status_code=503, detail="czsc 未安装, 请执行 pip install czsc 后重启网关")
    if req.period not in _CZSC_PERIODS:
        raise HTTPException(status_code=400, detail=f"非法周期: '{req.period}', 需为 daily/weekly/monthly")
    if not req.symbols:
        raise HTTPException(status_code=400, detail="symbols 不能为空")
    if len(req.symbols) > _SCREEN_MAX_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"单次最多扫描 {_SCREEN_MAX_SYMBOLS} 只股票 (AKShare 限流保护), 当前 {len(req.symbols)} 只",
        )

    # 逐只校验代码格式, 非法代码记入 errors 而不是整体报错
    valid_symbols: List[str] = []
    errors: List[Dict[str, str]] = []
    for s in req.symbols:
        try:
            sym = _validate_symbol(str(s))
            if sym not in valid_symbols:
                valid_symbols.append(sym)
        except HTTPException as exc:
            errors.append({"symbol": str(s), "error": str(exc.detail)})

    end_date = _validate_date(req.end_date, "end_date") or datetime.now().strftime(_DATE_FMT)
    start_date = _validate_date(req.start_date, "start_date") or (
        datetime.now() - timedelta(days=550)
    ).strftime(_DATE_FMT)

    started = time.monotonic()

    def screen_one(symbol: str) -> tuple:
        """单只股票的取数+过滤, 返回 (symbol, result|None, meta|None, err|None)。"""
        try:
            result, meta = _get_or_compute(
                symbol, req.period, start_date, end_date, allow_live=not req.cache_only
            )
            if "error" in result or "structures" not in result:
                return symbol, None, None, result.get("error", "insufficient_data")
            return symbol, result, meta, None
        except Exception as exc:  # noqa: BLE001 - 单只失败不影响整体
            logger.warning("screen_pool: symbol=%s 失败: %s", symbol, exc)
            return symbol, None, None, str(exc)

    pool: List[Dict[str, Any]] = []
    scanned = 0
    source_counter: Dict[str, int] = {}
    order = {sym: i for i, sym in enumerate(valid_symbols)}
    outputs: List[tuple] = []
    if valid_symbols:
        workers = 1 if req.cache_only else min(_REFRESH_WORKERS, len(valid_symbols))
        with ThreadPoolExecutor(max_workers=workers) as tp:
            outputs = list(tp.map(screen_one, valid_symbols))
    # 按原始顺序归并, 保证候选池输出稳定可复现
    for symbol, result, meta, err in sorted(outputs, key=lambda o: order[o[0]]):
        if err is not None:
            errors.append({"symbol": symbol, "error": err})
            continue
        scanned += 1
        src = (meta or {}).get("source", "?")
        source_counter[src] = source_counter.get(src, 0) + 1
        hit = _match_screen_rule(result)
        if hit is not None:
            pool.append(hit)

    # ------------------------------------------------------------------
    # 财务排雷 (优先级1-2): 只拦截"已通过技术粗筛"的股票, 控制网络开销;
    # 原则是宁可漏杀不可误杀 —— 数据缺失默认放行。
    # ------------------------------------------------------------------
    blocked: List[Dict[str, Any]] = []
    if req.mine_sweep and pool and _MINE_AVAILABLE:
        try:
            name_map = {item["symbol"]: item.get("name", "")
                        for item in _stock_list_cached()}
        except Exception:  # noqa: BLE001 - 名称映射失败退化为逐个查询/放行
            name_map = {}
        kept: List[Dict[str, Any]] = []
        for hit in pool:
            try:
                chk = _mine_check(hit["symbol"], name_map=name_map)
            except Exception as exc:  # noqa: BLE001 - 排雷异常不误杀
                logger.warning("screen_pool mine_sweep %s error, pass: %s", hit["symbol"], exc)
                chk = {"passed": True, "reasons": [], "snapshot": {"source": "error"}}
            if chk.get("passed"):
                hit["financial"] = chk.get("snapshot")
                kept.append(hit)
            else:
                hit["blocked_reasons"] = chk.get("reasons", [])
                blocked.append(hit)
        pool = kept

    # ------------------------------------------------------------------
    # vectorbt 正期望检验 (优先级1-1, 可选): 读回测缓存优先, 未命中现算。
    # 结论作为 edge 字段附在候选记录上, 供 Layer2 LLM 决策加权。
    # ------------------------------------------------------------------
    if req.edge_check and pool and _bt_is_available():
        kept = []
        for hit in pool:
            sym = hit["symbol"]
            bt = _bt_cache_get(sym, _BT_BUY_TYPES, _BT_SELL_TYPES)
            if bt is None:
                try:
                    df = _retry_call(lambda: _ak_hist_throttled(
                        sym, req.period,
                        (datetime.now() - timedelta(days=1100)).strftime(_DATE_FMT), end_date))
                    if df is not None and not df.empty:
                        bt = _run_backtest(df, sym, period=req.period)
                        if "error" not in bt:
                            _bt_cache_put(sym, _BT_BUY_TYPES, _BT_SELL_TYPES, bt)
                except Exception as exc:  # noqa: BLE001 - 回测失败不影响出池
                    logger.warning("screen_pool edge_check %s failed: %s", sym, exc)
            dropped = False
            if bt and "metrics" in bt:
                m = bt["metrics"]
                hit["edge"] = {
                    "avg_trade_return_pct": m["avg_trade_return_pct"],
                    "win_rate_pct": m["win_rate_pct"],
                    "trade_count": m["trade_count"],
                    "positive_edge": m["positive_edge"],
                }
                # 硬拦截开关: 样本充足(有成交)且期望为负 -> 移入 blocked;
                # 无成交/数据缺失一律放行 (宁可漏杀不可误杀)
                if (req.drop_negative_edge and m["trade_count"] > 0
                        and (m["avg_trade_return_pct"] or 0.0) <= 0):
                    hit["blocked_reasons"] = [
                        f"历史回测负期望: 单笔均收益 {m['avg_trade_return_pct']}% "
                        f"(成交{m['trade_count']}笔)"
                    ]
                    blocked.append(hit)
                    dropped = True
            if not dropped:
                kept.append(hit)
        pool = kept

    elapsed = round(time.monotonic() - started, 2)
    logger.info(
        "screen_pool: scanned=%d pool=%d errors=%d sources=%s cache_only=%s elapsed=%.2fs window=[%s,%s]",
        scanned, len(pool), len(errors), source_counter, req.cache_only, elapsed, start_date, end_date,
    )
    return {
        "code": 0,
        "message": "success",
        "data": {
            "trade_date": datetime.now().strftime("%Y-%m-%d"),
            "period": req.period,
            "window": f"{start_date}~{end_date}",
            "cache_only": req.cache_only,
            "mine_sweep": req.mine_sweep,
            "edge_check": req.edge_check,
            "total_requested": len(req.symbols),
            "total_scanned": scanned,
            "pool_size": len(pool),
            "blocked_size": len(blocked),
            "elapsed_seconds": elapsed,
            "sources": source_counter,
            "pool": pool,
            "blocked": blocked,
            "errors": errors,
        },
    }


# ---------------------------------------------------------------------------
# vectorbt 回测接口 (基石验证层)
# ---------------------------------------------------------------------------

class BacktestRequest(BaseModel):
    """POST /api/czsc/backtest/rules 的请求体。"""

    symbols: List[str] = Field(..., description="待回测股票代码列表")
    buy_types: Optional[List[str]] = Field(None, description="入场买点集合, 默认 ['1B','2B','3B']")
    sell_types: Optional[List[str]] = Field(None, description="离场卖点集合, 默认 ['1S','2S','3S']")
    start_date: Optional[str] = Field(None, description="回测窗口开始 YYYYMMDD, 默认近3年")
    end_date: Optional[str] = Field(None, description="回测窗口结束 YYYYMMDD, 默认今天")
    period: str = Field("daily", description="周期: daily/weekly/monthly")
    stride: int = Field(5, ge=1, le=20, description="信号重放步长 (越小越精确越慢)")
    fees: Optional[float] = Field(None, description="双边费率, 默认 0.0013")
    use_cache: bool = Field(True, description="true=优先读7天内的回测磁盘缓存")


def _backtest_one(symbol: str, req: BacktestRequest,
                  buy_t: tuple, sell_t: tuple,
                  start_date: str, end_date: str) -> Dict[str, Any]:
    """单只股票回测 worker: 缓存优先, 未命中则拉数据现算并写回缓存。"""
    if req.use_cache:
        hit = _bt_cache_get(symbol, buy_t, sell_t)
        if hit is not None:
            return hit
    df = _retry_call(lambda: _ak_hist_throttled(symbol, req.period, start_date, end_date))
    if df is None or df.empty:
        return {"symbol": symbol, "error": "no_data"}
    kwargs: Dict[str, Any] = dict(period=req.period, buy_types=buy_t, sell_types=sell_t,
                                 stride=req.stride)
    if req.fees is not None:
        kwargs["fees"] = req.fees
    result = _run_backtest(df, symbol, **kwargs)
    if "error" not in result:
        _bt_cache_put(symbol, buy_t, sell_t, result)
    return result


@app.post("/api/czsc/backtest/rules")
def post_czsc_backtest_rules(req: BacktestRequest) -> Dict[str, Any]:
    """批量回测 czsc 粗筛规则的历史期望收益 (交给 AI 之前的"基石验证")。

    对每只股票滚动重放买卖点 -> vectorbt 组合绩效; 跨股票聚合出规则级结论:
    verdict=positive_expectancy 表示该信号集在样本内平均单笔收益为正。
    单只失败仅记入 errors; 结果按 symbol+规则集 磁盘缓存 7 天。
    """
    if not _bt_is_available():
        raise HTTPException(status_code=503, detail="vectorbt/czsc 未安装, 请 pip install vectorbt")
    if req.period not in _CZSC_PERIODS:
        raise HTTPException(status_code=400, detail=f"非法周期: '{req.period}'")
    if not req.symbols:
        raise HTTPException(status_code=400, detail="symbols 不能为空")
    if len(req.symbols) > _SCREEN_MAX_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"单次最多 {_SCREEN_MAX_SYMBOLS} 只")

    valid: List[str] = []
    errors: List[Dict[str, str]] = []
    for s in req.symbols:
        try:
            sym = _validate_symbol(str(s))
            if sym not in valid:
                valid.append(sym)
        except HTTPException as exc:
            errors.append({"symbol": str(s), "error": str(exc.detail)})

    known_buy = set(_BT_BUY_TYPES) | {"Observation"}
    known_sell = set(_BT_SELL_TYPES)
    buy_t = tuple(b for b in (req.buy_types or _BT_BUY_TYPES) if b in known_buy) or _BT_BUY_TYPES
    sell_t = tuple(x for x in (req.sell_types or _BT_SELL_TYPES) if x in known_sell) or _BT_SELL_TYPES

    end_date = _validate_date(req.end_date, "end_date") or datetime.now().strftime(_DATE_FMT)
    # 回测默认用更长窗口 (~3年) 以获得足够交易样本
    start_date = _validate_date(req.start_date, "start_date") or (
        datetime.now() - timedelta(days=1100)).strftime(_DATE_FMT)

    started = time.monotonic()
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(_REFRESH_WORKERS, max(len(valid), 1))) as tp:
        futures = {sym: tp.submit(_backtest_one, sym, req, buy_t, sell_t, start_date, end_date)
                   for sym in valid}
        for sym in valid:
            try:
                r = futures[sym].result()
                if "error" in r:
                    errors.append({"symbol": sym, "error": r["error"]})
                else:
                    results.append(r)
            except Exception as exc:  # noqa: BLE001 - 单只失败不影响整体
                logger.warning("backtest %s failed: %s", sym, exc)
                errors.append({"symbol": sym, "error": str(exc)[:200]})

    rule_name = f"{'+'.join(buy_t)} entry / {'+'.join(sell_t)} exit"
    summary = _summarize_rule_perf(results, rule_name)
    elapsed = round(time.monotonic() - started, 2)
    logger.info("backtest rules done: symbols=%d verdict=%s elapsed=%.1fs",
                len(results), summary.get("verdict"), elapsed)
    return {
        "code": 0,
        "message": "success",
        "data": {
            "rule_summary": summary,
            "window": f"{start_date}~{end_date}",
            "stride": req.stride,
            "elapsed_seconds": elapsed,
            "results": [
                {"symbol": r["symbol"], "date_range": r.get("date_range"),
                 "signal_stats": r.get("signal_stats", {}), "metrics": r.get("metrics"),
                 "recent_trades": r.get("recent_trades", [])[:6],
                 "source": r.get("source", "live")}
                for r in results
            ],
            "errors": errors,
        },
    }


@app.get("/api/czsc/backtest/{symbol}")
def get_czsc_backtest_symbol(
    symbol: str,
    period: str = Query("daily", description="周期: daily/weekly/monthly"),
    start_date: Optional[str] = Query(None, description="YYYYMMDD, 默认近3年"),
    end_date: Optional[str] = Query(None, description="YYYYMMDD, 默认今天"),
    use_cache: bool = Query(True, description="优先读回测缓存"),
) -> Dict[str, Any]:
    """单只股票的完整回测报告 (含逐笔成交与信号事件明细)。"""
    if not _bt_is_available():
        raise HTTPException(status_code=503, detail="vectorbt/czsc 未安装")
    symbol = _validate_symbol(symbol)
    if period not in _CZSC_PERIODS:
        raise HTTPException(status_code=400, detail=f"非法周期: '{period}'")
    end_date = _validate_date(end_date, "end_date") or datetime.now().strftime(_DATE_FMT)
    start_date = _validate_date(start_date, "start_date") or (
        datetime.now() - timedelta(days=1100)).strftime(_DATE_FMT)

    req = BacktestRequest(symbols=[symbol], period=period, use_cache=use_cache)
    try:
        result = _backtest_one(symbol, req, _BT_BUY_TYPES, _BT_SELL_TYPES, start_date, end_date)
    except Exception as exc:  # noqa: BLE001
        raise _gateway_error(500, "czsc(backtest)", exc)
    if "error" in result:
        return {"code": 0, "message": "failed", "symbol": symbol, "data": result}
    return {"code": 0, "message": "success", "symbol": symbol, "data": result}


@app.get("/api/czsc/backtest/{symbol}/per_signal")
def get_czsc_backtest_per_signal(
    symbol: str,
    period: str = Query("daily", description="周期: daily/weekly/monthly"),
    start_date: Optional[str] = Query(None, description="YYYYMMDD, 默认近3年"),
    end_date: Optional[str] = Query(None, description="YYYYMMDD, 默认今天"),
    stride: int = Query(5, ge=1, le=20, description="信号重放步长"),
) -> Dict[str, Any]:
    """分信号类型统计 (回答"三买 vs 一买 vs 二买谁更准")。

    对每类买点给出触发次数/胜率/平均单笔收益; 每类卖点给出"信号后下跌规避率"。
    注意: 该口径为近似事件研究 (未含成本, 离场点=下一反向信号), 用于横向比较
    信号质量, 绝对数值以 /rules 组合回测为准。计算 O(n^2), 不缓存、按需调用。
    """
    if not _bt_is_available():
        raise HTTPException(status_code=503, detail="vectorbt/czsc 未安装")
    symbol = _validate_symbol(symbol)
    if period not in _CZSC_PERIODS:
        raise HTTPException(status_code=400, detail=f"非法周期: '{period}'")
    end_date = _validate_date(end_date, "end_date") or datetime.now().strftime(_DATE_FMT)
    start_date = _validate_date(start_date, "start_date") or (
        datetime.now() - timedelta(days=1100)).strftime(_DATE_FMT)
    try:
        df = _retry_call(lambda: _ak_hist_throttled(symbol, period, start_date, end_date))
        if df is None or df.empty:
            return {"code": 0, "message": "failed",
                    "data": {"symbol": symbol, "error": "no_data"}}
        stats = _per_signal_stats(df, symbol, period=period, stride=stride)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise _gateway_error(500, "czsc(per_signal)", exc)
    stats["window"] = f"{start_date}~{end_date}"
    return {"code": 0, "message": "success", "symbol": symbol, "data": stats}


# ---------------------------------------------------------------------------
# 财务排雷独立接口 (供 Go 端单独调用复查某只股票)
# ---------------------------------------------------------------------------

@app.get("/api/financial/mine_check")
def get_financial_mine_check(
    symbol: str = Query(..., description="6位股票代码"),
) -> Dict[str, Any]:
    """单只股票财务排雷检查: ST/连亏/高负债/低ROE 四规则。"""
    if not _MINE_AVAILABLE:
        raise HTTPException(status_code=503, detail="mine_sweeper 不可用 (需 akshare)")
    symbol = _validate_symbol(symbol)
    name_map: Dict[str, str] = {}
    try:
        name_map = {item["symbol"]: item.get("name", "") for item in _stock_list_cached()}
    except Exception:  # noqa: BLE001
        pass
    result = _mine_check(symbol, name_map=name_map)
    return {"code": 0, "message": "success", "data": result}


class MineBatchRequest(BaseModel):
    """POST /api/financial/mine_batch 的请求体。"""

    symbols: List[str] = Field(..., description="股票代码列表")
    include_blocked_detail: bool = Field(
        True, description="false=被拦截股票只返回代码+理由, 减小响应体积")


@app.post("/api/financial/mine_batch")
def post_financial_mine_batch(req: MineBatchRequest) -> Dict[str, Any]:
    """批量财务排雷 (Layer1 预过滤入口)。

    与 screen_pool 内嵌排雷的区别: 本接口在技术粗筛**之前**对整个自选股池
    做一次性垃圾股剔除, 输出 clean/blocked 两份清单, 让后续 czsc 扫描与
    AI 精选只处理"干净标的", 节省算力与 Token。
    数据源为季频财务缓存 (TTL 7 天), 重复调用近零成本; 单只异常默认放行。
    """
    if not _MINE_AVAILABLE:
        raise HTTPException(status_code=503, detail="mine_sweeper 不可用 (需 akshare)")
    if not req.symbols:
        raise HTTPException(status_code=400, detail="symbols 不能为空")
    if len(req.symbols) > _SCREEN_MAX_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail=f"单次最多 {_SCREEN_MAX_SYMBOLS} 只, 当前 {len(req.symbols)} 只")

    valid: List[str] = []
    errors: List[Dict[str, str]] = []
    for s in req.symbols:
        try:
            sym = _validate_symbol(str(s))
            if sym not in valid:
                valid.append(sym)
        except HTTPException as exc:
            errors.append({"symbol": str(s), "error": str(exc.detail)})

    name_map: Dict[str, str] = {}
    try:
        name_map = {item["symbol"]: item.get("name", "") for item in _stock_list_cached()}
    except Exception:  # noqa: BLE001 - 名称映射失败退化为逐个查询/放行
        pass

    started = time.monotonic()
    sweep = _mine_sweep(valid, name_map=name_map)
    elapsed = round(time.monotonic() - started, 2)
    logger.info("mine_batch: requested=%d clean=%d blocked=%d elapsed=%.2fs",
                len(valid), len(sweep["clean"]), len(sweep["blocked"]), elapsed)
    if not req.include_blocked_detail:
        sweep["blocked"] = [{"symbol": b["symbol"], "reasons": b["reasons"]}
                            for b in sweep["blocked"]]
    return {
        "code": 0,
        "message": "success",
        "data": {
            "total_requested": len(req.symbols),
            "total_checked": len(valid),
            "clean_count": len(sweep["clean"]),
            "blocked_count": len(sweep["blocked"]),
            "elapsed_seconds": elapsed,
            "clean_symbols": [c["symbol"] for c in sweep["clean"]],
            "clean": sweep["clean"],
            "blocked": sweep["blocked"],
            "errors": errors,
        },
    }


# ---------------------------------------------------------------------------
# A股特色情绪接口 (龙虎榜席位分类 + 主力资金流向, 供 Eino Tool 消费)
# ---------------------------------------------------------------------------

@app.get("/api/sentiment/fund_flow")
def get_sentiment_fund_flow(
    symbol: str = Query(..., description="6位股票代码"),
    days: int = Query(10, ge=3, le=30, description="回看交易日数"),
) -> Dict[str, Any]:
    """主力资金净流入趋势: 近5日合计/趋势方向/连续流入天数。"""
    if not _SENTIMENT_AVAILABLE:
        raise HTTPException(status_code=503, detail="sentiment 模块不可用 (需 akshare)")
    symbol = _validate_symbol(symbol)
    result = _fund_flow_trend(symbol, days=days)
    if "error" in result:
        return {"code": 0, "message": "failed", "symbol": symbol, "data": result}
    return {"code": 0, "message": "success", "symbol": symbol, "data": result}


@app.get("/api/sentiment/lhb")
def get_sentiment_lhb(
    symbol: str = Query(..., description="6位股票代码"),
    lookback_days: int = Query(30, ge=5, le=90, description="回看自然日数"),
) -> Dict[str, Any]:
    """龙虎榜上榜记录 + 席位三分法 (机构/游资/其他) 净额聚合。"""
    if not _SENTIMENT_AVAILABLE:
        raise HTTPException(status_code=503, detail="sentiment 模块不可用 (需 akshare)")
    symbol = _validate_symbol(symbol)
    result = _lhb_analysis(symbol, lookback_days=lookback_days)
    if "error" in result:
        return {"code": 0, "message": "failed", "symbol": symbol, "data": result}
    return {"code": 0, "message": "success", "symbol": symbol, "data": result}


@app.get("/api/sentiment/summary")
def get_sentiment_summary(
    symbol: str = Query(..., description="6位股票代码"),
    include_lhb: bool = Query(True, description="是否合并龙虎榜信息"),
) -> Dict[str, Any]:
    """资金流+龙虎榜合并的 LLM 友好一句话摘要 (Eino SentimentTool 主入口)。"""
    if not _SENTIMENT_AVAILABLE:
        raise HTTPException(status_code=503, detail="sentiment 模块不可用 (需 akshare)")
    symbol = _validate_symbol(symbol)
    result = _sentiment_summary(symbol, include_lhb=include_lhb)
    return {"code": 0, "message": "success", "symbol": symbol, "data": result}


# ---------------------------------------------------------------------------
# 启动入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    try:
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
    finally:
        # 服务退出时尽量释放 baostock 会话
        with _bs_lock:
            _bs_logout_quietly()
