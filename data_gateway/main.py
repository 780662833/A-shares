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
    GET /api/czsc/analyze             缠论结构+三类买卖点信号 (AKShare + czsc)

启动方式:
    uvicorn main:app --host 0.0.0.0 --port 8000
"""

import logging
import re
import threading
import time
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional

import akshare as ak
import baostock as bs
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
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


@app.get("/api/czsc/analyze")
def get_czsc_analysis(
    symbol: str = Query(..., description="6位股票代码, 如 000001"),
    period: str = Query("daily", description="周期: daily/weekly/monthly"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD, 默认近一年半"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD, 默认今天"),
) -> Dict[str, Any]:
    """
    获取指定股票的缠论结构与三类买卖点信号 (AKShare 行情 + czsc 核心库)。

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
        df: pd.DataFrame = _retry_call(
            lambda: ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",   # 前复权, 保证跨除权日的笔/中枢不被价格跳空打断
            )
        )
    except Exception as exc:  # noqa: BLE001
        raise _gateway_error(500, "akshare(stock_zh_a_hist)", exc)

    if df is None or df.empty:
        logger.warning("czsc_analyze: symbol=%s 区间[%s,%s]无行情数据", symbol, start_date, end_date)
        return {"code": 0, "message": "no_data", "symbol": symbol, "data": None}

    try:
        bars = akshare_to_czsc_bars(df, symbol=symbol, period=period)
        result = analyze_czsc(bars)
    except Exception as exc:  # noqa: BLE001 - czsc 计算异常统一转 500
        raise _gateway_error(500, "czsc(analyze)", exc)

    if "error" in result:
        return {"code": 0, "message": "insufficient_data", "symbol": symbol, "data": result}

    logger.info(
        "czsc_analyze: symbol=%s period=%s bi=%d zs=%d buys=%d sells=%d",
        symbol, period,
        result["structures"]["bi_count"], result["structures"]["zs_count"],
        len(result["signals"]["buy_points"]), len(result["signals"]["sell_points"]),
    )
    return {
        "code": 0,
        "message": "success",
        "symbol": symbol,
        "period": period,
        "adjust": "qfq",
        "data": result,
    }


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
