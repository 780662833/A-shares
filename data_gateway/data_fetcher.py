"""防弹衣数据获取层: 本地 SQLite 行情缓存 + BaoStock(主) / AKShare(备) 多源自动降级。

设计目标 (架构师"防弹衣"方案):
    1. 绝不重复请求 —— 日线数据每天收盘后只变一次, 拉取一次即持久化到
       local_cache.db; 当日 15:30 之后已入库的数据直接读本地, 零网络请求。
    2. 多源自动降级 —— 主源 BaoStock 失败/超时自动切备用源 AKShare;
       两源都挂时兜底返回缓存中最近一天的数据并打 is_stale_data 标记,
       保证下游 czsc 计算不会因为"今天拉不到数据"而崩溃。
    3. 列名归一 —— 无论来自哪个源, 对外统一输出 AKShare 中文列
       (日期/开盘/收盘/最高/最低/成交量/成交额), 与 akshare_to_czsc_bars
       及既有测试夹具完全兼容, 上层转换代码零改动。

缓存布局 (local_cache.db, 线程安全 WAL):
    daily_kline_cache(symbol, date, open, high, low, close, volume, amount,
                      source, update_time, PRIMARY KEY(symbol, date))

新鲜度约定:
    "该股票请求窗口末端日期的 K 线已存在且 update_time >= 今日 15:30" 才算
    命中; 盘中请求昨日及更早的历史部分同样可以直接命中 (历史K线不可变)。
    注意 qfq 前复权在除权日会导致历史整体平移, 全量 refresh 会覆盖重写,
    MVP 阶段接受此近似 (与 czsc_cache 的约定一致)。
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger("data_gateway.data_fetcher")

_DATE_FMT = "%Y%m%d"
_CN_COLS = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额"]

# 收盘数据"可信时刻": 当日 15:30 之后写入的缓存视为最终数据, 不再回源
_SETTLE_HOUR, _SETTLE_MINUTE = 15, 30

_DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache", "local_cache.db")


def _now() -> datetime:
    """统一取当前时间 (测试可通过 monkeypatch data_fetcher._now 冻结时钟)。"""
    return datetime.now()


def _last_workday_on_or_before(date_compact: str) -> Optional[str]:
    """<= date_compact(YYYYMMDD) 的最近工作日 (周一至周五), 非法日期返回 None。"""
    try:
        d = datetime.strptime(date_compact, _DATE_FMT)
    except ValueError:
        return None
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime(_DATE_FMT)


class LocalKlineCache:
    """日线行情的本地 SQLite 缓存 (WAL + 可注入连接, 便于内存模式单测)。"""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._lock = threading.Lock()
        if db_path == ":memory:":
            # 内存模式: 同一连接共享给本实例的所有操作 (仅单测使用)
            self._conn: Optional[sqlite3.Connection] = sqlite3.connect(":memory:", check_same_thread=False)
            self._db_path = ":memory:"
        else:
            self._conn = None
            self._db_path = db_path or os.environ.get("LOCAL_KLINE_DB", _DEFAULT_DB)
            parent = os.path.dirname(self._db_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        self._ensure_schema()

    # ------------------------------------------------------------------ core
    def _open(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        conn = sqlite3.connect(self._db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _exec(self, sql: str, params: tuple = ()) -> List[tuple]:
        with self._lock:
            own = self._conn is None
            conn = self._open()
            try:
                rows = conn.execute(sql, params).fetchall()
                if own:
                    conn.commit()
                return rows
            finally:
                if own:
                    conn.close()

    def _ensure_schema(self) -> None:
        self._exec(
            """
            CREATE TABLE IF NOT EXISTS daily_kline_cache (
                symbol      TEXT NOT NULL,
                date        TEXT NOT NULL,   -- YYYY-MM-DD
                open        REAL, high REAL, low REAL, close REAL,
                volume      REAL,            -- 手
                amount      REAL,            -- 元
                source      TEXT,            -- baostock / akshare
                update_time TEXT,            -- ISO8601 写入时间
                PRIMARY KEY (symbol, date)
            )
            """
        )
        self._exec("CREATE INDEX IF NOT EXISTS idx_dkc_symbol ON daily_kline_cache(symbol)")

    # ------------------------------------------------------------------- io
    def upsert_df(self, symbol: str, df: pd.DataFrame, source: str) -> int:
        """把归一为中文列的 DataFrame 批量写入缓存, 返回写入行数。"""
        if df is None or df.empty:
            return 0
        now = datetime.now().isoformat(timespec="seconds")
        rows: List[tuple] = []
        for _, r in df.iterrows():
            try:
                day = str(r["日期"])[:10]
                rows.append((
                    symbol, day,
                    float(r["开盘"]), float(r["最高"]), float(r["最低"]), float(r["收盘"]),
                    float(r["成交量"]), float(r["成交额"]) if pd.notna(r.get("成交额")) else 0.0,
                    source, now,
                ))
            except (TypeError, ValueError, KeyError):
                continue  # 脏行跳过
        if not rows:
            return 0
        with self._lock:
            own = self._conn is None
            conn = self._open()
            try:
                conn.executemany(
                    """INSERT INTO daily_kline_cache
                       (symbol, date, open, high, low, close, volume, amount, source, update_time)
                       VALUES (?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(symbol, date) DO UPDATE SET
                         open=excluded.open, high=excluded.high, low=excluded.low,
                         close=excluded.close, volume=excluded.volume, amount=excluded.amount,
                         source=excluded.source, update_time=excluded.update_time""",
                    rows,
                )
                if own:
                    conn.commit()
            finally:
                if own:
                    conn.close()
        return len(rows)

    def fetch_df(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """按区间读取缓存, 输出中文列 DataFrame (可能为空)。日期参数 YYYYMMDD。"""
        sd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        ed = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        rows = self._exec(
            """SELECT date, open, high, low, close, volume, amount
               FROM daily_kline_cache WHERE symbol=? AND date BETWEEN ? AND ?
               ORDER BY date""",
            (symbol, sd, ed),
        )
        if not rows:
            return pd.DataFrame(columns=_CN_COLS)
        df = pd.DataFrame(
            rows, columns=["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        )
        return df[_CN_COLS]

    def last_cached_day(self, symbol: str, before_or_equal: str) -> Optional[str]:
        """<= end_date(YYYYMMDD) 的最大已缓存交易日, 用于 stale 兜底判断。"""
        ed = f"{before_or_equal[:4]}-{before_or_equal[4:6]}-{before_or_equal[6:]}"
        rows = self._exec(
            "SELECT MAX(date) FROM daily_kline_cache WHERE symbol=? AND date<=?",
            (symbol, ed),
        )
        day = rows[0][0] if rows and rows[0][0] else None
        return str(day) if day else None

    def coverage_for_request(self, symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """评估缓存对本次请求是否"足够新鲜"。

        规则 (对齐架构师要求):
          - 空缓存 -> miss;
          - 若请求末端包含"今天"(或未来), 则要求今日 15:30 之后有写入且缓存
            最大日期 >= 最近一个应落库的日子 —— 简化判定: 缓存最大日期 == 请求
            有效末端日 (min(end_date, today)) 且该 symbol 最新 update_time 在
            今日 15:30 之后 -> fresh; 否则 partial/stale;
          - 若请求末端是过去日期 (历史补数), 只要缓存覆盖了 [start,end] 两端
            之间已存在的交易日即可视为 usable (历史K线不可变)。
        """
        today = _now().strftime(_DATE_FMT)
        effective_end = min(end_date, today)
        max_day = self.last_cached_day(symbol, effective_end)
        if max_day is None:
            return {"status": "miss", "max_day": None}
        compact = max_day.replace("-", "")
        # 交易日历感知: 请求末端落在周末/节假日时, 最近一个交易日即最终数据
        # (与 czsc_cache._cache_is_fresh 的"非交易日命中"约定一致),
        # 否则每个周末 screen_pool 都会对全池无谓回源。
        last_wd = _last_workday_on_or_before(effective_end)
        if last_wd is not None and compact >= last_wd:
            # 缓存已覆盖"最近一个应落库的工作日" -> 原则上就是最终数据。
            # 唯一例外: 请求末端恰好是"今天"(工作日), 而缓存是今日 15:30 之前
            # 写入的盘中半成品时, 必须回源刷新 (见用例8); 末端早于今天, 或
            # 末端是周末/节假日 (回退到周五比较), 历史不可变, 直接 fresh。
            if compact == effective_end == today:
                rows = self._exec(
                    "SELECT MAX(update_time) FROM daily_kline_cache WHERE symbol=?",
                    (symbol,),
                )
                last_update = str(rows[0][0]) if rows and rows[0][0] else ""
                try:
                    lu = datetime.fromisoformat(last_update)
                    cutoff = datetime.combine(
                        _now().date(), dtime(_SETTLE_HOUR, _SETTLE_MINUTE))
                    if lu < cutoff:
                        return {"status": "partial", "max_day": max_day}
                except ValueError:
                    pass
            return {"status": "fresh", "max_day": max_day}
        eff_day = f"{effective_end[:4]}-{effective_end[4:6]}-{effective_end[6:]}"
        rows = self._exec(
            "SELECT MAX(update_time) FROM daily_kline_cache WHERE symbol=?", (symbol,)
        )
        last_update = str(rows[0][0]) if rows and rows[0][0] else ""
        settled_today = False
        try:
            lu = datetime.fromisoformat(last_update)
            cutoff = datetime.combine(_now().date(), dtime(_SETTLE_HOUR, _SETTLE_MINUTE))
            settled_today = lu >= cutoff
        except ValueError:
            pass
        if compact == eff_day:
            # 末端日已落库: 若末端日是今天, 还要求是 15:30 后写入的最终数据;
            # 若末端日是过去的交易日, 历史不可变, 直接 fresh。
            if effective_end < today or settled_today:
                return {"status": "fresh", "max_day": max_day}
            return {"status": "partial", "max_day": max_day}
        return {"status": "stale", "max_day": max_day}

    def mark_stale(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """双源全挂时的兜底: 返回 <= end_date 的最近缓存数据 (可能早于请求末端)。"""
        max_day = self.last_cached_day(symbol, end_date)
        if max_day is None:
            return pd.DataFrame(columns=_CN_COLS)
        # 用 max_day 作为实际末端重新取数 (start 保持请求值)
        ed_compact = max_day.replace("-", "")
        return self.fetch_df(symbol, start_date, ed_compact)


# ===========================================================================
# 多源适配器: 统一输出 AKShare 中文列 DataFrame
# ===========================================================================

def _bs_daily_df(bs_module: Any, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """BaoStock 主源: 日K + 后复权(注: bs 提供 adjustflag 1后复权/2前复权/3不复权)。

    这里用 adjustflag='2' 前复权, 与 AKShare qfq 语义对齐。
    BaoStock 成交量单位为股, AKShare 为手, 统一折算为手 (/100)。
    """
    code = f"sh.{symbol}" if symbol.startswith(("6", "9")) else (
        f"bj.{symbol}" if symbol.startswith(("4", "8")) else f"sz.{symbol}"
    )
    sd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
    ed = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
    rs = bs_module.query_history_k_data_plus(
        code,
        "date,open,high,low,close,volume,amount",
        start_date=sd, end_date=ed, frequency="d", adjustflag="2",
    )
    if rs.error_code != "0":
        raise RuntimeError(f"baostock 查询失败: {rs.error_code} {rs.error_msg}")
    data: List[List[str]] = []
    while rs.next():
        data.append(rs.get_row_data())
    if not data:
        return pd.DataFrame(columns=_CN_COLS)
    df = pd.DataFrame(data, columns=["date", "open", "high", "low", "close", "volume", "amount"])
    for col in ("open", "high", "low", "close", "volume", "amount"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    out = pd.DataFrame({
        "日期": df["date"],
        "开盘": df["open"],
        "收盘": df["close"],
        "最高": df["high"],
        "最低": df["low"],
        "成交量": df["volume"] / 100.0,   # 股 -> 手, 与 AKShare 对齐
        "成交额": df["amount"],
    })
    return out.dropna(subset=["开盘", "收盘", "最高", "最低"]).reset_index(drop=True)


def get_daily_kline_safe(
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    cache: LocalKlineCache,
    throttle: Optional[threading.Thread] = None,  # noqa: ARG001 - 占位说明并发由调用方限速
    bs_login: Optional[Any] = None,
    bs_query: Optional[Any] = None,
    ak_hist: Optional[Any] = None,
    retry_call: Optional[Any] = None,
    allow_live: bool = True,
) -> Dict[str, Any]:
    """防弹衣主入口: 缓存优先 -> BaoStock(主) -> AKShare(备) -> 旧缓存兜底。

    依赖全部可注入 (bs_login/bs_query/ak_hist/retry_call), 默认从 main 模块
    延迟解析, 单元测试可用桩件替换。返回:
        {"df": DataFrame(中文列), "source": "cache|baostock|akshare|stale_cache",
         "is_stale_data": bool, "error": str(可选)}
    """
    # ---- Step 1: 查本地缓存 -------------------------------------------
    cov = cache.coverage_for_request(symbol, start_date, end_date)
    if cov["status"] == "fresh":
        df = cache.fetch_df(symbol, start_date, end_date)
        if not df.empty:
            logger.info("Cache hit for %s [%s,%s] source=%s", symbol, start_date, end_date, cov["status"])
            return {"df": df, "source": "cache", "is_stale_data": False}

    if not allow_live:
        # cache_only 模式 (screen_pool 盘后快速通道): 不回源, 能给多少给多少
        df = cache.fetch_df(symbol, start_date, end_date)
        if not df.empty:
            stale = cov["status"] != "fresh"
            return {"df": df, "source": "stale_cache" if stale else "cache", "is_stale_data": stale}
        return {"df": pd.DataFrame(columns=_CN_COLS), "source": "miss",
                "is_stale_data": False, "error": "cache_miss"}

    # ---- Step 2: 主数据源 BaoStock ------------------------------------
    # ---- Step 3: 备用数据源 AKShare (自动降级) -------------------------
    sources: List[tuple] = []
    if bs_query is not None:
        sources.append(("baostock", lambda: bs_query(symbol, start_date, end_date)))
    if ak_hist is not None:
        sources.append(("akshare", lambda: ak_hist(symbol, "daily", start_date, end_date)))

    last_exc: Optional[Exception] = None
    for name, fn in sources:
        try:
            raw = retry_call(fn) if retry_call is not None else fn()
            if raw is None or raw.empty:
                raise RuntimeError("empty_response")
            n = cache.upsert_df(symbol, raw, name)
            logger.info("Fetching %s from %s ok, cached_rows=%d", symbol, name, n)
            df = cache.fetch_df(symbol, start_date, end_date)
            return {"df": df if not df.empty else raw, "source": name, "is_stale_data": False}
        except Exception as exc:  # noqa: BLE001 - 单源失败降级下一源
            last_exc = exc
            logger.warning("数据源 %s 获取 %s 失败: %s", name, symbol, exc)

    # ---- Step 4: 双源全挂 -> 旧缓存兜底 (打上 is_stale_data) -----------
    stale_df = cache.mark_stale(symbol, start_date, end_date)
    if not stale_df.empty:
        logger.error("双源均失败, %s 返回最近缓存数据 (stale): last_day=%s, err=%s",
                     symbol, stale_df.iloc[-1]["日期"], last_exc)
        return {"df": stale_df, "source": "stale_cache", "is_stale_data": True,
                "error": str(last_exc) if last_exc else "all_sources_failed"}

    return {"df": pd.DataFrame(columns=_CN_COLS), "source": "none",
            "is_stale_data": False,
            "error": f"all_sources_failed_and_no_cache: {last_exc}"}
