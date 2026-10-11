"""czsc 分析结果磁盘缓存 (长期方案 C: 收盘后定时预计算 + 请求读缓存)。

设计目标:
    AKShare 免费接口限速导致 screen_pool 全量扫描耗时数分钟, 且实时联网不稳定。
    本模块把每只股票的 czsc 分析结果落盘为 JSON 文件, 形成三层收益:
      1. 定时任务(每日收盘后)批量预计算 -> 写入缓存;
      2. analyze / screen_pool 请求优先命中当日缓存 -> 毫秒级响应, 不打 AKShare;
      3. 当日缓存缺失时回退"旧缓存 + 增量拉取": 只请求缓存最后一根K线之后的新数据,
         与历史拼接后重新分析 -> 单只股票的网络传输量从 ~500 根降到几根。

缓存布局:
    <cache_dir>/czsc/<period>/<symbol>.json
    内容 = {"schema": 1, "symbol", "period", "trade_date"(最后K线日),
            "window_start", "generated_at", "result"(analyze_czsc 完整输出)}

一致性约定:
    - result.window_end == trade_date (已按交易日对齐, 非交易日跑增量会直接命中);
    - qfq(前复权)在除权除息日会导致历史价格整体平移, 因此增量拼接仅在
      窗口未跨过新的除权日时成立; 网关每天收盘后的全量刷新会覆盖该误差,
      MVP 阶段接受此近似 (详见 refresh 接口注释)。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger("data_gateway.czsc_cache")

_SCHEMA_VERSION = 1
_DATE_FMT_ISO = "%Y-%m-%d"

# 环境变量可覆盖缓存目录, 默认 data_gateway/.cache
_DEFAULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")


def _safe_name(period: str) -> str:
    """period 参与路径拼接, 做一次白名单清洗防目录穿越。"""
    allowed = {"daily", "weekly", "monthly"}
    return period if period in allowed else "daily"


class CzscResultCache:
    """线程安全的 czsc 结果文件缓存 (原子写, 损坏文件视为 miss)。"""

    def __init__(self, base_dir: Optional[str] = None) -> None:
        self.base_dir = base_dir or os.environ.get("CZSC_CACHE_DIR", _DEFAULT_DIR)
        self._lock = threading.Lock()
        # 简单内存索引: key -> (mtime, payload)，避免每次请求都读磁盘
        self._mem: Dict[str, tuple] = {}

    @staticmethod
    def _now_iso() -> str:
        """当前时间戳 (秒级 ISO)。抽成方法便于测试冻结时钟 —— put() 写入的
        generated_at 与 main._cache_is_fresh 读取的 now() 必须同源, 否则跨模块
        时钟不一致会让"当日新鲜"判定漂移 (test_cache_hit_zero_network 根因)。"""
        return datetime.now().isoformat(timespec="seconds")

    # ------------------------------------------------------------------ path
    def _path(self, symbol: str, period: str) -> str:
        d = os.path.join(self.base_dir, "czsc", _safe_name(period))
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{symbol}.json")

    # ------------------------------------------------------------------ get
    def get(self, symbol: str, period: str) -> Optional[Dict[str, Any]]:
        """读取缓存条目; 不存在/损坏/schema 不匹配返回 None。"""
        path = self._path(symbol, period)
        if not os.path.exists(path):
            return None
        try:
            mtime = os.path.getmtime(path)
            key = f"{period}/{symbol}"
            with self._lock:
                hit = self._mem.get(key)
            if hit and hit[0] == mtime:
                return hit[1]
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if not isinstance(obj, dict) or obj.get("schema") != _SCHEMA_VERSION:
                logger.warning("cache entry invalid schema: %s", path)
                return None
            result = obj.get("result")
            if not isinstance(result, dict):
                return None
            entry = {
                "result": result,
                "trade_date": obj.get("trade_date", ""),
                "window_start": obj.get("window_start", ""),
                "window_end": obj.get("trade_date", ""),
                "generated_at": obj.get("generated_at", ""),
                "source": "cache",
            }
            with self._lock:
                self._mem[key] = (mtime, entry)
            return entry
        except Exception as exc:  # noqa: BLE001 - 缓存损坏不能拖垮请求
            logger.warning("cache read failed %s: %s", path, exc)
            return None

    # ------------------------------------------------------------------ put
    def put(
        self,
        symbol: str,
        period: str,
        result: Dict[str, Any],
        window_start: str,
        window_end: str,
    ) -> None:
        """原子写入缓存 (先写 .tmp 再 rename), window_* 为 YYYYMMDD。"""
        path = self._path(symbol, period)
        payload = {
            "schema": _SCHEMA_VERSION,
            "symbol": symbol,
            "period": _safe_name(period),
            "trade_date": _iso(window_end),
            "window_start": _iso(window_start),
            # 落盘窗口末端: get() 会把它回填到 entry["window_end"] 供新鲜度判定。
            # 缺失该字段时 entry.window_end 恒为空 -> _cache_is_fresh 永远 False
            # -> 每次请求都走增量/回源 (test_cache_hit_zero_network 的根因)。
            "window_end": _iso(window_end),
            "generated_at": self._now_iso(),
            "result": result,
        }
        tmp = f"{path}.tmp.{os.getpid()}.{int(time.time()*1000)}"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp, path)
            with self._lock:
                self._mem.pop(f"{payload['period']}/{symbol}", None)
            logger.debug("cache put %s trade_date=%s", path, payload["trade_date"])
        except Exception as exc:  # noqa: BLE001 - 写失败仅降级, 不影响本次响应
            logger.warning("cache write failed %s: %s", path, exc)
            try:
                os.remove(tmp)
            except OSError:
                pass

    # ------------------------------------------------------------------ util
    def stats(self) -> Dict[str, Any]:
        """统计各周期缓存条目数与最新交易日分布 (供 /api/czsc/cache_status 使用)。"""
        out: Dict[str, Any] = {}
        root = os.path.join(self.base_dir, "czsc")
        if not os.path.isdir(root):
            return {"base_dir": self.base_dir, "periods": {}}
        for period in sorted(os.listdir(root)):
            pdir = os.path.join(root, period)
            if not os.path.isdir(pdir):
                continue
            files = [f for f in os.listdir(pdir) if f.endswith(".json")]
            dates: Dict[str, int] = {}
            for fn in files:
                try:
                    with open(os.path.join(pdir, fn), "r", encoding="utf-8") as f:
                        head = json.load(f)
                    td = str(head.get("trade_date", ""))[:10]
                    dates[td] = dates.get(td, 0) + 1
                except Exception:  # noqa: BLE001
                    dates["corrupt"] = dates.get("corrupt", 0) + 1
            out[period] = {"count": len(files), "by_trade_date": dates}
        return {"base_dir": self.base_dir, "periods": out}

    def clear(self, period: Optional[str] = None) -> int:
        """清空缓存 (period=None 全部), 返回删除文件数。调试用。"""
        root = os.path.join(self.base_dir, "czsc")
        removed = 0
        periods = [_safe_name(period)] if period else (
            os.listdir(root) if os.path.isdir(root) else []
        )
        for p in periods:
            pdir = os.path.join(root, p)
            if not os.path.isdir(pdir):
                continue
            for fn in os.listdir(pdir):
                if fn.endswith(".json"):
                    try:
                        os.remove(os.path.join(pdir, fn))
                        removed += 1
                    except OSError:
                        pass
        with self._lock:
            self._mem.clear()
        logger.info("cache cleared period=%s removed=%d", period, removed)
        return removed


def _iso(yyyymmdd: str) -> str:
    """YYYYMMDD -> YYYY-MM-DD; 已是 ISO 或非法格式则原样返回。"""
    s = str(yyyymmdd).strip()
    try:
        return datetime.strptime(s, "%Y%m%d").strftime(_DATE_FMT_ISO)
    except ValueError:
        return s


def _compact(iso: str) -> str:
    """YYYY-MM-DD -> YYYYMMDD。"""
    s = str(iso).strip().replace("-", "")
    return s if len(s) == 8 and s.isdigit() else ""


# 进程内单例 (main.py 与 scheduler.py 共享同一实例)
cache = CzscResultCache()
