# A股 AI 模拟交易系统（ashare-quant）

> Go 调度/交易核心 + Python 数据网关的双进程量化系统。LLM 只有"选股权"，仓位由硬编码风控计算，成交由规则引擎模拟撮合 —— **AI 负责选，代码负责管钱**。

- **模式**：模拟盘（MockBroker），全链路真实行情、真实新闻、真实缠论结构，仅撮合为仿真。
- **周期任务**：工作日四个 cron（动态池更新 → 缓存预热 → 日终复盘 → 次日撮合），支持 HTTP 手动触发调试。
- **技术栈**：Go 1.19+ / Eino (LLM Graph) / GORM+SQLite / robfig-cron；Python 3.12 / FastAPI / AKShare+BaoStock / czsc(缠论) / vectorbt。

---

## 目录

1. [系统架构](#1-系统架构)
2. [每日执行时序](#2-每日执行时序)
3. [核心流水线详解](#3-核心流水线详解)
4. [模块与目录结构](#4-模块与目录结构)
5. [HTTP API 一览](#5-http-api-一览)
6. [数据库表结构](#6-数据库表结构)
7. [配置项（环境变量）](#7-配置项环境变量)
8. [快速开始](#8-快速开始)
9. [容错与降级设计](#9-容错与降级设计)
10. [测试体系](#10-测试体系)
11. [已知坑与FAQ](#11-已知坑与faq)

---

## 1. 系统架构

```
┌─────────────────────────────┐         ┌──────────────────────────────┐
│   Go 主进程 (:8080)          │  HTTP   │  Python 数据网关 (:8000)       │
│   cmd/main.go               │────────►│  data_gateway/main.py         │
│                             │ localhost│                              │
│  · cron 调度器 (4个任务)      │         │  · AKShare/BaoStock 取数       │
│  · Eino 六节点漏斗 Graph     │         │    (全局令牌桶限速+重试)        │
│  · MockBroker 撮合引擎       │         │  · czsc 缠论分析 + 磁盘缓存    │
│  · risk 仓位硬校验           │         │  · SQLite 日线缓存层           │
│  · 手动触发接口 /trigger/*   │         │  · 财务排雷 / 情绪 / 新闻       │
└──────────┬──────────────────┘         │  · 动态自选股池生成(Layer 0.5) │
           │                            └──────────────────────────────┘
           ▼
   SQLite trading.db
   accounts / orders / positions / candidate_pools /
   ai_decision_logs / watchlist_items / stocks / daily_klines
```

**分层职责**

| 层 | 位置 | 职责 | 不做什么 |
|---|---|---|---|
| 调度层 | `cmd/main.go` | cron 注册、流程编排、互斥锁、优雅退出 | 不做业务计算 |
| 决策层 | `internal/graph` | 六节点漏斗：粗筛→资讯→LLM→落库→风控→挂单 | 不直接触网取行情（全部经 Tool→网关） |
| 工具层 | `internal/tool` | 网关 HTTP 封装为 Eino InvokableTool，信封式错误 | 失败不抛 Go error，降级进上下文 |
| 交易层 | `internal/broker` `internal/risk` | 次日开盘撮合、T+1、止损、仓位公式 | 不接受 LLM 给的数量 |
| 数据层 | `data_gateway/*` | 真实行情/新闻/财务/缠论结构与缓存 | 无状态决策 |

---

## 2. 每日执行时序

时区 Asia/Shanghai，周一至周五（cron 表达式均可用环境变量覆盖）：

| 时刻 | 任务 | cron 默认 | 开关 | 说明 |
|---|---|---|---|---|
| **15:00** | ① 动态自选股池更新 | `0 15 * * 1-5` | `ASHARE_DYNAMIC_WATCHLIST=1` | 调 `/api/watchlist/generate`，结果 upsert 进 `watchlist_items` 表 |
| **15:10** | ② czsc 缓存预计算 | `10 15 * * 1-5` | 常开 | 对「symbols」force 全量重拉 K 线→czsc 分析→落盘，把分钟级限速耗时挪出复盘窗口 |
| **15:30** | ③ 日终复盘 | `30 15 * * 1-5` | 常开 | SyncDailyKlines 落库今日行情 → 六节点漏斗 Graph → 生成次日 PENDING 订单 |
| **09:25**(次日) | ④ 开盘撮合 | `25 9 * * 1-5` | 常开 | T+1 结转 → 止损强制扫描 → 逐单按开盘价模拟成交/拒单 |

```
Day N                          Day N+1
15:00 池子更新 ─┐
15:10 缓存预热 ─┼─► 15:30 复盘漏斗 ─► PENDING 订单 ═══► 09:25 撮合成交 ─► 持仓/账户更新
                └──────────────────────────────────────────────┘
                     （09:25 撮合依赖当日 daily_klines 判定一字板）
```

**symbols（扫描边界）的来源**：
- 静态模式（默认）：`ASHARE_WATCHLIST ∪ 当前持仓股`。
- 动态模式：`watchlist_items` 表中 ACTIVE 记录（上限 `ASHARE_WATCHLIST_MAX`，默认 100，持仓股不占配额）∪ 持仓股；表为空时自动回退静态名单，保证首日不断流。
- 持仓股必须始终纳入 —— 次日卖出撮合需要其行情与止损契约。

---

## 3. 核心流水线详解

### 3.1 Layer 0.5：动态自选股池（`data_gateway/watchlist_generator.py`）

四来源合并去重，优先级 `MANUAL > ETF > SECTOR > FUND`：

| 来源 | 逻辑 | 默认参数 |
|---|---|---|
| `SECTOR_LEADER` 行业龙头 | 资金净流入 Top N 板块 → 每板块市值 Top K 成分股 | N=3, K=3 |
| `ETF_MOMENTUM` ETF动量 | 主流行业/跨境 ETF 近 90 日日线 → 20 日涨幅 Top M | M=8（约占池 1/3） |
| `FUND_FLOW` 资金异动 | 当日龙虎榜"机构专用"席位净买入 ≥ 5000 万，剔除 ST 与市值 < 50 亿 | — |
| `MANUAL` 人工白名单 | 调用方传入，Python 只透传标记，**永不被自动清理**，入池前校验 6 位数字代码 | — |

弱依赖设计：任一 AKShare 接口失败仅写入 `warnings` 跳过该步，其余照常返回，接口几乎永不整体失败。

**Go 侧落库（Upsert）**：新命中→INSERT(ACTIVE, 非 MANUAL 设 7 天过期)；已存在且再命中→UPDATE 续期；过期未命中且非 MANUAL→置 INACTIVE（保留历史）；MANUAL/BLACKLIST 不被覆盖；网关失败沿用昨日池。

### 3.2 复盘漏斗：Eino 六节点 Graph（`internal/graph/funnel_graph.go`）

```
START → FetchPool → FetchNews → ManagerDecision → SaveDecision → RiskBudget → PlaceOrders → END
```

| # | 节点 | 输入→输出 | 关键实现 |
|---|---|---|---|
| 1 | **FetchPool** | symbols → 候选池(技术面命中) | ScreenPoolTool POST `/api/czsc/screen_pool`；命中落库 `candidate_pools`（按日期+代码 upsert 幂等） |
| 2 | **FetchNews** | 候选池 → 每股新闻上下文 | goroutine+信号量并发（默认5）调 GET `/api/news`；单只失败写 `{ok:false}` 降级，不阻断整批 |
| 3 | **ManagerDecision** | 池JSON+新闻JSON → BUY/SELL/DROP 决策 | LLM 公募基金经理人设 Prompt（`internal/agent`）；脏输出清洗(stripCodeFence/括号配平)+错误回灌重试+`filterKnownSymbols` 剔除幻觉代码 |
| 4 | **SaveDecision** | 决策 → 留痕 | 落库 `ai_decision_logs`；从 czsc 结构回填止损契约字段 EffectiveStop |
| 5 | **RiskBudget** | 决策 → 定数量/闸门 | GET `/api/market/environment` 大盘闸门(BULL/SIDEWAYS/BEAR，失败按最保守30%)；BUY 数量由 `risk.CalculatePosition` 硬算（LLM 无权给 quantity）；超限降级 OBSERVE；SELL 取可用持仓(T+1) |
| 6 | **PlaceOrders** | 校验后决策 → 次日 PENDING 订单 | 写 `orders` 表，等待次日 09:25 撮合 |

每个节点均有 `xxxWith(...)` 可测试内核（依赖注入），单测无需起服务。

### 3.3 Python 端 screen_pool 过滤链（`main.py`）

```
非法代码隔离(errors) → 缓存优先取 czsc 结构(cache_only 时零联网)
→ _match_screen_rule 技术筛选(有买点 / 有卖点 / Observation 趋势观察)
→ base_filter 流动性拦截(非ST、价≥1元、20日均成交额≥阈值)
→ mine_sweep 财务排雷(ROE/负债率/连亏; 缺数据放行; 只拦已过技术筛的股)
→ 按请求原始顺序归并输出 pool + blocked + errors
```

验收不变式：`len(pool)+len(blocked)+len(errors) == len(symbols)`，字段完整、顺序稳定。

### 3.4 撮合引擎（`internal/broker/mock_broker.go` + `stop_monitor.go`）

`SimulateNextDayOpen(tradeDate)` 一个交易日只跑一遍（trade_date 去重）：

1. **T+1 结转**：昨日买入持仓解锁可卖数量；
2. **EnforceStops 止损扫描**：跳空破止损线强制市价卖出（已在挂单队列/无当日行情/无止损线的分别跳过）；
3. **逐单处理 PENDING**：
   - 加载当日 K 线判一字板：开盘==涨停且买 → REJECTED；开盘==跌停且卖 → REJECTED；
   - 资金/持仓校验（余额不足、可用数量不足 → REJECTED 带原因）；
   - 成交规则：买入滑点 ×1.002、佣金 max(额×0.00025, 5)；卖出滑点 ×0.998、另加印花税 ×0.0005；成交价=开盘价±滑点；
   - 系统性故障（如行情缺失但非风控拒绝）保持 PENDING 待重试，不误杀。

### 3.5 仓位公式（`internal/risk/position.go`）

三重封顶取最小值：

```
数量 = min( 总资产×RiskPerTradePct(1%) ÷ 止损距离 ,
            总资产×MaxPositionPct(20%) ÷ 入场价 ,
            大盘闸门 GlobalMaxPct 剩余空间 )
止损距离保护: (entry−stop)/entry < MinStopDistancePct(1.5%) → 拒绝/降级 OBSERVE
```

---

## 4. 模块与目录结构

```
.
├── cmd/                        # Go 入口
│   ├── main.go                 #   配置加载/建库/cron注册/HTTP触发/优雅退出
│   └── chatmodel.go            #   LLM 装配：已配置→编译漏斗Graph，未配置→降级实现
├── internal/
│   ├── agent/                  # System Prompt（基金经理排雷人设）与决策结构体
│   ├── broker/                 # MockBroker 撮合 + stop_monitor 止损
│   ├── db/                     # SQLite/GORM 初始化（幂等建表+默认账户100万）
│   ├── graph/                  # 六节点漏斗 Graph + order_writer
│   ├── model/                  # 9 张表 Model（显式 TableName）
│   ├── risk/                   # CalculatePosition 仓位公式
│   ├── sync/                   # data_syncer：复盘前落库当日K线
│   └── tool/                   # Eino 工具集（screen_pool/news/market/czsc/walk_forward/watchlist/mock_trading）
├── data_gateway/               # Python FastAPI 网关
│   ├── main.py                 #   23 个端点 + 令牌桶限速 + 重试框架
│   ├── data_fetcher.py         #   日线 SQLite 缓存（BaoStock主/AKShare备源）
│   ├── czsc_engine.py          #   AKBar→RawBar→CZSC：分型/笔/中枢/买卖点
│   ├── czsc_cache.py           #   磁盘缓存 put/get/stats/clear（当日新鲜度）
│   ├── czsc_backtest.py        #   规则回测 + walk-forward
│   ├── mine_sweeper.py         #   财务排雷 + Layer0 流动性过滤
│   ├── sentiment.py            #   资金流向/龙虎榜席位分类/综合情绪（TTL缓存）
│   ├── news.py                 #   东财新闻三级容错链 + 巨潮公告
│   ├── watchlist_generator.py  #   Layer 0.5 动态池四来源生成器
│   ├── requirements.txt        #   锁定版本清单
│   └── test_*.py               #   pytest 用例（httptest 风格离线全链路）
└── docs/TEST_PLAN.md           # L1~L4 四层测试计划与用例清单
```

---

## 5. HTTP API 一览

### 5.1 Python 网关（:8000）

| 分组 | 方法/路径 | 用途 |
|---|---|---|
| 健康 | `GET /health` | 存活探针 |
| 行情 | `GET /api/daily_kline` | 日线（缓存优先，qfq） |
| | `GET /api/stock_list` | 全市场列表 |
| | `GET /api/financial_indicator` | 财务指标 |
| 缠论 | `POST /api/czsc/refresh` | 批量预热缓存（线程池并发，force 全量重拉） |
| | `POST /api/czsc/screen_pool` | **候选池粗筛**（漏斗第1节点的下游） |
| | `GET /api/czsc/analyze` | 单股结构详情 |
| | `GET /api/czsc/cache_status` `DELETE /api/czsc/cache` | 缓存运维 |
| | `POST /api/czsc/backtest/rules` `GET /api/czsc/backtest/{symbol}[/per_signal]` | 规则回测 |
| | `POST /api/czsc/walk_forward/rules` `GET /api/czsc/walk_forward/{symbol}` | 走样验证 |
| 排雷 | `GET /api/financial/mine_check` `POST /api/financial/mine_batch` | 财务体检 |
| 情绪 | `GET /api/sentiment/fund_flow` `/lhb` `/summary` | 资金/龙虎榜/综合 |
| | `GET /api/news` | 个股新闻（三级容错） |
| | `GET /api/market/environment` | 大盘闸门（上证 vs MA20/MA60 → BULL/SIDEWAYS/BEAR） |
| 动态池 | `POST /api/watchlist/generate` | Layer 0.5 池生成 |

### 5.2 Go 手动触发（:8080，调试用；`ASHARE_HTTP_ADDR=off` 禁用）

| 路径 | 等价 cron |
|---|---|
| `POST /trigger/watchlist` | 15:00 动态池更新 |
| `POST /trigger/refresh` | 15:10 缓存预热 |
| `POST /trigger/review` | 15:30 日终复盘 |
| `POST /trigger/simulate` | 次日 09:25 撮合 |
| `GET /healthz` | 存活探针 |

同名流程以进程内互斥锁防并发（cron 与手动触发互斥，后来者直接拒绝）。

---

## 6. 数据库表结构（trading.db）

| 表 | 关键字段 | 角色 |
|---|---|---|
| `accounts` | cash, total_assets | 首次 InitDB 建默认账户，现金 1,000,000 |
| `positions` | symbol, quantity, **available_qty(T+1)**, cost_price, effective_stop | 持仓与止损契约 |
| `orders` | client_seq, side, price, quantity, status(PENDING/FILLED/REJECTED), reject_reason, trade_date | 漏斗产出→撮合消费 |
| `candidate_pools` | trade_date, symbol, rule_hit, czsc 摘要 | 复盘第1节点落库（date+symbol upsert 幂等） |
| `ai_decision_logs` | action, entry, stop, confidence, raw_output, degraded | LLM 决策全留痕审计 |
| `watchlist_items` | symbol, source(MANUAL/SECTOR/ETF/FUND), status(ACTIVE/INACTIVE/BLACKLIST), expire_date | 动态池持久化 |
| `stocks` / `daily_klines` | code/name；OHLC+涨跌停价 | 行情镜像（syncer 复盘前落库，供次日一字板判定） |

Python 网关另有独立存储：日线 SQLite 缓存 + czsc 磁盘缓存 + `.cache/` 新闻/情绪 JSON，互不与 Go 库混写（避免 SQLite 锁冲突，**只有 Go 进程写 trading.db**）。

---

## 7. 配置项（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `ASHARE_DB_PATH` | `./data/trading.db` | SQLite 路径 |
| `ASHARE_GATEWAY_URL` | `http://127.0.0.1:8000` | Python 网关地址 |
| `ASHARE_HTTP_ADDR` | `:8080` | 手动触发服务监听（`off` 禁用） |
| `ASHARE_WATCHLIST` | 空 | 静态候选名单，逗号分隔，如 `000001,600519`（空=仅复盘持仓股） |
| `ASHARE_DYNAMIC_WATCHLIST` | 关 | `=1` 启用动态池模式（watchlist_items 为 symbols 主来源） |
| `ASHARE_WATCHLIST_MAX` | `100` | 动态池参与粗筛的最大股票数（持仓股不占配额） |
| `ASHARE_CRON_REFRESH` | `10 15 * * 1-5` | 缓存预热 cron（可覆盖） |
| `ASHARE_CRON_WATCHLIST` | `0 15 * * 1-5` | 动态池更新 cron（可覆盖） |
| `ASHARE_LLM_BASE_URL` / `ASHARE_MODEL` / `ASHARE_API_KEY` | — | LLM 三件套；未配置则复盘走降级实现 |

> 注：复盘 `30 15 * * 1-5`、撮合 `25 9 * * 1-5` 目前为代码内默认值（`CronReview`/`CronSimul`），仅 refresh/watchlist 两项支持环境变量覆盖。

---

## 8. 快速开始

### 8.1 启动 Python 数据网关

```bash
cd data_gateway
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 127.0.0.1 --port 8000
# 验证: curl http://127.0.0.1:8000/health
```

### 8.2 启动 Go 主程序

```bash
export ASHARE_WATCHLIST="000001,600519,300750"     # 或启用动态池:
export ASHARE_DYNAMIC_WATCHLIST=1
export ASHARE_LLM_BASE_URL=https://... ASHARE_MODEL=... ASHARE_API_KEY=...
go run ./cmd
```

### 8.3 正式环境首日验收顺序

```bash
curl -X POST localhost:8080/trigger/watchlist   # ① 动态池生成+落库（动态模式）
curl -X POST localhost:8080/trigger/refresh     # ② czsc 预热（分钟级，看 stats 命中率）
curl -X POST localhost:8080/trigger/review      # ③ 复盘漏斗 → 检查 PENDING 订单
# 次日 09:25 自动撮合，或调试: curl -X POST localhost:8080/trigger/simulate
```

日志核对要点：`[watchlist] applied ... generated/inserted/updated/inactivated`；`[refresh] symbols=N`；`[review] fetch_pool=N → fetch_news N/N → decisions → exec_report`；`[simulate] FILLED/REJECTED + 账户资金快照`。

---

## 9. 容错与降级设计

| 层级 | 机制 |
|---|---|
| 数据源 | BaoStock 主源 + AKShare 备源；全局令牌桶限速防封；指数退避重试；**确定性异常识别后放弃重试**（如 pyarrow ArrowInvalid `\u` 转义 bug，不再空耗 ~10s） |
| 新闻 | 三级容错链：akshare 原生 → **猴补丁通道**（运行时改写 akshare bug 行 `r"\u3000"` 后 exec，列名/排序与上游 100% 一致）→ 直连东财自建 DataFrame；三级全败才返回 `{error}` |
| 网关 | 单股失败只记 `errors` 不阻断整批扫描；czsc 未安装返回 503 明确提示；screen_pool 保证 pool+blocked+errors 守恒 |
| Graph | 工具失败在信封内降级为上下文文本；LLM 脏输出清洗+错误回灌重试；幻觉代码剔除；仓位算不出降级 OBSERVE 留痕 |
| 动态池 | 生成失败沿用现有池子；watchlist_items 表空自动回退静态名单 |
| 撮合 | 事务失败保 PENDING 不误杀；trade_date 去重防重复撮合；T+1 可用数量硬校验 |
| 调度 | `SkipIfStillRunning` + `Recover`；cron 与手动触发进程内互斥；SIGINT/SIGTERM 优雅退出（停 cron→关 HTTP→关 DB） |

---

## 10. 测试体系（详见 `docs/TEST_PLAN.md`）

| 层级 | 范围 | 命令 |
|---|---|---|
| L1 Go 单测 | model/db/broker/tool/agent/graph（httptest+临时sqlite，`xxxWith` 内核注入） | `go test ./...` |
| L1 Python 单测 | 网关各模块离线全链路（monkeypatch `_fetch_*`） | `cd data_gateway && pytest test_gateway.py test_data_fetcher.py` |
| L2 集成 | 网关↔Go 工具契约一致性、故障注入 500 不崩溃 | 双进程起真实 uvicorn 后 `go test` |
| L3 E2E | 复盘→PENDING→撮合→账实相符闭环 | mock ChatModel + 真实 DB |
| L4 调度冒烟 | cron 注册、/trigger/* 手动触发、SIGTERM 退出 | 见 §8.3 |
| 真实源集成 | 新闻真数据（可选门控） | `ASHARE_NEWS_REAL_TEST=1 go test ./internal/tool/ -run NewsRealDataSource` |

**缺陷分级**：P0=资损/数据错乱(必修) · P1=功能不可用(本轮必修) · P2=体验/健壮性(可排期)。

---

## 11. 已知坑与 FAQ

**Q1：日志出现 `ArrowInvalid: invalid escape sequence: \u`？**
pandas 3.x 默认 Arrow 字符串后端（RE2 引擎）不支持 `\u` 转义，撞上 akshare `stock_news_em` 的 `r"\u3000"` 写法。本仓库已通过"确定性错误快速失败 + 猴补丁 + 直连兜底"三级容错恢复数据（日志应看到 `已走直连兜底通道恢复数据` INFO 行）。想彻底消除 ERROR 噪音：`pip install -U akshare`（新版已修复该行）。

**Q2：为什么 screen_pool 要我传 symbols？**
symbols 不是人工输入，是 Go 调度层自动收集的**任务边界**（动态池或静态名单∪持仓），用于保护免费接口限流、解耦计算范围。日常只需维护 `ASHARE_WATCHLIST` 或开启动态池。

**Q3：两个进程会抢 SQLite 吗？**
不会。Go 独占写 trading.db；Python 网关用自己的行情缓存库和磁盘缓存文件。

**Q4：改了 ASHARE_WATCHLIST 何时生效？**
重启 Go 进程生效（配置在启动时读取）。动态模式下改的是 `watchlist_items` 表，次日 15:00 池更新后生效。

**Q5：复盘跑了但没有任何 PENDING 订单？**
依次查：`candidate_pools` 是否为空（缓存未预热→先跑 /trigger/refresh）→ `ai_decision_logs` 中 LLM 是否全 DROP → RiskBudget 是否因大盘 BEAR 闸门/止损距离不足降级 OBSERVE（reject_reason 有留痕）。

**Q6：requirements.txt 装不上 pandas==3.0.6？**
按发布渠道核对可用版本；本仓库新闻模块已同时兼容 object 后端与 Arrow 后端两种情况。

---

*文档与代码同步基线：`main@6b4e808`（动态自选股池 Layer 0.5 接入调度链）。*
