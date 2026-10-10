# A股日线 AI 模拟交易系统 — 测试计划

版本: v1.0 | 制定日期: 2026-10-10 | 状态: 执行中

## 0. 总览与准入/准出标准

| 层级 | 范围 | 工具 | 通过标准 |
|---|---|---|---|
| L1 单元测试 | internal/model, db, broker, tool, agent, graph | go test + httptest + sqlite(临时文件) | 全部 PASS，核心业务规则重点覆盖 |
| L2 集成测试 | Python 网关 ↔ Go 工具链 | uvicorn + go test | 三接口契约字段一致；故障注入返回 500 不崩溃 |
| L3 端到端 (E2E) | Graph 全链路：复盘→PENDING→撮合→账户更新 | mock ChatModel + 真实 DB + 真实撮合引擎 | 一轮完整闭环后订单/持仓/资金三者账实相符 |
| L4 调度冒烟 | cmd/main.go cron 注册、HTTP 触发、优雅退出 | 手动触发接口 + 信号注入 | /trigger/review、/trigger/simulate 可用；SIGTERM 干净退出 |

**缺陷分级**: P0=资损/数据错乱(必须修) P1=功能不可用(本轮必修) P2=体验/健壮性(可排期)

---

## 1. L1 单元测试用例清单

### 1.1 internal/db (init)
- T1.1.1 InitDB 建表成功，五张表存在
- T1.1.2 首次初始化生成默认 Account，现金=1,000,000
- T1.1.3 重复 InitDB 幂等（不重复插入账户、不报错）

### 1.2 internal/broker (MockBroker) —— 最高优先级
- T1.2.1 买入成交：滑点价=开盘×1.002；佣金=max(成交额×0.00025, 5)；现金扣减=成交额+佣金；Position 建仓，可用数量=0（T+1）
- T1.2.2 卖出成交：滑点价=开盘×0.998；费用=佣金+印花税(×0.0005)；现金增加；持仓减少
- T1.2.3 T+1 拒单：当日买入(可用=0)挂 SELL → REJECTED
- T1.2.4 一字涨停买不进：开盘==涨停价 且委托价>=涨停价 → REJECTED
- T1.2.5 一字跌停卖不出：开盘==跌停价 且委托价<=跌停价 → REJECTED
- T1.2.6 资金不足 → REJECTED，且不产生部分成交
- T1.2.7 非整手数量(如150股) → REJECTED
- T1.2.8 无当日 K线数据 → REJECTED（不得 panic）
- T1.2.9 多笔 PENDING 批量撮合，账户余额逐笔正确累计
- T1.2.10 FILLED 订单不可被二次撮合（幂等）

### 1.3 internal/tool
- T1.3.1 MarketDataTool.Info()：名称/描述/参数 Schema 合法
- T1.3.2 MarketDataTool.Invoke：httptest 模拟网关正常返回 → JSON 含 date/open/close/high/low/volume/amount
- T1.3.3 非法 symbol / 错误日期格式 → 明确错误信息
- T1.3.4 网关宕机/超时 → 返回 error 而非 panic
- T1.3.5 MockTradingTool：buy 指令 → orders 表新增 PENDING，回执文案正确
- T1.3.6 MockTradingTool：sell 指令同样落库 PENDING（不做实时撮合）
- T1.3.7 quantity 非整手 / direction 非法值 → 拒绝写入

### 1.4 internal/agent (prompts)
- T1.4.1 ParseTradePlans：裸 JSON 数组解析成功
- T1.4.2 ```json 围栏包裹解析成功
- T1.4.3 JSON 前后夹杂自然语言 → 截取解析成功
- T1.4.4 完全非 JSON → 返回 error（供重试）
- T1.4.5 空数组 [] → 合法"明日不交易"
- T1.4.6 direction 大小写/中英文变体归一化

### 1.5 internal/graph (四节点)
- T1.5.1 FetchData：调用行情工具并把账户/持仓事实注入 state
- T1.5.2 AnalyzeAndPlan：mock ChatModel 返回计划 → state.plans 正确
- T1.5.3 RiskCheck：单票仓位>20% → 该项剔除并记录原因
- T1.5.4 RiskCheck：卖出数量>可用持仓 → 剔除
- T1.5.5 RiskCheck：总仓位>80% → 后续买入项截断
- T1.5.6 ExecutePlan：通过的项全部落库为 PENDING
- T1.5.7 Graph 编译：拓扑 START→Fetch→Analyze→Risk→Exec→END 合法

## 2. L2 集成测试
- T2.1 uvicorn 起真实网关 → Go MarketDataTool 拉取 000001 K线，字段对齐
- T2.2 网关 stop 后 Go 侧行为 = 软失败降级，Graph 不中断
- T2.3 /api/stock_list、/api/financial_indicator 响应结构快照校验

## 3. L3 E2E 场景剧本
- S1 基线: 初始 100 万 → 复盘产出买入计划 → 次日撮合 → 校验持仓/现金/冻结
- S2 止损: 已有持仓+盈利 → 计划卖出 → 撮合后现金回笼、印花税扣除正确
- S3 极端: 候选池含一字板股票 → 复盘风控拦截或撮合拒单，账本无损
- S4 空仓日: LLM 输出 [] → 无新订单，系统正常收尾

## 4. L4 调度冒烟
- T4.1 cron 表达式注册成功（日志确认两条任务）
- T4.2 POST /trigger/review 手动触发复盘
- T4.3 POST /trigger/simulate 手动触发撮合
- T4.4 SIGINT/SIGTERM → 优雅退出，无 goroutine 泄漏报错

## 5. 执行顺序与进度追踪

| 阶段 | 内容 | 状态 |
|---|---|---|
| R1 | 环境体检(go build/vet/test 全量基线) | ⏳ |
| R2 | 修复既有 FAIL（broker/db 遗留） | ⏳ |
| R3 | L1 补齐缺失单测（按 1.2→1.3→1.4→1.5 优先级） | ⏳ |
| R4 | L2 集成（Python↔Go 契约） | ⏳ |
| R5 | L3 E2E 四剧本 | ⏳ |
| R6 | L4 调度冒烟 | ⏳ |
| R7 | 汇总报告 + 提交 git | ⏳ |
