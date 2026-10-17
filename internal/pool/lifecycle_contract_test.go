package pool

// ---------------------------------------------------------------------------
// 第 3.4.4 步（合同冻结轮）：生命周期转换回归测试。
// 本文件只新增测试, 不修改任何生产代码; 断言依据已冻结的转换合同:
//   - OK            : 命中续期; 不再命中 -> 保留/到期(自然日截止), 绝不"增加未命中计数";
//   - SUCCESS_EMPTY : 唯一允许把"本次没有命中"计为确认未命中的状态;
//   - PARTIAL/FAILED/STALE/未知: 不增加未命中计数、不批量删除旧证据;
//   - MANUAL        : PrimarySource==MANUAL **或 Sources 含 MANUAL 证据**都不得被自动生命周期降级/删除;
//   - 未知数据日期(data_as_of) : 当前快照无该字段(3.4.3 缺失) -> 基于数据日期的衰减必须保持禁用,
//     本文件用"防误启哨兵"锁定该边界: 若未来引入 DataAsOf 而记录该字段为空, Transition 不得据此过期;
//   - 幂等          : 同一 run_id 重试不重复计数——持久层唯一键 (run_id,symbol,source) + HitThisRun 门控。
// 预期结果如实记录: 已知缺陷用例(合同冲突点)会 FAIL, 这是合同锁, 不是待掩盖的噪音。
// ---------------------------------------------------------------------------

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"
)

// mkManualEvidenceRec 构造一条主来源为 SECTOR、但 Sources 中含 MANUAL 证据的记录。
func mkManualEvidenceRec() AggEntry {
	r := mkRec(sourceSector)
	r.Sources = []SourceHit{
		{SourceType: sourceETF, Status: StatusOK},
		{SourceType: sourceManual, SectorName: "人工自选", Status: StatusOK},
		{SourceType: sourceSector, Status: StatusOK},
	}
	return r
}

// 合同行 1+2: OK/SUCCESS_EMPTY 与未命中计数。
// 当前快照无 MissCount 字段(见 TestLifecycleContract_NoMissCounterFieldExists),
// 因此这两行的可观测代理是"是否进入过期/清除路径":
//   - OK + persistedHit=false 且未过 ExpireDate -> LifeKeep, 记录原样(不算未命中、不清除);
//   - SUCCESS_EMPTY + persistedHit=false 且已过 ExpireDate -> LifeExpire(确认未命中的既定处理)。
//
// 注意现状偏差(合同冲突点 A): SUCCESS_EMPTY 命中时走 TOUCH 分支刷新 LastSeenAt/ExpireDate,
// 与 OK 完全共用——按冻结合同, SUCCESS_EMPTY 语义是"请求成功但无命中", 不应视为有效命中续期。
// 该子用例当前应失败, 修复前禁止改动其他用例来凑绿。
func TestTransitionContract_OKAndSuccessEmpty(t *testing.T) {
	t.Run("OK命中->续期且不落未命中分支", func(t *testing.T) {
		out, act := Transition(mkRec(sourceSector), true, StatusOK, 7, testNow)
		if act != LifeTouch || out.SourceLifecycle != "ACTIVE" {
			t.Fatalf("act=%s lc=%s want TOUCH/ACTIVE", act, out.SourceLifecycle)
		}
	})
	t.Run("OK不再命中未到期->保留不计数", func(t *testing.T) {
		rec := mkRec(sourceSector)
		fut := testNow.AddDate(0, 0, 3)
		rec.ExpireDate = &fut
		out, act := Transition(rec, false, StatusOK, 7, testNow)
		if act != LifeKeep || out.SourceLifecycle != "ACTIVE" {
			t.Fatalf("act=%s lc=%s want KEEP/ACTIVE", act, out.SourceLifecycle)
		}
	})
	t.Run("SUCCESS_EMPTY不再命中已过截止->确认未命中允许过期", func(t *testing.T) {
		rec := mkRec(sourceSector) // ExpireDate=昨天, 已到期
		_, act := Transition(rec, false, StatusSuccessEmpty, 7, testNow)
		if act != LifeExpire {
			t.Fatalf("act=%s want EXPIRE (SUCCESS_EMPTY 是唯一允许的自然过期入口)", act)
		}
	})
	t.Run("SUCCESS_EMPTY命中->不得伪装成有效命中续期(合同冲突点A)", func(t *testing.T) {
		rec := mkRec(sourceSector)
		past := testNow.AddDate(0, 0, -1)
		rec.ExpireDate = &past
		out, act := Transition(rec, true, StatusSuccessEmpty, 7, testNow)
		// 冻结合同: SUCCESS_EMPTY = 请求成功但合法无命中。对某条既有记录而言,
		// "本轮来源成功但该证券不在结果中"才是真实语义; 若调用方声称 persistedHit=true,
		// 那属于输入矛盾, 状态机不得把它当成 OK 命中一样无条件延长有效期。
		// 期望保守行为: 保留原状(KEEP), 由接入层的 persistedHit 计算保证一致性。
		if act == LifeTouch && out.ExpireDate.After(past) {
			t.Errorf("SUCCESS_EMPTY must not silently refresh expire date like OK hit (got %s, expire moved to %s)",
				act, out.ExpireDate.Format("2006-01-02"))
		}
	})
}

// 合同行 3: PARTIAL / FAILED / STALE / 未知 —— 一律不增加未命中计数、不清除旧证据。
// 关键反例锁定: 这些状态下绝不允许出现 LifeExpire, 也不得改写 LastSeenAt/ExpireDate。
func TestTransitionContract_DegradedStatesNeverClear(t *testing.T) {
	for _, st := range []SourceStatus{StatusPartial, StatusFailed, StatusStale, SourceStatus("WEIRD")} {
		name := string(st)
		if name == "" {
			name = "<empty>"
		}
		t.Run(name+"不再命中->禁止过期/禁止改锚点", func(t *testing.T) {
			rec := mkRec(sourceSector)
			past := testNow.AddDate(0, 0, -1)
			rec.ExpireDate = &past // 即使已过自然日截止
			before := rec.LastSeenAt
			out, act := Transition(rec, false, st, 7, testNow)
			if act == LifeExpire {
				t.Fatalf("%s must never trigger EXPIRE (degraded source is not a confirmed miss)", st)
			}
			if !out.LastSeenAt.Equal(before) {
				t.Errorf("%s must not move last_seen_at", st)
			}
			if out.ExpireDate != nil && !out.ExpireDate.Equal(past) {
				t.Errorf("%s must not rewrite expire_date", st)
			}
			if out.SourceLifecycle != "STALE" {
				t.Errorf("%s lifecycle=%s want STALE(保留记录禁止当有效信号)", st, out.SourceLifecycle)
			}
		})
	}
	// PARTIAL + 已验证分片命中 -> 允许正常续期(3.4.1 契约: 已成功分片的记录可刷新)。
	t.Run("PARTIAL已验证命中->续期", func(t *testing.T) {
		out, act := Transition(mkRec(sourceSector), true, StatusPartial, 7, testNow)
		if act != LifeTouch || out.SourceLifecycle != "ACTIVE" {
			t.Fatalf("act=%s lc=%s want TOUCH/ACTIVE", act, out.SourceLifecycle)
		}
	})
}

// 合同行 4: MANUAL 保护必须覆盖"多来源记录含 MANUAL 证据"的情形。
// 现状缺陷(合同冲突点 B): Transition 只检查 PrimarySource==MANUAL;
// 聚合后 PrimarySource 取最高优先级来源, 若 MANUAL 不在首位(例如上游 Entry 缺失导致
// Sources 顺序异常, 或未来优先级调整), 该记录会被当作纯自动来源参与过期。
// 端到端补充: ApplyWatchlistPool 的 inactivate sweep 只看 DB 行级 source_type,
// 多来源证据在 watchlist_items 单列模型下无法表达——登记为持久化缺口(见交付报告)。
func TestTransitionContract_ManualEvidenceProtection(t *testing.T) {
	t.Run("PrimarySource=MANUAL->PROTECT(现状已满足)", func(t *testing.T) {
		_, act := Transition(mkRec(sourceManual), false, StatusOK, 7, testNow)
		if act != LifeProtect {
			t.Fatalf("act=%s want PROTECT", act)
		}
	})
	t.Run("Sources含MANUAL证据即使主来源非MANUAL->禁止过期(合同冲突点B)", func(t *testing.T) {
		rec := mkManualEvidenceRec()
		past := testNow.AddDate(0, 0, -1)
		rec.ExpireDate = &past
		out, act := Transition(rec, false, StatusSuccessEmpty, 7, testNow)
		if act == LifeExpire || out.SourceLifecycle == "EXPIRED" {
			t.Errorf("record carrying MANUAL evidence must never be expired by auto lifecycle, got act=%s lc=%s",
				act, out.SourceLifecycle)
		}
	})
}

// 合同行 5: 未知数据日期不得触发时间衰减。
// 当前快照没有 data_as_of / DataAsOf 字段(3.4.3 成果不在本工作区), 因此:
//   - 先证明字段确实不存在(防误启哨兵的前提);
//   - 再锁定"LastSeenAt 为零值(新鲜度锚点未知)时, Transition 必须诚实 STALE 而非伪造过期/永久保留"。
//
// 未来接入 DataAsOf 时, 本哨兵会强制作者处理"空 data_as_of 不参与衰减"这一分支。
func TestLifecycleContract_NoMissCounterFieldExists(t *testing.T) {
	// 防误启哨兵: 本阶段冻结"不新建第二套计数器"。AggEntry 上若出现 MissCount/DataAsOf 字段,
	// 说明有人引入了新计数机制或数据日期字段, 必须先回到合同评审而不是直接让测试变绿。
	_ = AggEntry{} // 结构体存在性检查占位

	// 使用 go/ast 解析 AggEntry 的**结构体字段标识符**, 逐个精确比较, 不使用
	// strings.Contains 子串匹配——后者会被注释文本、既有财务快照字段 FinDataAsOf
	// (aggregate.go L158, json:"fin_data_as_of", 属阶段6功能, 并非独立 DataAsOf)
	// 以及缩进/换行差异误命中, 造成哨兵假阳性。
	fields := aggEntryFieldNames(t)
	if len(fields) == 0 {
		t.Fatal("防误启哨兵失效: 未能从 aggregate.go 解析出 AggEntry 结构体字段")
	}
	for _, name := range fields {
		if name == "MissCount" {
			t.Error("AggEntry 出现 MissCount 字段: 违反『不另建第二套计数机制』合同, 需先评审")
		}
		if name == "DataAsOf" {
			t.Error("AggEntry 出现 DataAsOf 字段但本工作区未包含 3.4.3 的新鲜度合同实现, 需先对齐后再启用衰减")
		}
	}
	// 反向自检: 解析器必须能看到已知存在的生命周期字段, 否则说明 AST 提取范围错误,
	// 哨兵将退化为恒真(永远看不到任何字段)。
	seen := map[string]bool{}
	for _, name := range fields {
		seen[name] = true
	}
	for _, must := range []string{"HitThisRun", "SourceLifecycle", "ExpireDate", "FinDataAsOf"} {
		if !seen[must] {
			t.Fatalf("防误启哨兵失效: AST 未解析出已知字段 %s(提取范围错误), 共解析到 %d 个字段", must, len(fields))
		}
	}
}

// aggEntryFieldNames 用 go/parser + go/ast 从 aggregate.go 提取 AggEntry 结构体的
// 全部字段标识符(仅顶层字段名, 不含注释/JSON tag/其他类型)。
func aggEntryFieldNames(t *testing.T) []string {
	t.Helper()
	fileName := "aggregate.go"
	if _, err := os.Stat(fileName); err != nil {
		t.Skipf("source file unavailable: %v", err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, fileName, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", fileName, err)
	}
	var names []string
	ast.Inspect(file, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "AggEntry" {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			return false
		}
		for _, field := range st.Fields.List {
			for _, ident := range field.Names {
				names = append(names, ident.Name)
			}
		}
		return false
	})
	return names
}

func TestTransitionContract_UnknownFreshnessAnchor(t *testing.T) {
	t.Run("无ExpireDate且LastSeenAt零值->诚实STALE不伪造到期", func(t *testing.T) {
		rec := AggEntry{Symbol: "600999", PrimarySource: sourceSector, SourceLifecycle: "ACTIVE"}
		out, act := Transition(rec, false, StatusOK, 7, testNow)
		if act != LifeMarkStale || out.SourceLifecycle != "STALE" {
			t.Fatalf("act=%s lc=%s want MARK_STALE/STALE", act, out.SourceLifecycle)
		}
		if out.ExpireDate != nil {
			t.Error("新鲜度锚点未知时不得伪造 ExpireDate")
		}
	})
	t.Run("autoExpireDays<=0命中->STALE而非now+0天", func(t *testing.T) {
		out, act := Transition(mkRec(sourceSector), true, StatusOK, 0, testNow)
		if act != LifeMarkStale || out.SourceLifecycle != "STALE" {
			t.Fatalf("act=%s lc=%s want MARK_STALE/STALE", act, out.SourceLifecycle)
		}
	})
}

// 合同行 6: 已命中的有效证据不因同源部分失败被洗掉。
// aggregateEntries 层面: PARTIAL 证据行必须保留在 Sources[] 且状态为 PARTIAL(3.4.1 已验收),
// 这里补生命周期视角: 上一轮 ACTIVE + 本轮 PARTIAL(未命中该证券) -> 只标 STALE, 证据不减。
func TestTransitionContract_EvidenceNotWashed(t *testing.T) {
	rec := mkRec(sourceSector)
	fut := testNow.AddDate(0, 0, 3)
	rec.ExpireDate = &fut
	rec.Sources = []SourceHit{{SourceType: sourceSector, Reason: "龙头", Status: StatusOK}}
	out, act := Transition(rec, false, StatusPartial, 7, testNow)
	if act == LifeExpire {
		t.Fatal("PARTIAL must not expire records with preserved evidence")
	}
	if len(out.Sources) != 1 || out.Sources[0].Reason != "龙头" {
		t.Errorf("evidence rows must survive transition untouched: %+v", out.Sources)
	}
}

// 合同行 7: 同一次运行重试不重复计数(幂等)。
// 现有幂等机制 = Result.RunID(由 cmd 注入的时间戳口径) + 持久层唯一约束 upsert
// (screening_runs UNIQUE(run_id); watchlist_source_hits UNIQUE(run_id,symbol,source))。
// 本用例证明: 相同 RunID、相同证据重复聚合/重复序列化不产生第二行、不改变 HitThisRun;
// 内存层 Transition 对同一 (rec,hit,status,now) 重复调用输出一致(纯函数)。
func TestLifecycleContract_RetryIdempotent(t *testing.T) {
	t.Run("Transition纯函数重放一致", func(t *testing.T) {
		rec := mkRec(sourceSector)
		a1, act1 := Transition(rec, false, StatusSuccessEmpty, 7, testNow)
		a2, act2 := Transition(rec, false, StatusSuccessEmpty, 7, testNow)
		if act1 != act2 || a1.SourceLifecycle != a2.SourceLifecycle {
			t.Fatalf("replay mismatch: %s/%s vs %s/%s", act1, a1.SourceLifecycle, act2, a2.SourceLifecycle)
		}
		if a1.ExpireDate != nil && a2.ExpireDate != nil && !a1.ExpireDate.Equal(*a2.ExpireDate) {
			t.Error("replay must produce identical expire date")
		}
	})
	t.Run("同run重复聚合不追加重复证据行", func(t *testing.T) {
		e := rawEntry("600519", sourceSector)
		out1 := aggregateEntries([]Entry{e}, "2026-10-13", testNow, nil)
		out2 := aggregateEntries([]Entry{e}, "2026-10-13", testNow, nil)
		if len(out1[0].Sources) != len(out2[0].Sources) {
			t.Fatalf("retry changed evidence count: %d vs %d", len(out1[0].Sources), len(out2[0].Sources))
		}
	})
	// 持久层 UNIQUE(run_id,symbol,source) 的读回一致性由 internal/persist 的
	// pool_store_test.go(幂等更新用例)负责, 本阶段不重复建设; 3.4.4 的 DB 往返
	// 生命周期字段测试因 WatchlistItem 缺少 lifecycle 列而暂不可写——登记为缺口, 不造字段。
}

// ---- 端到端 PARTIAL 用例的候选淘汰定位(检查点 4 证据留存, 只读诊断, 不改夹具) ----
// 结论(已在审计中定位): baseFake 的 f.stocks 全市场快照不含 601127 之外的板块外股票属正常;
// 真正淘汰点在 Generate 的来源调度: ETF 来源先于 SECTOR 执行(genETFMomentum L418 早于
// genSectorLeaders L427), srcStatus 注入循环(L462-468)按 map 查表给每条 Entry 打 HitStatus,
// 时序本身正确; 但 fake 的 ETFCloseSeries 与 StockAmounts 组合下, 600519/601127 进入
// refinePool 的 stockIdx 后依赖 f.stockBars/f.stockAmounts ——baseFake 已提供;
// 实测(pool 内容)显示 SECTOR 条目在进入 aggregateEntries 前即缺失, 根因指向
// genSectorLeaders 的成分股流动性门槛 screenSet: TieredPipeline 默认开启时 screenSet=nil
// 不设门槛, 但 partial_status_test 使用 DefaultConfig() ——需要以实际返回为准。
// 该诊断用例仅打印各阶段候选, 不做断言, 避免"为绿而改"。
func TestDiagStageTraceSectorPath(t *testing.T) {
	f := baseFake()
	f.sectorRank = append(f.sectorRank, SectorFlow{Code: "BK9999", Name: "失败板块", MainNetInflow: 9e9})
	f.constituentsErr = map[string]error{"BK9999": errors.New("board down")}
	g := NewGenerator(f, DefaultConfig())
	liq, idx, _, _ := g.liquidityScreen(context.Background())
	screenSet := liq
	if g.cfg.TieredPipeline {
		screenSet = nil
	}
	entries, st, aggErr := g.genSectorLeaders(context.Background(), screenSet, idx)
	t.Logf("stage A raw sector entries=%d status=%s err=%v", len(entries), st, aggErr)
	codes := []string{}
	for _, e := range entries {
		codes = append(codes, e.Symbol)
	}
	t.Logf("stage A codes=%v", codes)
	if st != StatusPartial {
		t.Errorf("checkpoint1: genSectorLeaders direct call must return PARTIAL, got %s", st)
	}
	if len(entries) == 0 {
		t.Error("checkpoint1: partial success must still yield usable candidates")
	}
	if aggErr == nil || !strings.Contains(aggErr.Error(), "BK9999") {
		t.Errorf("checkpoint1: aggregated error must surface failing board, got %v", aggErr)
	}
}

var _ = time.Now
