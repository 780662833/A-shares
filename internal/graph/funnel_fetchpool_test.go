// T5.x FetchPool 节点单元测试：
//   - httptest 假网关模拟 POST /api/czsc/screen_pool（经真实 ScreenPoolTool 走 HTTP）
//   - fake poolSaver 注入落库动作，断言 upsert 内容
//   - 真 SQLite (db.InitDB) 验证 gormCandidateSaver 生产路径 + 同日重跑幂等
package graph

import (
"context"
"fmt"
"net/http"
"net/http/httptest"
"strings"
"sync"
"testing"

"ashare-quant/internal/db"
dbmodel "ashare-quant/internal/model"
qtool "ashare-quant/internal/tool"
)

// fakeScreenSrv 返回固定候选池的假网关；captured 记录收到的请求体。
func fakeScreenSrv(t *testing.T, captured *string, body string) *httptest.Server {
t.Helper()
return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
if r.URL.Path != "/api/czsc/screen_pool" {
w.WriteHeader(http.StatusNotFound)
return
}
buf := make([]byte, 1<<20)
n, _ := r.Body.Read(buf)
if captured != nil {
*captured = string(buf[:n])
}
w.Header().Set("Content-Type", "application/json")
fmt.Fprint(w, body)
}))
}

const screenOKBody = `{
"code": 0, "message": "success",
"data": {
"trade_date": "2026-10-11", "period": "daily", "window": "20250401~20261011",
"total_requested": 3, "total_scanned": 3, "pool_size": 2, "blocked_size": 0,
"pool": [
{"symbol":"600519","signal_type":"3B","zs_count":2,"bi_count":15,
 "trend_direction":"up","summary":"出现第三类买点"},
{"symbol":"300750","signal_type":"Observation","zs_count":3,"bi_count":20,
 "trend_direction":"up","summary":"3个中枢且趋势向上"}
],
"blocked": [], "errors": []
}}`

// recordingSaver 记录 fetchPoolWith 的落库调用。
type recordingSaver struct {
mu    sync.Mutex
calls []savedCandidate
errOn string // 指定某只股票落库失败，验证"单条失败不阻断"
}

type savedCandidate struct {
Date string
Hit  qtool.ScreenPoolHit
}

func (s *recordingSaver) SaveCandidate(date string, hit qtool.ScreenPoolHit) error {
s.mu.Lock()
defer s.mu.Unlock()
if s.errOn != "" && hit.Symbol == s.errOn {
return fmt.Errorf("simulated db failure for %s", hit.Symbol)
}
s.calls = append(s.calls, savedCandidate{Date: date, Hit: hit})
return nil
}

func newScreenToolFor(t *testing.T, url string) *qtool.ScreenPoolTool {
t.Helper()
st, err := qtool.NewScreenPoolTool(qtool.ScreenPoolConfig{GatewayBaseURL: url})
if err != nil {
t.Fatalf("NewScreenPoolTool: %v", err)
}
return st
}

// ---- FP.1 正常链路: 粗筛结果进 state, 每只候选股按 trade_date 落库 ----
func TestFetchPool_Normal(t *testing.T) {
var got string
srv := fakeScreenSrv(t, &got, screenOKBody)
defer srv.Close()

saver := &recordingSaver{}
in := FunnelInput{TradeDate: "2026-10-11", Symbols: []string{"600519", "300750", "000001"}}
state, err := fetchPoolWith(context.Background(), newScreenToolFor(t, srv.URL), saver, in)
if err != nil {
t.Fatalf("fetchPoolWith: %v", err)
}

pool, ok := state[keyPool].([]qtool.ScreenPoolHit)
if !ok || len(pool) != 2 {
t.Fatalf("state[%s] bad: %#v", keyPool, state[keyPool])
}
if pool[0].Symbol != "600519" || pool[0].SignalType != "3B" {
t.Errorf("pool[0]=%+v", pool[0])
}
if pj, _ := state[keyPoolJSON].(string); !strings.Contains(pj, "600519") || !strings.Contains(pj, "300750") {
t.Errorf("pool json=%.200s", pj)
}
if td, _ := state[keyTradeDate].(string); td != "2026-10-11" {
t.Errorf("trade_date=%q", td)
}
if len(saver.calls) != 2 {
t.Fatalf("expect 2 SaveCandidate calls, got %d", len(saver.calls))
}
for i, c := range saver.calls {
if c.Date != "2026-10-11" {
t.Errorf("call[%d].Date=%q", i, c.Date)
}
}
if saver.calls[0].Hit.ZsCount != 2 || saver.calls[0].Hit.TrendDirection != "up" ||
saver.calls[0].Hit.Summary != "出现第三类买点" {
t.Errorf("hit fields lost: %+v", saver.calls[0].Hit)
}
// Graph 传给工具的默认参数: period=daily, symbols 原样
if !strings.Contains(got, `"period":"daily"`) || !strings.Contains(got, "600519") {
t.Errorf("request body=%s", got)
}
}

// ---- FP.2 输入校验: 非法日期 / 空代码列表 直接报错, 不发 HTTP ----
func TestFetchPool_InputValidation(t *testing.T) {
var called bool
srv := fakeScreenSrv(t, nil, screenOKBody)
defer srv.Close()
// 用捕获变量包装: 若走到网络则 called=true —— 通过 captured body 无法做, 改用计数 handler
srv.Close()
srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
called = true
fmt.Fprint(w, screenOKBody)
}))
defer srv.Close()

cases := []struct {
name string
in   FunnelInput
}{
{"bad date", FunnelInput{TradeDate: "20261011", Symbols: []string{"600519"}}},
{"empty date", FunnelInput{TradeDate: "", Symbols: []string{"600519"}}},
{"nil symbols", FunnelInput{TradeDate: "2026-10-11"}},
{"blank symbols", FunnelInput{TradeDate: "2026-10-11", Symbols: []string{" ", ""}}},
}
saver := &recordingSaver{}
for _, cs := range cases {
_, err := fetchPoolWith(context.Background(), newScreenToolFor(t, srv.URL), saver, cs.in)
if err == nil {
t.Errorf("%s: expect error, got nil", cs.name)
}
}
if called {
t.Error("invalid input must not reach gateway")
}
if len(saver.calls) != 0 {
t.Errorf("no candidate should be saved on validation failure")
}
}

// ---- FP.3 符号去重: normalizeSymbols 去空白/重复后再送网关 ----
func TestFetchPool_SymbolDedupe(t *testing.T) {
var got string
srv := fakeScreenSrv(t, &got, screenOKBody)
defer srv.Close()

in := FunnelInput{TradeDate: "2026-10-11", Symbols: []string{"600519", " 600519 ", "300750", "600519"}}
if _, err := fetchPoolWith(context.Background(), newScreenToolFor(t, srv.URL), &recordingSaver{}, in); err != nil {
t.Fatal(err)
}
n := strings.Count(got, "600519")
if n != 1 {
t.Errorf("600519 should appear once after dedupe, got %d, body=%s", n, got)
}
}

// ---- FP.4 上游业务失败(code!=0) -> 工具输出 ok=true+空池提示 ->
// 节点必须以"空候选池"中止流水线(否则 Manager 会收到空列表空转 LLM) ----
func TestFetchPool_GatewaySoftFail(t *testing.T) {
// 上游 code!=0 且 data=null 时 ScreenPoolTool 的既定行为:
// 返回 {"ok":true,"status":"upstream timeout","summary":"网关未返回候选池数据"}
srv := fakeScreenSrv(t, nil, `{"code":1,"message":"upstream timeout","data":null}`)
defer srv.Close()

saver := &recordingSaver{}
_, err := fetchPoolWith(context.Background(), newScreenToolFor(t, srv.URL), saver,
FunnelInput{TradeDate: "2026-10-11", Symbols: []string{"600519"}})
if err == nil || !strings.Contains(err.Error(), "empty pool") {
t.Fatalf(`expect "empty pool" abort error, got %v`, err)
}
if len(saver.calls) != 0 {
t.Error("failed screening must not save candidates")
}
}

// ---- FP.4b 工具链路软失败: 网关 HTTP 502 -> ScreenPoolTool 返回 ok=false
// -> fetchPoolWith 必须转成节点 error 且不落库 (与 FP.4 裸信封互补) ----
func TestFetchPool_ToolSoftFailViaHTTP502(t *testing.T) {
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
w.WriteHeader(http.StatusBadGateway)
fmt.Fprint(w, `<html>502 Bad Gateway</html>`)
}))
defer srv.Close()

saver := &recordingSaver{}
_, err := fetchPoolWith(context.Background(), newScreenToolFor(t, srv.URL), saver,
FunnelInput{TradeDate: "2026-10-11", Symbols: []string{"600519"}})
if err == nil || !strings.Contains(err.Error(), "screening failed") {
t.Fatalf("expect wrapped 'screening failed' error, got %v", err)
}
if len(saver.calls) != 0 {
t.Error("failed screening must not save candidates")
}
}

// ---- FP.5 网关宕机: tool 返回 ok=false JSON -> 同样转 error, 不 panic ----
func TestFetchPool_GatewayDown(t *testing.T) {
srv := fakeScreenSrv(t, nil, screenOKBody)
url := srv.URL
srv.Close() // 宕机

if _, err := fetchPoolWith(context.Background(), newScreenToolFor(t, url), &recordingSaver{},
FunnelInput{TradeDate: "2026-10-11", Symbols: []string{"600519"}}); err == nil {
t.Fatal("expect error when gateway down")
}
}

// ---- FP.6 脏响应: 非 JSON / code!=0 无 data 均安全失败 ----
func TestFetchPool_DirtyResponses(t *testing.T) {
bodies := map[string]string{
"not json":      `<html>502 Bad Gateway</html>`,
"empty envelope": `{"code":1,"message":"upstream timeout","data":null}`,
}
for name, body := range bodies {
srv := fakeScreenSrv(t, nil, body)
err := func() error {
defer srv.Close()
_, e := fetchPoolWith(context.Background(), newScreenToolFor(t, srv.URL), &recordingSaver{},
FunnelInput{TradeDate: "2026-10-11", Symbols: []string{"600519"}})
return e
}()
switch name {
case "not json":
if err == nil {
t.Errorf("%s: expect error on non-JSON response", name)
}
case "empty envelope":
// data=null -> 工具 ok=true 但池为空 -> 节点按 FP.4 语义报 empty pool
if err == nil || !strings.Contains(err.Error(), "empty pool") {
t.Errorf("%s: expect 'empty pool' abort error, got %v", name, err)
}
}
}
}

// ---- FP.7 单条落库失败仅告警, 其余候选照常入 state ----
func TestFetchPool_PartialSaveFailureTolerated(t *testing.T) {
srv := fakeScreenSrv(t, nil, screenOKBody)
defer srv.Close()

saver := &recordingSaver{errOn: "600519"}
state, err := fetchPoolWith(context.Background(), newScreenToolFor(t, srv.URL), saver,
FunnelInput{TradeDate: "2026-10-11", Symbols: []string{"600519", "300750"}})
if err != nil {
t.Fatalf("single save failure must not abort node: %v", err)
}
if pool := state[keyPool].([]qtool.ScreenPoolHit); len(pool) != 2 {
t.Errorf("state pool should stay complete, got %d", len(pool))
}
}

// ---- FP.8 生产路径端到端: gormCandidateSaver 真 SQLite + 同日重跑幂等(upsert 覆盖) ----
func TestFetchPool_GormUpsertIdempotent(t *testing.T) {
srv := fakeScreenSrv(t, nil, screenOKBody)
defer srv.Close()

gdb, err := db.InitDB(t.TempDir() + "/funnel.db")
if err != nil {
t.Fatalf("InitDB: %v", err)
}
in := FunnelInput{TradeDate: "2026-10-11", Symbols: []string{"600519", "300750"}}

// 第一次跑
if _, err := fetchPool(context.Background(), newScreenToolFor(t, srv.URL), gdb, in); err != nil {
t.Fatalf("run1: %v", err)
}
var rows []dbmodel.CandidatePool
gdb.Order("code").Find(&rows)
if len(rows) != 2 {
t.Fatalf("expect 2 candidates, got %d", len(rows))
}
if rows[0].Code != "300750" || rows[0].SignalType != "Observation" || rows[0].ZsCount != 3 {
t.Errorf("row[0]=%+v", rows[0])
}
if rows[1].Code != "600519" || rows[1].CzscSummary != "出现第三类买点" {
t.Errorf("row[1]=%+v", rows[1])
}
firstID := rows[1].ID

// 第二次同日重跑: 不应产生重复行, 字段被覆盖更新
if _, err := fetchPool(context.Background(), newScreenToolFor(t, srv.URL), gdb, in); err != nil {
t.Fatalf("run2: %v", err)
}
var rows2 []dbmodel.CandidatePool
gdb.Order("code").Find(&rows2)
if len(rows2) != 2 {
t.Fatalf("rerun must upsert not duplicate, got %d rows", len(rows2))
}
if rows2[1].ID != firstID {
t.Errorf("same-day rerun changed PK: %d -> %d (expected in-place update)", firstID, rows2[1].ID)
}

// 不同交易日应新增独立记录
in2 := in
in2.TradeDate = "2026-10-12"
if _, err := fetchPool(context.Background(), newScreenToolFor(t, srv.URL), gdb, in2); err != nil {
t.Fatalf("run3: %v", err)
}
var cnt int64
gdb.Model(&dbmodel.CandidatePool{}).Count(&cnt)
if cnt != 4 {
t.Errorf("expect 4 rows across 2 dates, got %d", cnt)
}
}
