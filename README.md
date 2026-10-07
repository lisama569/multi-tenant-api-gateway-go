# multi-tenant-api-gateway-go

可本地运行的多租户 API 网关请求入口。用本地内存状态模拟租户、令牌与配额，
保证鉴权、限流、有界等待、超时与故障隔离在并发和异常情况下给出一致且
可解释的结论。

## 结构

```
cmd/gateway/main.go         本地演示入口
internal/gateway/
  token.go    令牌签发（Mint）与校验（Validate），HMAC-SHA256 签名
  quota.go    按租户的并发配额与 FIFO 有界等待队列（互斥锁保护计数）
  audit.go    审计记录与线程安全的内存 Sink
  errors.go   对外稳定的错误分类与归一化（含 Retryable / RetryAfter）
  gateway.go  请求入口编排：鉴权 -> 隔离 -> 有界限流 -> 下游(超时/崩溃隔离) -> 审计
```

## 鉴权与隔离规则

令牌格式为 `base64url(payload).base64url(hmac-sha256)`，payload 为 JSON 声明
（`tenant_id`、`sub`、`exp`、`iat`）。校验顺序与失败原因一一对应：

| 场景 | 错误码 |
|---|---|
| 令牌缺失 | `TOKEN_MISSING` |
| 段数/base64/JSON 非法 | `TOKEN_MALFORMED` |
| 签名不匹配 | `TOKEN_INVALID_SIGNATURE` |
| 缺少 `tenant_id`/`sub`/`exp` | `TOKEN_MISSING_CLAIM` |
| `now >= exp`（边界即过期） | `TOKEN_EXPIRED` |
| 令牌租户 ≠ 目标资源租户 | `CROSS_TENANT_ACCESS` |

校验通过后，请求携带 `Claims` 中的租户上下文继续处理；越权访问其它租户
资源一律拒绝。`TokenValidator` 的时钟可注入，相同输入的判定可复现。

> 兼容性：以上规则与错误码完全保持不变；所有新增结论都走新的错误码，
> 不改变任何既有判定路径。

## 配额与有界等待

- 每租户一个并发上限（`SetLimit`），占用与释放在同一互斥锁下增减计数：
  并发下不超配、不丢计数；释放函数用 `sync.Once` 保证重复释放不重复扣减。
- 配额满时返回 `QUOTA_EXCEEDED`，携带 `RetryAfter` 与 `Retryable=true`，
  并附判定时刻的 `QuotaSnapshot`（`max / inFlight / waiting`）。
- **有界等待（FIFO）**：请求可带 `WaitTimeout`。
  - 配额满且愿意等时进入该租户的等待队列，前面的请求释放槽位后按
    到达先后（严格 FIFO）放行；
  - 等待窗口耗尽仍未轮到，或排队期间调用方取消请求，统一返回
    `QUOTA_EXCEEDED`（消息区分 `wait deadline elapsed` /
    `wait canceled before admission`），不占槽位、不留幽灵排队项；
  - `WaitTimeout <= 0` 保持原来的“立即拒绝”语义。
- 配额规则变化（`SetLimit`）后立即生效，上调上限时会顺手放行队首等待者。

## 请求级超时

请求可带 `Timeout` 声明最长处理时间：

- 下游在独立 goroutine 中执行；到达上限后网关**立即**返回
  `REQUEST_TIMEOUT`，不继续等待慢下游；
- 结论稳定、可与其它失败区分，并带 `Retryable=true` 与 `RetryAfter`，
  调用方可以稍后重试；
- 超时结论给出的同时，占用的并发槽位经 `defer` 确定性归还，同租户后续
  请求不会被悬挂的额度挡住；
- 上游（调用方）在下游执行期间主动取消不会被误判为 `REQUEST_TIMEOUT`。

## 下游故障隔离

- 下游 handler 的 panic 在单个请求的边界内被 `recover`：进程不退出、
  其它租户与同租户后续请求不受影响；
- 对外统一为稳定、可重试的 `DOWNSTREAM_UNAVAILABLE`，panic 细节（堆栈、
  内部地址等）只进服务端日志（`DOWNSTREAM_PANIC` + 堆栈）和 cause，
  不透出到对外消息；
- 崩溃请求的审计正常写一条 `DENY`，并发槽位确定性释放，故障只记在
  发起请求的租户名下，不算到其它租户头上。

## 错误分类与审计

- 对外只暴露 `*gateway.Error`（`Code` + `Message` + `Retryable` +
  `RetryAfter`）。未知内部异常经 `Normalize` 统一映射为 `INTERNAL`，
  内部细节只保留在 `Unwrap()` 的 cause 中，不透出到对外消息。

  | 错误码 | 含义 | Retryable |
  |---|---|---|
  | `TOKEN_*` / `CROSS_TENANT_ACCESS` | 鉴权与隔离失败 | false |
  | `QUOTA_EXCEEDED` | 配额满 / 等待窗口耗尽 / 排队被取消 | true |
  | `REQUEST_TIMEOUT` | 超过请求级最长处理时间 | true |
  | `DOWNSTREAM_UNAVAILABLE` | 下游 panic 等不可恢复崩溃 | true |
  | `INTERNAL` | 未归一化的未知内部异常 | false |

- 每个请求（含被拒绝、超时、崩溃的）都写且只写一条审计记录：租户、
  主体、路径、判定（ALLOW/DENY）、判定依据（错误码）与配额快照。
- 审计写入失败不改变主流程业务结论，但会以 `AUDIT_WRITE_FAILED` 日志
  可观测。
- 每个请求结束时输出 `CONCLUSION` 日志：租户、主体、判定、结论原因，
  以及判定时刻的 `{max, inFlight, waiting}` 配额占用，便于事后解释
  “是哪个租户、当时占了多少额度、最后给了什么结论”。

## 本地验证

```bash
go run ./cmd/gateway          # 演示：鉴权/越权/限流/超时/崩溃/有界等待
go test ./internal/gateway/ -v -race
```

测试以并发与边界场景为主：

- 令牌过期边界（exp == now）、跨租户越权；
- 100 goroutine 争抢 3 个槽位的配额临界、审计写入失败注入；
- **请求超时**：结论为 `REQUEST_TIMEOUT`、可重试、槽位立即归还、
  后续同租户请求立即可用、审计一条不少；
- **下游崩溃**：结论为 `DOWNSTREAM_UNAVAILABLE`、细节不透出、
  一个租户连续崩溃不影响另一租户、计数归零、审计归因精确；
- **有界等待**：窗口内释放即放行（审计 ALLOW）、严格 FIFO 放行顺序、
  等待窗口耗尽返回 `QUOTA_EXCEEDED`、排队期间取消后队列与计数干净、
  30 goroutine 风暴下不超配不漏计数；
- 日志中断言可见租户、配额占用与最终结论。

