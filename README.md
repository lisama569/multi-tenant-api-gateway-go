# multi-tenant-api-gateway-go

可本地运行的多租户 API 网关请求入口。用本地内存状态模拟租户、令牌与配额，
保证鉴权、限流与审计在并发和异常情况下给出一致且可解释的结论。

## 结构

```
cmd/gateway/main.go        本地演示入口
internal/gateway/
  token.go    令牌签发（Mint）与校验（Validate），HMAC-SHA256 签名
  quota.go    按租户的并发配额（互斥锁保护计数）
  audit.go    审计记录与线程安全的内存 Sink
  errors.go   对外稳定的错误分类与归一化
  gateway.go  请求入口编排：鉴权 -> 隔离 -> 限流 -> 下游 -> 审计
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

## 配额模型

- 每租户一个并发上限（`SetLimit`），`Acquire`/`release` 在同一互斥锁下
  增减计数：并发下不超配、不丢计数；释放函数用 `sync.Once` 保证重复
  释放不重复扣减。
- 配额满时返回 `QUOTA_EXCEEDED`，并携带 `RetryAfter` 可重试元信息与
  判定时刻的 `QuotaSnapshot`。
- 配额规则变化（`SetLimit`）后立即生效，相同输入得到可复现的新判定。

## 有界等待（排队放行）

在立即拒绝之外，调用方可通过 `Request.MaxQueueWait` 声明愿意排队等待的
最长时间：

- 配额未满时立即放行；已满且 `MaxQueueWait > 0` 时按到达先后进入
  每租户的 FIFO 队列，槽位释放时直接移交给队首等待者，不经过第三方
  重新争抢，保证公平且不超配。
- 等待窗口耗尽仍未轮到的返回 `QUOTA_EXCEEDED`（带 `RetryAfter`，
  `Retryable=true`）；`ctx` 被取消的返回 `REQUEST_CANCELLED`。
- 超时/取消与槽位移交的竞态在同一互斥锁下仲裁：已移交的槽位会被代为
  归还并继续移交下一位，未移交的标记取消后惰性出队——任何交错下计数
  都不会错乱、槽位不会泄漏。
- `MaxQueueWait <= 0`（含未设置）时行为与原来一致：配额满立即拒绝。

## 单请求超时

`Request.MaxDuration` 声明单个请求的最长处理时间（`<=0` 表示不限制）：

- 下游处理超时后，网关立即返回 `DEADLINE_EXCEEDED`（`Retryable=true`，
  调用方可稍后重试），不等下游收尾。
- 超时返回的同时并发额度即归还，不会因下游 goroutine 仍在运行而悬占
  槽位、挡住同租户后续请求；下游迟到的结果会被丢弃。
- 调用方 `ctx` 取消时返回 `REQUEST_CANCELLED`，与超时结论可区分。

## 故障隔离

- 下游处理器 panic 被 `recover` 隔离：进程不会随之退出，其它租户的
  请求不受任何影响。
- 崩溃请求对外的稳定结论为 `UPSTREAM_FAILURE`（panic 细节不透出到
  对外消息，只以 `UPSTREAM_PANIC` 日志与错误 cause 形式内部可见）。
- 崩溃请求照常归属到发起租户：占用的额度正常归还，审计记录按该租户
  记为 `DENY / UPSTREAM_FAILURE`，不会算到其它租户头上。

## 错误分类与审计

- 对外只暴露 `*gateway.Error`（`Code` + `Message` + `RetryAfter` +
  `Retryable`）。未知内部异常经 `Normalize` 统一映射为 `INTERNAL`，
  内部细节只保留在 `Unwrap()` 的 cause 中，不透出到对外消息。
- 每个请求（含被拒绝、超时、下游崩溃的）都会写审计记录：租户、主体、
  路径、判定（ALLOW/DENY）、判定依据（错误码）与配额快照。
- 每次判定同时输出 `DECISION tenant=... decision=... reason=... quota=在用/上限`
  日志，可直接看清归属租户、当时配额占用与最终结论。
- 审计写入失败不改变主流程业务结论，但会以 `AUDIT_WRITE_FAILED` 日志
  可观测。

## 本地验证

```bash
go run ./cmd/gateway          # 演示：合法/越权/过期/限流/超时/崩溃/排队放行/等待超时 八类场景
go test ./internal/gateway/ -v -race
```

测试以并发与边界场景为主：令牌过期边界（exp == now）、跨租户越权、
100 goroutine 争抢 3 个槽位的配额临界、审计写入失败注入、下游超时后
额度即时归还、下游 panic 隔离且其它租户不受牵连、排队请求按到达先后
放行、等待窗口耗尽返回限流结论、等待中被取消，以及混合失败下额度回到
干净状态、审计一条不少；日志中打印租户、配额快照与归一化错误。
