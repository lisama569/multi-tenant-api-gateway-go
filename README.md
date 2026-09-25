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

## 错误分类与审计

- 对外只暴露 `*gateway.Error`（`Code` + `Message` + `RetryAfter`）。
  未知内部异常经 `Normalize` 统一映射为 `INTERNAL`，内部细节只保留在
  `Unwrap()` 的 cause 中，不透出到对外消息。
- 每个请求（含被拒绝的）都会写审计记录：租户、主体、路径、判定
  （ALLOW/DENY）、判定依据（错误码）与配额快照。
- 审计写入失败不改变主流程业务结论，但会以 `AUDIT_WRITE_FAILED` 日志
  可观测。

## 本地验证

```bash
go run ./cmd/gateway          # 演示：合法/越权/过期/限流 四类场景
go test ./internal/gateway/ -v -race
```

测试以并发与边界场景为主：令牌过期边界（exp == now）、跨租户越权、
100 goroutine 争抢 3 个槽位的配额临界、审计写入失败注入；日志中打印
租户、配额快照与归一化错误。
