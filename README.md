# multi-tenant-api-gateway-go

可本地运行的多租户 API 网关请求入口。以本地内存状态模拟租户、令牌与配额，
保证鉴权、限流与审计在并发和异常情况下给出一致且可解释的结论。

## 结构

```
cmd/gateway/        本地演示入口
internal/token/     HMAC-SHA256 令牌签发与解析（声明：tid/sub/iat/exp）
internal/auth/      令牌鉴权 + 租户隔离校验
internal/quota/     按租户的并发配额限流器
internal/audit/     审计记录（内存存储 + 失败可观测的记录器）
internal/gateway/   请求入口编排：鉴权 → 隔离 → 限流 → 下游 → 审计
internal/gwerrors/  对外稳定的错误类型与错误码
```

## 鉴权与隔离规则

令牌格式：`base64url(claims-json).base64url(hmac-sha256-signature)`。
解析顺序固定，失败原因两两可区分：

| 场景 | 错误码 | 可重试 |
|---|---|---|
| 令牌缺失 | `TOKEN_MISSING` | 否 |
| 格式非法（分段/编码/JSON） | `TOKEN_MALFORMED` | 否 |
| 签名非法 | `TOKEN_BAD_SIGNATURE` | 否 |
| 缺少必需声明（tid/sub/exp） | `CLAIM_MISSING` | 否 |
| 已过期（now >= exp） | `TOKEN_EXPIRED` | 否 |
| 访问其它租户资源 | `TENANT_FORBIDDEN` | 否 |

鉴权通过后注入租户上下文 `{TenantID, Subject}`；`TargetTenant` 必须与上下文
中的租户一致，否则按越权拒绝。租户间配额、审计相互隔离。

## 配额模型

- 每租户一个并发名额上限（`quota.NewLimiter(map[tenant]limit)`）。
- `Acquire`/`release` 严格配对，计数由互斥锁保护；`release` 幂等
  （`sync.Once`），杜绝计数丢失与重复扣减。
- 超限返回 `QUOTA_EXCEEDED`（**可重试**），调用方可退避重试。
- 配额表在构造时拷贝，判定只取决于当前用量快照，相同输入得到可复现结论。

## 错误分类与审计

- 所有对外错误均为 `*gwerrors.Error{Code, Message, Retryable}`。
- 下游/内部异常（含 panic）统一映射为 `INTERNAL`（可重试），不保留 cause
  链，内部细节无法经 `errors.Is/As` 透出。
- 每个被接受（与被拒绝）的请求都写入审计记录：租户、判定
  （accepted/rejected）、判定依据（错误码或 OK）、配额快照。
- 审计写入失败不改变主流程业务结论，失败次数经
  `Recorder.WriteFailures()` 可观测。

## 本地验证

```bash
go run ./cmd/gateway          # 演示：合法/越权/过期/缺令牌/畸形令牌并发请求 + 审计输出
go test ./... -race -count=1  # 并发与边界测试（竞态检测）
go test ./internal/gateway/ -v   # 查看租户、配额快照与归一化错误日志
go test ./internal/quota/ -v     # 查看配额临界与并发不超发日志
```

测试覆盖：令牌过期与各类非法令牌（错误码可区分）、跨租户越权、配额临界
（恰好 limit 成功、limit+1 拒绝且可重试、释放后判定可复现）、200 并发下
用量不超配额且计数归零、重复释放安全、审计写入失败时业务结论不变且失败
次数可观测、下游错误/panic 归一化且不透出内部细节。
