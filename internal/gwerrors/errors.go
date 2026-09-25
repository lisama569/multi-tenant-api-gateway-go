// Package gwerrors 定义网关对外暴露的稳定错误类型与错误码。
// 内部异常不得直接透出，必须归一化为本包定义的错误。
package gwerrors

// Code 是可机器区分的失败原因。
type Code string

const (
	CodeTokenMissing    Code = "TOKEN_MISSING"
	CodeTokenMalformed  Code = "TOKEN_MALFORMED"
	CodeTokenBadSig     Code = "TOKEN_BAD_SIGNATURE"
	CodeTokenExpired    Code = "TOKEN_EXPIRED"
	CodeClaimMissing    Code = "CLAIM_MISSING"
	CodeTenantForbidden Code = "TENANT_FORBIDDEN"
	CodeQuotaExceeded   Code = "QUOTA_EXCEEDED"
	CodeInternal        Code = "INTERNAL"
)

// Error 是网关对外统一的错误类型。
type Error struct {
	Code      Code
	Message   string
	Retryable bool
	// cause 仅供内部日志，不对外透出。
	cause error
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

func (e *Error) Unwrap() error { return e.cause }

// New 构造对外错误，cause 仅用于内部诊断。
func New(code Code, msg string, retryable bool, cause error) *Error {
	return &Error{Code: code, Message: msg, Retryable: retryable, cause: cause}
}
