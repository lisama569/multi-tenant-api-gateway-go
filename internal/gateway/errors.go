package gateway

import (
	"errors"
	"fmt"
	"time"
)

// ErrorCode 是对外稳定的错误分类，内部异常不得直接透出。
type ErrorCode string

const (
	ErrCodeTokenMissing          ErrorCode = "TOKEN_MISSING"
	ErrCodeTokenMalformed        ErrorCode = "TOKEN_MALFORMED"
	ErrCodeTokenInvalidSignature ErrorCode = "TOKEN_INVALID_SIGNATURE"
	ErrCodeTokenExpired          ErrorCode = "TOKEN_EXPIRED"
	ErrCodeTokenMissingClaim     ErrorCode = "TOKEN_MISSING_CLAIM"
	ErrCodeCrossTenant           ErrorCode = "CROSS_TENANT_ACCESS"
	ErrCodeQuotaExceeded         ErrorCode = "QUOTA_EXCEEDED"
	ErrCodeRequestTimeout        ErrorCode = "REQUEST_TIMEOUT"
	ErrCodeDownstreamUnavailable ErrorCode = "DOWNSTREAM_UNAVAILABLE"
	ErrCodeInternal              ErrorCode = "INTERNAL"
)

// Error 是网关对外返回的唯一错误类型。
type Error struct {
	Code       ErrorCode
	Message    string
	RetryAfter time.Duration // 可重试结论携带的建议等待时长
	Retryable  bool          // 调用方稍后重试是否可能成功
	cause      error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

func newError(code ErrorCode, msg string, cause error) *Error {
	return &Error{Code: code, Message: msg, cause: cause}
}

// Normalize 将任意错误归一化为 *Error；未知错误统一映射为 INTERNAL，
// 内部异常细节只保留在 cause 中，不透出到 Message。
func Normalize(err error) *Error {
	if err == nil {
		return nil
	}
	var ge *Error
	if errors.As(err, &ge) {
		return ge
	}
	return &Error{Code: ErrCodeInternal, Message: "internal error", cause: err}
}
