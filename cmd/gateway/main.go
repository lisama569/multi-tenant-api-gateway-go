// 本地可运行的网关演示入口：签发令牌、构造网关、并发发起请求并打印审计。
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/multi-tenant-api-gateway-go/internal/audit"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/auth"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gateway"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gwerrors"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/quota"
	"github.com/lisama569/multi-tenant-api-gateway-go/internal/token"
)

func main() {
	now := time.Now()
	signer := token.NewSigner([]byte("local-demo-secret"), nil)
	mk := func(tenant, sub string, ttl time.Duration) string {
		t, err := signer.Sign(token.Claims{
			TenantID: tenant, Subject: sub,
			IssuedAt: now, ExpiresAt: now.Add(ttl),
		})
		if err != nil {
			panic(err)
		}
		return t
	}

	tokA := mk("tenant-a", "alice", time.Hour)
	tokB := mk("tenant-b", "bob", time.Hour)
	tokExpired := mk("tenant-a", "alice", -time.Minute)

	sink := audit.NewMemorySink()
	gw := gateway.New(
		auth.NewAuthenticator(signer),
		quota.NewLimiter(map[string]int{"tenant-a": 2, "tenant-b": 1}),
		audit.NewRecorder(sink),
	)

	reqs := []gateway.Request{
		{Token: tokA, TargetTenant: "tenant-a", Payload: "a-1"},
		{Token: tokA, TargetTenant: "tenant-a", Payload: "a-2"},
		{Token: tokA, TargetTenant: "tenant-a", Payload: "a-3"},
		{Token: tokB, TargetTenant: "tenant-b", Payload: "b-1"},
		{Token: tokA, TargetTenant: "tenant-b", Payload: "cross-tenant"}, // 越权
		{Token: tokExpired, TargetTenant: "tenant-a", Payload: "expired"},
		{Token: "", TargetTenant: "tenant-a", Payload: "no-token"},
		{Token: "not-a-token", TargetTenant: "tenant-a", Payload: "malformed"},
	}

	var wg sync.WaitGroup
	for i, r := range reqs {
		wg.Add(1)
		go func(i int, r gateway.Request) {
			defer wg.Done()
			resp, err := gw.Handle(context.Background(), r)
			if err != nil {
				ge := err.(*gwerrors.Error)
				fmt.Printf("[req %d] payload=%-12s -> REJECTED code=%s retryable=%v\n",
					i, r.Payload, ge.Code, ge.Retryable)
				return
			}
			fmt.Printf("[req %d] payload=%-12s -> OK %s\n", i, r.Payload, resp.Body)
		}(i, r)
	}
	wg.Wait()

	fmt.Println("\n--- audit records ---")
	for _, rec := range sink.Records() {
		fmt.Printf("tenant=%-9s decision=%-8s reason=%-18s quota=%s\n",
			rec.TenantID, rec.Decision, rec.Reason, rec.Quota)
	}
}
