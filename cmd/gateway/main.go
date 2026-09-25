package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/lisama569/multi-tenant-api-gateway-go/internal/gateway"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	secret := []byte("local-demo-secret")
	validator := gateway.NewTokenValidator(secret, nil)
	quota := gateway.NewQuotaManager(100 * time.Millisecond)
	quota.SetLimit("tenant-a", 2)
	quota.SetLimit("tenant-b", 1)
	audit := gateway.NewInMemoryAuditSink()

	handler := func(ctx context.Context, req gateway.Request) (string, error) {
		time.Sleep(20 * time.Millisecond) // 模拟下游耗时
		return "hello from " + req.TenantID + req.Path, nil
	}
	gw := gateway.NewGateway(validator, quota, audit, handler, logger)

	mint := func(tenant string, ttl time.Duration) string {
		return validator.Mint(gateway.Claims{
			TenantID: tenant, Subject: "user-1",
			IssuedAt: time.Now(), ExpiresAt: time.Now().Add(ttl),
		})
	}

	// 1. 合法请求
	resp, err := gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-a", time.Minute), TenantID: "tenant-a", Path: "/orders",
	})
	fmt.Printf("[ok]           resp=%+v err=%v\n", resp, err)

	// 2. 跨租户越权
	_, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-a", time.Minute), TenantID: "tenant-b", Path: "/orders",
	})
	fmt.Printf("[cross-tenant] err=%v\n", err)

	// 3. 过期令牌
	_, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-a", -time.Minute), TenantID: "tenant-a", Path: "/orders",
	})
	fmt.Printf("[expired]      err=%v\n", err)

	// 4. 并发触发限流（tenant-b 上限为 1）
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := gw.Handle(context.Background(), gateway.Request{
				Token: mint("tenant-b", time.Minute), TenantID: "tenant-b", Path: fmt.Sprintf("/job/%d", i),
			})
			fmt.Printf("[quota]        i=%d err=%v\n", i, err)
		}(i)
	}
	wg.Wait()

	fmt.Printf("audit records: %d\n", len(audit.Records()))
	for _, r := range audit.Records() {
		fmt.Printf("  tenant=%s decision=%s reason=%s quota=%+v\n", r.TenantID, r.Decision, r.Reason, r.Quota)
	}
}
