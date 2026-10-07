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
		switch req.Path {
		case "/slow":
			time.Sleep(300 * time.Millisecond) // 模拟下游超时
		case "/boom":
			panic("simulated downstream crash") // 模拟下游不可恢复异常
		default:
			time.Sleep(20 * time.Millisecond) // 模拟下游耗时
		}
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

	// 5. 单请求超时：下游耗时 300ms，上限 50ms
	_, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-a", time.Minute), TenantID: "tenant-a", Path: "/slow",
		MaxDuration: 50 * time.Millisecond,
	})
	fmt.Printf("[deadline]     err=%v\n", err)

	// 6. 下游崩溃：panic 被隔离为稳定结论，进程与其它租户不受影响
	_, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-a", time.Minute), TenantID: "tenant-a", Path: "/boom",
	})
	fmt.Printf("[panic]        err=%v\n", err)
	resp, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-b", time.Minute), TenantID: "tenant-b", Path: "/after-panic",
	})
	fmt.Printf("[after-panic]  resp=%+v err=%v\n", resp, err)

	// 7. 有界等待：tenant-b 上限 1，等待窗口内排队放行
	hold := make(chan struct{})
	blocking := gateway.NewGateway(validator, quota, audit,
		func(ctx context.Context, req gateway.Request) (string, error) {
			if req.Path == "/hold" {
				<-hold
			}
			return "ok " + req.Path, nil
		}, logger)
	go func() {
		_, _ = blocking.Handle(context.Background(), gateway.Request{
			Token: mint("tenant-b", time.Minute), TenantID: "tenant-b", Path: "/hold",
		})
	}()
	time.Sleep(20 * time.Millisecond) // 确认 /hold 已占住槽位
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(hold)
	}()
	_, err = blocking.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-b", time.Minute), TenantID: "tenant-b", Path: "/queued",
		MaxQueueWait: time.Second,
	})
	fmt.Printf("[wait-admit]   err=%v\n", err)

	// 8. 等待窗口耗尽：槽位被长期占住，等待者耗完窗口后收到限流结论
	hold2 := make(chan struct{})
	blocking2 := gateway.NewGateway(validator, quota, audit,
		func(ctx context.Context, req gateway.Request) (string, error) {
			if req.Path == "/hold2" {
				<-hold2
			}
			return "ok " + req.Path, nil
		}, logger)
	go func() {
		_, _ = blocking2.Handle(context.Background(), gateway.Request{
			Token: mint("tenant-b", time.Minute), TenantID: "tenant-b", Path: "/hold2",
		})
	}()
	time.Sleep(20 * time.Millisecond) // 确认 /hold2 已占住槽位
	_, err = blocking2.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-b", time.Minute), TenantID: "tenant-b", Path: "/queued-timeout",
		MaxQueueWait: 30 * time.Millisecond,
	})
	fmt.Printf("[wait-timeout] err=%v\n", err)
	close(hold2)
	time.Sleep(20 * time.Millisecond) // 让 /hold2 收尾并写审计

	fmt.Printf("audit records: %d\n", len(audit.Records()))
	for _, r := range audit.Records() {
		fmt.Printf("  tenant=%s decision=%s reason=%s quota=%+v\n", r.TenantID, r.Decision, r.Reason, r.Quota)
	}
}
