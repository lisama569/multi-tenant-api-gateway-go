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
	hold := make(chan struct{}) // 阻塞 /hold 请求，用于演示有界等待

	handler := func(ctx context.Context, req gateway.Request) (string, error) {
		switch req.Path {
		case "/slow":
			select {
			case <-time.After(300 * time.Millisecond):
				return "slow result", nil
			case <-ctx.Done():
				time.Sleep(100 * time.Millisecond) // 模拟被取消后仍收尾的下游
				return "", ctx.Err()
			}
		case "/boom":
			panic("downstream exploded: secret=should-not-leak")
		case "/hold":
			<-hold // 占住槽位直到演示结束
		}
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

	// 5. 请求级超时（最多等 50ms，下游要 300ms）
	_, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-a", time.Minute), TenantID: "tenant-a", Path: "/slow",
		Timeout: 50 * time.Millisecond,
	})
	fmt.Printf("[timeout]      err=%v\n", err)

	// 6. 下游崩溃：进程不挂、结论稳定、细节不透出
	_, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-a", time.Minute), TenantID: "tenant-a", Path: "/boom",
	})
	fmt.Printf("[crash]        err=%v\n", err)

	// 7. 崩溃后同租户请求照常成功（额度已归还、进程存活）
	resp, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-a", time.Minute), TenantID: "tenant-a", Path: "/orders",
	})
	fmt.Printf("[post-crash]   resp=%+v err=%v\n", resp, err)

	// 8. 有界等待：tenant-c 上限 1，先占住槽位不放；
	//    第二个请求排队，窗口内被放行（释放槽位），第三个请求窗口耗尽得到限流结论。
	quota.SetLimit("tenant-c", 1)
	go func() {
		_, e := gw.Handle(context.Background(), gateway.Request{
			Token: mint("tenant-c", time.Minute), TenantID: "tenant-c", Path: "/hold",
		})
		fmt.Printf("[hold-ended]   err=%v\n", e)
	}()
	time.Sleep(30 * time.Millisecond) // 等待槽位被占住

	waitAdmitted := make(chan error, 1)
	go func() {
		_, e := gw.Handle(context.Background(), gateway.Request{
			Token: mint("tenant-c", time.Minute), TenantID: "tenant-c", Path: "/orders",
			WaitTimeout: 300 * time.Millisecond,
		})
		waitAdmitted <- e
	}()
	time.Sleep(30 * time.Millisecond) // 等待排队入队

	_, err = gw.Handle(context.Background(), gateway.Request{
		Token: mint("tenant-c", time.Minute), TenantID: "tenant-c", Path: "/late",
		WaitTimeout: 50 * time.Millisecond,
	})
	fmt.Printf("[wait-expired] err=%v\n", err)

	close(hold) // 释放槽位：队首等待者应被放行
	if e := <-waitAdmitted; e != nil {
		fmt.Printf("[wait-admit]   unexpected err=%v\n", e)
	} else {
		fmt.Println("[wait-admit]   queued request admitted after release")
	}

	fmt.Printf("audit records: %d\n", len(audit.Records()))
	for _, r := range audit.Records() {
		fmt.Printf("  tenant=%s decision=%s reason=%s quota=%+v\n", r.TenantID, r.Decision, r.Reason, r.Quota)
	}
}
