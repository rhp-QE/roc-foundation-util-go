package otel

import (
	"context"
	"testing"

	"github.com/cloudwego/kitex/pkg/klog"
)

func TestInitOTEL(t *testing.T) {
	ctx := context.Background()

	// 一键初始化
	kit, err := InitOTEL(ctx,
		WithServiceName("test-service"),
		WithEndpoint("localhost:4317"),
		WithInsecure(true),
		WithJSONFormat(true),
	)

	if err != nil {
		t.Skipf("跳过 OTEL 测试（需要 OTEL Collector）: %v", err)
		return
	}

	if kit == nil {
		t.Fatal("kit should not be nil")
	}

	if kit.Logger == nil {
		t.Fatal("logger should not be nil")
	}

	if kit.LogProvider == nil {
		t.Fatal("log provider should not be nil")
	}

	defer kit.Shutdown(ctx)

	// 测试日志（已自动设置为全局 logger）
	klog.Info("测试日志")
	klog.Infof("格式化日志: %s", "test")

	// 测试带上下文的日志
	klog.CtxInfof(ctx, "上下文日志", "key", "value")
}

func TestOTELKitShutdown(t *testing.T) {
	ctx := context.Background()

	kit, err := InitOTEL(ctx,
		WithServiceName("shutdown-test"),
		WithEndpoint("localhost:4317"),
		WithInsecure(true),
	)

	if err != nil {
		t.Skipf("跳过测试: %v", err)
		return
	}

	// 发送一些日志
	klog.Info("测试日志1")
	klog.Info("测试日志2")

	// 测试 ForceFlush
	err = kit.ForceFlush(ctx)
	if err != nil {
		t.Errorf("ForceFlush failed: %v", err)
	}

	// 测试 Shutdown
	err = kit.Shutdown(ctx)
	if err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}
}

func TestOTELKitUsage(t *testing.T) {
	ctx := context.Background()

	// 演示完整使用流程
	kit, err := InitOTEL(ctx,
		WithServiceName("demo-service"),
		WithServiceVersion("1.0.0"),
		WithEnvironment("test"),
		WithEndpoint("localhost:4317"),
		WithInsecure(true),
	)

	if err != nil {
		t.Skipf("跳过测试: %v", err)
		return
	}
	defer kit.Shutdown(ctx)

	// 直接使用 klog，无需额外配置
	klog.Info("服务启动")
	klog.Infof("监听端口: %d", 8080)

	// 带上下文的日志，自动关联 trace
	klog.CtxInfof(ctx, "处理请求",
		"user_id", "12345",
		"action", "login",
	)

	klog.CtxWarnf(ctx, "警告信息", "code", 1001)
}

