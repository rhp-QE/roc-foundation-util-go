package otel

import (
	"context"
	"testing"

	"github.com/cloudwego/kitex/pkg/klog"
)

func TestNewOtelLogger(t *testing.T) {
	// 通过接口类型调用
	var logger klog.FullLogger
	logger = NewOtelLogger("test-service")

	if logger == nil {
		t.Fatal("logger should not be nil")
	}

	// 测试基本日志
	logger.Info("测试信息日志")
	logger.Infof("格式化日志: %s", "test")
	logger.Debug("调试日志")
	logger.Warn("警告日志")
}

func TestNewOtelLoggerWithFormat(t *testing.T) {
	var logger klog.FullLogger

	// JSON 格式
	logger = NewOtelLoggerWithFormat("test-json", true)
	logger.Info("JSON format log")

	// 文本格式
	logger = NewOtelLoggerWithFormat("test-text", false)
	logger.Info("Text format log")
}

func TestNewOtelLoggerWithConfig(t *testing.T) {
	ctx := context.Background()

	// 使用配置创建
	logger, provider, err := NewOtelLoggerWithConfig(ctx,
		WithServiceName("test-service"),
		WithServiceVersion("1.0.0"),
		WithEnvironment("test"),
		WithEndpoint("localhost:4317"),
		WithInsecure(true),
		WithJSONFormat(true),
	)

	if err != nil {
		t.Skipf("跳过 OTEL 配置测试（需要 OTEL Collector）: %v", err)
		return
	}

	if logger == nil {
		t.Fatal("logger should not be nil")
	}

	if provider == nil {
		t.Fatal("provider should not be nil")
	}

	defer provider.Shutdown(ctx)

	// 测试日志
	logger.Info("配置日志测试")
	logger.Infof("服务名称: %s", "test-service")
}

func TestLoggerLevels(t *testing.T) {
	var logger klog.FullLogger
	logger = NewOtelLogger("test-levels")

	// 测试所有级别
	logger.Trace("trace log")
	logger.Debug("debug log")
	logger.Info("info log")
	logger.Notice("notice log")
	logger.Warn("warn log")
	logger.Error("error log")
}

func TestLoggerWithContext(t *testing.T) {
	var logger klog.FullLogger
	logger = NewOtelLogger("test-ctx")

	ctx := context.Background()

	// 测试带上下文的日志
	logger.CtxInfof(ctx, "context log", "key", "value")
	logger.CtxDebugf(ctx, "debug with context", "request_id", "123")
	logger.CtxWarnf(ctx, "warn with context", "user_id", "456")
}

func TestLoggerSetLevel(t *testing.T) {
	var logger klog.FullLogger
	logger = NewOtelLogger("test-level")

	// 设置日志级别
	logger.SetLevel(klog.LevelWarn)

	// Debug 和 Info 不应输出
	logger.Debug("should not output")
	logger.Info("should not output")

	// Warn 和 Error 应该输出
	logger.Warn("should output")
	logger.Error("should output")
}

func TestLoggerInterface(t *testing.T) {
	// 确保实现了接口
	var _ klog.FullLogger = NewOtelLogger("test")
	var _ klog.Logger = NewOtelLogger("test")
	var _ klog.FormatLogger = NewOtelLogger("test")
	var _ klog.CtxLogger = NewOtelLogger("test")
	var _ klog.Control = NewOtelLogger("test")
}
