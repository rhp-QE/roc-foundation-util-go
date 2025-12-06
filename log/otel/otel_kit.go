package otel

import (
	"context"
	"fmt"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/kitex-contrib/obs-opentelemetry/provider"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// OTELKit OpenTelemetry 工具集，统一管理所有 OTEL 组件
type OTELKit struct {
	// Logger Kitex logger 实例（已设置为全局 logger）
	Logger klog.FullLogger

	// TraceProvider Trace/Metrics Provider（链路追踪）
	TraceProvider provider.OtelProvider

	// LogProvider 日志 Provider
	LogProvider *sdklog.LoggerProvider
}

// InitOTEL 一键初始化 OpenTelemetry 完整系统（Trace + Log）
//
// 这个方法会：
//  1. 创建并配置 TracerProvider（链路追踪）
//  2. 创建并配置 LoggerProvider（日志上报）
//  3. 创建 Kitex 兼容的 Logger
//  4. 自动设置为 Kitex 全局 logger
//
// 使用示例：
//
//	kit, err := otel.InitOTEL(ctx,
//	    otel.WithServiceName("my-service"),
//	    otel.WithEndpoint("localhost:4317"),
//	    otel.WithInsecure(true),
//	)
//	if err != nil {
//	    panic(err)
//	}
//	defer kit.Shutdown(ctx)
//
//	// 之后直接使用 klog，自动关联 trace_id
//	klog.Info("服务启动")
//	klog.CtxInfof(ctx, "处理请求", "user_id", 123)
func InitOTEL(ctx context.Context, options ...Option) (*OTELKit, error) {
	// 应用配置
	config := DefaultConfig()
	for _, opt := range options {
		opt(config)
	}

	// 1. 创建 Trace Provider（链路追踪 + 指标）
	traceProvider := provider.NewOpenTelemetryProvider(
		provider.WithServiceName(config.ServiceName),
		provider.WithExportEndpoint(config.Endpoint),
		provider.WithInsecure(),
	)

	// 2. 创建 Log Provider 和 Logger
	logger, logProvider, err := NewOtelLoggerWithConfig(ctx, options...)
	if err != nil {
		traceProvider.Shutdown(ctx) // 清理已创建的 trace provider
		return nil, fmt.Errorf("failed to init otel logger: %w", err)
	}

	// 3. 自动设置为 Kitex 全局 logger
	klog.SetLogger(logger)

	// 4. 返回统一的工具集
	return &OTELKit{
		Logger:        logger,
		TraceProvider: traceProvider,
		LogProvider:   logProvider,
	}, nil
}

// Shutdown 优雅关闭所有 Provider，确保数据都已发送
func (k *OTELKit) Shutdown(ctx context.Context) error {
	var err error

	// 关闭 Trace Provider
	if k.TraceProvider != nil {
		if e := k.TraceProvider.Shutdown(ctx); e != nil {
			err = fmt.Errorf("trace provider shutdown failed: %w", e)
		}
	}

	// 关闭 Log Provider
	if k.LogProvider != nil {
		if e := k.LogProvider.Shutdown(ctx); e != nil {
			if err != nil {
				err = fmt.Errorf("%v; log provider shutdown failed: %w", err, e)
			} else {
				err = fmt.Errorf("log provider shutdown failed: %w", e)
			}
		}
	}

	return err
}

// ForceFlush 强制刷新缓存的日志
func (k *OTELKit) ForceFlush(ctx context.Context) error {
	if k.LogProvider != nil {
		return k.LogProvider.ForceFlush(ctx)
	}
	return nil
}
