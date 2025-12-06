/*
 * Copyright 2021 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package otel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// LogEntry 表示结构化的日志条目，对齐 zap 格式
type LogEntry struct {
	Level     string                 `json:"level"`
	Timestamp string                 `json:"timestamp"`
	Message   string                 `json:"message"`
	Caller    string                 `json:"caller,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// impl OpenTelemetry logger 的内部实现
// 实现 klog.FullLogger 接口
type impl struct {
	logger       *slog.Logger
	level        klog.Level
	output       io.Writer
	jsonFormat   bool
	enableCaller bool // 是否启用调用者信息（文件名和行号）
	mu           sync.RWMutex
}

// NewOtelLogger 创建新的 OpenTelemetry logger 实例
//
// 参数：
//   - loggerName: logger 名称
//
// 返回：
//   - klog.FullLogger 接口实例
func NewOtelLogger(loggerName string) klog.FullLogger {
	// 使用全局的 LoggerProvider
	otelLogger := otelslog.NewLogger(loggerName)

	return &impl{
		logger:       otelLogger,
		level:        klog.LevelInfo, // 默认级别
		output:       nil,            // 不使用本地输出，直接发送到 OTEL
		jsonFormat:   true,           // 默认使用 JSON 格式，对齐 zap
		enableCaller: true,           // 默认启用调用者信息
	}
}

// NewOtelLoggerWithFormat 创建指定格式的 OpenTelemetry logger 实例
func NewOtelLoggerWithFormat(loggerName string, jsonFormat bool) klog.FullLogger {
	otelLogger := otelslog.NewLogger(loggerName)

	return &impl{
		logger:       otelLogger,
		level:        klog.LevelInfo,
		output:       nil,
		jsonFormat:   jsonFormat,
		enableCaller: true, // 默认启用调用者信息
	}
}

// NewOtelLoggerWithProvider 使用指定的 LoggerProvider 创建 logger
func NewOtelLoggerWithProvider(loggerName string, provider *sdklog.LoggerProvider) klog.FullLogger {
	global.SetLoggerProvider(provider)
	otelLogger := otelslog.NewLogger(loggerName)

	return &impl{
		logger:     otelLogger,
		level:      klog.LevelInfo,
		output:     nil,
		jsonFormat: true,
	}
}

// Config OTEL Logger 配置
type Config struct {
	// Endpoint OTLP 端点地址
	Endpoint string

	// Insecure 是否使用不安全连接
	Insecure bool

	// ServiceName 服务名称
	ServiceName string

	// ServiceVersion 服务版本
	ServiceVersion string

	// Environment 部署环境
	Environment string

	// JSONFormat 是否使用 JSON 格式
	JSONFormat bool

	// EnableCaller 是否启用调用者信息（文件名和行号）
	// 注意：启用后会增加性能开销（每次日志调用需要遍历调用栈）
	// 对于高频日志场景，建议禁用以提升性能
	EnableCaller bool
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Endpoint:       "localhost:4317",
		Insecure:       true,
		ServiceName:    "default-service",
		ServiceVersion: "1.0.0",
		Environment:    "development",
		JSONFormat:     true,
		EnableCaller:   true, // 默认启用，便于调试
	}
}

// Option 配置选项
type Option func(*Config)

// WithEndpoint 设置 OTLP 端点
func WithEndpoint(endpoint string) Option {
	return func(c *Config) {
		c.Endpoint = endpoint
	}
}

// WithInsecure 设置是否使用不安全连接
func WithInsecure(insecure bool) Option {
	return func(c *Config) {
		c.Insecure = insecure
	}
}

// WithServiceName 设置服务名称
func WithServiceName(name string) Option {
	return func(c *Config) {
		c.ServiceName = name
	}
}

// WithServiceVersion 设置服务版本
func WithServiceVersion(version string) Option {
	return func(c *Config) {
		c.ServiceVersion = version
	}
}

// WithEnvironment 设置部署环境
func WithEnvironment(env string) Option {
	return func(c *Config) {
		c.Environment = env
	}
}

// WithJSONFormat 设置日志格式
func WithJSONFormat(jsonFormat bool) Option {
	return func(c *Config) {
		c.JSONFormat = jsonFormat
	}
}

// WithEnableCaller 设置是否启用调用者信息（文件名和行号）
// 启用后会增加性能开销，对于高频日志场景建议禁用
func WithEnableCaller(enable bool) Option {
	return func(c *Config) {
		c.EnableCaller = enable
	}
}

// NewOtelLoggerWithConfig 使用配置参数构建 LoggerProvider 并创建 logger
func NewOtelLoggerWithConfig(ctx context.Context, options ...Option) (klog.FullLogger, *sdklog.LoggerProvider, error) {
	// 应用配置
	config := DefaultConfig()
	for _, opt := range options {
		opt(config)
	}

	// 构建 log exporter
	var logExporter sdklog.Exporter
	var err error

	if config.Insecure {
		logExporter, err = otlploggrpc.New(ctx,
			otlploggrpc.WithInsecure(),
			otlploggrpc.WithEndpoint(config.Endpoint),
		)
	} else {
		logExporter, err = otlploggrpc.New(ctx,
			otlploggrpc.WithEndpoint(config.Endpoint),
		)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize log exporter: %w", err)
	}

	// 创建资源信息
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(config.ServiceName),
			semconv.ServiceVersionKey.String(config.ServiceVersion),
			semconv.DeploymentEnvironmentKey.String(config.Environment),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// 构建 LoggerProvider
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(
			sdklog.NewSimpleProcessor(logExporter),
		),
	)

	// 设置全局 provider
	global.SetLoggerProvider(lp)

	// 创建 logger
	otelLogger := otelslog.NewLogger(config.ServiceName)

	return &impl{
		logger:       otelLogger,
		level:        klog.LevelInfo,
		output:       nil,
		jsonFormat:   config.JSONFormat,
		enableCaller: config.EnableCaller,
	}, lp, nil
}

// levelToString 返回级别的字符串表示
func levelToString(l klog.Level) string {
	switch l {
	case klog.LevelTrace:
		return "TRACE"
	case klog.LevelDebug:
		return "DEBUG"
	case klog.LevelInfo:
		return "INFO"
	case klog.LevelNotice:
		return "NOTICE"
	case klog.LevelWarn:
		return "WARN"
	case klog.LevelError:
		return "ERROR"
	case klog.LevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// createJSONLog 创建 JSON 格式的日志条目，对齐 zap 格式
func (ol *impl) createJSONLog(level string, message string, fields map[string]interface{}, caller string) string {
	entry := LogEntry{
		Level:     level,
		Timestamp: time.Now().Format(time.RFC3339), // 使用 RFC3339 格式，对齐 zap
		Message:   message,
		Caller:    caller,
		Fields:    fields,
	}

	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		// 如果 JSON 序列化失败，返回简单的字符串格式
		return fmt.Sprintf(`{"level":"%s","timestamp":"%s","message":"%s","caller":"%s","error":"json_marshal_failed"}`,
			level, entry.Timestamp, message, caller)
	}

	return string(jsonBytes)
}

// getCaller 获取调用者信息（文件名、行号和函数名）
// 自动跳过 klog 包内部的调用，找到用户代码的调用位置
func getCaller(skip int) string {
	// 从 skip 开始，最多查找 10 层调用栈
	for i := skip; i < skip+10; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		// 获取函数名
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}

		funcName := fn.Name()

		// 跳过 klog 包内部的调用（包括 github.com/cloudwego/kitex/pkg/klog 和当前 otel 包）
		if strings.Contains(funcName, "github.com/cloudwego/kitex/pkg/klog") ||
			strings.Contains(funcName, "roc-foundation-util-go/log/otel") {
			continue
		}

		// 找到用户代码，提取信息
		funcNameBase := filepath.Base(funcName)
		fileName := filepath.Base(file)

		// 格式：file:line:function（完整格式，便于调试）
		return fmt.Sprintf("%s:%d:%s", fileName, line, funcNameBase)
	}

	// 如果找不到用户代码，返回空字符串
	return ""
}

// logWithFormat 根据配置决定是否使用 JSON 格式输出
func (ol *impl) logWithFormat(level klog.Level, message string, fields map[string]interface{}) {
	ol.mu.RLock()
	useJSON := ol.jsonFormat
	enableCaller := ol.enableCaller
	ol.mu.RUnlock()

	// 根据配置决定是否获取调用者信息
	var caller string
	if enableCaller {
		// 获取调用者信息（跳过 4 层：getCaller -> logWithFormat -> impl.Infof -> klog.Infof -> 用户代码）
		caller = getCaller(4)
	}

	if useJSON {
		jsonLog := ol.createJSONLog(levelToString(level), message, fields, caller)
		// 使用 slog 输出 JSON 格式的日志
		switch level {
		case klog.LevelTrace, klog.LevelDebug:
			ol.logger.Debug(jsonLog)
		case klog.LevelInfo, klog.LevelNotice:
			ol.logger.Info(jsonLog)
		case klog.LevelWarn:
			ol.logger.Warn(jsonLog)
		case klog.LevelError, klog.LevelFatal:
			ol.logger.Error(jsonLog)
		}
	} else {
		// 非 JSON 格式也包含调用者信息（如果启用）
		var messageWithCaller string
		if enableCaller && caller != "" {
			messageWithCaller = fmt.Sprintf("[%s] %s", caller, message)
		} else {
			messageWithCaller = message
		}
		switch level {
		case klog.LevelTrace, klog.LevelDebug:
			ol.logger.Debug(messageWithCaller)
		case klog.LevelInfo, klog.LevelNotice:
			ol.logger.Info(messageWithCaller)
		case klog.LevelWarn:
			ol.logger.Warn(messageWithCaller)
		case klog.LevelError, klog.LevelFatal:
			ol.logger.Error(messageWithCaller)
		}
	}
}

// Logger 接口实现
func (ol *impl) Trace(v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelTrace {
		ol.logWithFormat(klog.LevelTrace, fmt.Sprint(v...), nil)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Debug(v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelDebug {
		ol.logWithFormat(klog.LevelDebug, fmt.Sprint(v...), nil)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Info(v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelInfo {
		ol.logWithFormat(klog.LevelInfo, fmt.Sprint(v...), nil)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Notice(v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelNotice {
		ol.logWithFormat(klog.LevelNotice, fmt.Sprint(v...), nil)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Warn(v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelWarn {
		ol.logWithFormat(klog.LevelWarn, fmt.Sprint(v...), nil)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Error(v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelError {
		ol.logWithFormat(klog.LevelError, fmt.Sprint(v...), nil)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Fatal(v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelFatal {
		ol.logWithFormat(klog.LevelFatal, fmt.Sprint(v...), nil)
	}
	ol.mu.RUnlock()
}

// FormatLogger 接口实现
func (ol *impl) Tracef(format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelTrace {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormat(klog.LevelTrace, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Debugf(format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelDebug {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormat(klog.LevelDebug, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Infof(format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelInfo {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormat(klog.LevelInfo, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Noticef(format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelNotice {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormat(klog.LevelNotice, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Warnf(format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelWarn {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormat(klog.LevelWarn, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Errorf(format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelError {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormat(klog.LevelError, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) Fatalf(format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelFatal {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormat(klog.LevelFatal, message, fields)
	}
	ol.mu.RUnlock()
}

// logWithFormatContext 根据配置决定是否使用 JSON 格式输出（带上下文）
func (ol *impl) logWithFormatContext(ctx context.Context, level klog.Level, message string, fields map[string]interface{}) {
	ol.mu.RLock()
	useJSON := ol.jsonFormat
	enableCaller := ol.enableCaller
	ol.mu.RUnlock()

	// 根据配置决定是否获取调用者信息
	var caller string
	if enableCaller {
		// 获取调用者信息（跳过 4 层：getCaller -> logWithFormatContext -> impl.CtxInfof -> klog.CtxInfof -> 用户代码）
		caller = getCaller(4)
	}

	if useJSON {
		jsonLog := ol.createJSONLog(levelToString(level), message, fields, caller)
		// 使用 slog 输出 JSON 格式的日志
		switch level {
		case klog.LevelTrace, klog.LevelDebug:
			ol.logger.DebugContext(ctx, jsonLog)
		case klog.LevelInfo, klog.LevelNotice:
			ol.logger.InfoContext(ctx, jsonLog)
		case klog.LevelWarn:
			ol.logger.WarnContext(ctx, jsonLog)
		case klog.LevelError, klog.LevelFatal:
			ol.logger.ErrorContext(ctx, jsonLog)
		}
	} else {
		// 非 JSON 格式也包含调用者信息（如果启用）
		var messageWithCaller string
		if enableCaller && caller != "" {
			messageWithCaller = fmt.Sprintf("[%s] %s", caller, message)
		} else {
			messageWithCaller = message
		}
		switch level {
		case klog.LevelTrace, klog.LevelDebug:
			ol.logger.DebugContext(ctx, messageWithCaller)
		case klog.LevelInfo, klog.LevelNotice:
			ol.logger.InfoContext(ctx, messageWithCaller)
		case klog.LevelWarn:
			ol.logger.WarnContext(ctx, messageWithCaller)
		case klog.LevelError, klog.LevelFatal:
			ol.logger.ErrorContext(ctx, messageWithCaller)
		}
	}
}

// parseKeyValuePairs 解析键值对参数
func parseKeyValuePairs(args []interface{}) (string, map[string]interface{}) {
	if len(args) == 0 {
		return "", nil
	}

	// 检查是否有键值对（偶数个参数且第一个是字符串）
	if len(args)%2 == 0 && len(args) > 0 {
		fields := make(map[string]interface{})
		for i := 0; i < len(args); i += 2 {
			if i+1 < len(args) {
				if key, ok := args[i].(string); ok {
					fields[key] = args[i+1]
				}
			}
		}
		if len(fields) > 0 {
			return "", fields
		}
	}

	// 否则作为格式化参数处理
	return fmt.Sprint(args...), nil
}

// CtxLogger 接口实现
func (ol *impl) CtxTracef(ctx context.Context, format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelTrace {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormatContext(ctx, klog.LevelTrace, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) CtxDebugf(ctx context.Context, format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelDebug {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormatContext(ctx, klog.LevelDebug, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) CtxInfof(ctx context.Context, format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelInfo {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormatContext(ctx, klog.LevelInfo, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) CtxNoticef(ctx context.Context, format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelNotice {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormatContext(ctx, klog.LevelNotice, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) CtxWarnf(ctx context.Context, format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelWarn {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormatContext(ctx, klog.LevelWarn, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) CtxErrorf(ctx context.Context, format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelError {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormatContext(ctx, klog.LevelError, message, fields)
	}
	ol.mu.RUnlock()
}

func (ol *impl) CtxFatalf(ctx context.Context, format string, v ...interface{}) {
	ol.mu.RLock()
	if ol.level <= klog.LevelFatal {
		message, fields := parseKeyValuePairs(v)
		// 如果有字段（键值对模式），使用 format 作为消息；否则使用 format 格式化参数
		if len(fields) > 0 {
			// 键值对模式：使用 format 作为消息模板
			if message == "" {
				message = format
			}
		} else {
			// 非键值对模式：使用 format 格式化参数
			message = fmt.Sprintf(format, v...)
		}
		ol.logWithFormatContext(ctx, klog.LevelFatal, message, fields)
	}
	ol.mu.RUnlock()
}

// SetLevel 实现 Control 接口
func (ol *impl) SetLevel(level klog.Level) {
	ol.mu.Lock()
	ol.level = level
	ol.mu.Unlock()
}

func (ol *impl) SetOutput(w io.Writer) {
	ol.mu.Lock()
	ol.output = w
	ol.mu.Unlock()
}

// SetJSONFormat 设置是否使用 JSON 格式输出
func (ol *impl) SetJSONFormat(useJSON bool) {
	ol.mu.Lock()
	ol.jsonFormat = useJSON
	ol.mu.Unlock()
}

// IsJSONFormat 返回是否使用 JSON 格式
func (ol *impl) IsJSONFormat() bool {
	ol.mu.RLock()
	defer ol.mu.RUnlock()
	return ol.jsonFormat
}

// GetLevel 获取当前日志级别
func (ol *impl) GetLevel() klog.Level {
	ol.mu.RLock()
	defer ol.mu.RUnlock()
	return ol.level
}

// GetLogger 获取底层的 slog.Logger
func (ol *impl) GetLogger() *slog.Logger {
	return ol.logger
}

// 编译时检查，确保 impl 实现了 klog.FullLogger 接口
var _ klog.FullLogger = (*impl)(nil)
