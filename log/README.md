# Log 模块

日志抽象层，基于 OpenTelemetry 实现，完全兼容 Kitex klog 接口。

## 特性

- ✅ **完全兼容 Kitex**: 实现 `klog.FullLogger` 接口，可直接集成到 Kitex
- 🚀 **OTEL 上报**: 日志自动上报到 OpenTelemetry Collector
- 📊 **结构化日志**: 支持 JSON 格式，对齐 zap 日志规范
- 🎯 **多级别支持**: Trace/Debug/Info/Notice/Warn/Error/Fatal
- 🔄 **上下文传递**: 支持带 context 的日志，自动关联 trace

## 快速使用

### 基本使用

```go
import (
    "github.com/roc/roc-foundation-util-go/log/otel"
    "github.com/cloudwego/kitex/pkg/klog"
)

// 创建 logger
var logger klog.FullLogger
logger = otel.NewOtelLogger("my-service")

// 使用日志
logger.Info("服务启动")
logger.Infof("用户登录: %s", userID)
logger.Warn("警告信息")
```

### 带上下文日志

```go
// 自动关联分布式追踪
logger.CtxInfof(ctx, "处理请求", 
    "request_id", reqID, 
    "user_id", userID,
    "latency", latency,
)
```

### 完整配置（OTEL 上报）

```go
import (
    "context"
    "github.com/roc/roc-foundation-util-go/log/otel"
)

ctx := context.Background()

// 配置 OTEL Collector 端点
logger, provider, err := otel.NewOtelLoggerWithConfig(ctx,
    otel.WithServiceName("user-service"),
    otel.WithServiceVersion("1.0.0"),
    otel.WithEnvironment("production"),
    otel.WithEndpoint("otel-collector:4317"),
    otel.WithInsecure(true),
    otel.WithJSONFormat(true),
)
if err != nil {
    panic(err)
}
defer provider.Shutdown(context.Background())

// 日志自动上报到 OTEL Collector
logger.Info("Service started", "port", 8080)
```

### 集成到 Kitex

```go
import (
    "github.com/cloudwego/kitex/pkg/klog"
    "github.com/roc/roc-foundation-util-go/log/otel"
)

func main() {
    // 创建 OTEL logger
    logger := otel.NewOtelLogger("my-kitex-service")
    
    // 设置为 Kitex 全局 logger
    klog.SetLogger(logger)
    
    // Kitex 框架日志自动上报
    // 所有 klog.Info/Error 等调用都会上报到 OTEL
}
```

## 配置选项

### WithEndpoint

设置 OTLP 端点地址。

```go
otel.WithEndpoint("localhost:4317")
```

### WithInsecure

设置是否使用不安全连接（不使用 TLS）。

```go
otel.WithInsecure(true)  // 测试环境
otel.WithInsecure(false) // 生产环境
```

### WithServiceName

设置服务名称，用于标识日志来源。

```go
otel.WithServiceName("user-service")
```

### WithServiceVersion

设置服务版本。

```go
otel.WithServiceVersion("1.0.0")
```

### WithEnvironment

设置部署环境。

```go
otel.WithEnvironment("production")
otel.WithEnvironment("staging")
otel.WithEnvironment("development")
```

### WithJSONFormat

设置日志格式。

```go
otel.WithJSONFormat(true)  // JSON 格式
otel.WithJSONFormat(false) // 文本格式
```

## 日志级别

```go
logger.SetLevel(klog.LevelTrace)   // 追踪
logger.SetLevel(klog.LevelDebug)   // 调试
logger.SetLevel(klog.LevelInfo)    // 信息（默认）
logger.SetLevel(klog.LevelNotice)  // 注意
logger.SetLevel(klog.LevelWarn)    // 警告
logger.SetLevel(klog.LevelError)   // 错误
logger.SetLevel(klog.LevelFatal)   // 致命
```

## klog.FullLogger 接口

完全实现了 Kitex klog.FullLogger 接口：

### Logger 接口
```go
Trace(v ...interface{})
Debug(v ...interface{})
Info(v ...interface{})
Notice(v ...interface{})
Warn(v ...interface{})
Error(v ...interface{})
Fatal(v ...interface{})
```

### FormatLogger 接口
```go
Tracef(format string, v ...interface{})
Debugf(format string, v ...interface{})
Infof(format string, v ...interface{})
Noticef(format string, v ...interface{})
Warnf(format string, v ...interface{})
Errorf(format string, v ...interface{})
Fatalf(format string, v ...interface{})
```

### CtxLogger 接口
```go
CtxTracef(ctx context.Context, format string, v ...interface{})
CtxDebugf(ctx context.Context, format string, v ...interface{})
CtxInfof(ctx context.Context, format string, v ...interface{})
CtxNoticef(ctx context.Context, format string, v ...interface{})
CtxWarnf(ctx context.Context, format string, v ...interface{})
CtxErrorf(ctx context.Context, format string, v ...interface{})
CtxFatalf(ctx context.Context, format string, v ...interface{})
```

### Control 接口
```go
SetLevel(Level)
SetOutput(io.Writer)
```

## OpenTelemetry Collector 配置示例

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  logging:
    loglevel: debug
  elasticsearch:
    endpoints: ["http://elasticsearch:9200"]
    logs_index: "app-logs"
  loki:
    endpoint: "http://loki:3100/loki/api/v1/push"

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [logging, elasticsearch, loki]
```

## 使用场景

### 微服务日志统一收集

```go
// 所有微服务使用统一配置
logger, provider, _ := otel.NewOtelLoggerWithConfig(ctx,
    otel.WithServiceName("order-service"),
    otel.WithEndpoint("otel-collector:4317"),
)
defer provider.Shutdown(context.Background())

// 日志自动上报，统一收集
logger.Info("订单创建", "order_id", orderID, "amount", amount)
```

### 分布式追踪关联

```go
// 日志自动关联 trace_id 和 span_id
logger.CtxInfof(ctx, "处理请求",
    "user_id", userID,
    "action", "create_order",
)
```

### 结构化日志查询

```go
// 键值对自动解析为结构化字段
logger.Infof("用户操作",
    "action", "login",
    "user_id", "12345",
    "ip", "192.168.1.1",
    "success", true,
)

// 输出 JSON（可在 ES/Loki 中查询）:
// {
//   "level":"INFO",
//   "timestamp":"2024-12-04T10:30:00Z",
//   "message":"用户操作",
//   "fields":{
//     "action":"login",
//     "user_id":"12345",
//     "ip":"192.168.1.1",
//     "success":true
//   }
// }
```

## 测试

```bash
# 运行测试
cd log/otel
go test -v

# 测试特定功能
go test -v -run TestNewOtelLogger
go test -v -run TestLoggerWithContext
```

## 依赖

- `github.com/cloudwego/kitex` - Kitex klog 接口定义
- `go.opentelemetry.io/contrib/bridges/otelslog` - OTEL slog 桥接
- `go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc` - OTLP 日志导出器
- `go.opentelemetry.io/otel/sdk/log` - OTEL 日志 SDK

## 优势

| 特性 | OTEL Logger | 标准 slog | zap |
|------|-------------|-----------|-----|
| **Kitex 兼容** | ✅ | ❌ | ✅ |
| **自动上报** | ✅ | ❌ | ❌ |
| **分布式追踪** | ✅ | ❌ | ❌ |
| **结构化日志** | ✅ | ✅ | ✅ |
| **性能** | 高 | 高 | 极高 |
| **可观测性** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
