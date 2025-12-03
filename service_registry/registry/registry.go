package registry

import "context"

// Registry 服务注册中心接口
// 定义了服务注册、注销、发现等核心功能
type Registry interface {
	// Register 注册服务实例
	Register(ctx context.Context, instance *ServiceInstance) error

	// Deregister 注销服务实例
	Deregister(ctx context.Context, instance *ServiceInstance) error

	// Discover 发现服务实例
	Discover(ctx context.Context, serviceName string) ([]ServiceInstance, error)

	// Watch 监听服务变化
	// 返回一个通道，当服务实例发生变化时，会通过通道发送最新的实例列表
	Watch(ctx context.Context, serviceName string) (<-chan []ServiceInstance, error)

	// Subscribe 订阅服务变化
	// 使用回调函数处理服务实例变化
	Subscribe(ctx context.Context, serviceName string, handler InstanceChangeHandler) error

	// Heartbeat 发送心跳
	Heartbeat(ctx context.Context, instance *ServiceInstance) error

	// UpdateStatus 更新实例状态
	UpdateStatus(ctx context.Context, instance *ServiceInstance, status InstanceStatus) error

	// UpdateMetadata 更新实例元数据
	UpdateMetadata(ctx context.Context, instance *ServiceInstance, metadata map[string]string) error

	// GetService 获取指定服务的详细信息
	GetService(ctx context.Context, serviceName string, instanceID string) (*ServiceInstance, error)

	// ListServices 列出所有服务名称
	ListServices(ctx context.Context) ([]string, error)

	// Close 关闭注册中心连接
	Close() error

	// HealthCheck 健康检查
	HealthCheck(ctx context.Context) error
}

// RegistryOption 注册中心配置选项
type RegistryOption func(interface{})

// WithNamespace 设置命名空间
func WithNamespace(namespace string) RegistryOption {
	return func(r interface{}) {
		if setter, ok := r.(interface{ SetNamespace(string) }); ok {
			setter.SetNamespace(namespace)
		}
	}
}

// WithTimeout 设置超时时间
func WithTimeout(timeout int) RegistryOption {
	return func(r interface{}) {
		if setter, ok := r.(interface{ SetTimeout(int) }); ok {
			setter.SetTimeout(timeout)
		}
	}
}

// WithTTL 设置服务实例 TTL
func WithTTL(ttl int64) RegistryOption {
	return func(r interface{}) {
		if setter, ok := r.(interface{ SetTTL(int64) }); ok {
			setter.SetTTL(ttl)
		}
	}
}
