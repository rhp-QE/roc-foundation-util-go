package registry

import "time"

// ServiceInstance 服务实例
type ServiceInstance struct {
	ServiceName string            // 服务名称
	InstanceID  string            // 实例唯一ID
	Host        string            // 主机地址
	Port        int               // 端口号
	Metadata    map[string]string // 元数据
	Status      InstanceStatus    // 实例状态
	Weight      int               // 权重（用于负载均衡）
	Version     string            // 服务版本
	RegisterAt  time.Time         // 注册时间
	UpdateAt    time.Time         // 更新时间
}

// InstanceStatus 实例状态
type InstanceStatus string

const (
	StatusHealthy   InstanceStatus = "healthy"   // 健康
	StatusUnhealthy InstanceStatus = "unhealthy" // 不健康
	StatusStarting  InstanceStatus = "starting"  // 启动中
	StatusStopping  InstanceStatus = "stopping"  // 停止中
)

// String 返回实例的字符串表示
func (s *ServiceInstance) String() string {
	return s.Host + ":" + string(rune(s.Port))
}

// GetAddress 获取实例地址
func (s *ServiceInstance) GetAddress() string {
	return s.String()
}

// IsHealthy 判断实例是否健康
func (s *ServiceInstance) IsHealthy() bool {
	return s.Status == StatusHealthy
}

// InstanceChangeHandler 实例变更处理函数
type InstanceChangeHandler func(serviceName string, instances []ServiceInstance)

// ServiceFilter 服务过滤器
type ServiceFilter func(instance ServiceInstance) bool

// HealthyFilter 健康状态过滤器
func HealthyFilter(instance ServiceInstance) bool {
	return instance.IsHealthy()
}

// VersionFilter 版本过滤器
func VersionFilter(version string) ServiceFilter {
	return func(instance ServiceInstance) bool {
		return instance.Version == version
	}
}

// MetadataFilter 元数据过滤器
func MetadataFilter(key, value string) ServiceFilter {
	return func(instance ServiceInstance) bool {
		if v, ok := instance.Metadata[key]; ok {
			return v == value
		}
		return false
	}
}
