package loadbalancer

import (
	"errors"

	"github.com/roc/roc-foundation-util-go/service_registry/registry"
)

var (
	ErrNoAvailableInstance = errors.New("no available instance")
	ErrInvalidKey          = errors.New("invalid key for load balancer")
)

// LoadBalancer 负载均衡器接口
type LoadBalancer interface {
	// Select 从实例列表中选择一个实例
	// instances: 可用的服务实例列表
	// key: 可选的键（用于一致性哈希等算法）
	Select(instances []registry.ServiceInstance, key ...string) (*registry.ServiceInstance, error)

	// GetName 获取负载均衡器名称
	GetName() string
}

// Algorithm 负载均衡算法类型
type Algorithm string

const (
	RoundRobin     Algorithm = "round_robin"     // 轮询
	Random         Algorithm = "random"          // 随机
	WeightedRandom Algorithm = "weighted_random" // 加权随机
	LeastConn      Algorithm = "least_conn"      // 最少连接
	ConsistentHash Algorithm = "consistent_hash" // 一致性哈希
)

// Factory 负载均衡器工厂接口
type Factory interface {
	// Create 创建负载均衡器
	Create(algorithm Algorithm) (LoadBalancer, error)
}

// DefaultFactory 默认负载均衡器工厂
type DefaultFactory struct{}

// Create 创建负载均衡器
func (f *DefaultFactory) Create(algorithm Algorithm) (LoadBalancer, error) {
	switch algorithm {
	case RoundRobin:
		return NewRoundRobinLoadBalancer(), nil
	case Random:
		return NewRandomLoadBalancer(), nil
	case WeightedRandom:
		return NewWeightedRandomLoadBalancer(), nil
	case ConsistentHash:
		return NewConsistentHashLoadBalancer(150), nil
	default:
		return NewRoundRobinLoadBalancer(), nil
	}
}

// FilterHealthy 过滤出健康的实例
func FilterHealthy(instances []registry.ServiceInstance) []registry.ServiceInstance {
	healthy := make([]registry.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		if instance.IsHealthy() {
			healthy = append(healthy, instance)
		}
	}
	return healthy
}
