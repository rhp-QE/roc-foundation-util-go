package loadbalancer

import (
	"sync/atomic"

	"github.com/roc/roc-foundation-util-go/service_registry/registry"
)

// RoundRobinLoadBalancer 轮询负载均衡器
type RoundRobinLoadBalancer struct {
	counter uint64 // 计数器
}

// NewRoundRobinLoadBalancer 创建轮询负载均衡器
func NewRoundRobinLoadBalancer() *RoundRobinLoadBalancer {
	return &RoundRobinLoadBalancer{
		counter: 0,
	}
}

// Select 使用轮询算法选择实例
func (lb *RoundRobinLoadBalancer) Select(instances []registry.ServiceInstance, key ...string) (*registry.ServiceInstance, error) {
	if len(instances) == 0 {
		return nil, ErrNoAvailableInstance
	}

	// 过滤健康实例
	healthy := FilterHealthy(instances)
	if len(healthy) == 0 {
		return nil, ErrNoAvailableInstance
	}

	// 原子获取并递增计数器
	idx := atomic.AddUint64(&lb.counter, 1) - 1
	idx = idx % uint64(len(healthy))
	return &healthy[idx], nil
}

// GetName 获取负载均衡器名称
func (lb *RoundRobinLoadBalancer) GetName() string {
	return "RoundRobin"
}
