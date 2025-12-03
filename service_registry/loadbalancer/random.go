package loadbalancer

import (
	"math/rand"
	"time"

	"github.com/roc/roc-foundation-util-go/service_registry/registry"
)

// RandomLoadBalancer 随机负载均衡器
type RandomLoadBalancer struct {
	rnd *rand.Rand
}

// NewRandomLoadBalancer 创建随机负载均衡器
func NewRandomLoadBalancer() *RandomLoadBalancer {
	return &RandomLoadBalancer{
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Select 使用随机算法选择实例
func (lb *RandomLoadBalancer) Select(instances []registry.ServiceInstance, key ...string) (*registry.ServiceInstance, error) {
	if len(instances) == 0 {
		return nil, ErrNoAvailableInstance
	}

	// 过滤健康实例
	healthy := FilterHealthy(instances)
	if len(healthy) == 0 {
		return nil, ErrNoAvailableInstance
	}

	// 随机选择
	idx := lb.rnd.Intn(len(healthy))
	return &healthy[idx], nil
}

// GetName 获取负载均衡器名称
func (lb *RandomLoadBalancer) GetName() string {
	return "Random"
}

// WeightedRandomLoadBalancer 加权随机负载均衡器
type WeightedRandomLoadBalancer struct {
	rnd *rand.Rand
}

// NewWeightedRandomLoadBalancer 创建加权随机负载均衡器
func NewWeightedRandomLoadBalancer() *WeightedRandomLoadBalancer {
	return &WeightedRandomLoadBalancer{
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Select 使用加权随机算法选择实例
func (lb *WeightedRandomLoadBalancer) Select(instances []registry.ServiceInstance, key ...string) (*registry.ServiceInstance, error) {
	if len(instances) == 0 {
		return nil, ErrNoAvailableInstance
	}

	// 过滤健康实例
	healthy := FilterHealthy(instances)
	if len(healthy) == 0 {
		return nil, ErrNoAvailableInstance
	}

	// 计算总权重
	totalWeight := 0
	for _, instance := range healthy {
		weight := instance.Weight
		if weight <= 0 {
			weight = 1 // 默认权重为1
		}
		totalWeight += weight
	}

	// 生成随机数
	randomWeight := lb.rnd.Intn(totalWeight)

	// 根据权重选择实例
	for _, instance := range healthy {
		weight := instance.Weight
		if weight <= 0 {
			weight = 1
		}
		randomWeight -= weight
		if randomWeight < 0 {
			result := instance
			return &result, nil
		}
	}

	// 兜底返回第一个实例
	return &healthy[0], nil
}

// GetName 获取负载均衡器名称
func (lb *WeightedRandomLoadBalancer) GetName() string {
	return "WeightedRandom"
}
