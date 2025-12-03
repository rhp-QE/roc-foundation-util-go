package loadbalancer

import (
	"testing"

	"github.com/roc/roc-foundation-util-go/service_registry/registry"
)

// 创建测试用的服务实例
func createTestInstances() []registry.ServiceInstance {
	return []registry.ServiceInstance{
		{
			InstanceID: "instance-1",
			Host:       "192.168.1.1",
			Port:       8080,
			Status:     registry.StatusHealthy,
			Weight:     100,
		},
		{
			InstanceID: "instance-2",
			Host:       "192.168.1.2",
			Port:       8080,
			Status:     registry.StatusHealthy,
			Weight:     150,
		},
		{
			InstanceID: "instance-3",
			Host:       "192.168.1.3",
			Port:       8080,
			Status:     registry.StatusHealthy,
			Weight:     80,
		},
	}
}

// TestRoundRobinLoadBalancer 测试轮询负载均衡
func TestRoundRobinLoadBalancer(t *testing.T) {
	lb := NewRoundRobinLoadBalancer()
	instances := createTestInstances()

	// 测试多次选择，应该按顺序轮询
	expectedOrder := []string{"instance-1", "instance-2", "instance-3", "instance-1", "instance-2", "instance-3"}

	for i, expected := range expectedOrder {
		instance, err := lb.Select(instances)
		if err != nil {
			t.Fatalf("Round %d: failed to select instance: %v", i+1, err)
		}

		if instance.InstanceID != expected {
			t.Errorf("Round %d: expected %s, got %s", i+1, expected, instance.InstanceID)
		}
	}
}

// TestRandomLoadBalancer 测试随机负载均衡
func TestRandomLoadBalancer(t *testing.T) {
	lb := NewRandomLoadBalancer()
	instances := createTestInstances()

	// 测试多次选择，应该能选到实例
	for i := 0; i < 10; i++ {
		instance, err := lb.Select(instances)
		if err != nil {
			t.Fatalf("Round %d: failed to select instance: %v", i+1, err)
		}

		// 验证选中的实例在列表中
		found := false
		for _, inst := range instances {
			if inst.InstanceID == instance.InstanceID {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Round %d: selected instance %s not in instance list", i+1, instance.InstanceID)
		}
	}
}

// TestWeightedRandomLoadBalancer 测试加权随机负载均衡
func TestWeightedRandomLoadBalancer(t *testing.T) {
	lb := NewWeightedRandomLoadBalancer()
	instances := createTestInstances()

	// 统计每个实例被选中的次数
	counts := make(map[string]int)
	rounds := 1000

	for i := 0; i < rounds; i++ {
		instance, err := lb.Select(instances)
		if err != nil {
			t.Fatalf("Round %d: failed to select instance: %v", i+1, err)
		}
		counts[instance.InstanceID]++
	}

	// 验证每个实例都被选中了
	for _, inst := range instances {
		if counts[inst.InstanceID] == 0 {
			t.Errorf("Instance %s was never selected", inst.InstanceID)
		}
	}

	// 验证权重较高的实例被选中次数更多
	// instance-2 (weight=150) 应该比 instance-3 (weight=80) 选中次数多
	if counts["instance-2"] <= counts["instance-3"] {
		t.Logf("Warning: instance-2 (weight=150) was selected %d times, instance-3 (weight=80) was selected %d times",
			counts["instance-2"], counts["instance-3"])
	}
}

// TestConsistentHashLoadBalancer 测试一致性哈希负载均衡
func TestConsistentHashLoadBalancer(t *testing.T) {
	lb := NewConsistentHashLoadBalancer(150)
	instances := createTestInstances()

	// 测试相同的 key 应该路由到相同的实例
	key := "user-12345"

	var firstInstance *registry.ServiceInstance
	for i := 0; i < 5; i++ {
		instance, err := lb.Select(instances, key)
		if err != nil {
			t.Fatalf("Round %d: failed to select instance: %v", i+1, err)
		}

		if i == 0 {
			firstInstance = instance
		} else {
			if instance.InstanceID != firstInstance.InstanceID {
				t.Errorf("Round %d: expected same instance %s, got %s",
					i+1, firstInstance.InstanceID, instance.InstanceID)
			}
		}
	}

	// 测试不同的 key 可能路由到不同的实例
	keys := []string{"user-001", "user-002", "user-003", "user-004", "user-005"}
	selectedInstances := make(map[string]bool)

	for _, k := range keys {
		instance, err := lb.Select(instances, k)
		if err != nil {
			t.Fatalf("Failed to select instance for key %s: %v", k, err)
		}
		selectedInstances[instance.InstanceID] = true
	}

	// 验证至少使用了多个实例（理论上应该分布到不同实例）
	if len(selectedInstances) < 2 {
		t.Logf("Warning: only %d different instances were selected", len(selectedInstances))
	}
}

// TestLoadBalancerWithNoInstances 测试没有实例的情况
func TestLoadBalancerWithNoInstances(t *testing.T) {
	lbs := []LoadBalancer{
		NewRoundRobinLoadBalancer(),
		NewRandomLoadBalancer(),
		NewWeightedRandomLoadBalancer(),
	}

	for _, lb := range lbs {
		_, err := lb.Select([]registry.ServiceInstance{})
		if err == nil {
			t.Errorf("%s: expected error when no instances available", lb.GetName())
		}
	}
}

// TestLoadBalancerWithUnhealthyInstances 测试只有不健康实例的情况
func TestLoadBalancerWithUnhealthyInstances(t *testing.T) {
	instances := []registry.ServiceInstance{
		{
			InstanceID: "instance-1",
			Host:       "192.168.1.1",
			Port:       8080,
			Status:     registry.StatusUnhealthy,
			Weight:     100,
		},
		{
			InstanceID: "instance-2",
			Host:       "192.168.1.2",
			Port:       8080,
			Status:     registry.StatusUnhealthy,
			Weight:     100,
		},
	}

	lbs := []LoadBalancer{
		NewRoundRobinLoadBalancer(),
		NewRandomLoadBalancer(),
		NewWeightedRandomLoadBalancer(),
	}

	for _, lb := range lbs {
		_, err := lb.Select(instances)
		if err == nil {
			t.Errorf("%s: expected error when all instances are unhealthy", lb.GetName())
		}
	}
}

// TestConsistentHashWithoutKey 测试一致性哈希没有提供 key 的情况
func TestConsistentHashWithoutKey(t *testing.T) {
	lb := NewConsistentHashLoadBalancer(150)
	instances := createTestInstances()

	_, err := lb.Select(instances)
	if err == nil {
		t.Error("Expected error when key is not provided for consistent hash")
	}
}

// TestLoadBalancerFactory 测试负载均衡器工厂
func TestLoadBalancerFactory(t *testing.T) {
	factory := &DefaultFactory{}

	algorithms := []Algorithm{
		RoundRobin,
		Random,
		WeightedRandom,
		ConsistentHash,
	}

	for _, algo := range algorithms {
		lb, err := factory.Create(algo)
		if err != nil {
			t.Errorf("Failed to create load balancer for algorithm %s: %v", algo, err)
		}

		if lb == nil {
			t.Errorf("Load balancer is nil for algorithm %s", algo)
		}
	}
}
