package discovery

import (
	"context"
	"testing"
	"time"

	"github.com/roc/roc-foundation-util-go/service_registry/loadbalancer"
	"github.com/roc/roc-foundation-util-go/service_registry/registry"
	"github.com/roc/roc-foundation-util-go/service_registry/registry/etcd"
)

// 注意：etcd 客户端已经默认配置为只显示 Error 级别日志
// 这样可以过滤掉测试中的 "context canceled" 等正常的 warn 日志

// TestNewDiscovery 测试创建服务发现客户端
func TestNewDiscovery(t *testing.T) {
	reg, err := etcd.NewEtcdRegistry(
		etcd.WithEndpoints([]string{"localhost:2379"}),
		etcd.WithNamespace("/test/services"),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	lb := loadbalancer.NewRoundRobinLoadBalancer()
	client := NewDiscovery(reg, lb)
	defer client.Close()

	if client == nil {
		t.Fatal("Client should not be nil")
	}
}

// TestGetInstance 测试获取服务实例
func TestGetInstance(t *testing.T) {
	reg, err := etcd.NewEtcdRegistry(
		etcd.WithEndpoints([]string{"localhost:2379"}),
		etcd.WithNamespace("/test/services"),
		etcd.WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	// 注册测试服务
	instance := &registry.ServiceInstance{
		ServiceName: "test-discovery-service",
		InstanceID:  "test-discovery-001",
		Host:        "127.0.0.1",
		Port:        9001,
		Status:      registry.StatusHealthy,
		Weight:      100,
		Version:     "v1.0.0",
	}

	err = reg.Register(ctx, instance)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}
	defer reg.Deregister(ctx, instance)

	// 创建客户端
	lb := loadbalancer.NewRoundRobinLoadBalancer()
	client := NewDiscovery(reg, lb, WithCache(true))
	defer client.Close()

	// 等待注册完成
	time.Sleep(1 * time.Second)

	// 获取实例
	selectedInstance, err := client.GetInstance(ctx, "test-discovery-service")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	if selectedInstance.InstanceID != instance.InstanceID {
		t.Errorf("Expected instance %s, got %s", instance.InstanceID, selectedInstance.InstanceID)
	}
}

// TestGetInstances 测试获取所有实例
func TestGetInstances(t *testing.T) {
	reg, err := etcd.NewEtcdRegistry(
		etcd.WithEndpoints([]string{"localhost:2379"}),
		etcd.WithNamespace("/test/services"),
		etcd.WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	// 注册多个测试服务
	instances := []*registry.ServiceInstance{
		{
			ServiceName: "test-multi-service",
			InstanceID:  "test-multi-001",
			Host:        "127.0.0.1",
			Port:        9001,
			Status:      registry.StatusHealthy,
			Weight:      100,
		},
		{
			ServiceName: "test-multi-service",
			InstanceID:  "test-multi-002",
			Host:        "127.0.0.1",
			Port:        9002,
			Status:      registry.StatusHealthy,
			Weight:      100,
		},
	}

	for _, inst := range instances {
		err = reg.Register(ctx, inst)
		if err != nil {
			t.Fatalf("Failed to register service: %v", err)
		}
		defer reg.Deregister(ctx, inst)
	}

	// 创建客户端
	lb := loadbalancer.NewRoundRobinLoadBalancer()
	client := NewDiscovery(reg, lb)
	defer client.Close()

	// 等待注册完成
	time.Sleep(1 * time.Second)

	// 获取所有实例
	discoveredInstances, err := client.GetInstances(ctx, "test-multi-service")
	if err != nil {
		t.Fatalf("Failed to get instances: %v", err)
	}

	if len(discoveredInstances) != len(instances) {
		t.Errorf("Expected %d instances, got %d", len(instances), len(discoveredInstances))
	}
}

// TestGetHealthyInstances 测试获取健康实例
func TestGetHealthyInstances(t *testing.T) {
	reg, err := etcd.NewEtcdRegistry(
		etcd.WithEndpoints([]string{"localhost:2379"}),
		etcd.WithNamespace("/test/services"),
		etcd.WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	// 注册多个测试服务，包括不健康的
	instances := []*registry.ServiceInstance{
		{
			ServiceName: "test-health-service",
			InstanceID:  "test-health-001",
			Host:        "127.0.0.1",
			Port:        9001,
			Status:      registry.StatusHealthy,
			Weight:      100,
		},
		{
			ServiceName: "test-health-service",
			InstanceID:  "test-health-002",
			Host:        "127.0.0.1",
			Port:        9002,
			Status:      registry.StatusUnhealthy,
			Weight:      100,
		},
		{
			ServiceName: "test-health-service",
			InstanceID:  "test-health-003",
			Host:        "127.0.0.1",
			Port:        9003,
			Status:      registry.StatusHealthy,
			Weight:      100,
		},
	}

	for _, inst := range instances {
		err = reg.Register(ctx, inst)
		if err != nil {
			t.Fatalf("Failed to register service: %v", err)
		}
		defer reg.Deregister(ctx, inst)
	}

	// 创建客户端
	lb := loadbalancer.NewRoundRobinLoadBalancer()
	client := NewDiscovery(reg, lb)
	defer client.Close()

	// 等待注册完成
	time.Sleep(1 * time.Second)

	// 获取健康实例
	healthyInstances, err := client.GetHealthyInstances(ctx, "test-health-service")
	if err != nil {
		t.Fatalf("Failed to get healthy instances: %v", err)
	}

	// 应该只有2个健康实例
	expectedHealthy := 2
	if len(healthyInstances) != expectedHealthy {
		t.Errorf("Expected %d healthy instances, got %d", expectedHealthy, len(healthyInstances))
	}

	// 验证返回的都是健康实例
	for _, inst := range healthyInstances {
		if inst.Status != registry.StatusHealthy {
			t.Errorf("Expected healthy status, got %s", inst.Status)
		}
	}
}

// TestClientCache 测试客户端缓存
func TestClientCache(t *testing.T) {
	reg, err := etcd.NewEtcdRegistry(
		etcd.WithEndpoints([]string{"localhost:2379"}),
		etcd.WithNamespace("/test/services"),
		etcd.WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	// 注册测试服务
	instance := &registry.ServiceInstance{
		ServiceName: "test-cache-service",
		InstanceID:  "test-cache-001",
		Host:        "127.0.0.1",
		Port:        9001,
		Status:      registry.StatusHealthy,
		Weight:      100,
	}

	err = reg.Register(ctx, instance)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}
	defer reg.Deregister(ctx, instance)

	// 创建启用缓存的客户端
	lb := loadbalancer.NewRoundRobinLoadBalancer()
	client := NewDiscovery(reg, lb,
		WithCache(true),
		WithCacheTTL(30*time.Second),
	)
	defer client.Close()

	// 等待注册完成
	time.Sleep(1 * time.Second)

	// 第一次获取实例（从注册中心）
	_, err = client.GetInstances(ctx, "test-cache-service")
	if err != nil {
		t.Fatalf("Failed to get instances: %v", err)
	}

	// 验证缓存
	cachedServices := client.GetCachedServices()
	found := false
	for _, svc := range cachedServices {
		if svc == "test-cache-service" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Service not found in cache")
	}

	// 清空缓存
	client.ClearServiceCache("test-cache-service")

	// 验证缓存已清空
	cachedServices = client.GetCachedServices()
	for _, svc := range cachedServices {
		if svc == "test-cache-service" {
			t.Error("Service should not be in cache after clearing")
		}
	}
}

// TestClientRefresh 测试刷新缓存
func TestClientRefresh(t *testing.T) {
	var (
		reg registry.Registry
		err error
	)
	reg, err = etcd.NewEtcdRegistry(
		etcd.WithEndpoints([]string{"localhost:2379"}),
		etcd.WithNamespace("/test/services"),
		etcd.WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	// 注册测试服务
	instance := &registry.ServiceInstance{
		ServiceName: "test-refresh-service",
		InstanceID:  "test-refresh-001",
		Host:        "127.0.0.1",
		Port:        9001,
		Status:      registry.StatusHealthy,
		Weight:      100,
	}

	err = reg.Register(ctx, instance)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}
	defer reg.Deregister(ctx, instance)

	// 创建客户端
	lb := loadbalancer.NewRoundRobinLoadBalancer()
	client := NewDiscovery(reg, lb, WithCache(true))
	defer client.Close()

	// 等待注册完成
	time.Sleep(1 * time.Second)

	// 获取实例
	_, err = client.GetInstances(ctx, "test-refresh-service")
	if err != nil {
		t.Fatalf("Failed to get instances: %v", err)
	}

	// 刷新缓存
	err = client.Refresh(ctx, "test-refresh-service")
	if err != nil {
		t.Fatalf("Failed to refresh: %v", err)
	}

	// 再次获取实例，应该使用新的缓存
	_, err = client.GetInstances(ctx, "test-refresh-service")
	if err != nil {
		t.Fatalf("Failed to get instances after refresh: %v", err)
	}
}
