package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/roc/roc-foundation-util-go/service_registry/registry"
)

// TestNewEtcdRegistry 测试创建 etcd 注册中心
func TestNewEtcdRegistry(t *testing.T) {
	reg, err := NewEtcdRegistry(
		WithEndpoints([]string{"localhost:2379"}),
		WithNamespace("/test/services"),
		WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	// 健康检查
	ctx := context.Background()
	err = reg.HealthCheck(ctx)
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

// TestRegisterAndDiscover 测试服务注册和发现
func TestRegisterAndDiscover(t *testing.T) {
	reg, err := NewEtcdRegistry(
		WithEndpoints([]string{"localhost:2379"}),
		WithNamespace("/test/services"),
		WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	// 创建测试实例
	instance := &registry.ServiceInstance{
		ServiceName: "test-service",
		InstanceID:  "test-instance-001",
		Host:        "127.0.0.1",
		Port:        8080,
		Status:      registry.StatusHealthy,
		Weight:      100,
		Version:     "v1.0.0",
		Metadata: map[string]string{
			"region": "test",
		},
	}

	// 注册服务
	err = reg.Register(ctx, instance)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}

	// 等待注册完成
	time.Sleep(1 * time.Second)

	// 发现服务
	instances, err := reg.Discover(ctx, "test-service")
	if err != nil {
		t.Fatalf("Failed to discover service: %v", err)
	}

	if len(instances) == 0 {
		t.Fatalf("Expected at least 1 instance, got 0")
	}

	// 验证实例信息
	found := false
	for _, inst := range instances {
		if inst.InstanceID == instance.InstanceID {
			found = true
			if inst.Host != instance.Host {
				t.Errorf("Expected host %s, got %s", instance.Host, inst.Host)
			}
			if inst.Port != instance.Port {
				t.Errorf("Expected port %d, got %d", instance.Port, inst.Port)
			}
			break
		}
	}

	if !found {
		t.Errorf("Instance not found in discovered instances")
	}

	// 清理：注销服务
	err = reg.Deregister(ctx, instance)
	if err != nil {
		t.Errorf("Failed to deregister service: %v", err)
	}
}

// TestUpdateStatus 测试更新实例状态
func TestUpdateStatus(t *testing.T) {
	reg, err := NewEtcdRegistry(
		WithEndpoints([]string{"localhost:2379"}),
		WithNamespace("/test/services"),
		WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	instance := &registry.ServiceInstance{
		ServiceName: "test-service",
		InstanceID:  "test-instance-002",
		Host:        "127.0.0.1",
		Port:        8081,
		Status:      registry.StatusHealthy,
		Weight:      100,
		Version:     "v1.0.0",
	}

	// 注册服务
	err = reg.Register(ctx, instance)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}
	defer reg.Deregister(ctx, instance)

	// 更新状态
	err = reg.UpdateStatus(ctx, instance, registry.StatusUnhealthy)
	if err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// 等待更新完成
	time.Sleep(1 * time.Second)

	// 验证状态已更新
	updatedInstance, err := reg.GetService(ctx, "test-service", "test-instance-002")
	if err != nil {
		t.Fatalf("Failed to get service: %v", err)
	}

	if updatedInstance.Status != registry.StatusUnhealthy {
		t.Errorf("Expected status %s, got %s", registry.StatusUnhealthy, updatedInstance.Status)
	}
}

// TestUpdateMetadata 测试更新元数据
func TestUpdateMetadata(t *testing.T) {
	reg, err := NewEtcdRegistry(
		WithEndpoints([]string{"localhost:2379"}),
		WithNamespace("/test/services"),
		WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	instance := &registry.ServiceInstance{
		ServiceName: "test-service",
		InstanceID:  "test-instance-003",
		Host:        "127.0.0.1",
		Port:        8082,
		Status:      registry.StatusHealthy,
		Weight:      100,
		Version:     "v1.0.0",
		Metadata: map[string]string{
			"region": "test",
		},
	}

	// 注册服务
	err = reg.Register(ctx, instance)
	if err != nil {
		t.Fatalf("Failed to register service: %v", err)
	}
	defer reg.Deregister(ctx, instance)

	// 更新元数据
	newMetadata := map[string]string{
		"region":  "prod",
		"updated": "true",
	}
	err = reg.UpdateMetadata(ctx, instance, newMetadata)
	if err != nil {
		t.Fatalf("Failed to update metadata: %v", err)
	}

	// 等待更新完成
	time.Sleep(1 * time.Second)

	// 验证元数据已更新
	updatedInstance, err := reg.GetService(ctx, "test-service", "test-instance-003")
	if err != nil {
		t.Fatalf("Failed to get service: %v", err)
	}

	if updatedInstance.Metadata["region"] != "prod" {
		t.Errorf("Expected region 'prod', got '%s'", updatedInstance.Metadata["region"])
	}

	if updatedInstance.Metadata["updated"] != "true" {
		t.Errorf("Expected updated 'true', got '%s'", updatedInstance.Metadata["updated"])
	}
}

// TestListServices 测试列出所有服务
func TestListServices(t *testing.T) {
	reg, err := NewEtcdRegistry(
		WithEndpoints([]string{"localhost:2379"}),
		WithNamespace("/test/services"),
		WithTTL(10),
	)
	if err != nil {
		t.Skipf("Skipping test, etcd not available: %v", err)
		return
	}
	defer reg.Close()

	ctx := context.Background()

	// 注册多个服务
	services := []string{"service-a", "service-b", "service-c"}
	for i, svcName := range services {
		instance := &registry.ServiceInstance{
			ServiceName: svcName,
			InstanceID:  svcName + "-001",
			Host:        "127.0.0.1",
			Port:        8080 + i,
			Status:      registry.StatusHealthy,
			Weight:      100,
			Version:     "v1.0.0",
		}
		err = reg.Register(ctx, instance)
		if err != nil {
			t.Fatalf("Failed to register service: %v", err)
		}
		defer reg.Deregister(ctx, instance)
	}

	// 等待注册完成
	time.Sleep(1 * time.Second)

	// 列出所有服务
	allServices, err := reg.ListServices(ctx)
	if err != nil {
		t.Fatalf("Failed to list services: %v", err)
	}

	if len(allServices) < len(services) {
		t.Errorf("Expected at least %d services, got %d", len(services), len(allServices))
	}
}
