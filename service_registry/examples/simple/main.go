package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/roc/roc-foundation-util-go/service_registry/discovery"
	"github.com/roc/roc-foundation-util-go/service_registry/loadbalancer"
	"github.com/roc/roc-foundation-util-go/service_registry/registry"
	"github.com/roc/roc-foundation-util-go/service_registry/registry/etcd"
)

func main() {
	// 1. 创建 etcd 注册中心
	reg, err := etcd.NewEtcdRegistry(
		etcd.WithEndpoints([]string{"localhost:2379"}),
		etcd.WithNamespace("/demo/services"),
		etcd.WithTTL(10),
	)
	if err != nil {
		log.Fatalf("Failed to create registry: %v", err)
	}
	defer reg.Close()

	// 2. 创建服务实例
	instance := &registry.ServiceInstance{
		ServiceName: "user-service",
		InstanceID:  "user-service-001",
		Host:        "127.0.0.1",
		Port:        8080,
		Status:      registry.StatusHealthy,
		Weight:      100,
		Version:     "v1.0.0",
		Metadata: map[string]string{
			"region": "beijing",
			"zone":   "zone-a",
		},
	}

	// 3. 注册服务
	ctx := context.Background()
	err = reg.Register(ctx, instance)
	if err != nil {
		log.Fatalf("Failed to register service: %v", err)
	}
	fmt.Printf("Service registered: %s:%d\n", instance.Host, instance.Port)

	// 4. 创建服务发现客户端
	lb := loadbalancer.NewRoundRobinLoadBalancer()
	client := discovery.NewDiscovery(reg, lb,
		discovery.WithCache(true),
		discovery.WithCacheTTL(30*time.Second),
		discovery.WithWatch(true),
	)
	defer client.Close()

	// 5. 发现服务
	instances, err := client.GetInstances(ctx, "user-service")
	if err != nil {
		log.Fatalf("Failed to discover service: %v", err)
	}
	fmt.Printf("Discovered %d instances:\n", len(instances))
	for _, inst := range instances {
		fmt.Printf("  - %s:%d (status: %s, version: %s)\n",
			inst.Host, inst.Port, inst.Status, inst.Version)
	}

	// 6. 使用负载均衡获取实例
	selectedInstance, err := client.GetInstance(ctx, "user-service")
	if err != nil {
		log.Fatalf("Failed to get instance: %v", err)
	}
	fmt.Printf("Selected instance: %s:%d\n", selectedInstance.Host, selectedInstance.Port)

	// 7. 订阅服务变化
	err = client.Subscribe(ctx, "user-service", func(serviceName string, instances []registry.ServiceInstance) {
		fmt.Printf("Service '%s' changed, now has %d instances\n", serviceName, len(instances))
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// 8. 更新实例状态
	time.Sleep(2 * time.Second)
	err = reg.UpdateStatus(ctx, instance, registry.StatusHealthy)
	if err != nil {
		log.Printf("Failed to update status: %v", err)
	}

	// 9. 更新元数据
	newMetadata := map[string]string{
		"region":  "beijing",
		"zone":    "zone-b",
		"updated": "true",
	}
	err = reg.UpdateMetadata(ctx, instance, newMetadata)
	if err != nil {
		log.Printf("Failed to update metadata: %v", err)
	}

	// 10. 保持运行一段时间
	fmt.Println("Service is running... Press Ctrl+C to exit")
	time.Sleep(30 * time.Second)

	// 11. 注销服务
	err = reg.Deregister(ctx, instance)
	if err != nil {
		log.Printf("Failed to deregister service: %v", err)
	}
	fmt.Println("Service deregistered")
}
