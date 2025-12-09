package loadbalancer

import (
	"crypto/sha1"
	"fmt"
	"sort"

	"github.com/rhp-QE/roc-foundation-util-go/service_registry/registry"
)

// ConsistentHashLoadBalancer 一致性哈希负载均衡器
type ConsistentHashLoadBalancer struct {
	virtualNodes int // 虚拟节点数量
}

// NewConsistentHashLoadBalancer 创建一致性哈希负载均衡器
func NewConsistentHashLoadBalancer(virtualNodes int) *ConsistentHashLoadBalancer {
	if virtualNodes <= 0 {
		virtualNodes = 150 // 默认150个虚拟节点
	}
	return &ConsistentHashLoadBalancer{
		virtualNodes: virtualNodes,
	}
}

// hashRing 哈希环
type hashRing struct {
	nodes map[uint32]registry.ServiceInstance // 哈希值到实例的映射
	keys  []uint32                            // 排序的哈希值列表
}

// newHashRing 创建哈希环
func (lb *ConsistentHashLoadBalancer) newHashRing(instances []registry.ServiceInstance) *hashRing {
	ring := &hashRing{
		nodes: make(map[uint32]registry.ServiceInstance),
		keys:  make([]uint32, 0),
	}

	// 过滤健康实例
	healthy := FilterHealthy(instances)

	// 为每个健康实例创建虚拟节点
	for _, instance := range healthy {
		for i := 0; i < lb.virtualNodes; i++ {
			virtualKey := fmt.Sprintf("%s:%d#%d", instance.Host, instance.Port, i)
			hash := lb.hash(virtualKey)
			ring.nodes[hash] = instance
			ring.keys = append(ring.keys, hash)
		}
	}

	// 排序哈希值
	sort.Slice(ring.keys, func(i, j int) bool {
		return ring.keys[i] < ring.keys[j]
	})

	return ring
}

// Select 根据一致性哈希选择实例
func (lb *ConsistentHashLoadBalancer) Select(instances []registry.ServiceInstance, key ...string) (*registry.ServiceInstance, error) {
	if len(instances) == 0 {
		return nil, ErrNoAvailableInstance
	}

	// 检查是否提供了哈希键
	var hashKey string
	if len(key) > 0 && key[0] != "" {
		hashKey = key[0]
	} else {
		return nil, ErrInvalidKey
	}

	ring := lb.newHashRing(instances)
	if len(ring.keys) == 0 {
		return nil, ErrNoAvailableInstance
	}

	// 计算请求的哈希值
	hash := lb.hash(hashKey)

	// 在哈希环上找到第一个大于等于请求哈希值的节点
	idx := sort.Search(len(ring.keys), func(i int) bool {
		return ring.keys[i] >= hash
	})

	// 如果没找到，选择第一个节点（环形结构）
	if idx == len(ring.keys) {
		idx = 0
	}

	selectedInstance := ring.nodes[ring.keys[idx]]
	return &selectedInstance, nil
}

// hash 计算字符串的哈希值
func (lb *ConsistentHashLoadBalancer) hash(key string) uint32 {
	h := sha1.New()
	h.Write([]byte(key))
	hashBytes := h.Sum(nil)

	// 取前4个字节作为uint32
	return uint32(hashBytes[0])<<24 | uint32(hashBytes[1])<<16 | uint32(hashBytes[2])<<8 | uint32(hashBytes[3])
}

// GetName 获取负载均衡器名称
func (lb *ConsistentHashLoadBalancer) GetName() string {
	return "ConsistentHash"
}
