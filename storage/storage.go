package storage

import (
	"context"
)

// Storage 存储接口
//
// 定义了数据存储的核心功能，让使用者快速了解所有可用方法。
//
// 使用示例：
//
//	store := mongodb.NewMongoStorage(
//	    mongodb.WithURI("mongodb://localhost:27017"),
//	    mongodb.WithDatabase("mydb"),
//	)
//	defer store.Close()
//
//	// 插入文档
//	store.InsertOne(ctx, "users", user)
//
//	// 查询文档
//	var user User
//	store.FindOne(ctx, "users", bson.M{"_id": id}, &user)
type Storage interface {
	// InsertOne 插入单个文档
	InsertOne(ctx context.Context, collection string, document interface{}) (interface{}, error)

	// InsertMany 插入多个文档
	InsertMany(ctx context.Context, collection string, documents []interface{}) ([]interface{}, error)

	// FindOne 查询单个文档
	FindOne(ctx context.Context, collection string, filter interface{}, result interface{}) error

	// Find 查询多个文档
	Find(ctx context.Context, collection string, filter interface{}, results interface{}, opts ...FindOption) error

	// UpdateOne 更新单个文档
	UpdateOne(ctx context.Context, collection string, filter interface{}, update interface{}) error

	// UpdateMany 更新多个文档
	UpdateMany(ctx context.Context, collection string, filter interface{}, update interface{}) (int64, error)

	// DeleteOne 删除单个文档
	DeleteOne(ctx context.Context, collection string, filter interface{}) error

	// DeleteMany 删除多个文档
	DeleteMany(ctx context.Context, collection string, filter interface{}) (int64, error)

	// ReplaceOne 替换单个文档
	ReplaceOne(ctx context.Context, collection string, filter interface{}, replacement interface{}) error

	// ==================== 原子操作 ====================

	// FindOneAndUpdate 查找并更新文档（原子操作）
	FindOneAndUpdate(ctx context.Context, collection string, filter interface{}, update interface{}, result interface{}) error

	// FindOneAndDelete 查找并删除文档（原子操作）
	FindOneAndDelete(ctx context.Context, collection string, filter interface{}, result interface{}) error

	// FindOneAndReplace 查找并替换文档（原子操作）
	FindOneAndReplace(ctx context.Context, collection string, filter interface{}, replacement interface{}, result interface{}) error

	// ==================== 查询与统计 ====================

	// Count 统计文档数量
	Count(ctx context.Context, collection string, filter interface{}) (int64, error)

	// Distinct 获取不重复的字段值
	Distinct(ctx context.Context, collection string, field string, filter interface{}) ([]interface{}, error)

	// Aggregate 聚合查询
	Aggregate(ctx context.Context, collection string, pipeline interface{}, results interface{}) error

	// ==================== 索引与集合管理 ====================

	// CreateIndex 创建索引
	CreateIndex(ctx context.Context, collection string, keys interface{}, unique bool) error

	// DropIndex 删除索引
	DropIndex(ctx context.Context, collection string, name string) error

	// ListIndexes 列出所有索引
	ListIndexes(ctx context.Context, collection string) ([]string, error)

	// DropCollection 删除集合
	DropCollection(ctx context.Context, collection string) error

	// ListCollections 列出所有集合
	ListCollections(ctx context.Context) ([]string, error)

	// ==================== 批量操作 ====================

	// BulkWrite 批量写操作
	BulkWrite(ctx context.Context, collection string, operations []BulkOperation) (*BulkResult, error)

	// ==================== 配置与管理 ====================

	// GetConfig 获取配置
	GetConfig() *Config

	// SetConfig 更新配置
	SetConfig(config *Config)

	// Close 关闭连接
	Close() error

	// Ping 健康检查
	Ping(ctx context.Context) error
}

// BulkOperation 批量操作
type BulkOperation struct {
	Type         BulkOperationType // 操作类型
	Filter       interface{}       // 过滤条件
	Document     interface{}       // 文档数据
	Update       interface{}       // 更新数据
	Upsert       bool              // 是否 upsert
	ArrayFilters interface{}       // 数组过滤器
}

// BulkOperationType 批量操作类型
type BulkOperationType string

const (
	BulkInsert  BulkOperationType = "insert"
	BulkUpdate  BulkOperationType = "update"
	BulkDelete  BulkOperationType = "delete"
	BulkReplace BulkOperationType = "replace"
)

// BulkResult 批量操作结果
type BulkResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	DeletedCount  int64
	UpsertedCount int64
	UpsertedIDs   map[int64]interface{}
}

// Config 存储配置
type Config struct {
	// CollectionPrefix 集合前缀
	CollectionPrefix string

	// EnableMetrics 是否启用监控
	EnableMetrics bool

	// EnableTracing 是否启用追踪
	EnableTracing bool
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		CollectionPrefix: "",
		EnableMetrics:    false,
		EnableTracing:    false,
	}
}

// Option 配置选项
type Option func(*Config)

// WithCollectionPrefix 设置集合前缀
func WithCollectionPrefix(prefix string) Option {
	return func(c *Config) {
		c.CollectionPrefix = prefix
	}
}

// WithMetrics 设置是否启用监控
func WithMetrics(enabled bool) Option {
	return func(c *Config) {
		c.EnableMetrics = enabled
	}
}

// WithTracing 设置是否启用追踪
func WithTracing(enabled bool) Option {
	return func(c *Config) {
		c.EnableTracing = enabled
	}
}

// FindOptions 查询选项
type FindOptions struct {
	Skip  int64
	Limit int64
	Sort  interface{}
}

// FindOption 查询选项函数
type FindOption func(*FindOptions)

// WithSkip 设置跳过的文档数量
func WithSkip(skip int64) FindOption {
	return func(opts *FindOptions) {
		opts.Skip = skip
	}
}

// WithLimit 设置返回的文档数量
func WithLimit(limit int64) FindOption {
	return func(opts *FindOptions) {
		opts.Limit = limit
	}
}

// WithSort 设置排序
func WithSort(sort interface{}) FindOption {
	return func(opts *FindOptions) {
		opts.Sort = sort
	}
}
