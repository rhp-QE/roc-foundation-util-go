package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/roc/roc-foundation-util-go/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// impl MongoDB 存储的内部实现
//
// 实现 storage.Storage 接口，对外不暴露。
type impl struct {
	client   *mongo.Client
	database *mongo.Database
	config   *storage.Config
	timeout  time.Duration
}

// NewMongoStorage 创建 MongoDB 存储实例
//
// 参数：
//   - options: MongoDB 配置选项
//   - storageOptions: 存储通用配置选项
//
// 返回：
//   - storage.Storage 接口实例
func NewMongoStorage(options []Option, storageOptions ...storage.Option) (storage.Storage, error) {
	// MongoDB 配置
	mongoConfig := DefaultConfig()
	for _, opt := range options {
		opt(mongoConfig)
	}

	// 存储通用配置
	storageConfig := storage.DefaultConfig()
	for _, opt := range storageOptions {
		opt(storageConfig)
	}

	// 创建 MongoDB 客户端
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, mongoConfig.ToMongoOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongodb: %w", err)
	}

	// 测试连接
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongodb: %w", err)
	}

	return &impl{
		client:   client,
		database: client.Database(mongoConfig.Database),
		config:   storageConfig,
		timeout:  mongoConfig.Timeout,
	}, nil
}

// buildCollectionName 构建完整的集合名
func (i *impl) buildCollectionName(collection string) string {
	if i.config.CollectionPrefix == "" {
		return collection
	}
	return i.config.CollectionPrefix + collection
}

// getCollection 获取集合
func (i *impl) getCollection(collection string) *mongo.Collection {
	return i.database.Collection(i.buildCollectionName(collection))
}

// InsertOne 插入单个文档
func (i *impl) InsertOne(ctx context.Context, collection string, document interface{}) (interface{}, error) {
	coll := i.getCollection(collection)
	result, err := coll.InsertOne(ctx, document)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, storage.ErrDuplicateKey
		}
		return nil, fmt.Errorf("mongodb insertone error: %w", err)
	}
	return result.InsertedID, nil
}

// InsertMany 插入多个文档
func (i *impl) InsertMany(ctx context.Context, collection string, documents []interface{}) ([]interface{}, error) {
	if len(documents) == 0 {
		return []interface{}{}, nil
	}

	coll := i.getCollection(collection)
	result, err := coll.InsertMany(ctx, documents)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, storage.ErrDuplicateKey
		}
		return nil, fmt.Errorf("mongodb insertmany error: %w", err)
	}
	return result.InsertedIDs, nil
}

// FindOne 查询单个文档
func (i *impl) FindOne(ctx context.Context, collection string, filter interface{}, result interface{}) error {
	coll := i.getCollection(collection)
	err := coll.FindOne(ctx, filter).Decode(result)
	if err == mongo.ErrNoDocuments {
		return storage.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("mongodb findone error: %w", err)
	}
	return nil
}

// Find 查询多个文档
func (i *impl) Find(ctx context.Context, collection string, filter interface{}, results interface{}, opts ...storage.FindOption) error {
	// 构建查询选项
	findOpts := &storage.FindOptions{}
	for _, opt := range opts {
		opt(findOpts)
	}

	mongoOpts := options.Find()
	if findOpts.Skip > 0 {
		mongoOpts.SetSkip(findOpts.Skip)
	}
	if findOpts.Limit > 0 {
		mongoOpts.SetLimit(findOpts.Limit)
	}
	if findOpts.Sort != nil {
		mongoOpts.SetSort(findOpts.Sort)
	}

	coll := i.getCollection(collection)
	cursor, err := coll.Find(ctx, filter, mongoOpts)
	if err != nil {
		return fmt.Errorf("mongodb find error: %w", err)
	}
	defer cursor.Close(ctx)

	if err := cursor.All(ctx, results); err != nil {
		return fmt.Errorf("mongodb cursor decode error: %w", err)
	}
	return nil
}

// UpdateOne 更新单个文档
func (i *impl) UpdateOne(ctx context.Context, collection string, filter interface{}, update interface{}) error {
	coll := i.getCollection(collection)
	result, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return storage.ErrDuplicateKey
		}
		return fmt.Errorf("mongodb updateone error: %w", err)
	}
	if result.MatchedCount == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// UpdateMany 更新多个文档
func (i *impl) UpdateMany(ctx context.Context, collection string, filter interface{}, update interface{}) (int64, error) {
	coll := i.getCollection(collection)
	result, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return 0, storage.ErrDuplicateKey
		}
		return 0, fmt.Errorf("mongodb updatemany error: %w", err)
	}
	return result.ModifiedCount, nil
}

// DeleteOne 删除单个文档
func (i *impl) DeleteOne(ctx context.Context, collection string, filter interface{}) error {
	coll := i.getCollection(collection)
	result, err := coll.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("mongodb deleteone error: %w", err)
	}
	if result.DeletedCount == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// DeleteMany 删除多个文档
func (i *impl) DeleteMany(ctx context.Context, collection string, filter interface{}) (int64, error) {
	coll := i.getCollection(collection)
	result, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("mongodb deletemany error: %w", err)
	}
	return result.DeletedCount, nil
}

// Count 统计文档数量
func (i *impl) Count(ctx context.Context, collection string, filter interface{}) (int64, error) {
	coll := i.getCollection(collection)
	count, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("mongodb count error: %w", err)
	}
	return count, nil
}

// Aggregate 聚合查询
func (i *impl) Aggregate(ctx context.Context, collection string, pipeline interface{}, results interface{}) error {
	coll := i.getCollection(collection)
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("mongodb aggregate error: %w", err)
	}
	defer cursor.Close(ctx)

	if err := cursor.All(ctx, results); err != nil {
		return fmt.Errorf("mongodb aggregate decode error: %w", err)
	}
	return nil
}

// CreateIndex 创建索引
func (i *impl) CreateIndex(ctx context.Context, collection string, keys interface{}, unique bool) error {
	coll := i.getCollection(collection)

	indexModel := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetUnique(unique),
	}

	_, err := coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("mongodb create index error: %w", err)
	}
	return nil
}

// DropCollection 删除集合
func (i *impl) DropCollection(ctx context.Context, collection string) error {
	coll := i.getCollection(collection)
	err := coll.Drop(ctx)
	if err != nil {
		return fmt.Errorf("mongodb drop collection error: %w", err)
	}
	return nil
}

// GetConfig 获取配置
func (i *impl) GetConfig() *storage.Config {
	// 返回配置的副本
	configCopy := *i.config
	return &configCopy
}

// SetConfig 更新配置
func (i *impl) SetConfig(config *storage.Config) {
	if config != nil {
		i.config = config
	}
}

// Close 关闭连接
func (i *impl) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return i.client.Disconnect(ctx)
}

// Ping 健康检查
func (i *impl) Ping(ctx context.Context) error {
	return i.client.Ping(ctx, nil)
}

// ReplaceOne 替换单个文档
func (i *impl) ReplaceOne(ctx context.Context, collection string, filter interface{}, replacement interface{}) error {
	coll := i.getCollection(collection)
	result, err := coll.ReplaceOne(ctx, filter, replacement)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return storage.ErrDuplicateKey
		}
		return fmt.Errorf("mongodb replaceone error: %w", err)
	}
	if result.MatchedCount == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// ==================== 原子操作实现 ====================

// FindOneAndUpdate 查找并更新文档（原子操作）
func (i *impl) FindOneAndUpdate(ctx context.Context, collection string, filter interface{}, update interface{}, result interface{}) error {
	coll := i.getCollection(collection)
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(result)
	if err == mongo.ErrNoDocuments {
		return storage.ErrNotFound
	}
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return storage.ErrDuplicateKey
		}
		return fmt.Errorf("mongodb findoneandupdate error: %w", err)
	}
	return nil
}

// FindOneAndDelete 查找并删除文档（原子操作）
func (i *impl) FindOneAndDelete(ctx context.Context, collection string, filter interface{}, result interface{}) error {
	coll := i.getCollection(collection)
	err := coll.FindOneAndDelete(ctx, filter).Decode(result)
	if err == mongo.ErrNoDocuments {
		return storage.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("mongodb findoneanddelete error: %w", err)
	}
	return nil
}

// FindOneAndReplace 查找并替换文档（原子操作）
func (i *impl) FindOneAndReplace(ctx context.Context, collection string, filter interface{}, replacement interface{}, result interface{}) error {
	coll := i.getCollection(collection)
	opts := options.FindOneAndReplace().SetReturnDocument(options.After)
	err := coll.FindOneAndReplace(ctx, filter, replacement, opts).Decode(result)
	if err == mongo.ErrNoDocuments {
		return storage.ErrNotFound
	}
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return storage.ErrDuplicateKey
		}
		return fmt.Errorf("mongodb findoneandreplace error: %w", err)
	}
	return nil
}

// ==================== 查询与统计实现 ====================

// Distinct 获取不重复的字段值
func (i *impl) Distinct(ctx context.Context, collection string, field string, filter interface{}) ([]interface{}, error) {
	coll := i.getCollection(collection)
	values, err := coll.Distinct(ctx, field, filter)
	if err != nil {
		return nil, fmt.Errorf("mongodb distinct error: %w", err)
	}
	return values, nil
}

// ==================== 索引管理实现 ====================

// DropIndex 删除索引
func (i *impl) DropIndex(ctx context.Context, collection string, name string) error {
	coll := i.getCollection(collection)
	_, err := coll.Indexes().DropOne(ctx, name)
	if err != nil {
		return fmt.Errorf("mongodb drop index error: %w", err)
	}
	return nil
}

// ListIndexes 列出所有索引
func (i *impl) ListIndexes(ctx context.Context, collection string) ([]string, error) {
	coll := i.getCollection(collection)
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("mongodb list indexes error: %w", err)
	}
	defer cursor.Close(ctx)

	var indexes []string
	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			continue
		}
		if name, ok := index["name"].(string); ok {
			indexes = append(indexes, name)
		}
	}
	return indexes, nil
}

// ListCollections 列出所有集合
func (i *impl) ListCollections(ctx context.Context) ([]string, error) {
	collections, err := i.database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("mongodb list collections error: %w", err)
	}
	return collections, nil
}

// ==================== 批量操作实现 ====================

// BulkWrite 批量写操作
func (i *impl) BulkWrite(ctx context.Context, collection string, operations []storage.BulkOperation) (*storage.BulkResult, error) {
	if len(operations) == 0 {
		return &storage.BulkResult{}, nil
	}

	coll := i.getCollection(collection)
	models := make([]mongo.WriteModel, 0, len(operations))

	for _, op := range operations {
		switch op.Type {
		case storage.BulkInsert:
			model := mongo.NewInsertOneModel().SetDocument(op.Document)
			models = append(models, model)

		case storage.BulkUpdate:
			model := mongo.NewUpdateOneModel().
				SetFilter(op.Filter).
				SetUpdate(op.Update).
				SetUpsert(op.Upsert)
			models = append(models, model)

		case storage.BulkDelete:
			model := mongo.NewDeleteOneModel().SetFilter(op.Filter)
			models = append(models, model)

		case storage.BulkReplace:
			model := mongo.NewReplaceOneModel().
				SetFilter(op.Filter).
				SetReplacement(op.Document).
				SetUpsert(op.Upsert)
			models = append(models, model)
		}
	}

	result, err := coll.BulkWrite(ctx, models)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, storage.ErrDuplicateKey
		}
		return nil, fmt.Errorf("mongodb bulkwrite error: %w", err)
	}

	return &storage.BulkResult{
		InsertedCount: result.InsertedCount,
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		DeletedCount:  result.DeletedCount,
		UpsertedCount: result.UpsertedCount,
		UpsertedIDs:   result.UpsertedIDs,
	}, nil
}

// 编译时检查，确保 impl 实现了 storage.Storage 接口
var _ storage.Storage = (*impl)(nil)

// ==================== Helper 函数 ====================

// BuildIDFilter 构建 ID 过滤器
func BuildIDFilter(id interface{}) bson.M {
	return bson.M{"_id": id}
}

// BuildUpdateSet 构建 $set 更新操作
func BuildUpdateSet(update interface{}) bson.M {
	return bson.M{"$set": update}
}

// BuildUpdateInc 构建 $inc 更新操作
func BuildUpdateInc(update interface{}) bson.M {
	return bson.M{"$inc": update}
}

// BuildUpdatePush 构建 $push 更新操作
func BuildUpdatePush(field string, value interface{}) bson.M {
	return bson.M{"$push": bson.M{field: value}}
}

// BuildUpdatePull 构建 $pull 更新操作
func BuildUpdatePull(field string, value interface{}) bson.M {
	return bson.M{"$pull": bson.M{field: value}}
}

// BuildSortAsc 构建升序排序
func BuildSortAsc(fields ...string) bson.D {
	sort := bson.D{}
	for _, field := range fields {
		sort = append(sort, bson.E{Key: field, Value: 1})
	}
	return sort
}

// BuildSortDesc 构建降序排序
func BuildSortDesc(fields ...string) bson.D {
	sort := bson.D{}
	for _, field := range fields {
		sort = append(sort, bson.E{Key: field, Value: -1})
	}
	return sort
}
