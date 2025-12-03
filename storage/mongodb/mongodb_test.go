package mongodb

import (
	"context"
	"testing"

	"github.com/roc/roc-foundation-util-go/storage"
	"go.mongodb.org/mongo-driver/bson"
)

type TestUser struct {
	ID    string `bson:"_id"`
	Name  string `bson:"name"`
	Email string `bson:"email"`
	Age   int    `bson:"age"`
}

// setupTest 创建测试用的存储实例
func setupTest(t *testing.T) storage.Storage {
	var (
		store storage.Storage
		err   error
	)
	// 尝试使用认证连接
	store, err = NewMongoStorage(
		[]Option{
			WithURI("mongodb://admin:mongodb123@localhost:27017"),
			WithDatabase("test"),
			WithAuth("admin", "mongodb123"),
		},
		storage.WithCollectionPrefix("test_"),
	)
	if err != nil {
		t.Skipf("Skipping test, mongodb not available: %v", err)
		return nil
	}

	ctx := context.Background()

	// 测试连接
	if err := store.Ping(ctx); err != nil {
		store.Close()
		t.Skipf("Skipping test, mongodb ping failed: %v", err)
		return nil
	}

	return store
}

// TestNewMongoStorage 测试创建 MongoDB 存储
func TestNewMongoStorage(t *testing.T) {
	store := setupTest(t)
	if store == nil {
		return
	}
	defer store.Close()

	ctx := context.Background()
	err := store.Ping(ctx)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

// TestInsertAndFind 测试插入和查询
func TestInsertAndFind(t *testing.T) {
	store := setupTest(t)
	if store == nil {
		return
	}
	defer store.Close()

	ctx := context.Background()
	collectionName := "users"
	defer store.DropCollection(ctx, collectionName)

	// 插入文档
	user := TestUser{
		ID:    "user-001",
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   30,
	}

	id, err := store.InsertOne(ctx, collectionName, user)
	if err != nil {
		t.Fatalf("InsertOne failed: %v", err)
	}
	if id != "user-001" {
		t.Errorf("Expected id 'user-001', got %v", id)
	}

	// 查询文档
	var foundUser TestUser
	filter := BuildIDFilter("user-001")
	err = store.FindOne(ctx, collectionName, filter, &foundUser)
	if err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}
	if foundUser.Name != "Alice" {
		t.Errorf("Expected name 'Alice', got %s", foundUser.Name)
	}
}

// TestUpdateOperations 测试更新操作
func TestUpdateOperations(t *testing.T) {
	store := setupTest(t)
	if store == nil {
		return
	}
	defer store.Close()

	ctx := context.Background()
	collectionName := "users"
	defer store.DropCollection(ctx, collectionName)

	// 插入测试数据
	user := TestUser{ID: "user-002", Name: "Bob", Email: "bob@example.com", Age: 25}
	store.InsertOne(ctx, collectionName, user)

	// UpdateOne
	filter := BuildIDFilter("user-002")
	update := BuildUpdateSet(bson.M{"age": 26})
	err := store.UpdateOne(ctx, collectionName, filter, update)
	if err != nil {
		t.Fatalf("UpdateOne failed: %v", err)
	}

	// 验证更新
	var updatedUser TestUser
	store.FindOne(ctx, collectionName, filter, &updatedUser)
	if updatedUser.Age != 26 {
		t.Errorf("Expected age 26, got %d", updatedUser.Age)
	}
}

// TestFindOneAndUpdate 测试原子更新操作
func TestFindOneAndUpdate(t *testing.T) {
	store := setupTest(t)
	if store == nil {
		return
	}
	defer store.Close()

	ctx := context.Background()
	collectionName := "users"
	defer store.DropCollection(ctx, collectionName)

	// 插入测试数据
	user := TestUser{ID: "user-003", Name: "Charlie", Email: "charlie@example.com", Age: 35}
	store.InsertOne(ctx, collectionName, user)

	// FindOneAndUpdate
	filter := BuildIDFilter("user-003")
	update := BuildUpdateSet(bson.M{"age": 36})
	var result TestUser
	err := store.FindOneAndUpdate(ctx, collectionName, filter, update, &result)
	if err != nil {
		t.Fatalf("FindOneAndUpdate failed: %v", err)
	}

	// 验证返回的是更新后的文档
	if result.Age != 36 {
		t.Errorf("Expected age 36, got %d", result.Age)
	}
}

// TestDeleteOperations 测试删除操作
func TestDeleteOperations(t *testing.T) {
	store := setupTest(t)
	if store == nil {
		return
	}
	defer store.Close()

	ctx := context.Background()
	collectionName := "users"
	defer store.DropCollection(ctx, collectionName)

	// 插入测试数据
	users := []interface{}{
		TestUser{ID: "user-004", Name: "David", Email: "david@example.com", Age: 28},
		TestUser{ID: "user-005", Name: "Eve", Email: "eve@example.com", Age: 29},
	}
	store.InsertMany(ctx, collectionName, users)

	// DeleteOne
	filter := BuildIDFilter("user-004")
	err := store.DeleteOne(ctx, collectionName, filter)
	if err != nil {
		t.Fatalf("DeleteOne failed: %v", err)
	}

	// 验证已删除
	var deletedUser TestUser
	err = store.FindOne(ctx, collectionName, filter, &deletedUser)
	if err != storage.ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// DeleteMany
	count, err := store.DeleteMany(ctx, collectionName, bson.M{})
	if err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected deleted count 1, got %d", count)
	}
}

// TestCountAndDistinct 测试统计操作
func TestCountAndDistinct(t *testing.T) {
	store := setupTest(t)
	if store == nil {
		return
	}
	defer store.Close()

	ctx := context.Background()
	collectionName := "users"
	defer store.DropCollection(ctx, collectionName)

	// 插入测试数据
	users := []interface{}{
		TestUser{ID: "user-006", Name: "Alice", Email: "alice1@example.com", Age: 30},
		TestUser{ID: "user-007", Name: "Bob", Email: "bob1@example.com", Age: 30},
		TestUser{ID: "user-008", Name: "Charlie", Email: "charlie1@example.com", Age: 35},
	}
	store.InsertMany(ctx, collectionName, users)

	// Count
	count, err := store.Count(ctx, collectionName, bson.M{"age": 30})
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	// Distinct
	ages, err := store.Distinct(ctx, collectionName, "age", bson.M{})
	if err != nil {
		t.Fatalf("Distinct failed: %v", err)
	}
	if len(ages) != 2 { // 30 and 35
		t.Errorf("Expected 2 distinct ages, got %d", len(ages))
	}
}

// TestBulkWrite 测试批量写操作
func TestBulkWrite(t *testing.T) {
	store := setupTest(t)
	if store == nil {
		return
	}
	defer store.Close()

	ctx := context.Background()
	collectionName := "users"
	defer store.DropCollection(ctx, collectionName)

	// 批量操作
	operations := []storage.BulkOperation{
		{
			Type:     storage.BulkInsert,
			Document: TestUser{ID: "user-009", Name: "User9", Email: "user9@example.com", Age: 20},
		},
		{
			Type:     storage.BulkInsert,
			Document: TestUser{ID: "user-010", Name: "User10", Email: "user10@example.com", Age: 21},
		},
		{
			Type:   storage.BulkUpdate,
			Filter: BuildIDFilter("user-009"),
			Update: BuildUpdateSet(bson.M{"age": 22}),
		},
	}

	result, err := store.BulkWrite(ctx, collectionName, operations)
	if err != nil {
		t.Fatalf("BulkWrite failed: %v", err)
	}
	if result.InsertedCount != 2 {
		t.Errorf("Expected inserted count 2, got %d", result.InsertedCount)
	}
	if result.ModifiedCount != 1 {
		t.Errorf("Expected modified count 1, got %d", result.ModifiedCount)
	}
}
