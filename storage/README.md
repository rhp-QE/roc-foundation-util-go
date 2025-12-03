# Storage 模块

数据存储抽象层，提供统一的存储接口，当前实现了 MongoDB。

## 设计理念

- **接口导向**: `Storage` 接口清晰定义所有方法
- **隐藏实现**: `impl` 结构体对外不暴露
- **易于替换**: 可以轻松切换到 PostgreSQL 等其他实现

## 快速使用

```go
package main

import (
    "context"
    
    "github.com/roc/roc-foundation-util-go/storage/mongodb"
    "go.mongodb.org/mongo-driver/bson"
)

type User struct {
    ID   string `bson:"_id"`
    Name string `bson:"name"`
    Age  int    `bson:"age"`
}

func main() {
    // 创建 MongoDB 存储
    store, err := mongodb.NewMongoStorage(
        []mongodb.Option{
            mongodb.WithURI("mongodb://localhost:27017"),
            mongodb.WithDatabase("mydb"),
        },
    )
    if err != nil {
        panic(err)
    }
    defer store.Close()
    
    ctx := context.Background()
    
    // 插入文档
    user := User{ID: "1", Name: "Alice", Age: 30}
    store.InsertOne(ctx, "users", user)
    
    // 查询文档
    var result User
    filter := mongodb.BuildIDFilter("1")
    err = store.FindOne(ctx, "users", filter, &result)
    if err != nil {
        panic(err)
    }
    
    println("User:", result.Name)
}
```

## Storage 接口

```go
type Storage interface {
    InsertOne(ctx context.Context, collection string, document interface{}) (interface{}, error)
    InsertMany(ctx context.Context, collection string, documents []interface{}) ([]interface{}, error)
    FindOne(ctx context.Context, collection string, filter interface{}, result interface{}) error
    Find(ctx context.Context, collection string, filter interface{}, results interface{}, opts ...FindOption) error
    UpdateOne(ctx context.Context, collection string, filter interface{}, update interface{}) error
    UpdateMany(ctx context.Context, collection string, filter interface{}, update interface{}) (int64, error)
    DeleteOne(ctx context.Context, collection string, filter interface{}) error
    DeleteMany(ctx context.Context, collection string, filter interface{}) (int64, error)
    Count(ctx context.Context, collection string, filter interface{}) (int64, error)
    Aggregate(ctx context.Context, collection string, pipeline interface{}, results interface{}) error
    CreateIndex(ctx context.Context, collection string, keys interface{}, unique bool) error
    DropCollection(ctx context.Context, collection string) error
    GetConfig() *Config
    SetConfig(config *Config)
    Close() error
    Ping(ctx context.Context) error
}
```

## 配置选项

```go
store, _ := mongodb.NewMongoStorage(
    []mongodb.Option{
        mongodb.WithURI("mongodb://localhost:27017"),
        mongodb.WithDatabase("mydb"),
        mongodb.WithAuth("username", "password"),
        mongodb.WithPoolSize(10, 100),
        mongodb.WithTimeout(10*time.Second),
    },
)
```

## 扩展实现

可以轻松实现其他存储后端：

```go
package postgresql

type impl struct {
    db     *sql.DB
    config *storage.Config
}

func NewPostgreSQLStorage(options []Option) (storage.Storage, error) {
    // 实现 Storage 接口
}
```

