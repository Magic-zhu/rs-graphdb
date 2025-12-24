# Rust Graph Database

一个参考 Neo4j 架构实现的简化版 Rust 图数据库，适用于小项目和学习用途。

## 功能特性

- ✅ 内存存储引擎（支持节点、关系、属性）
- ✅ 基于 schema 的属性索引
- ✅ 链式查询 API（类似 Cypher 的函数式接口）
- ✅ 双向遍历支持（`out` / `in_`）
- ✅ HTTP REST API（基于 axum）

## 快速开始

### 1. 运行测试

```bash
cargo test
```

### 2. 启动 HTTP 服务器

```bash
cargo run --example demo_server
```

服务器将在 `http://127.0.0.1:3000` 启动。

### 3. 使用 HTTP API

#### 创建节点

```bash
curl -X POST http://127.0.0.1:3000/nodes \
  -H "Content-Type: application/json" \
  -d '{
    "labels": ["User"],
    "properties": {
      "name": "Alice",
      "age": 30
    }
  }'
```

响应示例：
```json
{"id": 0}
```

#### 创建关系

```bash
curl -X POST http://127.0.0.1:3000/rels \
  -H "Content-Type: application/json" \
  -d '{
    "start": 0,
    "end": 1,
    "rel_type": "FRIEND",
    "properties": {}
  }'
```

#### 查询节点

基础查询（按 label）：
```bash
curl -X POST http://127.0.0.1:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "label": "User"
  }'
```

索引查询（按 label + 属性）：
```bash
curl -X POST http://127.0.0.1:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "label": "User",
    "property": "name",
    "value": "Alice"
  }'
```

遍历查询（沿关系走）：
```bash
curl -X POST http://127.0.0.1:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "label": "User",
    "property": "name",
    "value": "Alice",
    "out_rel": "FRIEND"
  }'
```

## 项目结构

```
rust-graphdb/
├── src/
│   ├── values/          # 值类型定义（Int, Bool, Text, Float）
│   ├── storage/         # 存储引擎抽象 + 内存实现
│   ├── graph/           # 图数据库 API（节点、关系操作）
│   ├── index.rs         # 属性索引实现
│   ├── index_schema.rs  # 索引配置 schema
│   ├── query.rs         # 链式查询 API
│   ├── server.rs        # HTTP REST 服务
│   └── lib.rs
├── tests/               # 集成测试
└── examples/
    └── demo_server.rs   # 服务器 demo
```

## 使用 Rust API

```rust
use rust_graphdb::{GraphDatabase, values::{Properties, Value}};
use rust_graphdb::query::Query;

fn main() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建节点
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("Alice".to_string()));
    props.insert("age".to_string(), Value::Int(30));

    let alice = db.create_node(vec!["User"], props);

    // 链式查询
    let result = Query::new(&db)
        .from_label_and_prop_eq("User", "name", "Alice")
        .out("FRIEND")
        .collect_nodes();

    println!("Found {} nodes", result.len());
}
```

## 查询 API 方法

- `from_label(label)` - 按 label 全表扫描选起点
- `from_label_and_prop_eq(label, key, value)` - 使用索引按 label+属性选起点
- `from_label_and_prop_int_eq(label, key, value)` - 使用索引按 label+整型属性选起点
- `where_prop_eq(key, value)` - 按字符串属性过滤
- `where_prop_int_gt(key, min)` - 按整型属性 > 某值过滤
- `out(rel_type)` - 沿出边遍历一层
- `in_(rel_type)` - 沿入边遍历一层
- `distinct()` - 节点 ID 去重
- `collect_nodes()` - 收集为 `Vec<Node>`

## 索引配置

默认索引 `User.name` 和 `User.age`。自定义索引：

```rust
use rust_graphdb::index_schema::IndexSchema;

let mut schema = IndexSchema::new();
schema.add_index("Article", "slug");
schema.add_index("Article", "published_at");

let db = GraphDatabase::new_in_memory_with_schema(schema);
```

## 限制与未来改进

当前版本是"能在小项目里真用"的最小实现：

- ❌ 只支持内存存储（无持久化）
- ❌ 无并发事务支持
- ❌ 无 Cypher 文本解析器
- ❌ 无 Bolt 协议支持

未来可以：
- 使用 sled/SQLite 做持久化存储
- 实现简单 Cypher parser
- 添加多线程/异步查询支持

## 许可证

MIT
