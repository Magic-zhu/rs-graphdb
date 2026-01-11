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
rs-graphdb/
├── src/
│   ├── values/          # 值类型定义（Int, Bool, Text, Float）
│   ├── storage/         # 存储引擎抽象 + 内存/Sled实现
│   │   ├── mem_store.rs      # 内存存储引擎
│   │   ├── sled_store.rs     # Sled持久化存储
│   │   └── async_store.rs    # 异步存储接口
│   ├── graph/           # 图数据库核心 API
│   │   ├── db.rs             # GraphDatabase 主实现
│   │   ├── async_db.rs       # 异步图数据库
│   │   └── model.rs          # 节点/关系数据模型
│   ├── algorithms/      # 图算法实现
│   │   ├── pagerank.rs       # PageRank 算法
│   │   ├── centrality.rs     # 中心性算法
│   │   ├── shortest_path.rs  # 最短路径算法
│   │   ├── community.rs      # 社区发现算法
│   │   └── louvain.rs        # Louvain 算法
│   ├── cache/           # 多层缓存系统
│   │   ├── manager.rs        # 缓存管理器
│   │   ├── node_cache.rs     # 节点缓存
│   │   ├── adjacency_cache.rs # 邻接缓存
│   │   ├── query_cache.rs    # 查询缓存
│   │   ├── index_cache.rs    # 索引缓存
│   │   └── lru.rs            # LRU 缓存实现
│   ├── cypher/          # Cypher 查询语言支持
│   │   ├── parser.rs         # 词法与语法分析
│   │   ├── ast.rs            # 抽象语法树
│   │   └── executor.rs       # 查询执行器
│   ├── grpc/            # gRPC 服务模块
│   ├── index.rs         # 内存属性索引
│   ├── index_persistent.rs    # 持久化索引
│   ├── index_schema.rs  # 索引配置 schema
│   ├── query.rs         # 链式查询 API
│   ├── concurrent.rs    # 并发控制
│   ├── service.rs       # gRPC 服务实现
│   ├── server.rs        # HTTP REST 服务
│   └── lib.rs
├── tests/               # 集成测试
│   ├── basic.rs                    # 基础功能测试
│   ├── query.rs                    # 查询测试
│   ├── query_advanced.rs           # 高级查询测试
│   ├── query_extended.rs           # 扩展查询测试
│   ├── query_reverse.rs            # 反向遍历测试
│   ├── index_query.rs              # 索引查询测试
│   ├── cypher_test.rs              # Cypher 查询测试
│   ├── cypher_create_test.rs       # Cypher 创建测试
│   ├── cypher_delete_test.rs       # Cypher 删除测试
│   ├── cypher_extended.rs          # Cypher 扩展测试
│   ├── algorithms_test.rs          # 算法测试
│   ├── sled_persistence_test.rs    # Sled 持久化测试
│   ├── cache_integration_test.rs   # 缓存集成测试
│   ├── async_write_test.rs         # 异步写入测试
│   └── batch_write_test.rs         # 批量写入测试
├── examples/             # 示例程序
│   ├── demo_server.rs         # HTTP REST API 服务器
│   ├── cypher_demo.rs         # Cypher 查询示例
│   ├── algorithms_demo.rs     # 图算法演示
│   ├── concurrent_demo.rs     # 并发操作演示
│   ├── cache_demo.rs          # 缓存功能演示
│   ├── grpc_server.rs         # gRPC 服务器
│   └── grpc_client_test.rs    # gRPC 客户端测试
├── benches/              # 性能基准测试
│   ├── query_benchmarks.rs        # 查询性能测试
│   └── batch_write_benchmarks.rs  # 批量写入性能测试
├── proto/                # gRPC 协议定义
├── web-ui/               # Vue 3 Web 管理界面
├── static/               # 静态资源文件
├── build.rs              # 构建脚本
├── Cargo.toml            # 项目依赖配置
├── ALGORITHMS.md         # 算法实现文档
├── CYPHER_GUIDE.md       # Cypher 查询指南
└── FEATURES.md           # 功能特性文档
```

## 使用 Rust API

```rust
use rs_graphdb::{GraphDatabase, values::{Properties, Value}};
use rs_graphdb::query::Query;

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

## Web UI 界面

项目包含一个基于 **Vue 3 + Pinia + Tailwind CSS** 的可视化管理界面。

### 构建前端

```bash
cd web-ui
npm install
npm run build
```

构建后的文件将输出到 `static/` 目录。

### 启动服务器

```bash
cargo run --example demo_server
```

然后访问 `http://127.0.0.1:3000/ui` 查看可视化界面。

### 前端功能

- **仪表盘** - 数据库统计、标签和关系类型概览
- **节点管理** - 创建、查看、搜索节点
- **关系管理** - 创建关系、查看关系列表
- **查询功能** - 按标签查询、按属性查询、全局搜索
- **图可视化** - 交互式网络图、节点详情、邻居展开

### 前端开发

```bash
cd web-ui
npm run dev
```

开发服务器将在 `http://localhost:5173` 启动，支持热重载。

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
use rs_graphdb::index_schema::IndexSchema;

let mut schema = IndexSchema::new();
schema.add_index("Article", "slug");
schema.add_index("Article", "published_at");

let db = GraphDatabase::new_in_memory_with_schema(schema);
```

## 限制与未来改进

当前版本是"能在小项目里真用"的最小实现：
- ❌ 无并发事务支持
- ❌ 无 Cypher 文本解析器
- ❌ 无 Bolt 协议支持

- [] 实现简单 Cypher parser
- [] 添加多线程/异步查询支持

## 许可证

MIT
