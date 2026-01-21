# Rust Graph Database

一个参考 Neo4j 架构实现的简化版 Rust 图数据库，适用于小项目和学习用途。
还是玩具阶段

## 功能特性

### 图模型 (Graph Model)

| 功能                    | 状态      |
| ----------------------- | --------- |
| **节点 (Node)**         | ✅ 已实现 |
| **关系 (Relationship)** | ✅ 已实现 |
| **标签 (Labels)**       | ✅ 已实现 |
| **属性 (Properties)**   | ✅ 已实现 |
| **属性类型**            | ✅ 已实现 |
| **节点 ID**             | ✅ 已实现 |
| **关系 ID**             | ✅ 已实现 |
| **自环**                | ✅ 已实现 |
| **多重边**              | ✅ 已实现 |
| **有向性**              | ✅ 已实现 |

### 图遍历

| 功能           | 状态               |
| -------------- | ------------------ |
| **出边遍历**   | ✅ 已实现          |
| **入边遍历**   | ✅ 已实现          |
| **无向遍历**   | ✅ 已实现          |
| **变长路径**   | ✅ 已实现          |
| **最短路径**   | ✅ 已实现          |
| **全路径探索** | ✅ 已实现          |
| **路径过滤**   | ✅ 已实现          |
| **循环检测**   | ✅ 已实现 简单循环 |
| **深度限制**   | ✅ 已实现          |
| **广度优先**   | ✅ 已实现          |
| **深度优先**   | ✅ 已实现          |
| **路径返回**   | ✅ 已实现          |

| 功能              | 状态        |
| ----------------- | ----------- |
| **声明式查询**    | ⚠️ 部分实现 |
| **命令式查询**    | ✅ 已实现   |
| **标签过滤**      | ✅ 已实现   |
| **属性过滤**      | ✅ 已实现   |
| **复杂条件**      | ✅ 已实现   |
| **正则匹配**      | ✅ 已实现   |
| **存在性检查**    | ✅ 已实现   |
| **NULL 处理**     | ✅ 已实现   |
| **IN 操作符**     | ✅ 已实现   |
| **范围查询**      | ⚠️ 有限     |
| **多跳查询**      | ⚠️ 有限     |
| **路径变量**      | ✅ 已实现   |
| **查询优化**      | ✅ 已实现   |
| **多变量 RETURN** | ✅ 已实现   |

### 图修改

| 功能                      | 状态      |
| ------------------------- | --------- |
| **创建节点**              | ✅ 已实现 |
| **创建关系**              | ✅ 已实现 |
| **删除节点**              | ✅ 已实现 |
| **删除关系**              | ✅ 已实现 |
| **更新属性**              | ✅ 已实现 |
| **批量创建**              | ✅ 已实现 |
| **MERGE**                 | ✅ 已实现 |
| **FOREACH**               | ✅ 已实现 |
| **CALL 子查询**           | ✅ 已实现 |
| **UNION ALL**             | ✅ 已实现 |
| **事务写入**              | ✅ 已实现 |
| **事务超时**              | ✅ 已实现 |
| **保存点**                | ✅ 已实现 |
| **悲观锁**                | ✅ 已实现 |
| **BEGIN/COMMIT/ROLLBACK** | ✅ 已实现 |

### 图算法 (Graph Algorithms)

| 功能                    | 状态      |
| ----------------------- | --------- |
| **PageRank**            | ✅ 已实现 |
| **最短路径 (Dijkstra)** | ✅ 已实现 |
| **最短路径 (BFS)**      | ✅ 已实现 |
| **所有最短路径**        | ✅ 已实现 |
| **度中心性**            | ✅ 已实现 |
| **介数中心性**          | ✅ 已实现 |
| **连通分量**            | ✅ 已实现 |
| **Louvain 社区**        | ✅ 已实现 |
| **三角计数**            | ✅ 已实现 |
| **局部聚类系数**        | ✅ 已实现 |
| **全局聚类系数**        | ✅ 已实现 |
| **K-核心**              | ✅ 已实现 |
| **强连通分量 (SCC)**    | ✅ 已实现 |
| **A\***                 | ✅ 已实现 |
| **A\* 欧几里得**        | ✅ 已实现 |
| **A\* 曼哈顿**          | ✅ 已实现 |

### 异步操作

| 功能 | 状态 |
|------|------|
| **异步 API** | ✅ 已实现 |
| **异步创建** | ✅ 已实现 |
| **异步读取** | ✅ 已实现 |
| **批量异步** | ✅ 已实现 |
| **流式写入** | ✅ 已实现 |
| **并发创建** | ✅ 已实现|
| **流式查询** | ✅ 已实现 |
| **背压处理** | ✅ 已实现 |

### 并发控制

| 功能 | 状态 |
|------|------|
| **并发读取** | ⚠️ 有限 |
| **并发写入** | ⚠️ 有限 |
| **读写锁** | ✅ 已实现 |
| **事务隔离** | ✅ 已实现 |
| **死锁检测** | ✅ 已实现 |
| **乐观锁** | ✅ 已实现 |
| **悲观锁** | ✅ 已实现 |

### 索引支持

| 功能 | 状态 |
|------|------|
| **属性索引** | ✅ 已实现 |
| **标签索引** | ✅ 已实现 |
| **复合索引** | ✅ 已实现 |
| **全文索引** | ✅ 已实现 |
| **范围索引** | ✅ 已实现 |
| **自动索引** | ✅ 已实现 |
| **索引统计** | ✅ 已实现 |
| **索引持久化** | ✅ 已实现 |

### 聚合函数

| 功能 | 状态 |
|------|------|
| **COUNT** | ✅ 已实现 |
| **SUM** | ✅ 已实现 |
| **AVG** | ✅ 已实现 |
| **MIN** | ✅ 已实现 |
| **MAX** | ✅ 已实现 |
| **COLLECT** | ✅ 已实现 |
| **GROUP BY** | ✅ 已实现 |
| **DISTINCT** | ✅ 已实现 |
| **percentileCont** | ✅ 已实现 |
| **percentileDisc** | ✅ 已实现 |
| **stDev** | ✅ 已实现 |
| **stDevP** | ✅ 已实现 |

### 排序和分页

| 功能 | 状态 |
|------|------|
| **ORDER BY** | ✅ 已实现 |
| **SKIP** | ✅ 已实现 |
| **LIMIT** | ✅ 已实现 |
| **分页游标** | ❌ 缺失 |
| **多个排序** | ✅ 已实现 |
| **NULL 排序** | ✅ 已实现 |

## 快速开始

### 1. 运行测试

```
cargo test
```

### 2. 启动 HTTP 服务器

```
cargo run --example demo_server
```

服务器将在 `http://127.0.0.1:3000` 启动。

### 3. 使用 HTTP API

#### 创建节点

```
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

```
{ "id": 0 }
```

#### 创建关系

```
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

```
curl -X POST http://127.0.0.1:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "label": "User"
  }'
```

索引查询（按 label + 属性）：

```
curl -X POST http://127.0.0.1:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "label": "User",
    "property": "name",
    "value": "Alice"
  }'
```

遍历查询（沿关系走）：

```
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
│   ├── values/              # 值类型定义（Int, Bool, Text, Float）
│   ├── storage/             # 存储引擎抽象 + 实现
│   │   ├── mem_store.rs          # 内存存储引擎
│   │   ├── sled_store.rs         # Sled 持久化存储
│   │   ├── buffered_sled_store.rs # 缓冲 Sled 存储
│   │   ├── hybrid_store.rs       # 混合存储引擎
│   │   └── async_store.rs        # 异步存储接口
│   ├── graph/               # 图数据库核心 API
│   │   ├── db.rs                 # GraphDatabase 主实现
│   │   ├── async_db.rs           # 异步图数据库
│   │   └── model.rs              # 节点/关系数据模型
│   ├── algorithms/          # 图算法实现
│   │   ├── pagerank.rs           # PageRank 算法
│   │   ├── centrality.rs         # 中心性算法
│   │   ├── shortest_path.rs      # 最短路径算法
│   │   ├── astar.rs              # A* 寻路算法
│   │   ├── community.rs          # 社区发现算法
│   │   ├── louvain.rs            # Louvain 算法
│   │   ├── kcore.rs              # K-core 分解
│   │   ├── scc.rs                # 强连通分量
│   │   ├── triangle.rs           # 三角形计数
│   │   └── traversal.rs          # 图遍历算法
│   ├── cache/               # 多层缓存系统（可选特性）
│   │   ├── manager.rs            # 缓存管理器
│   │   ├── node_cache.rs         # 节点缓存
│   │   ├── adjacency_cache.rs    # 邻接缓存
│   │   ├── index_cache.rs        # 索引缓存
│   │   ├── lru.rs                # LRU 缓存实现
│   │   └── config.rs             # 缓存配置
│   ├── cypher/              # Cypher 查询语言支持
│   │   ├── parser.rs             # 词法与语法分析
│   │   ├── ast.rs                # 抽象语法树
│   │   ├── executor.rs           # 查询执行器
│   │   └── streaming.rs          # 流式执行
│   ├── transactions/        # 事务管理
│   │   ├── transaction.rs        # 事务核心逻辑
│   │   ├── isolation.rs          # 事务隔离
│   │   ├── deadlock.rs           # 死锁检测
│   │   ├── locks.rs              # 锁管理
│   │   ├── optimistic_lock.rs    # 乐观锁
│   │   └── snapshot.rs           # 快照管理
│   ├── constraints/         # 数据约束与验证
│   ├── visualization/       # 图可视化支持
│   │   ├── layout.rs             # 图布局算法
│   │   └── export.rs             # 导出功能
│   ├── grpc/                # gRPC 服务模块（可选特性）
│   ├── index.rs             # 内存属性索引
│   ├── index_schema.rs      # 索引配置 schema
│   ├── index_persistent.rs  # 持久化索引
│   ├── index_advanced.rs    # 高级索引类型
│   ├── index_composite.rs   # 复合索引
│   ├── query.rs             # 链式查询 API
│   ├── query_engine.rs      # 查询执行引擎
│   ├── query_stream.rs      # 流式查询支持
│   ├── concurrent.rs        # 并发控制
│   ├── service.rs           # gRPC 服务实现
│   ├── server.rs            # HTTP REST 服务
│   └── lib.rs               # 库入口
├── tests/                   # 集成测试
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
│   ├── batch_write_test.rs         # 批量写入测试
│   ├── transaction_test.rs         # 事务测试
│   └── constraint_test.rs          # 约束测试
├── examples/                 # 示例程序
│   ├── demo_server.rs             # HTTP REST API 服务器
│   ├── cypher_demo.rs             # Cypher 查询示例
│   ├── algorithms_demo.rs         # 图算法演示
│   ├── concurrent_demo.rs         # 并发操作演示
│   ├── cache_demo.rs              # 缓存功能演示
│   ├── grpc_server.rs             # gRPC 服务器
│   └── grpc_client_test.rs        # gRPC 客户端测试
├── benches/                  # 性能基准测试
│   ├── query_benchmarks.rs        # 查询性能测试
│   └── batch_write_benchmarks.rs  # 批量写入性能测试
├── proto/                    # gRPC 协议定义
│   └── graphdb.proto              # gRPC 服务定义
├── web-ui/                   # Vue 3 Web 管理界面
├── static/                   # 静态资源文件
├── build.rs                  # 构建脚本
├── Cargo.toml                # 项目依赖配置
├── ALGORITHMS.md             # 算法实现文档
├── CYPHER_GUIDE.md           # Cypher 查询指南
```

## 使用 Rust API

```
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

```
cd web-ui
npm install
npm run build
```

构建后的文件将输出到 `static/` 目录。

### 启动服务器

```
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

```
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

```
use rs_graphdb::index_schema::IndexSchema;

let mut schema = IndexSchema::new();
schema.add_index("Article", "slug");
schema.add_index("Article", "published_at");

let db = GraphDatabase::new_in_memory_with_schema(schema);
```

## 许可证

MIT
