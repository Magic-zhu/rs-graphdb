# 图算法库文档

## 已实现算法

### 1. 最短路径算法

#### BFS 最短路径（无权图）

```rust
use rs_graphdb::algorithms::bfs_shortest_path;

let path = bfs_shortest_path(&db, start_node, end_node);
if let Some(p) = path {
    println!("Path: {:?}", p);
    println!("Length: {}", p.len() - 1);
}
```

- **时间复杂度**: O(V + E)
- **适用场景**: 无权图或所有边权重相同
- **返回**: `Option<Vec<NodeId>>` - 从起点到终点的节点序列

#### Dijkstra 算法（加权图）

```rust
use rs_graphdb::algorithms::dijkstra;

let result = dijkstra(&db, start_node, end_node);
if let Some((path, cost)) = result {
    println!("Path: {:?}", path);
    println!("Total cost: {}", cost);
}
```

- **时间复杂度**: O((V + E) log V)
- **适用场景**: 加权图（当前版本所有边权重为 1）
- **返回**: `Option<(Vec<NodeId>, usize)>` - 路径和总代价

### 2. 中心性算法

#### 度中心性（Degree Centrality）

衡量节点的连接数量。

```rust
use rs_graphdb::algorithms::degree_centrality;

let centrality = degree_centrality(&db);
for (node_id, score) in centrality {
    println!("Node {}: centrality = {:.3}", node_id, score);
}
```

- **时间复杂度**: O(V + E)
- **返回**: `HashMap<NodeId, f64>` - 每个节点的归一化度中心性（0-1）
- **解释**: 值越高，节点连接越多

#### 介数中心性（Betweenness Centrality）

衡量节点在最短路径上的重要性。

```rust
use rs_graphdb::algorithms::betweenness_centrality;

let centrality = betweenness_centrality(&db);
let mut nodes: Vec<_> = centrality.iter().collect();
nodes.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());

// 打印 Top 5
for (node_id, score) in nodes.iter().take(5) {
    println!("Node {}: betweenness = {:.3}", node_id, score);
}
```

- **时间复杂度**: O(V² × (V + E))
- **返回**: `HashMap<NodeId, f64>` - 归一化的介数中心性
- **解释**: 值越高，节点在网络中越"关键"（桥梁作用）

### 3. 社区检测算法

#### 连通分量（Connected Components）

找出图中的所有连通子图。

```rust
use rs_graphdb::algorithms::connected_components;

let components = connected_components(&db);
for (node_id, component_id) in components {
    println!("Node {} belongs to component {}", node_id, component_id);
}
```

- **时间复杂度**: O(V + E)
- **返回**: `HashMap<NodeId, usize>` - 节点到分量 ID 的映射
- **适用场景**: 检测孤立子图、社交网络中的社区

#### 获取分量列表

```rust
use rs_graphdb::algorithms::community::get_components;

let components = get_components(&db);
println!("Found {} components", components.len());

for (i, component) in components.iter().enumerate() {
    println!("Component {}: {:?}", i, component);
}
```

- **返回**: `Vec<Vec<NodeId>>` - 每个连通分量的节点列表

## 使用示例

### 示例 1：社交网络分析

```rust
use rs_graphdb::{GraphDatabase, algorithms};
use rs_graphdb::values::{Properties, Value};

let mut db = GraphDatabase::new_in_memory();

// 创建社交网络
let alice = db.create_node(vec!["Person"], make_props("Alice"));
let bob = db.create_node(vec!["Person"], make_props("Bob"));
let carol = db.create_node(vec!["Person"], make_props("Carol"));

db.create_rel(alice, bob, "FRIEND", Properties::new());
db.create_rel(bob, carol, "FRIEND", Properties::new());

// 找最短路径
let path = algorithms::bfs_shortest_path(&db, alice, carol);
println!("Shortest path: {:?}", path);

// 找最有影响力的人
let centrality = algorithms::degree_centrality(&db);
let most_influential = centrality
    .iter()
    .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
    .unwrap();
println!("Most influential: Node {}", most_influential.0);
```

### 示例 2：路由网络分析

```rust
// 找出关键节点（移除后会影响网络连通性）
let betweenness = algorithms::betweenness_centrality(&db);
let critical_nodes: Vec<_> = betweenness
    .iter()
    .filter(|(_, &score)| score > 0.5)
    .collect();

println!("Critical nodes: {:?}", critical_nodes);
```

### 示例 3：孤立节点检测

```rust
let components = algorithms::get_components(&db);

// 找出孤立的单节点分量
let isolated: Vec<_> = components
    .iter()
    .filter(|comp| comp.len() == 1)
    .collect();

println!("Isolated nodes: {}", isolated.len());
```

## 性能考虑

| 算法 | 时间复杂度 | 空间复杂度 | 适用图大小 |
|------|-----------|-----------|----------|
| BFS 最短路径 | O(V + E) | O(V) | 中小型图 |
| Dijkstra | O((V + E) log V) | O(V) | 中小型图 |
| 度中心性 | O(V + E) | O(V) | 任意大小 |
| 介数中心性 | O(V² × (V + E)) | O(V²) | 小型图（< 1000 节点）|
| 连通分量 | O(V + E) | O(V) | 任意大小 |

**建议**：
- 介数中心性计算开销大，仅用于小规模图
- 对大图使用采样或近似算法（未实现）
- 可以在特定子图上运行算法

## 扩展计划

未来可能添加的算法：

### 最短路径
- [ ] A* 算法
- [ ] All Pairs Shortest Paths (Floyd-Warshall)
- [ ] k-shortest paths

### 中心性
- [ ] PageRank
- [ ] Closeness Centrality
- [ ] Eigenvector Centrality
- [ ] Katz Centrality

### 社区检测
- [ ] Louvain 算法
- [ ] Label Propagation
- [ ] Modularity 优化

### 图遍历
- [ ] 深度优先搜索（DFS）
- [ ] 拓扑排序
- [ ] 强连通分量（SCC）

### 其他
- [ ] 最小生成树（MST）
- [ ] 最大流
- [ ] 图着色
- [ ] 三角形计数
- [ ] Clustering Coefficient

## 运行 Demo

```bash
# 运行算法演示
cargo run --example algorithms_demo

# 输出：
# 🦀 Rust Graph Database - Algorithms Demo
#
# 📊 Graph created with 5 nodes and 5 relationships
#
# 1️⃣  Shortest Path (BFS):
#    Path from Alice to Eve: [0, 1, 3, 4]
#    Length: 3
# ...
```
