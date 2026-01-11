# Cypher 查询语言指南

## 支持的语法

### 1. MATCH 子句

基本模式匹配：
```cypher
MATCH (a:User) RETURN a
```

带属性的节点：
```cypher
MATCH (a:User {name: "Alice"}) RETURN a
MATCH (a:User {name: "Alice", age: 30}) RETURN a
```

关系遍历：
```cypher
MATCH (a:User)-[:FRIEND]->(b:User) RETURN a, b
MATCH (a)-[:FRIEND]->(b)-[:FRIEND]->(c) RETURN a, b, c
```

反向遍历：
```cypher
MATCH (a:User)<-[:FRIEND]-(b:User) RETURN a, b
```

### 2. OPTIONAL MATCH

可选匹配（类似 SQL 的 LEFT JOIN）：
```cypher
OPTIONAL MATCH (a:User)-[:FRIEND]->(b) RETURN a, b
```

### 3. WHERE 子句

条件过滤：
```cypher
MATCH (a:User) WHERE a.age > 25 RETURN a
MATCH (a:User) WHERE a.name = "Alice" RETURN a
MATCH (a:User) WHERE a.age < 30 RETURN a
```

多条件（AND）：
```cypher
MATCH (a:User) WHERE a.age > 25 AND a.age < 40 RETURN a
```

### 4. RETURN 子句

返回节点：
```cypher
MATCH (a:User) RETURN a
MATCH (a)-[:FRIEND]->(b) RETURN a, b
```

聚合函数：
```cypher
MATCH (a:User) RETURN COUNT(*)
MATCH (a:User) RETURN SUM(a.age)
MATCH (a:User) RETURN AVG(a.age)
MATCH (a:User) RETURN MIN(a.age)
MATCH (a:User) RETURN MAX(a.age)
```

### 5. ORDER BY

排序：
```cypher
MATCH (a:User) RETURN a ORDER BY a.age
MATCH (a:User) RETURN a ORDER BY a.age ASC
MATCH (a:User) RETURN a ORDER BY a.age DESC
```

### 6. LIMIT 和 SKIP

分页：
```cypher
MATCH (a:User) RETURN a LIMIT 10
MATCH (a:User) RETURN a SKIP 5 LIMIT 10
MATCH (a:User) RETURN a ORDER BY a.age DESC LIMIT 5
```

## 完整示例

### 示例 1：社交网络查询

找出 Alice 的所有朋友：
```cypher
MATCH (a:User {name: "Alice"})-[:FRIEND]->(b) RETURN b
```

找出 Alice 的朋友的朋友：
```cypher
MATCH (a:User {name: "Alice"})-[:FRIEND]->()-[:FRIEND]->(c) RETURN c
```

找出年龄大于 25 的用户：
```cypher
MATCH (a:User) WHERE a.age > 25 RETURN a ORDER BY a.age DESC
```

### 示例 2：聚合查询

统计用户总数：
```cypher
MATCH (a:User) RETURN COUNT(*)
```

计算平均年龄：
```cypher
MATCH (a:User) RETURN AVG(a.age)
```

找出最年轻和最年长的用户：
```cypher
MATCH (a:User) RETURN MIN(a.age), MAX(a.age)
```

### 示例 3：组合查询

找出 Alice 的朋友中年龄最大的 3 个人：
```cypher
MATCH (a:User {name: "Alice"})-[:FRIEND]->(b)
RETURN b
ORDER BY b.age DESC
LIMIT 3
```

找出所有有朋友的用户（去重）：
```cypher
MATCH (a:User)-[:FRIEND]->(b)
RETURN a
```

## 当前限制

### 不支持的功能

1. **CREATE / DELETE / SET**
   - 当前只支持查询，不支持通过 Cypher 修改数据
   - 使用 Rust API 或 HTTP API 创建数据

2. **OR 条件**
   - WHERE 子句只支持 AND
   - 不支持：`WHERE a.age > 25 OR a.name = "Alice"`

3. **复杂表达式**
   - 不支持：`WHERE a.age + 10 > 35`
   - 不支持：`WHERE a.name CONTAINS "Ali"`

4. **WITH 子句**
   - 不支持中间结果传递

5. **UNION / UNWIND**
   - 不支持集合操作

6. **路径变量**
   - 不支持：`MATCH p = (a)-[:FRIEND*]->(b)`

## 使用示例（Rust API）

```rust
use rs_graphdb::cypher;

let query_str = r#"
    MATCH (a:User {name: "Alice"})-[:FRIEND]->(b)
    WHERE b.age > 25
    RETURN b
    ORDER BY b.age DESC
    LIMIT 10
"#;

let query = cypher::parse_cypher(query_str)?;
let results = cypher::execute_cypher(&db, &query)?;

for node in results {
    println!("Found: {:?}", node);
}
```

## 扩展计划

未来可能支持的功能：

- [ ] CREATE / DELETE / SET / MERGE
- [ ] OR 条件
- [ ] 正则表达式匹配（CONTAINS / STARTS WITH / ENDS WITH）
- [ ] WITH 子句
- [ ] 可变长度路径 `[:FRIEND*1..3]`
- [ ] UNION / UNWIND
- [ ] 子查询
- [ ] CASE 表达式
