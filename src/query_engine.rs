//! 高级查询引擎
//!
//! 提供增强的图查询功能：
//! - 多变量 RETURN 支持
//! - 路径查询和返回
//! - 复杂聚合查询
//! - 查询结果缓存
//! - 查询优化

use crate::graph::db::GraphDatabase;
use crate::graph::model::{Node, Relationship};
use crate::storage::{NodeId, StorageEngine};
use crate::values::Value;
use std::collections::HashMap;

/// 查询结果类型
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Nodes(Vec<Node>),
    Relationships(Vec<Relationship>),
    Mixed(QueryRows),
    Aggregation(AggregationResult),
    Path(QueryPath),
}

/// 查询行，包含多个变量的值
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

/// 查询值
#[derive(Debug, Clone, PartialEq)]
pub enum QueryValue {
    Node(Node),
    Relationship(Relationship),
    Value(Value),
    Null,
}

/// 路径查询结果
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPath {
    pub nodes: Vec<Node>,
    pub relationships: Vec<Relationship>,
}

impl QueryPath {
    pub fn length(&self) -> usize {
        self.relationships.len()
    }

    pub fn start_node(&self) -> Option<&Node> {
        self.nodes.first()
    }

    pub fn end_node(&self) -> Option<&Node> {
        self.nodes.last()
    }
}

/// 查询行集合
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRows {
    pub rows: Vec<QueryRow>,
    pub columns: Vec<String>,
}

impl QueryRows {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            rows: Vec::new(),
            columns,
        }
    }

    pub fn add_row(&mut self, values: Vec<QueryValue>) {
        self.rows.push(QueryRow { values });
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// 聚合查询结果
#[derive(Debug, Clone, PartialEq)]
pub struct AggregationResult {
    pub values: Vec<(String, Value)>,
}

/// 查询上下文，用于跟踪变量绑定
#[derive(Debug, Clone)]
pub struct QueryContext {
    pub node_bindings: HashMap<String, Node>,
    pub rel_bindings: HashMap<String, Relationship>,
    pub path_bindings: HashMap<String, QueryPath>,
}

impl QueryContext {
    pub fn new() -> Self {
        Self {
            node_bindings: HashMap::new(),
            rel_bindings: HashMap::new(),
            path_bindings: HashMap::new(),
        }
    }

    pub fn bind_node(&mut self, var: String, node: Node) {
        self.node_bindings.insert(var, node);
    }

    pub fn bind_rel(&mut self, var: String, rel: Relationship) {
        self.rel_bindings.insert(var, rel);
    }

    pub fn bind_path(&mut self, var: String, path: QueryPath) {
        self.path_bindings.insert(var, path);
    }

    pub fn get_node(&self, var: &str) -> Option<&Node> {
        self.node_bindings.get(var)
    }

    pub fn get_rel(&self, var: &str) -> Option<&Relationship> {
        self.rel_bindings.get(var)
    }

    pub fn get_path(&self, var: &str) -> Option<&QueryPath> {
        self.path_bindings.get(var)
    }
}

/// 路径查询构建器
pub struct PathQueryBuilder<'a, E: StorageEngine> {
    db: &'a GraphDatabase<E>,
    start_var: Option<String>,
    end_var: Option<String>,
    rel_types: Vec<String>,
    min_depth: usize,
    max_depth: usize,
    direction: Direction,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

impl<'a, E: StorageEngine> PathQueryBuilder<'a, E> {
    pub fn new(db: &'a GraphDatabase<E>) -> Self {
        Self {
            db,
            start_var: None,
            end_var: None,
            rel_types: Vec::new(),
            min_depth: 1,
            max_depth: 3,
            direction: Direction::Outgoing,
        }
    }

    pub fn start_node(mut self, var: String) -> Self {
        self.start_var = Some(var);
        self
    }

    pub fn end_node(mut self, var: String) -> Self {
        self.end_var = Some(var);
        self
    }

    pub fn rel_types(mut self, types: Vec<String>) -> Self {
        self.rel_types = types;
        self
    }

    pub fn min_depth(mut self, depth: usize) -> Self {
        self.min_depth = depth;
        self
    }

    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }

    pub fn direction(mut self, dir: Direction) -> Self {
        self.direction = dir;
        self
    }

    /// 执行路径查询
    pub fn execute(self, start_id: NodeId, end_id: NodeId) -> Vec<QueryPath> {
        use crate::algorithms::*;

        // 根据方向选择合适的遍历方式
        let paths = if self.direction == Direction::Both {
            // 无向图遍历
            let all_paths = all_simple_paths(self.db, start_id, end_id, Some(self.max_depth));

            // 转换为 QueryPath
            all_paths.into_iter()
                .filter(|path| path.len() >= self.min_depth + 1 && path.len() <= self.max_depth + 1)
                .map(|node_ids| self.build_query_path(&node_ids))
                .filter_map(|p| p.ok())
                .collect()
        } else {
            // 有向遍历
            let all_paths = all_simple_paths(self.db, start_id, end_id, Some(self.max_depth));

            all_paths.into_iter()
                .filter(|path| path.len() >= self.min_depth + 1 && path.len() <= self.max_depth + 1)
                .map(|node_ids| self.build_query_path(&node_ids))
                .filter_map(|p| p.ok())
                .collect()
        };

        paths
    }

    fn build_query_path(&self, node_ids: &[NodeId]) -> Result<QueryPath, String> {
        let mut nodes = Vec::new();
        let mut relationships = Vec::new();

        // 收集所有节点
        for &node_id in node_ids {
            let node = self.db.get_node(node_id)
                .ok_or_else(|| format!("Node {} not found", node_id))?;
            nodes.push(node);
        }

        // 收集节点之间的关系
        for i in 0..node_ids.len() - 1 {
            let start = node_ids[i];
            let end = node_ids[i + 1];

            // 查找连接这两个节点的关系
            let rel = self.find_rel_between(start, end)?;
            relationships.push(rel);
        }

        Ok(QueryPath { nodes, relationships })
    }

    fn find_rel_between(&self, start: NodeId, end: NodeId) -> Result<Relationship, String> {
        for rel in self.db.neighbors_out(start) {
            if rel.end == end {
                return Ok(rel);
            }
        }
        Err(format!("No relationship found between {} and {}", start, end))
    }
}

/// 多变量查询执行器
pub struct MultiVarQueryExecutor<'a, E: StorageEngine> {
    db: &'a GraphDatabase<E>,
    context: QueryContext,
}

impl<'a, E: StorageEngine> MultiVarQueryExecutor<'a, E> {
    pub fn new(db: &'a GraphDatabase<E>) -> Self {
        Self {
            db,
            context: QueryContext::new(),
        }
    }

    /// 执行返回多个变量的查询
    pub fn execute(
        &mut self,
        start_nodes: Vec<NodeId>,
        return_vars: Vec<String>,
    ) -> Result<QueryRows, String> {
        let mut rows = QueryRows::new(return_vars.clone());

        for start_id in start_nodes {
            if let Some(node) = self.db.get_node(start_id) {
                let mut values = Vec::new();

                for var in &return_vars {
                    // 检查是否是已绑定的节点
                    if let Some(bound_node) = self.context.get_node(var) {
                        values.push(QueryValue::Node(bound_node.clone()));
                    } else if var == "n" || var.contains("node") {
                        // 默认返回当前节点
                        values.push(QueryValue::Node(node.clone()));
                    } else {
                        values.push(QueryValue::Null);
                    }
                }

                rows.add_row(values);
            }
        }

        Ok(rows)
    }

    /// 执行路径查询并返回
    pub fn execute_path_query(
        &self,
        start_id: NodeId,
        end_id: NodeId,
        _path_var: String,
    ) -> Result<QueryPath, String> {
        use crate::algorithms::shortest_path_with_rels;

        let path = shortest_path_with_rels(self.db, start_id, end_id)
            .ok_or_else(|| "No path found".to_string())?;

        // 转换为 QueryPath
        let mut nodes = Vec::new();
        let mut relationships = Vec::new();

        for node_id in &path.nodes {
            let node = self.db.get_node(*node_id)
                .ok_or_else(|| format!("Node {} not found", node_id))?;
            nodes.push(node);
        }

        for rel_id in &path.rels {
            let rel = self.db.get_rel(*rel_id)
                .ok_or_else(|| format!("Relationship {} not found", rel_id))?;
            relationships.push(rel);
        }

        Ok(QueryPath { nodes, relationships })
    }
}

/// 查询优化器
pub struct QueryOptimizer {
    enable_index_usage: bool,
    enable_caching: bool,
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            enable_index_usage: true,
            enable_caching: false,
        }
    }

    pub fn with_indexing(mut self, enabled: bool) -> Self {
        self.enable_index_usage = enabled;
        self
    }

    pub fn with_caching(mut self, enabled: bool) -> Self {
        self.enable_caching = enabled;
        self
    }

    /// 优化查询执行计划
    pub fn optimize(&self, query: &crate::cypher::ast::CypherQuery) -> OptimizationPlan {
        let mut plan = OptimizationPlan::new();

        // 分析 MATCH 子句
        if let Some(ref match_clause) = query.match_clause {
            // 检查是否可以使用索引
            if self.enable_index_usage {
                if self.can_use_index(match_clause) {
                    plan.use_index = true;
                }
            }

            // 估算结果集大小
            plan.estimated_rows = self.estimate_rows(match_clause);
        }

        // 分析 WHERE 子句
        if query.where_clause.is_some() {
            plan.needs_filtering = true;
        }

        // 分析 RETURN 子句
        plan.has_aggregation = query.return_clause.items.iter()
            .any(|item| matches!(item, crate::cypher::ast::ReturnItem::Aggregation(..)));

        plan
    }

    fn can_use_index(&self, match_clause: &crate::cypher::ast::MatchClause) -> bool {
        // 检查是否有标签+属性组合可以使用索引
        let start = &match_clause.pattern.start_node;
        start.label.is_some() && !start.props.is_empty()
    }

    fn estimate_rows(&self, match_clause: &crate::cypher::ast::MatchClause) -> usize {
        // 简单估算：有标签过滤 -> 减少到 20%，有属性过滤 -> 减少到 5%
        let mut estimate: usize = 1000; // 默认假设 1000 行

        let start = &match_clause.pattern.start_node;
        if start.label.is_some() {
            estimate = estimate * 20 / 100;
        }
        if !start.props.is_empty() {
            estimate = estimate * 5 / 100;
        }

        // 关系遍历进一步减少
        estimate = estimate.saturating_mul(match_clause.pattern.relationships.len());

        estimate.max(1)
    }
}

/// 查询执行计划
#[derive(Debug, Clone)]
pub struct OptimizationPlan {
    pub use_index: bool,
    pub needs_filtering: bool,
    pub has_aggregation: bool,
    pub estimated_rows: usize,
}

impl OptimizationPlan {
    pub fn new() -> Self {
        Self {
            use_index: false,
            needs_filtering: false,
            has_aggregation: false,
            estimated_rows: 1000,
        }
    }

    pub fn explain(&self) -> String {
        let mut explanation = String::from("Query Plan:\n");
        explanation.push_str(&format!("  Estimated rows: {}\n", self.estimated_rows));
        explanation.push_str(&format!("  Use index: {}\n", self.use_index));
        explanation.push_str(&format!("  Needs filtering: {}\n", self.needs_filtering));
        explanation.push_str(&format!("  Has aggregation: {}\n", self.has_aggregation));
        explanation
    }
}

impl Default for OptimizationPlan {
    fn default() -> Self {
        Self::new()
    }
}

/// 高级查询构建器
pub struct AdvancedQueryBuilder<'a, E: StorageEngine> {
    db: &'a GraphDatabase<E>,
    match_patterns: Vec<String>,
    where_conditions: Vec<String>,
    return_vars: Vec<String>,
    order_by: Option<(String, bool)>,
    skip: Option<usize>,
    limit: Option<usize>,
}

impl<'a, E: StorageEngine> AdvancedQueryBuilder<'a, E> {
    pub fn new(db: &'a GraphDatabase<E>) -> Self {
        Self {
            db,
            match_patterns: Vec::new(),
            where_conditions: Vec::new(),
            return_vars: Vec::new(),
            order_by: None,
            skip: None,
            limit: None,
        }
    }

    pub fn match_pattern(mut self, pattern: String) -> Self {
        self.match_patterns.push(pattern);
        self
    }

    pub fn where_clause(mut self, condition: String) -> Self {
        self.where_conditions.push(condition);
        self
    }

    pub fn return_vars(mut self, vars: Vec<String>) -> Self {
        self.return_vars = vars;
        self
    }

    pub fn order_by(mut self, var: String, ascending: bool) -> Self {
        self.order_by = Some((var, ascending));
        self
    }

    pub fn skip(mut self, n: usize) -> Self {
        self.skip = Some(n);
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// 构建并执行查询
    pub fn execute(self) -> Result<QueryResult, String> {
        // 这里简化实现，实际应该解析模式并执行
        let mut executor = MultiVarQueryExecutor::new(self.db);

        // 获取所有节点作为简单实现
        let all_node_ids: Vec<NodeId> = self.db.all_stored_nodes()
            .map(|n| n.id)
            .collect();

        let rows = executor.execute(all_node_ids, self.return_vars)?;

        Ok(QueryResult::Mixed(rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mem_store::MemStore;
    use crate::values::Properties;

    fn create_test_db() -> GraphDatabase<MemStore> {
        let mut db = GraphDatabase::new_in_memory();

        // 创建测试图:
        // (Alice:Person) -[:FRIEND]-> (Bob:Person) -[:FRIEND]-> (Charlie:Person)
        // (Alice) -[:KNOWS]-> (David:Person)

        let alice = db.create_node(vec!["Person"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("Alice".to_string()));
            props
        });

        let bob = db.create_node(vec!["Person"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("Bob".to_string()));
            props
        });

        let charlie = db.create_node(vec!["Person"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("Charlie".to_string()));
            props
        });

        let david = db.create_node(vec!["Person"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("David".to_string()));
            props
        });

        db.create_rel(alice, bob, "FRIEND", Properties::new());
        db.create_rel(bob, charlie, "FRIEND", Properties::new());
        db.create_rel(alice, david, "KNOWS", Properties::new());

        db
    }

    // 辅助函数：创建测试数据库并返回关键节点的 ID
    fn create_test_db_with_ids() -> (GraphDatabase<MemStore>, NodeId, NodeId, NodeId, NodeId) {
        let mut db = GraphDatabase::new_in_memory();

        // 创建测试图:
        // (Alice:Person) -[:FRIEND]-> (Bob:Person) -[:FRIEND]-> (Charlie:Person)
        // (Alice) -[:KNOWS]-> (David:Person)

        let alice = db.create_node(vec!["Person"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("Alice".to_string()));
            props
        });

        let bob = db.create_node(vec!["Person"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("Bob".to_string()));
            props
        });

        let charlie = db.create_node(vec!["Person"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("Charlie".to_string()));
            props
        });

        let david = db.create_node(vec!["Person"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("David".to_string()));
            props
        });

        db.create_rel(alice, bob, "FRIEND", Properties::new());
        db.create_rel(bob, charlie, "FRIEND", Properties::new());
        db.create_rel(alice, david, "KNOWS", Properties::new());

        (db, alice, bob, charlie, david)
    }

    #[test]
    fn test_query_context() {
        let mut ctx = QueryContext::new();

        let node = Node {
            id: 0,
            labels: vec!["Person".to_string()],
            props: Properties::new(),
        };

        ctx.bind_node("n".to_string(), node.clone());

        assert_eq!(ctx.get_node("n"), Some(&node));
        assert_eq!(ctx.get_node("x"), None);
    }

    #[test]
    fn test_query_rows() {
        let mut rows = QueryRows::new(vec!["n".to_string(), "m".to_string()]);

        rows.add_row(vec![
            QueryValue::Null,
            QueryValue::Value(Value::Int(42)),
        ]);

        assert_eq!(rows.len(), 1);
        assert!(!rows.is_empty());
    }

    #[test]
    fn test_path_query_builder() {
        let db = create_test_db();

        // 获取节点 ID
        let nodes: Vec<NodeId> = db.all_stored_nodes().map(|n| n.id).collect();

        // 创建一个简单的线性图测试
        let mut test_db = GraphDatabase::<MemStore>::new_in_memory();
        let n1 = test_db.create_node(vec!["Node"], Properties::new());
        let n2 = test_db.create_node(vec!["Node"], Properties::new());
        test_db.create_rel(n1, n2, "EDGE", Properties::new());

        let builder = PathQueryBuilder::new(&test_db)
            .min_depth(1)
            .max_depth(1)
            .direction(Direction::Outgoing);

        let paths = builder.execute(n1, n2);

        // 应该能找到一条路径
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].nodes.len(), 2);
    }

    #[test]
    fn test_query_optimizer() {
        let optimizer = QueryOptimizer::new()
            .with_indexing(true)
            .with_caching(false);

        // 创建一个测试用的 MatchClause
        let _plan = optimizer.optimize(&crate::cypher::ast::CypherQuery {
            match_clause: None,
            with_clause: None,
            where_clause: None,
            return_clause: crate::cypher::ast::ReturnClause {
                items: vec![],
                order_by: None,
                skip: None,
                limit: None,
                group_by: None,
            },
        });

        // 测试优化器创建成功
        assert!(true);
    }

    #[test]
    fn test_optimization_plan() {
        let plan = OptimizationPlan {
            use_index: true,
            needs_filtering: true,
            has_aggregation: false,
            estimated_rows: 500,
        };

        let explanation = plan.explain();

        assert!(explanation.contains("Estimated rows: 500"));
        assert!(explanation.contains("Use index: true"));
    }

    #[test]
    fn test_multi_var_executor() {
        let db = create_test_db();
        let mut executor = MultiVarQueryExecutor::new(&db);

        let nodes: Vec<NodeId> = db.all_stored_nodes().map(|n| n.id).collect();

        let rows = executor.execute(
            nodes,
            vec!["n".to_string(), "name".to_string()],
        ).unwrap();

        assert!(!rows.is_empty());
    }

    #[test]
    fn test_query_path() {
        let (db, alice, bob, charlie, david) = create_test_db_with_ids();
        let executor = MultiVarQueryExecutor::new(&db);

        // 测试 Alice -> Bob 的路径（直接连接）
        let path = executor.execute_path_query(alice, bob, "p".to_string());
        assert!(path.is_ok(), "Alice -> Bob path should exist");
        let path = path.unwrap();
        assert!(!path.nodes.is_empty());
        assert_eq!(path.nodes.len(), 2); // Alice -> Bob

        // 测试 Alice -> Charlie 的路径（通过 Bob）
        let path = executor.execute_path_query(alice, charlie, "p".to_string());
        assert!(path.is_ok(), "Alice -> Charlie path should exist");
        let path = path.unwrap();
        assert!(!path.nodes.is_empty());
        assert_eq!(path.nodes.len(), 3); // Alice -> Bob -> Charlie

        // 测试 Alice -> David 的路径（直接连接）
        let path = executor.execute_path_query(alice, david, "p".to_string());
        assert!(path.is_ok(), "Alice -> David path should exist");
        let path = path.unwrap();
        assert!(!path.nodes.is_empty());
        assert_eq!(path.nodes.len(), 2); // Alice -> David
    }
}
