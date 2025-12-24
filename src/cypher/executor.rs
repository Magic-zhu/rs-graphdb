use crate::graph::db::GraphDatabase;
use crate::graph::model::Node;
use crate::query::Query;
use crate::storage::{NodeId, StorageEngine};
use crate::values::{Properties, Value};

use super::ast::*;

/// 执行 Cypher 语句，支持：
/// - 读查询：MATCH / WHERE / RETURN（带 ORDER BY / SKIP / LIMIT）
/// - 写操作：CREATE / DELETE / SET
pub enum CypherResult {
    Nodes(Vec<Node>),
    Created { nodes: Vec<NodeId>, rels: usize },
    Deleted { nodes: usize, rels: usize },
    Updated { nodes: usize },
}

pub fn execute_statement<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    stmt: &CypherStatement,
) -> Result<CypherResult, String> {
    match stmt {
        CypherStatement::Query(q) => {
            let nodes = execute_query(db, q)?;
            Ok(CypherResult::Nodes(nodes))
        }
        CypherStatement::Create(c) => {
            let (node_ids, rel_count) = execute_create(db, c)?;
            Ok(CypherResult::Created {
                nodes: node_ids,
                rels: rel_count,
            })
        }
        CypherStatement::Delete(d) => {
            let (nodes_deleted, rels_deleted) = execute_delete(db, d)?;
            Ok(CypherResult::Deleted {
                nodes: nodes_deleted,
                rels: rels_deleted,
            })
        }
        CypherStatement::Set(s) => {
            let nodes_updated = execute_set(db, s)?;
            Ok(CypherResult::Updated {
                nodes: nodes_updated,
            })
        }
    }
}

/// 向后兼容：只返回节点的查询入口
pub fn execute_cypher<E: StorageEngine>(
    db: &GraphDatabase<E>,
    query: &CypherQuery,
) -> Result<Vec<Node>, String> {
    execute_query(db, query)
}

fn execute_query<E: StorageEngine>(
    db: &GraphDatabase<E>,
    query: &CypherQuery,
) -> Result<Vec<Node>, String> {
    // 1. 先用 MATCH 构建基础 Query
    let mut q = build_match_query(db, &query.match_clause)?;

    // 2. 应用 WHERE 子句（在内存中过滤）
    if let Some(where_clause) = &query.where_clause {
        let mut filtered_ids = Vec::new();
        for node in q.collect_nodes() {
            if eval_where_clause(&node, where_clause) {
                filtered_ids.push(node.id);
            }
        }
        // 用过滤后的 ID 重新构建 Query
        q = Query::new(db);
        q.current = filtered_ids;
    }

    // 3. 根据 RETURN 子句应用 ORDER BY / SKIP / LIMIT
    if let Some(order) = &query.return_clause.order_by {
        q = q.order_by(&order.prop, order.ascending);
    }
    if let Some(skip) = query.return_clause.skip {
        q = q.skip(skip);
    }
    if let Some(limit) = query.return_clause.limit {
        q = q.limit(limit);
    }

    Ok(q.collect_nodes())
}

fn execute_create<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    create: &CreateClause,
) -> Result<(Vec<NodeId>, usize), String> {
    let pattern = &create.pattern;

    // 创建起始节点
    let start_node = create_node_from_pattern(db, &pattern.start_node);
    let mut created_nodes = vec![start_node];
    let mut rel_count = 0;

    let mut prev_node = start_node;

    // 依次处理关系链：-[:REL]->(node)
    for (rel_pat, node_pat) in &pattern.relationships {
        let next_node = create_node_from_pattern(db, node_pat);
        created_nodes.push(next_node);

        // 创建关系
        if let Some(rel_type) = &rel_pat.rel_type {
            match rel_pat.direction {
                Direction::Outgoing => {
                    db.create_rel(prev_node, next_node, rel_type, Properties::new());
                }
                Direction::Incoming => {
                    db.create_rel(next_node, prev_node, rel_type, Properties::new());
                }
                Direction::Both => {
                    return Err("CREATE with undirected relationships not supported".to_string());
                }
            }
            rel_count += 1;
        }

        prev_node = next_node;
    }

    Ok((created_nodes, rel_count))
}

fn execute_delete<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    delete: &DeleteStatement,
) -> Result<(usize, usize), String> {
    // 1. 先用 MATCH 找到要删除的节点
    let mut q = build_match_query(db, &Some(delete.match_clause.clone()))?;

    // 2. 应用 WHERE 过滤
    if let Some(where_clause) = &delete.where_clause {
        let mut filtered_ids = Vec::new();
        for node in q.collect_nodes() {
            if eval_where_clause(&node, where_clause) {
                filtered_ids.push(node.id);
            }
        }
        q = Query::new(db);
        q.current = filtered_ids;
    }

    let nodes_to_delete: Vec<NodeId> = q.collect_nodes().into_iter().map(|n| n.id).collect();

    // 3. 删除节点（delete_node 会自动删除相关的关系）
    let mut nodes_deleted = 0;
    let mut rels_deleted = 0;

    for node_id in nodes_to_delete {
        // 统计关系数
        let out_rels = db.neighbors_out(node_id).count();
        let in_rels = db.neighbors_in(node_id).count();
        rels_deleted += out_rels + in_rels;

        if db.delete_node(node_id) {
            nodes_deleted += 1;
        }
    }

    Ok((nodes_deleted, rels_deleted))
}

fn execute_set<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    set: &SetStatement,
) -> Result<usize, String> {
    // 1. 先用 MATCH 找到要更新的节点
    let mut q = build_match_query(db, &Some(set.match_clause.clone()))?;

    // 2. 应用 WHERE 过滤
    if let Some(where_clause) = &set.where_clause {
        let mut filtered_ids = Vec::new();
        for node in q.collect_nodes() {
            if eval_where_clause(&node, where_clause) {
                filtered_ids.push(node.id);
            }
        }
        q = Query::new(db);
        q.current = filtered_ids;
    }

    let nodes_to_update: Vec<Node> = q.collect_nodes();
    let mut nodes_updated = 0;

    // 3. 对每个节点应用 SET 赋值
    // 注意：当前 StorageEngine 没有 update_node 方法，这里用删除+重建模拟
    for node in nodes_to_update {
        let mut new_props = node.props.clone();

        for assignment in &set.assignments {
            // 更新属性值
            match &assignment.value {
                PropertyValue::String(s) => {
                    new_props.insert(assignment.prop.clone(), Value::Text(s.clone()));
                }
                PropertyValue::Int(i) => {
                    new_props.insert(assignment.prop.clone(), Value::Int(*i));
                }
                PropertyValue::Variable(_) => {
                    // 暂不支持变量引用
                    return Err("SET with variable references not yet supported".to_string());
                }
            }
        }

        // 由于没有 update_node API，这里需要：
        // 1. 收集所有关系
        // 2. 删除节点（会删除所有关系）
        // 3. 重建节点
        // 4. 重建所有关系
        //
        // 简化实现：直接返回错误，提示需要实现 update_node
        return Err("SET operation requires update_node API which is not yet implemented. Consider implementing StorageEngine::update_node_props()".to_string());
    }

    Ok(nodes_updated)
}

fn create_node_from_pattern<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    node_pat: &NodePattern,
) -> NodeId {
    let labels: Vec<&str> = if let Some(ref label) = node_pat.label {
        vec![label.as_str()]
    } else {
        vec![]
    };

    let mut props = Properties::new();
    for (key, val) in &node_pat.props {
        match val {
            PropertyValue::String(s) => {
                props.insert(key.clone(), Value::Text(s.clone()));
            }
            PropertyValue::Int(i) => {
                props.insert(key.clone(), Value::Int(*i));
            }
            PropertyValue::Variable(_) => {
                // 变量暂不支持，跳过
            }
        }
    }

    db.create_node(labels, props)
}

fn build_match_query<'a, E: StorageEngine>(
    db: &'a GraphDatabase<E>,
    match_clause: &Option<MatchClause>,
) -> Result<Query<'a, E>, String> {
    let mut q = Query::new(db);

    if let Some(match_clause) = match_clause {
        let pattern = &match_clause.pattern;

        // 处理起始节点
        let start = &pattern.start_node;
        if let Some(label) = &start.label {
            // 如果有属性过滤，尝试用索引
            if let Some((prop_name, prop_val)) = start.props.first() {
                match prop_val {
                    PropertyValue::String(s) => {
                        q = q.from_label_and_prop_eq(label, prop_name, s);
                    }
                    PropertyValue::Int(i) => {
                        q = q.from_label_and_prop_int_eq(label, prop_name, *i);
                    }
                    PropertyValue::Variable(_) => {
                        // 变量在 WHERE 中处理
                        q = q.from_label(label);
                    }
                }
            } else {
                q = q.from_label(label);
            }
        }

        // 处理关系遍历
        for (rel, _node) in &pattern.relationships {
            if let Some(rel_type) = &rel.rel_type {
                match rel.direction {
                    Direction::Outgoing => {
                        q = q.out(rel_type);
                    }
                    Direction::Incoming => {
                        q = q.in_(rel_type);
                    }
                    Direction::Both => {
                        // 暂不支持无向遍历
                        return Err("Undirected relationships not yet supported".to_string());
                    }
                }
            }
        }
    }

    Ok(q)
}

fn eval_where_clause(node: &Node, where_clause: &WhereClause) -> bool {
    where_clause
        .conditions
        .iter()
        .all(|cond| eval_condition(node, cond))
}

fn eval_condition(node: &Node, cond: &Condition) -> bool {
    match cond {
        Condition::Eq(lhs, rhs) => eval_expr(node, lhs) == eval_expr(node, rhs),
        Condition::Gt(lhs, rhs) => match (eval_expr(node, lhs), eval_expr(node, rhs)) {
            (Some(Value::Int(a)), Some(Value::Int(b))) => a > b,
            _ => false,
        },
        Condition::Lt(lhs, rhs) => match (eval_expr(node, lhs), eval_expr(node, rhs)) {
            (Some(Value::Int(a)), Some(Value::Int(b))) => a < b,
            _ => false,
        },
        Condition::And(a, b) => eval_condition(node, a) && eval_condition(node, b),
        Condition::Or(a, b) => eval_condition(node, a) || eval_condition(node, b),
    }
}

fn eval_expr(node: &Node, expr: &Expression) -> Option<Value> {
    match expr {
        Expression::Property(_var, prop) => {
            node.props.get(prop).cloned()
        }
        Expression::Literal(pv) => match pv {
            PropertyValue::String(s) => Some(Value::Text(s.clone())),
            PropertyValue::Int(i) => Some(Value::Int(*i)),
            PropertyValue::Variable(_) => None,
        },
    }
}
