use crate::graph::db::GraphDatabase;
use crate::graph::model::Node;
use crate::query::Query;
use crate::storage::{NodeId, RelId, StorageEngine};
use crate::values::{Properties, Value};

use super::ast::*;
use regex::Regex;

/// 执行 Cypher 语句，支持：
/// - 读查询：MATCH / WHERE / RETURN（带 ORDER BY / SKIP / LIMIT）
/// - 写操作：CREATE / DELETE / SET
pub enum CypherResult {
    Nodes(Vec<Node>),
    Created { nodes: Vec<NodeId>, rels: usize },
    Deleted { nodes: usize, rels: usize },
    Updated { nodes: usize },
    TransactionStarted,
    TransactionCommitted,
    TransactionRolledBack,
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
        CypherStatement::Merge(m) => {
            let result = execute_merge(db, m)?;
            Ok(result)
        }
        CypherStatement::Foreach(f) => {
            let nodes_updated = execute_foreach(db, f)?;
            Ok(CypherResult::Updated {
                nodes: nodes_updated,
            })
        }
        CypherStatement::Call(c) => {
            let nodes = execute_call(db, c)?;
            Ok(CypherResult::Nodes(nodes))
        }
        CypherStatement::Union(u) => {
            let nodes = execute_union(db, u)?;
            Ok(CypherResult::Nodes(nodes))
        }
        CypherStatement::BeginTransaction => {
            execute_begin_transaction(db)?;
            Ok(CypherResult::TransactionStarted)
        }
        CypherStatement::CommitTransaction => {
            execute_commit_transaction(db)?;
            Ok(CypherResult::TransactionCommitted)
        }
        CypherStatement::RollbackTransaction => {
            execute_rollback_transaction(db)?;
            Ok(CypherResult::TransactionRolledBack)
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

    // 2. 应用 WITH 子句（投影和过滤）
    if let Some(with_clause) = &query.with_clause {
        // WITH 的 WHERE 过滤
        if let Some(where_clause) = &with_clause.where_clause {
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
        // TODO: 支持 WITH 的投影（提取特定变量/属性）
        // 当前简化实现：WITH 只是传递所有变量，不做投影
    }

    // 3. 应用查询的 WHERE 子句（在内存中过滤）
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

    // 4. 检查是否有聚合或 GROUP BY
    let has_aggregation = query.return_clause.items.iter().any(|item| {
        matches!(item, ReturnItem::Aggregation(_, _, _)
                 | ReturnItem::AggregationAs(_, _, _, _)
                 | ReturnItem::AggregationWithParam(_, _, _, _)
                 | ReturnItem::AggregationWithParamAs(_, _, _, _, _)
                 | ReturnItem::Count)
    });

    if has_aggregation || query.return_clause.group_by.is_some() {
        // 使用聚合执行路径
        return execute_aggregation_query(db, &q, &query.return_clause);
    }

    // 5. 根据 RETURN 子句应用 ORDER BY / SKIP / LIMIT（非聚合路径）
    if let Some(order) = &query.return_clause.order_by {
        // 多字段排序：从后往前应用，因为后面的排序会覆盖前面的
        for item in order.items.iter().rev() {
            q = q.order_by(&item.prop, item.ascending);
        }
    }
    if let Some(skip) = query.return_clause.skip {
        q = q.skip(skip);
    }
    if let Some(limit) = query.return_clause.limit {
        q = q.limit(limit);
    }

    Ok(q.collect_nodes())
}

/// 执行包含聚合函数和 GROUP BY 的查询
/// 返回包含聚合结果的虚拟节点
fn execute_aggregation_query<E: StorageEngine>(
    db: &GraphDatabase<E>,
    query: &Query<E>,
    return_clause: &ReturnClause,
) -> Result<Vec<Node>, String> {
    use std::collections::HashMap;

    // 收集所有节点（使用借用版本）
    let nodes = query.collect_nodes_ref();

    // 如果没有节点，返回空结果
    if nodes.is_empty() {
        return Ok(Vec::new());
    }

    // 如果有 GROUP BY，先进行分组
    let groups: HashMap<String, Vec<Node>> = if let Some(group_by) = &return_clause.group_by {
        // 按 GROUP BY 字段分组
        let mut groups: HashMap<String, Vec<Node>> = HashMap::new();
        for node in nodes {
            let key = extract_group_key(&node, group_by);
            groups.entry(key).or_default().push(node);
        }
        groups
    } else {
        // 没有 GROUP BY，所有数据作为一组
        let mut all_groups = HashMap::new();
        all_groups.insert("all".to_string(), nodes);
        all_groups
    };

    // 对每个分组应用聚合函数
    let mut result_nodes = Vec::new();
    for (group_key, group_nodes) in groups {
        let mut props = Properties::new();

        // 处理每个 RETURN 项
        for item in &return_clause.items {
            match item {
                ReturnItem::Aggregation(func, var, prop) => {
                    let value = compute_aggregation(func, &group_nodes, var, prop)?;
                    let prop_name = if prop.is_empty() {
                        format!("{}({})", func_str(func), var)
                    } else {
                        format!("{}({}.{})", func_str(func), var, prop)
                    };
                    props.insert(prop_name, value);
                }
                ReturnItem::AggregationAs(func, var, prop, alias) => {
                    let value = compute_aggregation(func, &group_nodes, var, prop)?;
                    props.insert(alias.clone(), value);
                }
                ReturnItem::AggregationWithParam(func, var, prop, param) => {
                    let value = compute_aggregation_with_param(func, &group_nodes, var, prop, *param)?;
                    let prop_name = format!("{}({}.{}, {})", func_str(func), var, prop, param);
                    props.insert(prop_name, value);
                }
                ReturnItem::AggregationWithParamAs(func, var, prop, param, alias) => {
                    let value = compute_aggregation_with_param(func, &group_nodes, var, prop, *param)?;
                    props.insert(alias.clone(), value);
                }
                ReturnItem::Count => {
                    props.insert("count".to_string(), Value::Int(group_nodes.len() as i64));
                }
                ReturnItem::Variable(var) => {
                    // 对于 GROUP BY 查询，返回分组字段的值
                    if let Some(group_by) = &return_clause.group_by {
                        if group_by.len() == 1 && (group_by[0] == *var || group_by[0].starts_with(&format!("{}.", var))) {
                            // 从 group_key 中提取值（简化处理）
                            if let Some(first_node) = group_nodes.first() {
                                if let Some(val) = extract_property_value(first_node, var) {
                                    props.insert(var.clone(), val);
                                }
                            }
                        }
                    }
                }
                ReturnItem::VariableAs(_, alias) => {
                    // 对于 GROUP BY 查询，返回分组字段的值（使用别名）
                    if let Some(group_by) = &return_clause.group_by {
                        if group_by.len() == 1 && (group_by[0] == *alias || group_by[0].starts_with(&format!("{}.", alias))) {
                            // 从 group_key 中提取值（简化处理）
                            if let Some(first_node) = group_nodes.first() {
                                if let Some(val) = extract_property_value(first_node, alias) {
                                    props.insert(alias.clone(), val);
                                }
                            }
                        }
                    }
                }
                ReturnItem::Property(var, prop) => {
                    // 返回分组中第一个节点的属性值
                    if let Some(first_node) = group_nodes.first() {
                        if let Some(val) = first_node.get(prop) {
                            props.insert(format!("{}.{}", var, prop), val.clone());
                        }
                    }
                }
                ReturnItem::PropertyAs(var, prop, alias) => {
                    // 返回分组中第一个节点的属性值（使用别名）
                    if let Some(first_node) = group_nodes.first() {
                        if let Some(val) = first_node.get(prop) {
                            props.insert(alias.clone(), val.clone());
                        }
                    }
                }
                _ => {
                    // 其他类型暂时忽略
                }
            }
        }

        // 创建虚拟节点来承载聚合结果（使用一个特殊的 ID）
        result_nodes.push(Node {
            id: u64::MAX, // 使用最大值作为虚拟节点的 ID
            labels: vec!["Aggregation".to_string()],
            props,
        });
    }

    // 应用 ORDER BY
    if let Some(order) = &return_clause.order_by {
        // 简化实现：只支持单字段排序
        if let Some(item) = order.items.first() {
            // 构建排序键：对于 COUNT(*)，使用 "count"；对于其他，使用 var.prop
            let sort_key = if item.var == "count" && item.prop == "*" {
                "count".to_string()
            } else if item.prop.is_empty() || item.prop == "*" {
                item.var.clone()
            } else {
                format!("{}.{}", item.var, item.prop)
            };

            result_nodes.sort_by(|a, b| {
                let a_val = a.get(&sort_key);
                let b_val = b.get(&sort_key);
                match (a_val, b_val) {
                    (Some(Value::Int(a)), Some(Value::Int(b))) => {
                        if item.ascending { a.cmp(b) } else { b.cmp(a) }
                    }
                    (Some(Value::Text(a)), Some(Value::Text(b))) => {
                        if item.ascending { a.cmp(b) } else { b.cmp(a) }
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            });
        }
    }

    // 应用 SKIP 和 LIMIT
    let skip = return_clause.skip.unwrap_or(0);
    let limit = return_clause.limit.unwrap_or(result_nodes.len());

    let result_nodes: Vec<Node> = result_nodes
        .into_iter()
        .skip(skip)
        .take(limit)
        .collect();

    Ok(result_nodes)
}

/// 从节点中提取分组键值
fn extract_group_key(node: &Node, group_by: &[String]) -> String {
    let keys: Vec<String> = group_by.iter().map(|key| {
        if key.contains('.') {
            let parts: Vec<&str> = key.splitn(2, '.').collect();
            let var = parts[0];
            let prop = parts[1];
            if let Some(val) = node.get(prop) {
                format_value(val)
            } else {
                "NULL".to_string()
            }
        } else {
            if let Some(val) = extract_property_value(node, key) {
                format_value(&val)
            } else {
                "NULL".to_string()
            }
        }
    }).collect();
    keys.join("|")
}

/// 从节点中提取属性值
fn extract_property_value(node: &Node, key: &str) -> Option<Value> {
    // 先尝试直接作为属性名
    if let Some(val) = node.get(key) {
        return Some(val.clone());
    }
    // 尝试解析为 var.prop 格式
    if let Some(dot_pos) = key.find('.') {
        let prop = &key[dot_pos + 1..];
        if let Some(val) = node.get(prop) {
            return Some(val.clone());
        }
    }
    None
}

/// 格式化值为字符串（用于分组键）
fn format_value(val: &Value) -> String {
    match val {
        Value::Text(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
        Value::List(_) => "LIST".to_string(),
    }
}

/// 计算聚合函数的值
fn compute_aggregation(
    func: &AggFunc,
    nodes: &[Node],
    var: &str,
    prop: &str,
) -> Result<Value, String> {
    if nodes.is_empty() {
        return Ok(Value::Null);
    }

    // 收集所有属性值
    let mut values: Vec<Value> = Vec::new();
    for node in nodes {
        let prop_to_use = if prop.is_empty() { var } else { prop };
        if let Some(val) = node.get(prop_to_use) {
            values.push(val.clone());
        }
    }

    match func {
        AggFunc::Count => {
            Ok(Value::Int(values.len() as i64))
        }
        AggFunc::Sum => {
            let mut sum: i64 = 0;
            for val in &values {
                match val {
                    Value::Int(i) => sum += i,
                    _ => return Err("SUM can only be applied to integer values".to_string()),
                }
            }
            Ok(Value::Int(sum))
        }
        AggFunc::Avg => {
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mut sum: i64 = 0;
            for val in &values {
                match val {
                    Value::Int(i) => sum += i,
                    _ => return Err("AVG can only be applied to integer values".to_string()),
                }
            }
            Ok(Value::Float(sum as f64 / values.len() as f64))
        }
        AggFunc::Min => {
            let mut min_val: Option<Value> = None;
            for val in &values {
                match val {
                    Value::Int(i) => {
                        if min_val.is_none() {
                            min_val = Some(Value::Int(*i));
                        } else if let Some(Value::Int(min_i)) = &min_val {
                            if i < min_i {
                                min_val = Some(Value::Int(*i));
                            }
                        }
                    }
                    Value::Text(s) => {
                        if min_val.is_none() {
                            min_val = Some(Value::Text(s.clone()));
                        } else if let Some(Value::Text(min_s)) = &min_val {
                            if s < min_s {
                                min_val = Some(Value::Text(s.clone()));
                            }
                        }
                    }
                    _ => continue,
                }
            }
            Ok(min_val.unwrap_or(Value::Null))
        }
        AggFunc::Max => {
            let mut max_val: Option<Value> = None;
            for val in &values {
                match val {
                    Value::Int(i) => {
                        if max_val.is_none() {
                            max_val = Some(Value::Int(*i));
                        } else if let Some(Value::Int(max_i)) = &max_val {
                            if i > max_i {
                                max_val = Some(Value::Int(*i));
                            }
                        }
                    }
                    Value::Text(s) => {
                        if max_val.is_none() {
                            max_val = Some(Value::Text(s.clone()));
                        } else if let Some(Value::Text(max_s)) = &max_val {
                            if s > max_s {
                                max_val = Some(Value::Text(s.clone()));
                            }
                        }
                    }
                    _ => continue,
                }
            }
            Ok(max_val.unwrap_or(Value::Null))
        }
        AggFunc::Collect => {
            // 收集所有值到一个列表
            Ok(Value::List(values))
        }
        AggFunc::StDev => {
            // 标准差计算（总体标准差，使用 n）
            // 少于 2 个值时返回 NULL
            if values.len() < 2 {
                return Ok(Value::Null);
            }

            let mut nums: Vec<f64> = Vec::new();
            for val in &values {
                match val {
                    Value::Int(i) => nums.push(*i as f64),
                    Value::Float(f) => nums.push(*f),
                    _ => return Err("STDEV can only be applied to numeric values".to_string()),
                }
            }

            let n = nums.len() as f64;
            let mean: f64 = nums.iter().sum::<f64>() / n;
            let variance: f64 = nums.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / n;
            Ok(Value::Float(variance.sqrt()))
        }
        AggFunc::PercentileCont | AggFunc::PercentileDisc => {
            // 百分位数计算（默认使用中位数 0.5）
            // 实际参数从外部传入，这里使用默认值
            compute_percentile(&values, 0.5, matches!(func, AggFunc::PercentileCont))
        }
    }
}

/// 获取聚合函数的字符串表示
fn func_str(func: &AggFunc) -> &str {
    match func {
        AggFunc::Sum => "sum",
        AggFunc::Avg => "avg",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
        AggFunc::Count => "count",
        AggFunc::Collect => "collect",
        AggFunc::StDev => "stdev",
        AggFunc::PercentileCont => "percentileCont",
        AggFunc::PercentileDisc => "percentileDisc",
    }
}

/// 计算百分位数
///
/// # 参数
/// - `values`: 值列表
/// - `percentile`: 百分位数 (0.0 到 1.0)
/// - `is_continuous`: 是否使用连续百分位数算法
fn compute_percentile(values: &[Value], percentile: f64, is_continuous: bool) -> Result<Value, String> {
    if values.is_empty() {
        return Ok(Value::Null);
    }

    if !(0.0..=1.0).contains(&percentile) {
        return Err("Percentile must be between 0 and 1".to_string());
    }

    // 提取数值
    let mut nums: Vec<f64> = Vec::new();
    for val in values {
        match val {
            Value::Int(i) => nums.push(*i as f64),
            Value::Float(f) => nums.push(*f),
            _ => return Err("Percentile can only be applied to numeric values".to_string()),
        }
    }

    if nums.is_empty() {
        return Ok(Value::Null);
    }

    // 排序
    nums.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let n = nums.len();

    if is_continuous {
        // 连续百分位数 (PERCENTILECONT)
        // 使用线性插值
        let idx = (n - 1) as f64 * percentile;
        let lower = idx.floor() as usize;
        let upper = idx.ceil() as usize;

        if lower == upper {
            return Ok(Value::Float(nums[lower]));
        }

        let weight = idx - lower as f64;
        let result = nums[lower] * (1.0 - weight) + nums[upper] * weight;
        Ok(Value::Float(result))
    } else {
        // 离散百分位数 (PERCENTILEDISC)
        // 使用 floor 获取最近的实际值
        let idx = ((n as f64 - 1.0) * percentile).floor() as usize;
        Ok(Value::Float(nums[idx]))
    }
}

/// 计算带参数的聚合函数
fn compute_aggregation_with_param(
    func: &AggFunc,
    nodes: &[Node],
    var: &str,
    prop: &str,
    param: f64,
) -> Result<Value, String> {
    // 收集所有属性值
    let mut values: Vec<Value> = Vec::new();
    for node in nodes {
        let prop_to_use = if prop.is_empty() { var } else { prop };
        if let Some(val) = node.get(prop_to_use) {
            values.push(val.clone());
        }
    }

    match func {
        AggFunc::PercentileCont => {
            compute_percentile(&values, param, true)
        }
        AggFunc::PercentileDisc => {
            compute_percentile(&values, param, false)
        }
        _ => Err(format!("Function {:?} does not support parameters", func)),
    }
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

        // 使用 update_node_props 更新节点
        if db.update_node_props(node.id, new_props) {
            nodes_updated += 1;
        }
    }

    Ok(nodes_updated)
}

/// 执行 MERGE 语句（优化版）
fn execute_merge<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    merge_stmt: &MergeStatement,
) -> Result<CypherResult, String> {
    // MERGE 的逻辑：
    // 1. 尝试匹配完整模式（节点+关系）
    // 2. 如果匹配成功，执行 ON MATCH SET
    // 3. 如果匹配失败，创建模式，执行 ON CREATE SET

    // 检查是否有关系
    let has_relationships = !merge_stmt.pattern.relationships.is_empty();

    if has_relationships {
        // 关系 MERGE
        execute_merge_with_relationships(db, merge_stmt)
    } else {
        // 单节点 MERGE（优化版）
        execute_merge_node(db, merge_stmt)
    }
}

/// 单节点 MERGE（性能优化）
fn execute_merge_node<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    merge_stmt: &MergeStatement,
) -> Result<CypherResult, String> {
    let node_pattern = &merge_stmt.pattern.start_node;

    // 性能优化：使用 find_matching_nodes_optimized
    let exact_matches = find_matching_nodes_optimized(db, node_pattern)?;

    if !exact_matches.is_empty() {
        // 匹配成功：执行 ON MATCH SET
        if let Some(assignments) = &merge_stmt.on_match {
            let mut nodes_updated = 0;

            for node in exact_matches {
                let mut new_props = node.props.clone();

                for assignment in assignments {
                    match &assignment.value {
                        PropertyValue::String(s) => {
                            new_props.insert(assignment.prop.clone(), Value::Text(s.clone()));
                        }
                        PropertyValue::Int(i) => {
                            new_props.insert(assignment.prop.clone(), Value::Int(*i));
                        }
                        PropertyValue::Variable(_) => {
                            return Err("MERGE ON MATCH with variable references not yet supported".to_string());
                        }
                    }
                }

                if db.update_node_props(node.id, new_props) {
                    nodes_updated += 1;
                }
            }

            return Ok(CypherResult::Updated { nodes: nodes_updated });
        }

        // 没有 ON MATCH SET，返回匹配的节点
        return Ok(CypherResult::Nodes(exact_matches));
    } else {
        // 匹配失败：创建节点
        let mut props = Properties::new();

        for (key, value) in &node_pattern.props {
            match value {
                PropertyValue::String(s) => {
                    props.insert(key.clone(), Value::Text(s.clone()));
                }
                PropertyValue::Int(i) => {
                    props.insert(key.clone(), Value::Int(*i));
                }
                PropertyValue::Variable(_) => {
                    return Err("MERGE CREATE with variable references not yet supported".to_string());
                }
            }
        }

        let labels: Vec<&str> = if let Some(label) = &node_pattern.label {
            vec![label.as_str()]
        } else {
            Vec::new()
        };

        let node_id = db.create_node(labels, props);
        let node = db.get_node(node_id).ok_or("Failed to retrieve created node")?;

        // 执行 ON CREATE SET
        if let Some(assignments) = &merge_stmt.on_create {
            let mut new_props = node.props.clone();

            for assignment in assignments {
                match &assignment.value {
                    PropertyValue::String(s) => {
                        new_props.insert(assignment.prop.clone(), Value::Text(s.clone()));
                    }
                    PropertyValue::Int(i) => {
                        new_props.insert(assignment.prop.clone(), Value::Int(*i));
                    }
                    PropertyValue::Variable(_) => {
                        return Err("MERGE ON CREATE with variable references not yet supported".to_string());
                    }
                }
            }

            db.update_node_props(node_id, new_props);
        }

        // 返回创建的节点
        return Ok(CypherResult::Created {
            nodes: vec![node_id],
            rels: 0,
        });
    }
}

/// 关系 MERGE
fn execute_merge_with_relationships<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    merge_stmt: &MergeStatement,
) -> Result<CypherResult, String> {
    let pattern = &merge_stmt.pattern;

    // 首先匹配起始节点
    let start_matches = find_matching_nodes_optimized(db, &pattern.start_node)?;

    // 如果只有一个关系（MERGE (a)-[r]->(b)）
    if pattern.relationships.len() == 1 {
        let (rel_pattern, end_node_pattern) = &pattern.relationships[0];

        // 尝试找到匹配的完整路径
        let mut matched_paths: Vec<(NodeId, NodeId, RelId)> = Vec::new();

        for start_node in &start_matches {
            // 获取该节点的所有出边（或入边/无向，取决于方向）
            let rels: Vec<RelId> = match rel_pattern.direction {
                Direction::Outgoing => db.engine.outgoing_rels(start_node.id).map(|r| r.id).collect(),
                Direction::Incoming => db.engine.incoming_rels(start_node.id).map(|r| r.id).collect(),
                Direction::Both => {
                    // 无向：同时获取出边和入边
                    let mut rels: Vec<RelId> = db.engine.outgoing_rels(start_node.id).map(|r| r.id).collect();
                    rels.extend(db.engine.incoming_rels(start_node.id).map(|r| r.id));
                    rels
                }
            };

            for rel_id in rels {
                if let Some(rel) = db.get_rel(rel_id) {
                    // 检查方向和类型
                    let direction_match = match rel_pattern.direction {
                        Direction::Outgoing => rel.start == start_node.id,
                        Direction::Incoming => rel.end == start_node.id,
                        Direction::Both => true, // 无向，都匹配
                    };

                    if !direction_match {
                        continue;
                    }

                    // 检查关系类型
                    if let Some(ref rel_type) = rel_pattern.rel_type {
                        if rel.typ != *rel_type {
                            continue;
                        }
                    }

                    // 获取目标节点
                    let target_id = if rel.start == start_node.id { rel.end } else { rel.start };
                    if let Some(target_node) = db.get_node(target_id) {
                        // 检查目标节点是否匹配模式
                        if node_pattern_matches(&target_node, end_node_pattern) {
                            matched_paths.push((start_node.id, target_node.id, rel_id));
                        }
                    }
                }
            }
        }

        if !matched_paths.is_empty() {
            // 找到匹配的路径：执行 ON MATCH SET
            if let Some(assignments) = &merge_stmt.on_match {
                // 对关系执行 SET（如果支持的话）
                let mut updated = 0;

                for (_, _, rel_id) in &matched_paths {
                    if let Some(rel) = db.get_rel(*rel_id) {
                        let mut new_props = std::collections::HashMap::new();

                        // 先保留原有属性
                        for (k, v) in &rel.props {
                            new_props.insert(k.clone(), v.clone());
                        }

                        // 添加/更新新属性
                        for assignment in assignments {
                            match &assignment.value {
                                PropertyValue::String(s) => {
                                    new_props.insert(assignment.prop.clone(), Value::Text(s.clone()));
                                }
                                PropertyValue::Int(i) => {
                                    new_props.insert(assignment.prop.clone(), Value::Int(*i));
                                }
                                PropertyValue::Variable(_) => {
                                    return Err("MERGE ON MATCH with variable references not yet supported".to_string());
                                }
                            }
                        }

                        if db.update_rel_props(*rel_id, new_props) {
                            updated += 1;
                        }
                    }
                }

                return Ok(CypherResult::Updated { nodes: updated });
            }

            return Ok(CypherResult::Nodes(vec![]));
        } else {
            // 没有找到匹配的路径：创建新节点和关系
            // 确保起始节点存在
            let start_id = if start_matches.is_empty() {
                // 创建起始节点
                create_node_from_pattern(db, &pattern.start_node)
            } else {
                start_matches[0].id
            };

            // 创建结束节点
            let end_id = create_node_from_pattern(db, end_node_pattern);

            // 创建关系
            let direction = rel_pattern.direction.clone();
            let rel_type = rel_pattern.rel_type.clone().unwrap_or("RELATED".to_string());

            let rel_id = match direction {
                Direction::Outgoing => db.create_rel(start_id, end_id, &rel_type, Properties::new()),
                Direction::Incoming => db.create_rel(end_id, start_id, &rel_type, Properties::new()),
                Direction::Both => db.create_rel(start_id, end_id, &rel_type, Properties::new()),
            };

            // 执行 ON CREATE SET
            if let Some(assignments) = &merge_stmt.on_create {
                if let Some(rel) = db.get_rel(rel_id) {
                    let mut new_props = std::collections::HashMap::new();

                    // 先保留原有属性
                    for (k, v) in &rel.props {
                        new_props.insert(k.clone(), v.clone());
                    }

                    // 添加/更新新属性
                    for assignment in assignments {
                        match &assignment.value {
                            PropertyValue::String(s) => {
                                new_props.insert(assignment.prop.clone(), Value::Text(s.clone()));
                            }
                            PropertyValue::Int(i) => {
                                new_props.insert(assignment.prop.clone(), Value::Int(*i));
                            }
                            PropertyValue::Variable(_) => {
                                return Err("MERGE ON CREATE with variable references not yet supported".to_string());
                            }
                        }
                    }

                    db.update_rel_props(rel_id, new_props);
                }
            }

            return Ok(CypherResult::Created {
                nodes: vec![start_id, end_id],
                rels: 1,
            });
        }
    } else {
        // 多关系 MERGE：MERGE (a)-[r1]->(b)-[r2]->(c)...
        return execute_merge_multiple_relationships(db, merge_stmt, &start_matches);
    }
}

/// 多关系 MERGE：MERGE (a)-[r1]->(b)-[r2]->(c)...
fn execute_merge_multiple_relationships<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    merge_stmt: &MergeStatement,
    start_matches: &[Node],
) -> Result<CypherResult, String> {
    let pattern = &merge_stmt.pattern;
    let relationships = &pattern.relationships;

    // 尝试找到现有的完整路径
    let mut matched_paths: Vec<Vec<NodeId>> = Vec::new();

    for start_node in start_matches {
        let mut current_path: Vec<NodeId> = vec![start_node.id];
        let mut current_id = start_node.id;
        let mut path_found = true;

        // 遍历每个关系，尝试找到完整路径
        for (rel_pattern, end_node_pattern) in relationships {
            // 查找从 current_id 出发的匹配关系
            let rels: Vec<RelId> = match rel_pattern.direction {
                Direction::Outgoing => db.engine.outgoing_rels(current_id).map(|r| r.id).collect(),
                Direction::Incoming => db.engine.incoming_rels(current_id).map(|r| r.id).collect(),
                Direction::Both => {
                    let mut rels: Vec<RelId> = db.engine.outgoing_rels(current_id).map(|r| r.id).collect();
                    rels.extend(db.engine.incoming_rels(current_id).map(|r| r.id));
                    rels
                }
            };

            let mut found_match = false;
            for rel_id in rels {
                if let Some(rel) = db.get_rel(rel_id) {
                    // 检查方向
                    let direction_match = match rel_pattern.direction {
                        Direction::Outgoing => rel.start == current_id,
                        Direction::Incoming => rel.end == current_id,
                        Direction::Both => true,
                    };

                    if !direction_match {
                        continue;
                    }

                    // 检查关系类型
                    if let Some(ref rel_type) = rel_pattern.rel_type {
                        if rel.typ != *rel_type {
                            continue;
                        }
                    }

                    // 获取目标节点
                    let target_id = if rel.start == current_id { rel.end } else { rel.start };
                    if let Some(target_node) = db.get_node(target_id) {
                        // 检查目标节点是否匹配
                        if node_pattern_matches(&target_node, end_node_pattern) {
                            current_path.push(target_id);
                            current_id = target_id;
                            found_match = true;
                            break;
                        }
                    }
                }
            }

            if !found_match {
                path_found = false;
                break;
            }
        }

        if path_found {
            matched_paths.push(current_path);
        }
    }

    // 如果找到了完整路径
    if !matched_paths.is_empty() {
        // 执行 ON MATCH SET（简化版：只更新第一个匹配路径的节点）
        if let Some(assignments) = &merge_stmt.on_match {
            let mut nodes_updated = 0;
            for path in &matched_paths {
                for node_id in path {
                    if let Some(node) = db.get_node(*node_id) {
                        let mut new_props = node.props.clone();
                        for assignment in assignments {
                            match &assignment.value {
                                PropertyValue::String(s) => {
                                    new_props.insert(assignment.prop.clone(), Value::Text(s.clone()));
                                }
                                PropertyValue::Int(i) => {
                                    new_props.insert(assignment.prop.clone(), Value::Int(*i));
                                }
                                PropertyValue::Variable(_) => {
                                    return Err("MERGE ON MATCH with variable references not yet supported".to_string());
                                }
                            }
                        }
                        if db.update_node_props(*node_id, new_props) {
                            nodes_updated += 1;
                        }
                    }
                }
            }
            return Ok(CypherResult::Updated { nodes: nodes_updated });
        }

        return Ok(CypherResult::Nodes(
            matched_paths[0].iter()
                .filter_map(|id| db.get_node(*id))
                .collect()
        ));
    }

    // 没有找到完整路径，创建新路径
    let mut created_nodes = Vec::new();
    let mut created_rels = 0;

    // 创建起始节点（如果不存在）
    let start_id = if !start_matches.is_empty() {
        start_matches[0].id
    } else {
        create_node_from_pattern(db, &pattern.start_node)
    };
    created_nodes.push(start_id);

    let mut current_id = start_id;

    // 依次创建每个关系和节点
    for (rel_pattern, end_node_pattern) in relationships {
        // 查找或创建目标节点
        let end_matches = find_matching_nodes_optimized(db, end_node_pattern)?;
        let end_id = if !end_matches.is_empty() {
            end_matches[0].id
        } else {
            create_node_from_pattern(db, end_node_pattern)
        };
        created_nodes.push(end_id);

        // 创建关系
        let direction = rel_pattern.direction.clone();
        let rel_type = rel_pattern.rel_type.clone().unwrap_or("RELATED".to_string());

        match direction {
            Direction::Outgoing => {
                db.create_rel(current_id, end_id, &rel_type, Properties::new());
            }
            Direction::Incoming => {
                db.create_rel(end_id, current_id, &rel_type, Properties::new());
            }
            Direction::Both => {
                db.create_rel(current_id, end_id, &rel_type, Properties::new());
            }
        }
        created_rels += 1;

        current_id = end_id;
    }

    // 执行 ON CREATE SET
    if let Some(assignments) = &merge_stmt.on_create {
        for node_id in &created_nodes {
            if let Some(node) = db.get_node(*node_id) {
                let mut new_props = node.props.clone();
                for assignment in assignments {
                    match &assignment.value {
                        PropertyValue::String(s) => {
                            new_props.insert(assignment.prop.clone(), Value::Text(s.clone()));
                        }
                        PropertyValue::Int(i) => {
                            new_props.insert(assignment.prop.clone(), Value::Int(*i));
                        }
                        PropertyValue::Variable(_) => {
                            return Err("MERGE ON CREATE with variable references not yet supported".to_string());
                        }
                    }
                }
                db.update_node_props(*node_id, new_props);
            }
        }
    }

    Ok(CypherResult::Created {
        nodes: created_nodes,
        rels: created_rels,
    })
}

/// 性能优化：快速查找匹配的节点
fn find_matching_nodes_optimized<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node_pattern: &NodePattern,
) -> Result<Vec<Node>, String> {
    use crate::values::Value;

    // 如果没有属性要求，直接按标签返回所有节点
    if node_pattern.props.is_empty() {
        if let Some(label) = &node_pattern.label {
            let mut result = Vec::new();
            for stored_node in db.all_stored_nodes() {
                if let Some(node) = db.get_node(stored_node.id) {
                    if node.labels.contains(label) {
                        result.push(node);
                    }
                }
            }
            return Ok(result);
        } else {
            // 没有标签也没有属性，返回所有节点
            let mut result = Vec::new();
            for stored_node in db.all_stored_nodes() {
                if let Some(node) = db.get_node(stored_node.id) {
                    result.push(node);
                }
            }
            return Ok(result);
        }
    }

    // 有属性要求：优先使用索引查找
    // 找到第一个可索引的属性
    let mut first_indexed_prop: Option<(&String, Value)> = None;
    for (key, value) in &node_pattern.props {
        match value {
            PropertyValue::String(s) => {
                first_indexed_prop = Some((key, Value::Text(s.clone())));
                break;
            }
            PropertyValue::Int(i) => {
                first_indexed_prop = Some((key, Value::Int(*i)));
                break;
            }
            PropertyValue::Variable(_) => {
                // 变量引用无法使用索引
            }
        }
    }

    // 如果有可索引的属性且有标签，检查是否被索引，如果是则使用索引查找
    if let (Some(label), Some((prop_name, prop_value))) = (&node_pattern.label, first_indexed_prop) {
        // 检查该属性是否被索引
        if db.schema.should_index(label, prop_name) {
            // 使用索引快速查找
            let node_ids = db.index.find(label, prop_name, &prop_value);

            if !node_ids.is_empty() {
                // 从索引结果中精确匹配
                let mut exact_matches: Vec<Node> = Vec::new();
                for node_id in node_ids {
                    if let Some(node) = db.get_node(node_id) {
                        if node_pattern_matches(&node, node_pattern) {
                            exact_matches.push(node);
                        }
                    }
                }
                return Ok(exact_matches);
            } else {
                // 索引中没有找到，直接返回空
                return Ok(Vec::new());
            }
        }
        // 如果没有被索引，继续使用下面的全扫描逻辑
    }

    // 没有可用的索引，回退到全扫描
    let mut candidates: Vec<Node> = Vec::new();

    // 第一阶段：按标签筛选
    if let Some(label) = &node_pattern.label {
        for stored_node in db.all_stored_nodes() {
            if let Some(node) = db.get_node(stored_node.id) {
                if node.labels.contains(label) {
                    candidates.push(node);
                }
            }
        }
    } else {
        // 没有标签，所有节点都是候选
        for stored_node in db.all_stored_nodes() {
            if let Some(node) = db.get_node(stored_node.id) {
                candidates.push(node);
            }
        }
    }

    // 第二阶段：按属性精确匹配
    let mut exact_matches: Vec<Node> = Vec::new();
    for node in candidates {
        if node_pattern_matches(&node, node_pattern) {
            exact_matches.push(node);
        }
    }

    Ok(exact_matches)
}

/// 检查节点是否匹配模式
fn node_pattern_matches(node: &Node, pattern: &NodePattern) -> bool {
    // 检查标签
    if let Some(ref label) = pattern.label {
        if !node.labels.contains(label) {
            return false;
        }
    }

    // 检查所有属性
    for (key, value) in &pattern.props {
        match node.props.get(key) {
            Some(node_value) => {
                if !property_value_equals_value(value, node_value) {
                    return false;
                }
            }
            None => {
                return false;
            }
        }
    }

    true
}

/// 辅助函数：比较 PropertyValue 和 Value
fn property_value_equals_value(prop_value: &PropertyValue, value: &Value) -> bool {
    match (prop_value, value) {
        (PropertyValue::String(s), Value::Text(t)) => s == t,
        (PropertyValue::Int(i), Value::Int(n)) => i == n,
        _ => false,
    }
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
            // 首先按 label 选择节点
            q = q.from_label(label);

            // 然后应用属性过滤（使用 where_prop_eq）
            for (prop_name, prop_val) in &start.props {
                match prop_val {
                    PropertyValue::String(s) => {
                        q = q.where_prop_eq(prop_name, s);
                    }
                    PropertyValue::Int(i) => {
                        // 对于整型，需要特殊处理
                        // 暂时使用 from_label_and_prop_int_eq（如果有索引）
                        // 否则跳过
                        use crate::values::Value;
                        let ids = db.index.find(label, prop_name, &Value::Int(*i));
                        if !ids.is_empty() {
                            // 如果索引中有结果，使用索引结果
                            q = Query::new(db);
                            q.current = ids;
                        } else {
                            // 没有索引，手动过滤
                            q = q.where_prop_int_eq(prop_name, *i);
                        }
                    }
                    PropertyValue::Variable(_) => {
                        // 变量在 WHERE 中处理
                    }
                }
            }
        }

        // 处理关系遍历
        for (rel, _node) in &pattern.relationships {
            // 检查是否是可变长度路径
            if let Some((min_hops, max_hops)) = &rel.var_length {
                let rel_type = rel.rel_type.as_deref().unwrap_or("");

                match rel.direction {
                    Direction::Outgoing => {
                        // 默认最小值为 1
                        let min = min_hops.unwrap_or(1);
                        q = q.out_variable_length(rel_type, min, *max_hops);
                    }
                    Direction::Incoming => {
                        let min = min_hops.unwrap_or(1);
                        q = q.in_variable_length(rel_type, min, *max_hops);
                    }
                    Direction::Both => {
                        let min = min_hops.unwrap_or(1);
                        q = q.undirected_variable_length(rel_type, min, *max_hops);
                    }
                }
            } else {
                // 固定长度路径（1 跳）
                if let Some(rel_type) = &rel.rel_type {
                    match rel.direction {
                        Direction::Outgoing => {
                            q = q.out(rel_type);
                        }
                        Direction::Incoming => {
                            q = q.in_(rel_type);
                        }
                        Direction::Both => {
                            // 使用可变长度路径，范围 1-1
                            q = q.undirected_variable_length(rel_type, 1, Some(1));
                        }
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
        Condition::Gte(lhs, rhs) => match (eval_expr(node, lhs), eval_expr(node, rhs)) {
            (Some(Value::Int(a)), Some(Value::Int(b))) => a >= b,
            _ => false,
        },
        Condition::Lte(lhs, rhs) => match (eval_expr(node, lhs), eval_expr(node, rhs)) {
            (Some(Value::Int(a)), Some(Value::Int(b))) => a <= b,
            _ => false,
        },
        Condition::Ne(lhs, rhs) => eval_expr(node, lhs) != eval_expr(node, rhs),
        Condition::And(a, b) => eval_condition(node, a) && eval_condition(node, b),
        Condition::Or(a, b) => eval_condition(node, a) || eval_condition(node, b),
        Condition::RegexMatch(expr, pattern) => {
            if let Some(Value::Text(s)) = eval_expr(node, expr) {
                match Regex::new(pattern) {
                    Ok(re) => re.is_match(&s),
                    Err(_) => false,
                }
            } else {
                false
            }
        }
        Condition::Exists(_var, prop) => {
            // 检查属性是否存在
            node.props.contains_key(prop)
        }
        Condition::IsNull(expr) => {
            eval_expr(node, expr).is_none()
        }
        Condition::IsNotNull(expr) => {
            eval_expr(node, expr).is_some()
        }
        Condition::In(expr, list) => {
            let val = eval_expr(node, expr);
            if val.is_none() {
                return false;
            }
            let val = val.unwrap();
            for item in list {
                if eval_expr_for_value(item) == Some(val.clone()) {
                    return true;
                }
            }
            false
        }
    }
}

fn eval_expr_for_value(expr: &Expression) -> Option<Value> {
    match expr {
        Expression::Literal(pv) => match pv {
            PropertyValue::String(s) => Some(Value::Text(s.clone())),
            PropertyValue::Int(i) => Some(Value::Int(*i)),
            PropertyValue::Variable(_) => None,
        },
        _ => None,
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
        Expression::List(_) => None, // 列表字面量不直接求值为单一值
    }
}

/// 执行 FOREACH 语句
/// FOREACH 遍历列表并对每个元素执行更新操作
fn execute_foreach<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    foreach_stmt: &ForeachStatement,
) -> Result<usize, String> {
    use std::collections::HashMap;

    // 1. 评估列表表达式
    let items = match &foreach_stmt.list_expr {
        Expression::List(exprs) => {
            // 列表字面量：转换为 NodeId 列表
            let mut ids = Vec::new();
            for expr in exprs {
                match expr {
                    Expression::Literal(PropertyValue::Int(i)) => {
                        ids.push(NodeId::from(*i as u64));
                    }
                    _ => return Err("FOREACH list literals only support integers".to_string()),
                }
            }
            ids
        }
        Expression::Property(var, _prop) => {
            // 变量引用：从数据库中查找节点
            // 简化实现：假设 var 是节点 ID
            // TODO: 支持更复杂的表达式
            return Err("FOREACH with variable references not yet fully supported".to_string());
        }
        _ => {
            return Err("FOREACH requires a list expression".to_string());
        }
    };

    // 2. 对每个元素执行更新操作
    let mut total_updated = 0;

    for node_id in items {
        // 对每个更新操作
        for assignment in &foreach_stmt.updates {
            // 构建属性 HashMap
            let mut new_props = HashMap::new();
            match &assignment.value {
                PropertyValue::String(s) => {
                    new_props.insert(assignment.prop.clone(), Value::Text(s.clone()));
                }
                PropertyValue::Int(i) => {
                    new_props.insert(assignment.prop.clone(), Value::Int(*i));
                }
                PropertyValue::Variable(_) => {
                    return Err("FOREACH with variable values not yet supported".to_string());
                }
            }

            // 更新节点属性
            if db.update_node_props(node_id, new_props) {
                total_updated += 1;
            }
        }
    }

    Ok(total_updated)
}

/// 执行 CALL 子查询
/// CALL { <subquery> } RETURN ...
fn execute_call<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    call_stmt: &CallStatement,
) -> Result<Vec<Node>, String> {
    // 1. 执行内层子查询
    let inner_result = execute_query(db, &call_stmt.inner_query)?;

    // 2. 当前简化实现：直接返回外层查询的结果
    // 实际上 CALL 子查询应该将内层结果传递给外层查询
    // 但由于我们的简化架构，我们暂时只执行内层查询

    // 3. 执行外层查询（目前 outer_query 只包含 RETURN 子句）
    // 如果 outer_query 有其他子句，可以执行它
    let result = if call_stmt.outer_query.match_clause.is_some()
        || call_stmt.outer_query.where_clause.is_some()
    {
        execute_query(db, &call_stmt.outer_query)?
    } else {
        // 如果外层只是 RETURN，返回内层查询的结果
        inner_result
    };

    Ok(result)
}

/// UNION ALL 执行：合并两个查询的结果
fn execute_union<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
    union_stmt: &UnionStatement,
) -> Result<Vec<Node>, String> {
    // 执行左侧查询
    let left_result = execute_query(db, &union_stmt.left)?;

    // 执行右侧查询
    let right_result = execute_query(db, &union_stmt.right)?;

    // 合并结果
    if union_stmt.all {
        // UNION ALL：保留所有结果（包括重复）
        let mut result = left_result;
        result.extend(right_result);
        Ok(result)
    } else {
        // UNION：去重
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for node in left_result.into_iter().chain(right_result.into_iter()) {
            if seen.insert(node.id) {
                result.push(node);
            }
        }

        Ok(result)
    }
}

/// 执行 BEGIN TRANSACTION 语句
fn execute_begin_transaction<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
) -> Result<(), String> {
    // 开始一个新事务
    let _tx = db.transactions.begin_transaction();
    Ok(())
}

/// 执行 COMMIT 语句
fn execute_commit_transaction<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
) -> Result<(), String> {
    // 提交最近的事务
    // 注意：这是一个简化实现，实际应用中应该跟踪当前活动的事务
    let tx_ids = db.transactions.active_transaction_ids();
    if let Some(tx_id) = tx_ids.last() {
        db.transactions.commit(*tx_id)
            .map_err(|e| format!("Commit failed: {}", e))?;
    } else {
        return Err("No active transaction to commit".to_string());
    }
    Ok(())
}

/// 执行 ROLLBACK 语句
fn execute_rollback_transaction<E: StorageEngine>(
    db: &mut GraphDatabase<E>,
) -> Result<(), String> {
    // 回滚最近的事务
    let tx_ids = db.transactions.active_transaction_ids();
    if let Some(tx_id) = tx_ids.last() {
        db.transactions.rollback(*tx_id)
            .map_err(|e| format!("Rollback failed: {}", e))?;
    } else {
        return Err("No active transaction to rollback".to_string());
    }
    Ok(())
}
