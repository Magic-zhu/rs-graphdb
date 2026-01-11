use crate::graph::db::GraphDatabase;
use crate::graph::model::Node;
use crate::storage::{NodeId, StorageEngine};
use crate::values::Value;

#[cfg(feature = "caching")]
use crate::cache::query_cache::{QueryCache, QueryFingerprint, QueryType};

/// 一个非常简化的查询 API：
/// - from_label：按标签选起点
/// - where_prop_eq / where_prop_int_gt：属性过滤
/// - out：沿指定关系走一层（可多次调用）
/// - distinct：ID 去重
pub struct Query<'a, E: StorageEngine> {
    db: &'a GraphDatabase<E>,
    pub(crate) current: Vec<NodeId>,
    #[cfg(feature = "caching")]
    fingerprint: Option<QueryFingerprint>,
}

impl<'a, E: StorageEngine> Query<'a, E> {
    /// 从所有节点开始（内部会过滤）
    pub fn new(db: &'a GraphDatabase<E>) -> Self {
        Self {
            db,
            current: Vec::new(),
            #[cfg(feature = "caching")]
            fingerprint: None,
        }
    }

    /// 从所有节点开始并启用查询缓存
    #[cfg(feature = "caching")]
    pub fn new_cached(db: &'a GraphDatabase<E>) -> Self {
        Self {
            db,
            current: Vec::new(),
            fingerprint: Some(QueryFingerprint::label_query("*")),
        }
    }

    /// 按 label 选出起始节点（不看属性，纯 label）
    pub fn from_label(mut self, label: &str) -> Self {
        let mut ids = Vec::new();
        for stored in self.db.all_stored_nodes() {
            let node = Node {
                id: stored.id,
                labels: stored.labels,
                props: stored.props,
            };
            if node.has_label(label) {
                ids.push(node.id);
            }
        }
        self.current = ids;
        self
    }

    /// 使用索引按 label + 文本属性 = 值 选起点
    pub fn from_label_and_prop_eq(mut self, label: &str, key: &str, expected: &str) -> Self {
        use crate::values::Value;
        let ids = self
            .db
            .index
            .find(label, key, &Value::Text(expected.to_string()));
        self.current = ids;
        self
    }

    /// 使用索引按 label + 整型属性 = 值 选起点
    pub fn from_label_and_prop_int_eq(mut self, label: &str, key: &str, expected: i64) -> Self {
        use crate::values::Value;
        let ids = self
            .db
            .index
            .find(label, key, &Value::Int(expected));
        self.current = ids;
        self
    }

    /// 进一步按属性等于过滤（只支持 Text 简单比较）
    pub fn where_prop_eq(mut self, key: &str, expected: &str) -> Self {
        let mut filtered = Vec::new();
        for id in self.current.iter().copied() {
            if let Some(node) = self.db.get_node(id) {
                if let Some(Value::Text(ref v)) = node.get(key) {
                    if v == expected {
                        filtered.push(id);
                    }
                }
            }
        }
        self.current = filtered;
        self
    }

    /// 按整型属性 > 某个值过滤
    pub fn where_prop_int_gt(mut self, key: &str, min: i64) -> Self {
        let mut filtered = Vec::new();
        for id in self.current.iter().copied() {
            if let Some(node) = self.db.get_node(id) {
                if let Some(Value::Int(v)) = node.get(key) {
                    if *v > min {
                        filtered.push(id);
                    }
                }
            }
        }
        self.current = filtered;
        self
    }

    /// 沿着指定类型的出边走一层
    pub fn out(mut self, rel_type: &str) -> Self {
        let mut next = Vec::new();
        for id in self.current.iter().copied() {
            for rel in self.db.neighbors_out(id) {
                if rel.typ == rel_type {
                    next.push(rel.end);
                }
            }
        }
        self.current = next;
        self
    }

    /// 沿着指定类型的入边走一层（反向遍历）
    pub fn in_(mut self, rel_type: &str) -> Self {
        let mut next = Vec::new();
        for id in self.current.iter().copied() {
            for rel in self.db.neighbors_in(id) {
                if rel.typ == rel_type {
                    next.push(rel.start);
                }
            }
        }
        self.current = next;
        self
    }

    /// 对当前 ID 集合去重
    pub fn distinct(mut self) -> Self {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        self.current.retain(|id| seen.insert(*id));
        self
    }

    /// 跳过前 N 个节点
    pub fn skip(mut self, n: usize) -> Self {
        if n < self.current.len() {
            self.current.drain(0..n);
        } else {
            self.current.clear();
        }
        self
    }

    /// 限制返回前 N 个节点
    pub fn limit(mut self, n: usize) -> Self {
        self.current.truncate(n);
        self
    }

    /// 按属性排序（支持整型和文本）
    pub fn order_by(mut self, key: &str, ascending: bool) -> Self {
        let key = key.to_string();
        let mut nodes_with_vals: Vec<(NodeId, Option<Value>)> = self
            .current
            .iter()
            .map(|&id| {
                let val = self.db.get_node(id).and_then(|n| n.props.get(&key).cloned());
                (id, val)
            })
            .collect();

        nodes_with_vals.sort_by(|(_, a), (_, b)| {
            match (a, b) {
                (Some(Value::Int(x)), Some(Value::Int(y))) => {
                    if ascending { x.cmp(y) } else { y.cmp(x) }
                }
                (Some(Value::Text(x)), Some(Value::Text(y))) => {
                    if ascending { x.cmp(y) } else { y.cmp(x) }
                }
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                _ => std::cmp::Ordering::Equal,
            }
        });

        self.current = nodes_with_vals.into_iter().map(|(id, _)| id).collect();
        self
    }

    /// 收集当前节点为 Node 对象
    pub fn collect_nodes(self) -> Vec<Node> {
        self.current
            .into_iter()
            .filter_map(|id| self.db.get_node(id))
            .collect()
    }

    /// 聚合：计数
    pub fn count(self) -> usize {
        self.current.len()
    }

    /// 聚合：对整型属性求和
    pub fn sum_int(self, key: &str) -> i64 {
        let key = key.to_string();
        self.current
            .into_iter()
            .filter_map(|id| self.db.get_node(id))
            .filter_map(|n| n.props.get(&key).cloned())
            .filter_map(|v| if let Value::Int(i) = v { Some(i) } else { None })
            .sum()
    }

    /// 聚合：对整型属性求平均值
    pub fn avg_int(self, key: &str) -> Option<f64> {
        let key = key.to_string();
        let values: Vec<i64> = self
            .current
            .iter()
            .filter_map(|&id| self.db.get_node(id))
            .filter_map(|n| n.props.get(&key).cloned())
            .filter_map(|v| if let Value::Int(i) = v { Some(i) } else { None })
            .collect();

        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum::<i64>() as f64 / values.len() as f64)
        }
    }

    // ========== 缓存查询方法 ==========

    /// 使用缓存收集节点
    #[cfg(feature = "caching")]
    pub fn collect_nodes_cached(self) -> Vec<Node> {
        if let (Some(cache), Some(fingerprint)) = (self.db.cache(), self.fingerprint.as_ref()) {
            let current_ids = self.current.clone();

            // 尝试从缓存获取
            if let Some(cached_ids) = cache.get_query(&fingerprint) {
                // 从缓存命中的ID列表收集节点
                return cached_ids
                    .into_iter()
                    .filter_map(|id| self.db.get_node(id))
                    .collect();
            }

            // 缓存未命中，执行查询并缓存结果
            let result: Vec<Node> = current_ids
                .into_iter()
                .filter_map(|id| self.db.get_node(id))
                .collect();

            let result_ids: Vec<NodeId> = result.iter().map(|n| n.id).collect();
            cache.put_query(fingerprint.clone(), result_ids);

            result
        } else {
            // 没有启用缓存，回退到常规方法
            self.collect_nodes()
        }
    }

    /// 使用缓存计数
    #[cfg(feature = "caching")]
    pub fn count_cached(self) -> usize {
        if let (Some(cache), Some(fingerprint)) = (self.db.cache(), self.fingerprint.as_ref()) {
            let current_ids = self.current.clone();

            // 尝试从缓存获取
            if let Some(cached_ids) = cache.get_query(&fingerprint) {
                return cached_ids.len();
            }

            // 缓存未命中，执行并缓存
            let count = current_ids.len();
            cache.put_query(fingerprint.clone(), current_ids);

            count
        } else {
            self.count()
        }
    }
}
