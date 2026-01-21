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

    /// 按整型属性等于过滤
    pub fn where_prop_int_eq(mut self, key: &str, expected: i64) -> Self {
        let mut filtered = Vec::new();
        for id in self.current.iter().copied() {
            if let Some(node) = self.db.get_node(id) {
                if let Some(Value::Int(v)) = node.get(key) {
                    if *v == expected {
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

    /// 可变长度路径遍历（出边）
    ///
    /// 从当前节点集合出发，沿着指定类型的关系遍历 min_hops 到 max_hops 跳
    ///
    /// # 参数
    /// - `rel_type`: 关系类型
    /// - `min_hops`: 最小跳数（inclusive）
    /// - `max_hops`: 最大跳数（inclusive），None 表示无限制
    ///
    /// # 示例
    /// ```ignore
    /// // 查找所有在 2-3 跳内可达的朋友
    /// query.out_variable_length("FRIEND", 2, Some(3))
    /// ```
    pub fn out_variable_length(mut self, rel_type: &str, min_hops: usize, max_hops: Option<usize>) -> Self {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();

        for start_id in self.current.iter().copied() {
            // BFS 遍历，记录每个节点所在的深度
            let mut queue = std::collections::VecDeque::new();

            // 从起始节点的邻居开始，深度为 1
            for rel in self.db.neighbors_out(start_id) {
                if rel.typ == rel_type {
                    let neighbor = rel.end;
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back((neighbor, 1));
                    }
                }
            }

            while let Some((node_id, depth)) = queue.pop_front() {
                // 如果达到最小跳数，将节点加入结果
                if depth >= min_hops {
                    result.push(node_id);
                }

                // 如果达到最大跳数，停止扩展
                if let Some(max) = max_hops {
                    if depth >= max {
                        continue;
                    }
                }

                // 扩展邻接节点
                for rel in self.db.neighbors_out(node_id) {
                    if rel.typ == rel_type {
                        let neighbor = rel.end;
                        if !visited.contains(&neighbor) {
                            visited.insert(neighbor);
                            queue.push_back((neighbor, depth + 1));
                        }
                    }
                }
            }
        }

        self.current = result;
        self
    }

    /// 可变长度路径遍历（入边）
    ///
    /// 从当前节点集合出发，沿着指定类型的关系反向遍历 min_hops 到 max_hops 跳
    ///
    /// # 参数
    /// - `rel_type`: 关系类型
    /// - `min_hops`: 最小跳数（inclusive）
    /// - `max_hops`: 最大跳数（inclusive），None 表示无限制
    pub fn in_variable_length(mut self, rel_type: &str, min_hops: usize, max_hops: Option<usize>) -> Self {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();

        for start_id in self.current.iter().copied() {
            // BFS 遍历，记录每个节点所在的深度
            let mut queue = std::collections::VecDeque::new();

            // 从起始节点的入边邻居开始，深度为 1
            for rel in self.db.neighbors_in(start_id) {
                if rel.typ == rel_type {
                    let neighbor = rel.start;
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back((neighbor, 1));
                    }
                }
            }

            while let Some((node_id, depth)) = queue.pop_front() {
                // 如果达到最小跳数，将节点加入结果
                if depth >= min_hops {
                    result.push(node_id);
                }

                // 如果达到最大跳数，停止扩展
                if let Some(max) = max_hops {
                    if depth >= max {
                        continue;
                    }
                }

                // 扩展邻接节点（反向）
                for rel in self.db.neighbors_in(node_id) {
                    if rel.typ == rel_type {
                        let neighbor = rel.start;
                        if !visited.contains(&neighbor) {
                            visited.insert(neighbor);
                            queue.push_back((neighbor, depth + 1));
                        }
                    }
                }
            }
        }

        self.current = result;
        self
    }

    /// 无向可变长度路径遍历
    ///
    /// 从当前节点集合出发，沿着指定类型的关系（双向）遍历 min_hops 到 max_hops 跳
    ///
    /// # 参数
    /// - `rel_type`: 关系类型
    /// - `min_hops`: 最小跳数（inclusive）
    /// - `max_hops`: 最大跳数（inclusive），None 表示无限制
    pub fn undirected_variable_length(mut self, rel_type: &str, min_hops: usize, max_hops: Option<usize>) -> Self {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();

        for start_id in self.current.iter().copied() {
            // BFS 遍历，记录每个节点所在的深度
            let mut queue = std::collections::VecDeque::new();

            // 从起始节点的邻居开始（双向），深度为 1
            // 出边
            for rel in self.db.neighbors_out(start_id) {
                if rel.typ == rel_type {
                    let neighbor = rel.end;
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back((neighbor, 1));
                    }
                }
            }
            // 入边
            for rel in self.db.neighbors_in(start_id) {
                if rel.typ == rel_type {
                    let neighbor = rel.start;
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back((neighbor, 1));
                    }
                }
            }

            while let Some((node_id, depth)) = queue.pop_front() {
                // 如果达到最小跳数，将节点加入结果
                if depth >= min_hops {
                    result.push(node_id);
                }

                // 如果达到最大跳数，停止扩展
                if let Some(max) = max_hops {
                    if depth >= max {
                        continue;
                    }
                }

                // 扩展出边
                for rel in self.db.neighbors_out(node_id) {
                    if rel.typ == rel_type {
                        let neighbor = rel.end;
                        if !visited.contains(&neighbor) {
                            visited.insert(neighbor);
                            queue.push_back((neighbor, depth + 1));
                        }
                    }
                }

                // 扩展入边
                for rel in self.db.neighbors_in(node_id) {
                    if rel.typ == rel_type {
                        let neighbor = rel.start;
                        if !visited.contains(&neighbor) {
                            visited.insert(neighbor);
                            queue.push_back((neighbor, depth + 1));
                        }
                    }
                }
            }
        }

        self.current = result;
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
    ///
    /// # 优化说明
    ///
    /// 使用 `split_off` 而不是 `drain`，当 skip 值很大时更高效：
    /// - `drain(0..n)`：需要移动剩余的所有元素
    /// - `split_off(n)`：直接分割Vec，O(1)操作
    pub fn skip(mut self, n: usize) -> Self {
        if n < self.current.len() {
            self.current = self.current.split_off(n);
        } else {
            self.current.clear();
        }
        self
    }

    /// 限制返回前 N 个节点
    ///
    /// # 优化说明
    ///
    /// 使用 `truncate`，这是 O(1) 操作，只修改长度不释放内存
    ///
    /// # 使用建议
    ///
    /// 尽可能先应用 `limit` 再应用 `skip`，以减少需要处理的数据量：
    /// ```ignore
    /// // 不推荐：先 skip 再 limit
    /// q.skip(1000).limit(100)
    ///
    /// // 推荐：先 limit 再 skip（当确定数据量时）
    /// q.limit(1100).skip(1000)
    /// ```
    pub fn limit(mut self, n: usize) -> Self {
        self.current.truncate(n);
        self
    }

    /// 组合应用 SKIP 和 LIMIT（分页查询优化版）
    ///
    /// # 参数
    ///
    /// * `offset` - 跳过的记录数
    /// * `limit_count` - 返回的最大记录数
    ///
    /// # 性能优势
    ///
    /// 相比分别调用 `skip` 和 `limit`，这个方法一次性完成两个操作，
    /// 避免中间步骤的内存分配和数据移动。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// // 第2页，每页100条
    /// q.paginate(100, 100)
    /// ```
    pub fn paginate(mut self, offset: usize, limit_count: usize) -> Self {
        let total = self.current.len();

        if offset >= total {
            self.current.clear();
            return self;
        }

        let end = (offset + limit_count).min(total);

        if offset == 0 {
            self.current.truncate(end);
        } else {
            // 使用 split_off 避免移动
            self.current = self.current.split_off(offset);
            self.current.truncate(limit_count);
        }

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

    /// 收集当前节点为 Node 对象（借用版本）
    pub fn collect_nodes_ref(&self) -> Vec<Node> {
        self.current
            .iter()
            .filter_map(|id| self.db.get_node(*id))
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

    /// 聚合：计算百分位数（连续）
    ///
    /// # 参数
    /// - `key`: 属性名
    /// - `percentile`: 百分位数 (0.0 到 1.0)，例如 0.5 表示中位数
    ///
    /// # 返回
    /// 百分位数值，如果没有数据则返回 None
    ///
    /// # 示例
    /// ```ignore
    /// let median = query.percentile_cont("age", 0.5); // 中位数
    /// let p95 = query.percentile_cont("score", 0.95);  // 95th 百分位
    /// ```
    pub fn percentile_cont(self, key: &str, percentile: f64) -> Option<f64> {
        if !(0.0..=1.0).contains(&percentile) {
            return None;
        }

        let key = key.to_string();
        let mut values: Vec<f64> = self
            .current
            .iter()
            .filter_map(|&id| self.db.get_node(id))
            .filter_map(|n| n.props.get(&key).cloned())
            .filter_map(|v| match v {
                Value::Int(i) => Some(i as f64),
                Value::Float(f) => Some(f),
                _ => None,
            })
            .collect();

        if values.is_empty() {
            return None;
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = values.len();
        if n == 1 {
            return Some(values[0]);
        }

        // 使用线性插值计算百分位数
        let index = percentile * (n - 1) as f64;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
        let fraction = index - lower as f64;

        if lower == upper {
            Some(values[lower])
        } else {
            Some(values[lower] + fraction * (values[upper] - values[lower]))
        }
    }

    /// 聚合：计算标准差（样本标准差）
    ///
    /// # 参数
    /// - `key`: 属性名
    ///
    /// # 返回
    /// 标准差值，如果数据少于2个则返回 None
    ///
    /// # 公式
    /// sqrt(sum((x - mean)^2) / (n - 1))
    pub fn stdev(self, key: &str) -> Option<f64> {
        let key = key.to_string();
        let values: Vec<f64> = self
            .current
            .iter()
            .filter_map(|&id| self.db.get_node(id))
            .filter_map(|n| n.props.get(&key).cloned())
            .filter_map(|v| match v {
                Value::Int(i) => Some(i as f64),
                Value::Float(f) => Some(f),
                _ => None,
            })
            .collect();

        if values.len() < 2 {
            return None;
        }

        let n = values.len();
        let mean: f64 = values.iter().sum::<f64>() / n as f64;
        let variance = values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / (n - 1) as f64;

        Some(variance.sqrt())
    }

    /// 聚合：计算方差（样本方差）
    ///
    /// # 参数
    /// - `key`: 属性名
    ///
    /// # 返回
    /// 方差值，如果数据少于2个则返回 None
    ///
    /// # 公式
    /// sum((x - mean)^2) / (n - 1)
    pub fn variance(self, key: &str) -> Option<f64> {
        let key = key.to_string();
        let values: Vec<f64> = self
            .current
            .iter()
            .filter_map(|&id| self.db.get_node(id))
            .filter_map(|n| n.props.get(&key).cloned())
            .filter_map(|v| match v {
                Value::Int(i) => Some(i as f64),
                Value::Float(f) => Some(f),
                _ => None,
            })
            .collect();

        if values.len() < 2 {
            return None;
        }

        let n = values.len();
        let mean: f64 = values.iter().sum::<f64>() / n as f64;
        let variance = values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / (n - 1) as f64;

        Some(variance)
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
