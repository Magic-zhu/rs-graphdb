use crate::graph::db::GraphDatabase;
use crate::graph::model::{Node, Relationship};
use crate::storage::{NodeId, RelId, StorageEngine};
use crate::values::Properties;
use std::sync::{Arc, RwLock};

/// 并发友好的图数据库包装器
///
/// 使用 Arc<RwLock<>> 实现多读单写的并发访问模式：
/// - 查询操作（get_node, neighbors_out 等）获取读锁，可以并发执行
/// - 修改操作（create_node, delete_node 等）获取写锁，独占访问
pub struct ConcurrentGraphDB<E: StorageEngine> {
    db: Arc<RwLock<GraphDatabase<E>>>,
}

impl<E: StorageEngine> ConcurrentGraphDB<E> {
    pub fn new(db: GraphDatabase<E>) -> Self {
        Self {
            db: Arc::new(RwLock::new(db)),
        }
    }

    pub fn clone_handle(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }

    // ========== 读操作（并发安全）==========

    pub fn get_node(&self, id: NodeId) -> Option<Node> {
        let db = self.db.read().unwrap();
        db.get_node(id)
    }

    pub fn get_rel(&self, id: RelId) -> Option<Relationship> {
        let db = self.db.read().unwrap();
        db.get_rel(id)
    }

    pub fn neighbors_out(&self, node: NodeId) -> Vec<Relationship> {
        let db = self.db.read().unwrap();
        db.neighbors_out(node).collect()
    }

    pub fn neighbors_in(&self, node: NodeId) -> Vec<Relationship> {
        let db = self.db.read().unwrap();
        db.neighbors_in(node).collect()
    }

    pub fn all_nodes(&self) -> Vec<Node> {
        let db = self.db.read().unwrap();
        db.all_stored_nodes()
            .map(|sn| Node {
                id: sn.id,
                labels: sn.labels,
                props: sn.props,
            })
            .collect()
    }

    // ========== 写操作（独占访问）==========

    pub fn create_node(&self, labels: Vec<&str>, props: Properties) -> NodeId {
        let mut db = self.db.write().unwrap();
        db.create_node(labels, props)
    }

    pub fn create_rel(
        &self,
        start: NodeId,
        end: NodeId,
        typ: &str,
        props: Properties,
    ) -> RelId {
        let mut db = self.db.write().unwrap();
        db.create_rel(start, end, typ, props)
    }

    pub fn delete_node(&self, id: NodeId) -> bool {
        let mut db = self.db.write().unwrap();
        db.delete_node(id)
    }

    pub fn delete_rel(&self, id: RelId) -> bool {
        let mut db = self.db.write().unwrap();
        db.delete_rel(id)
    }

    pub fn flush(&self) -> Result<(), String> {
        let mut db = self.db.write().unwrap();
        db.flush()
    }

    // ========== 统计信息（用于性能优化）==========

    /// 获取节点的出度
    pub fn out_degree(&self, node: NodeId) -> usize {
        let db = self.db.read().unwrap();
        db.neighbors_out(node).count()
    }

    /// 获取节点的入度
    pub fn in_degree(&self, node: NodeId) -> usize {
        let db = self.db.read().unwrap();
        db.neighbors_in(node).count()
    }

    /// 获取节点的总度数
    pub fn degree(&self, node: NodeId) -> usize {
        let db = self.db.read().unwrap();
        db.neighbors_out(node).count() + db.neighbors_in(node).count()
    }

    /// 获取图中节点总数
    pub fn node_count(&self) -> usize {
        let db = self.db.read().unwrap();
        db.all_stored_nodes().count()
    }
}

impl<E: StorageEngine> Clone for ConcurrentGraphDB<E> {
    fn clone(&self) -> Self {
        self.clone_handle()
    }
}

// 为了让 Send + Sync 正确传播，需要显式实现
unsafe impl<E: StorageEngine> Send for ConcurrentGraphDB<E> {}
unsafe impl<E: StorageEngine> Sync for ConcurrentGraphDB<E> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Value;
    use crate::GraphDatabase;

    #[test]
    fn test_concurrent_reads() {
        let db = GraphDatabase::new_in_memory();
        let concurrent_db = ConcurrentGraphDB::new(db);

        // 创建测试数据
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        let alice = concurrent_db.create_node(vec!["User"], props.clone());

        props.insert("name".to_string(), Value::Text("Bob".to_string()));
        let bob = concurrent_db.create_node(vec!["User"], props);

        concurrent_db.create_rel(alice, bob, "FRIEND", Properties::new());

        // 多线程并发读
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let db_clone = concurrent_db.clone_handle();
                std::thread::spawn(move || {
                    // 并发读取节点
                    let node = db_clone.get_node(alice);
                    assert!(node.is_some());

                    // 并发读取关系
                    let rels = db_clone.neighbors_out(alice);
                    assert_eq!(rels.len(), 1);

                    // 并发读取度数
                    let degree = db_clone.out_degree(alice);
                    assert_eq!(degree, 1);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_writes_and_reads() {
        let db = GraphDatabase::new_in_memory();
        let concurrent_db = ConcurrentGraphDB::new(db);

        let mut handles = vec![];

        // 启动写线程
        for i in 0..5 {
            let db_clone = concurrent_db.clone_handle();
            handles.push(std::thread::spawn(move || {
                let mut props = Properties::new();
                props.insert(
                    "name".to_string(),
                    Value::Text(format!("User{}", i)),
                );
                db_clone.create_node(vec!["User"], props)
            }));
        }

        // 等待所有写操作完成
        let node_ids: Vec<NodeId> = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        // 启动读线程验证
        let read_handles: Vec<_> = node_ids
            .into_iter()
            .map(|id| {
                let db_clone = concurrent_db.clone_handle();
                std::thread::spawn(move || {
                    let node = db_clone.get_node(id);
                    assert!(node.is_some());
                })
            })
            .collect();

        for handle in read_handles {
            handle.join().unwrap();
        }

        // 验证节点总数
        assert_eq!(concurrent_db.node_count(), 5);
    }
}
