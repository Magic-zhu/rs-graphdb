//! 异步写入功能测试
//!
//! 测试异步存储引擎的批量写入、并发写入等功能

use rs_graphdb::{AsyncGraphDB, GraphDatabase};
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::storage::sled_store::SledStore;
use rs_graphdb::values::{Properties, Value};

fn make_props(id: usize) -> Properties {
    let mut props = Properties::new();
    props.insert("id".to_string(), Value::Int(id as i64));
    props.insert("name".to_string(), Value::Text(format!("Node_{}", id)));
    props
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_async_vs_sync_performance() {
    // 异步写入
    let engine = MemStore::new();
    let async_db = AsyncGraphDB::from_engine(engine);

    let start = std::time::Instant::now();
    let nodes: Vec<_> = (0..1000)
        .map(|i| (vec!["Node".to_string()], make_props(i)))
        .collect();

    let ids = async_db.batch_create_nodes_async(nodes).await.unwrap();
    let async_duration = start.elapsed();

    println!("异步批量创建 1000 个节点耗时: {:?}", async_duration);
    assert_eq!(ids.len(), 1000);

    // 同步写入
    let mut sync_db = GraphDatabase::new_in_memory();
    let start = std::time::Instant::now();

    for i in 0..1000 {
        sync_db.create_node(
            vec!["Node"],
            make_props(i)
        );
    }

    let sync_duration = start.elapsed();
    println!("同步单个创建 1000 个节点耗时: {:?}", sync_duration);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_parallel_async_create() {
    let engine = MemStore::new();
    let db = AsyncGraphDB::from_engine(engine);

    // 并发创建 100 个节点
    let nodes: Vec<_> = (0..100)
        .map(|i| (vec!["Node".to_string()], make_props(i)))
        .collect();

    let start = std::time::Instant::now();
    let ids = db.parallel_create_nodes(nodes).await.unwrap();
    let duration = start.elapsed();

    println!("并发创建 100 个节点耗时: {:?}", duration);
    assert_eq!(ids.len(), 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stream_create_nodes() {
    let engine = MemStore::new();
    let db = AsyncGraphDB::from_engine(engine);

    // 流式创建 10000 个节点，分批处理
    let nodes: Vec<_> = (0..10000)
        .map(|i| (vec!["Node".to_string()], make_props(i)))
        .collect();

    let start = std::time::Instant::now();
    let ids = db.stream_create_nodes(nodes, 100).await.unwrap();
    let duration = start.elapsed();

    println!("流式创建 10000 个节点耗时: {:?}", duration);
    assert_eq!(ids.len(), 10000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_async_create_with_sled() {
    let dir = std::env::temp_dir().join("async_sled_test");
    let _ = std::fs::remove_dir_all(&dir);

    {
        let engine = SledStore::new(&dir).unwrap();
        let db = AsyncGraphDB::from_engine(engine);

        let nodes: Vec<_> = (0..100)
            .map(|i| (vec!["Node".to_string()], make_props(i)))
            .collect();

        let ids = db.batch_create_nodes_async(nodes).await.unwrap();
        assert_eq!(ids.len(), 100);

        // 验证数据
        let node = db.get_node_async(ids[0]).await.unwrap().unwrap();
        assert_eq!(node.get("id"), Some(&Value::Int(0)));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mixed_async_operations() {
    let engine = MemStore::new();
    let db = AsyncGraphDB::from_engine(engine);

    // 混合操作：批量创建节点 + 批量创建关系
    let nodes: Vec<_> = (0..10)
        .map(|i| (vec!["Node".to_string()], make_props(i)))
        .collect();

    let node_ids = db.batch_create_nodes_async(nodes).await.unwrap();
    assert_eq!(node_ids.len(), 10);

    // 创建关系
    let rels: Vec<_> = (0..9)
        .map(|i| (node_ids[i], node_ids[i + 1], "EDGE".to_string(), Properties::new()))
        .collect();

    let rel_ids = db.batch_create_rels_async(rels).await.unwrap();
    assert_eq!(rel_ids.len(), 9);

    // 验证节点
    let node = db.get_node_async(node_ids[5]).await.unwrap().unwrap();
    assert_eq!(node.get("id"), Some(&Value::Int(5)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_async_access() {
    let engine = MemStore::new();
    let db = AsyncGraphDB::from_engine(engine);

    // 多个任务并发创建节点
    let mut tasks = Vec::new();

    for i in 0..10 {
        let db_clone = db.clone();
        let task = tokio::spawn(async move {
            let nodes = vec![
                (vec!["Node".to_string()], make_props(i * 2)),
                (vec!["Node".to_string()], make_props(i * 2 + 1)),
            ];
            db_clone.batch_create_nodes_async(nodes).await.unwrap()
        });
        tasks.push(task);
    }

    let mut all_ids = Vec::new();
    for task in tasks {
        let ids = task.await.unwrap();
        all_ids.extend(ids);
    }

    assert_eq!(all_ids.len(), 20);
}
