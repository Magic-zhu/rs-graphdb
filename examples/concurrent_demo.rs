use rust_graphdb::{ConcurrentGraphDB, GraphDatabase};
use rust_graphdb::values::{Properties, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

fn make_user(name: &str, age: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props
}

fn main() {
    println!("🦀 Rust Graph Database - Concurrent Access Demo\n");

    let db = GraphDatabase::new_in_memory();
    let concurrent_db = ConcurrentGraphDB::new(db);

    // ========== Demo 1: 并发写入节点 ==========
    println!("📝 Demo 1: Concurrent Writes");
    let start = Instant::now();

    let write_handles: Vec<_> = (0..10)
        .map(|i| {
            let db_clone = concurrent_db.clone_handle();
            thread::spawn(move || {
                for j in 0..100 {
                    let name = format!("User{}_{}", i, j);
                    db_clone.create_node(vec!["User"], make_user(&name, (i * 100 + j) as i64));
                }
            })
        })
        .collect();

    for handle in write_handles {
        handle.join().unwrap();
    }

    let write_duration = start.elapsed();
    let node_count = concurrent_db.node_count();
    println!("   ✓ Created {} nodes across 10 threads", node_count);
    println!("   ⏱ Time: {:?}", write_duration);
    println!();

    // ========== Demo 2: 并发读取节点 ==========
    println!("📖 Demo 2: Concurrent Reads");
    let read_count = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let read_handles: Vec<_> = (0..20)
        .map(|_| {
            let db_clone = concurrent_db.clone_handle();
            let counter = Arc::clone(&read_count);
            thread::spawn(move || {
                for node_id in 0..1000 {
                    if db_clone.get_node(node_id).is_some() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for handle in read_handles {
        handle.join().unwrap();
    }

    let read_duration = start.elapsed();
    println!("   ✓ {} successful reads across 20 threads", read_count.load(Ordering::Relaxed));
    println!("   ⏱ Time: {:?}", read_duration);
    println!();

    // ========== Demo 3: 混合读写操作 ==========
    println!("🔀 Demo 3: Mixed Read/Write Operations");
    let start = Instant::now();

    let mut mixed_handles = vec![];

    // 5个写线程
    for i in 0..5 {
        let db_clone = concurrent_db.clone_handle();
        mixed_handles.push(thread::spawn(move || {
            for j in 0..50 {
                let name = format!("NewUser{}_{}", i, j);
                db_clone.create_node(vec!["User"], make_user(&name, (i * 50 + j) as i64));
            }
        }));
    }

    // 10个读线程
    let read_ops = Arc::new(AtomicUsize::new(0));
    for _ in 0..10 {
        let db_clone = concurrent_db.clone_handle();
        let counter = Arc::clone(&read_ops);
        mixed_handles.push(thread::spawn(move || {
            for node_id in 0..500 {
                if db_clone.get_node(node_id).is_some() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for handle in mixed_handles {
        handle.join().unwrap();
    }

    let mixed_duration = start.elapsed();
    println!("   ✓ Mixed operations completed");
    println!("   ✓ Read operations: {}", read_ops.load(Ordering::Relaxed));
    println!("   ✓ Final node count: {}", concurrent_db.node_count());
    println!("   ⏱ Time: {:?}", mixed_duration);
    println!();

    // ========== Demo 4: 关系创建和遍历 ==========
    println!("🔗 Demo 4: Concurrent Relationship Creation");
    let start = Instant::now();

    // 创建一些节点作为端点
    let source_nodes: Vec<_> = (0..10)
        .map(|i| {
            concurrent_db.create_node(
                vec!["Person"],
                make_user(&format!("Source{}", i), i * 10),
            )
        })
        .collect();

    let target_nodes: Vec<_> = (0..10)
        .map(|i| {
            concurrent_db.create_node(
                vec!["Person"],
                make_user(&format!("Target{}", i), i * 10 + 5),
            )
        })
        .collect();

    // 并发创建关系
    let rel_handles: Vec<_> = (0..5)
        .map(|i| {
            let db_clone = concurrent_db.clone_handle();
            let sources = source_nodes.clone();
            let targets = target_nodes.clone();
            thread::spawn(move || {
                for j in 0..sources.len() {
                    db_clone.create_rel(
                        sources[j],
                        targets[(j + i) % targets.len()],
                        "KNOWS",
                        Properties::new(),
                    );
                }
            })
        })
        .collect();

    for handle in rel_handles {
        handle.join().unwrap();
    }

    let rel_duration = start.elapsed();
    println!("   ✓ Created relationships");
    println!("   ⏱ Time: {:?}", rel_duration);

    // 验证关系
    let sample_node = source_nodes[0];
    let out_degree = concurrent_db.out_degree(sample_node);
    println!("   ✓ Sample node {} has out-degree: {}", sample_node, out_degree);
    println!();

    // ========== 性能总结 ==========
    println!("📊 Performance Summary:");
    println!("   Total nodes: {}", concurrent_db.node_count());
    println!("   Write throughput: {:.0} nodes/sec",
             1000.0 / write_duration.as_secs_f64());
    println!("   Read throughput: {:.0} reads/sec",
             read_count.load(Ordering::Relaxed) as f64 / read_duration.as_secs_f64());
}
