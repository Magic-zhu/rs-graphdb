//! 缓存系统演示
//!
//! 展示如何使用 rs-graphdb 的应用层缓存功能
//!
//! 运行方式:
//! ```bash
//! cargo run --features caching --example cache_demo
//! ```

#[cfg(feature = "caching")]
fn main() {
    use rs_graphdb::{
        cache::{CacheConfig, CacheManager},
        ConcurrentGraphDB, GraphDatabase, Query,
    };
    use rs_graphdb::values::{Properties, Value};

    println!("=== rs-graphdb 缓存系统演示 ===\n");

    // ============================================
    // 1. 创建带缓存的图数据库
    // ============================================
    println!("1. 创建图数据库并启用缓存...");

    let mut db = GraphDatabase::new_in_memory();

    // 使用默认缓存配置
    let cache_config = CacheConfig::default();
    println!("   - 使用默认配置:");
    println!("     * 节点缓存 TTL: {:?}", cache_config.node_ttl);
    println!("     * 邻接表缓存 TTL: {:?}", cache_config.adjacency_ttl);
    println!("     * 查询缓存 TTL: {:?}", cache_config.query_ttl);
    println!("     * 总内存比例: {:.1}%", cache_config.total_cache_ratio * 100.0);

    // 启用缓存
    db.set_cache(CacheManager::new(cache_config.clone()));

    println!("   ✓ 缓存已启用\n");

    // ============================================
    // 2. 创建测试数据
    // ============================================
    println!("2. 创建测试数据...");

    // 创建用户节点
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("Alice".to_string()));
    props.insert("age".to_string(), Value::Int(30));
    props.insert("city".to_string(), Value::Text("Beijing".to_string()));
    let alice = db.create_node(vec!["User"], props.clone());

    props.insert("name".to_string(), Value::Text("Bob".to_string()));
    props.insert("age".to_string(), Value::Int(25));
    props.insert("city".to_string(), Value::Text("Shanghai".to_string()));
    let bob = db.create_node(vec!["User"], props.clone());

    props.insert("name".to_string(), Value::Text("Charlie".to_string()));
    props.insert("age".to_string(), Value::Int(35));
    props.insert("city".to_string(), Value::Text("Beijing".to_string()));
    let charlie = db.create_node(vec!["User"], props);

    // 创建关系
    db.create_rel(alice, bob, "FRIEND", Properties::new());
    db.create_rel(alice, charlie, "COLLEAGUE", Properties::new());
    db.create_rel(bob, charlie, "FRIEND", Properties::new());

    println!("   ✓ 创建了 3 个用户节点和 3 条关系\n");

    // ============================================
    // 3. 演示节点缓存
    // ============================================
    println!("3. 演示节点缓存...");

    // 第一次读取 - 缓存未命中
    let _node1 = db.get_node(alice);

    // 第二次读取 - 从缓存获取
    let _node2 = db.get_node(alice);

    // 第三次读取 - 从缓存获取
    let _node3 = db.get_node(alice);

    let report = db.cache().unwrap().overall_report();
    println!("   节点缓存统计:");
    println!("   - 命中次数: {}", report.node.hits);
    println!("   - 未命中次数: {}", report.node.misses);
    println!("   - 命中率: {:.1}%", report.node.hit_rate * 100.0);
    println!();

    // ============================================
    // 4. 演示邻接表缓存
    // ============================================
    println!("4. 演示邻接表缓存...");

    // 多次读取邻居
    for _ in 0..3 {
        let _neighbors: Vec<_> = db.neighbors_out(alice).collect();
    }

    let report = db.cache().unwrap().overall_report();
    println!("   邻接表缓存统计:");
    println!("   - 命中次数: {}", report.adjacency.hits);
    println!("   - 未命中次数: {}", report.adjacency.misses);
    println!("   - 命中率: {:.1}%", report.adjacency.hit_rate * 100.0);
    println!();

    // ============================================
    // 5. 演示查询缓存
    // ============================================
    println!("5. 演示查询缓存...");

    // 执行相同查询多次
    for i in 0..3 {
        let result = Query::new_cached(&db)
            .from_label("User")
            .where_prop_int_gt("age", 25)
            .collect_nodes_cached();

        println!("   第 {} 次查询返回 {} 个节点", i + 1, result.len());
    }

    let report = db.cache().unwrap().overall_report();
    println!("   查询缓存统计:");
    println!("   - 命中次数: {}", report.query.hits);
    println!("   - 未命中次数: {}", report.query.misses);
    println!("   - 命中率: {:.1}%", report.query.hit_rate * 100.0);
    println!();

    // ============================================
    // 6. 演示缓存失效
    // ============================================
    println!("6. 演示缓存失效...");

    // 删除一个节点
    println!("   删除节点 Bob (ID: {})...", bob);
    db.delete_node(bob);

    // 尝试获取已删除的节点
    let deleted_node = db.get_node(bob);
    println!("   获取已删除节点: {:?}", deleted_node);

    let report = db.cache().unwrap().overall_report();
    let total_requests = report.node.total_requests + report.adjacency.total_requests
        + report.query.total_requests + report.index.total_requests;
    println!("   删除后的缓存统计:");
    println!("   - 总请求数: {}", total_requests);
    println!();

    // ============================================
    // 7. 演示缓存清空
    // ============================================
    println!("7. 演示缓存清空...");

    let report_before = db.cache().unwrap().overall_report();
    println!("   清空前 - 总内存使用: {:.2} MB", report_before.total_memory_mb as f64);

    db.cache().unwrap().clear_all();

    let report_after = db.cache().unwrap().overall_report();
    println!("   清空后 - 总内存使用: {:.2} MB", report_after.total_memory_mb as f64);
    println!();

    // ============================================
    // 8. 演示并发环境下的缓存
    // ============================================
    println!("8. 演示并发环境下的缓存...");

    let concurrent_db = ConcurrentGraphDB::new(db)
        .with_cache(CacheManager::new(cache_config));

    // 创建新节点
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("David".to_string()));
    props.insert("age".to_string(), Value::Int(28));
    let david = concurrent_db.create_node(vec!["User"], props);

    // 多线程读取
    use std::thread;

    let handles: Vec<_> = (0..5)
        .map(|_| {
            let db_clone = concurrent_db.clone_handle();
            thread::spawn(move || {
                let _node = db_clone.get_node(david);
                let _neighbors = db_clone.neighbors_out(david);
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let report = concurrent_db.get_cache_report().unwrap();
    println!("   并发访问后的缓存统计:");
    println!("   - 节点缓存命中率: {:.1}%", report.node.hit_rate * 100.0);
    println!("   - 总内存使用: {:.2} MB", report.total_memory_mb as f64);
    println!();

    // ============================================
    // 9. 显示完整缓存报告
    // ============================================
    println!("9. 完整缓存报告:");

    let report = concurrent_db.get_cache_report().unwrap();
    println!("   节点缓存:");
    println!("     - 命中: {}, 未命中: {}, 命中率: {:.1}%",
        report.node.hits,
        report.node.misses,
        report.node.hit_rate * 100.0
    );
    println!("     - 当前条目数: {}", report.node.current_entries);
    println!("     - 内存使用: {:.2} MB", report.node.memory_usage_mb as f64);

    println!("   邻接表缓存:");
    println!("     - 命中: {}, 未命中: {}, 命中率: {:.1}%",
        report.adjacency.hits,
        report.adjacency.misses,
        report.adjacency.hit_rate * 100.0
    );
    println!("     - 当前条目数: {}", report.adjacency.current_entries);
    println!("     - 内存使用: {:.2} MB", report.adjacency.memory_usage_mb as f64);

    println!("   查询缓存:");
    println!("     - 命中: {}, 未命中: {}, 命中率: {:.1}%",
        report.query.hits,
        report.query.misses,
        report.query.hit_rate * 100.0
    );
    println!("     - 当前条目数: {}", report.query.current_entries);
    println!("     - 内存使用: {:.2} MB", report.query.memory_usage_mb as f64);

    println!("   索引缓存:");
    println!("     - 命中: {}, 未命中: {}, 命中率: {:.1}%",
        report.index.hits,
        report.index.misses,
        report.index.hit_rate * 100.0
    );
    println!("     - 当前条目数: {}", report.index.current_entries);
    println!("     - 内存使用: {:.2} MB", report.index.memory_usage_mb as f64);

    println!("\n   总体统计:");
    println!("   - 总命中率: {:.1}%", report.total_hit_rate * 100.0);
    println!("   - 总内存使用: {:.2} MB", report.total_memory_mb as f64);

    println!("\n=== 演示完成 ===");
}

#[cfg(not(feature = "caching"))]
fn main() {
    println!("错误: 此示例需要启用 caching feature");
    println!("请使用: cargo run --features caching --example cache_demo");
}
