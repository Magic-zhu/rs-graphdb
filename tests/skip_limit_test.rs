//! SKIP/LIMIT 优化测试

use rs_graphdb::{GraphDatabase, Query};
use rs_graphdb::values::{Properties, Value};
use rs_graphdb::storage::mem_store::MemStore;

fn create_test_db() -> GraphDatabase<MemStore> {
    let mut db = GraphDatabase::new_in_memory();

    // 创建1000个测试节点
    for i in 0..1000 {
        let mut props = Properties::new();
        props.insert("id".to_string(), Value::Int(i));
        props.insert("name".to_string(), Value::Text(format!("User{}", i)));

        db.create_node(vec!["User"], props);
    }

    db
}

#[test]
fn test_skip_basic() {
    let db = create_test_db();

    // 跳过前100个
    let result = Query::new(&db).from_label("User").skip(100).collect_nodes();
    assert_eq!(result.len(), 900);

    // 跳过前900个
    let result = Query::new(&db).from_label("User").skip(900).collect_nodes();
    assert_eq!(result.len(), 100);

    // 跳过所有
    let result = Query::new(&db).from_label("User").skip(1000).collect_nodes();
    assert_eq!(result.len(), 0);

    // 跳过超过总数
    let result = Query::new(&db).from_label("User").skip(2000).collect_nodes();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_limit_basic() {
    let db = create_test_db();

    // 限制返回100个
    let result = Query::new(&db).from_label("User").limit(100).collect_nodes();
    assert_eq!(result.len(), 100);

    // 限制返回0个
    let result = Query::new(&db).from_label("User").limit(0).collect_nodes();
    assert_eq!(result.len(), 0);

    // 限制返回超过总数
    let result = Query::new(&db).from_label("User").limit(2000).collect_nodes();
    assert_eq!(result.len(), 1000);
}

#[test]
fn test_skip_and_limit_combined() {
    let db = create_test_db();

    // 第2页：跳过100，限制100
    let result = Query::new(&db).from_label("User").skip(100).limit(100).collect_nodes();
    assert_eq!(result.len(), 100);

    // 最后一页：跳过900，限制100
    let result = Query::new(&db).from_label("User").skip(900).limit(100).collect_nodes();
    assert_eq!(result.len(), 100);

    // 超出范围：跳过950，限制100
    let result = Query::new(&db).from_label("User").skip(950).limit(100).collect_nodes();
    assert_eq!(result.len(), 50);

    // 完全超出：跳过1000，限制100
    let result = Query::new(&db).from_label("User").skip(1000).limit(100).collect_nodes();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_paginate() {
    let db = create_test_db();

    // 第1页：offset=0, limit=100
    let page1 = Query::new(&db).from_label("User").paginate(0, 100).collect_nodes();
    assert_eq!(page1.len(), 100);

    // 第2页：offset=100, limit=100
    let page2 = Query::new(&db).from_label("User").paginate(100, 100).collect_nodes();
    assert_eq!(page2.len(), 100);

    // 第10页：offset=900, limit=100
    let page10 = Query::new(&db).from_label("User").paginate(900, 100).collect_nodes();
    assert_eq!(page10.len(), 100);

    // 第11页：offset=1000, limit=100 (超出范围)
    let page11 = Query::new(&db).from_label("User").paginate(1000, 100).collect_nodes();
    assert_eq!(page11.len(), 0);
}

#[test]
fn test_paginate_vs_skip_limit_consistency() {
    let db = create_test_db();

    // 使用 paginate
    let result1 = Query::new(&db).from_label("User").paginate(250, 100).collect_nodes();

    // 使用 skip + limit
    let result2 = Query::new(&db).from_label("User").skip(250).limit(100).collect_nodes();

    assert_eq!(result1.len(), result2.len());
}

#[test]
fn test_performance_skip_optimization() {
    let db = create_test_db();

    // 测试大skip值的性能
    let start = std::time::Instant::now();
    let _result = Query::new(&db).from_label("User").skip(900).collect_nodes();
    let elapsed = start.elapsed();

    println!("Skip 900 nodes: {:?}", elapsed);

    // 性能断言：应该在很短时间内完成（优化后）
    assert!(elapsed.as_millis() < 10, "Skip operation took too long: {:?}", elapsed);
}

#[test]
fn test_performance_large_paginate() {
    let db = create_test_db();

    let start = std::time::Instant::now();
    // 模拟访问第50页（每页20条）
    let _result = Query::new(&db).from_label("User").paginate(980, 20).collect_nodes();
    let elapsed = start.elapsed();

    println!("Paginate offset=980 limit=20: {:?}", elapsed);

    // 性能断言：应该在很短时间内完成
    assert!(elapsed.as_millis() < 10, "Paginate operation took too long: {:?}", elapsed);
}

#[test]
fn test_pagination_edge_cases() {
    let db = create_test_db();

    // 空数据库
    let empty_db = GraphDatabase::new_in_memory();
    let result = Query::new(&empty_db).from_label("User").paginate(0, 100).collect_nodes();
    assert_eq!(result.len(), 0);

    // limit=0
    let result = Query::new(&db).from_label("User").paginate(0, 0).collect_nodes();
    assert_eq!(result.len(), 0);

    // 超大limit
    let result = Query::new(&db).from_label("User").paginate(0, 10000).collect_nodes();
    assert_eq!(result.len(), 1000);
}

#[test]
fn test_chained_pagination() {
    let db = create_test_db();

    // 模拟分页遍历
    let page_size = 100;
    let mut all_ids = Vec::new();
    let mut page = 0;

    loop {
        let result = Query::new(&db)
            .from_label("User")
            .paginate(page * page_size, page_size)
            .collect_nodes();

        if result.is_empty() {
            break;
        }

        for node in &result {
            all_ids.push(node.id);
        }

        page += 1;

        // 防止无限循环
        if page > 20 {
            panic!("Too many pages");
        }
    }

    assert_eq!(all_ids.len(), 1000);
}

#[test]
fn test_skip_zero() {
    let db = create_test_db();
    let q = Query::new(&db).from_label("User");

    // skip(0) 应该不影响结果
    let result = q.skip(0).collect_nodes();
    assert_eq!(result.len(), 1000);
}

#[test]
fn test_limit_zero() {
    let db = create_test_db();
    let q = Query::new(&db).from_label("User");

    // limit(0) 应该返回空结果
    let result = q.limit(0).collect_nodes();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_order_by_with_pagination() {
    let db = create_test_db();

    // 先排序，再分页
    let result = Query::new(&db)
        .from_label("User")
        .order_by("id", true)
        .paginate(100, 50)
        .collect_nodes();

    assert_eq!(result.len(), 50);

    // 验证结果是否按预期排序和分页
    if let Some(first) = result.first() {
        if let Some(Value::Int(id)) = first.get("id") {
            // 应该从100开始
            assert!(*id >= 100);
        }
    }
}
