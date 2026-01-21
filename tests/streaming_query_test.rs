//! 流式查询集成测试

use rs_graphdb::{GraphDatabase, cypher::streaming::*};
use rs_graphdb::values::{Properties, Value};
use rs_graphdb::storage::mem_store::MemStore;

fn create_test_db_with_nodes(count: usize) -> GraphDatabase<MemStore> {
    let mut db = GraphDatabase::new_in_memory();

    for i in 0..count {
        let mut props = Properties::new();
        props.insert("id".to_string(), Value::Int(i as i64));
        props.insert("name".to_string(), Value::Text(format!("User{}", i)));
        props.insert("age".to_string(), Value::Int((20 + (i % 50)) as i64));

        db.create_node(vec!["User"], props);
    }

    db
}

#[test]
fn test_basic_pagination() {
    let db = create_test_db_with_nodes(250);

    // 第一页
    let page1 = query_paginated(&db, 0, 100).unwrap();
    assert_eq!(page1.page, 0);
    assert_eq!(page1.page_size, 100);
    assert_eq!(page1.total, 250);
    assert_eq!(page1.data.len(), 100);
    assert!(page1.has_more);
    assert!(page1.is_first_page());
    assert!(!page1.is_last_page());

    // 第二页
    let page2 = query_paginated(&db, 1, 100).unwrap();
    assert_eq!(page2.page, 1);
    assert_eq!(page2.data.len(), 100);
    assert!(page2.has_more);
    assert!(!page2.is_first_page());
    assert!(!page2.is_last_page());

    // 第三页（最后一页）
    let page3 = query_paginated(&db, 2, 100).unwrap();
    assert_eq!(page3.page, 2);
    assert_eq!(page3.data.len(), 50);
    assert!(!page3.has_more);
    assert!(!page3.is_first_page());
    assert!(page3.is_last_page());

    // 第四页（超出范围）
    let page4 = query_paginated(&db, 3, 100).unwrap();
    assert_eq!(page4.data.len(), 0);
    assert!(!page4.has_more);
}

#[test]
fn test_pagination_with_small_page_size() {
    let db = create_test_db_with_nodes(25);

    // 每页10个，应该有3页
    let page1 = query_paginated(&db, 0, 10).unwrap();
    assert_eq!(page1.data.len(), 10);
    assert_eq!(page1.total_pages(), 3);

    let page2 = query_paginated(&db, 1, 10).unwrap();
    assert_eq!(page2.data.len(), 10);

    let page3 = query_paginated(&db, 2, 10).unwrap();
    assert_eq!(page3.data.len(), 5);
    assert!(!page3.has_more);
}

#[test]
fn test_pagination_empty_database() {
    let db = GraphDatabase::new_in_memory();

    let page = query_paginated(&db, 0, 100).unwrap();
    assert_eq!(page.data.len(), 0);
    assert_eq!(page.total, 0);
    assert_eq!(page.total_pages(), 0);
    assert!(!page.has_more);
    assert!(page.is_first_page());
    assert!(page.is_last_page());
}

#[test]
fn test_cursor_iteration() {
    let db = create_test_db_with_nodes(250);
    let mut cursor = QueryCursor::new(&db, 100);

    // 第一页
    let page1 = cursor.next_page().unwrap();
    assert_eq!(page1.data.len(), 100);
    assert!(cursor.has_more());

    // 第二页
    let page2 = cursor.next_page().unwrap();
    assert_eq!(page2.data.len(), 100);
    assert!(cursor.has_more());

    // 第三页
    let page3 = cursor.next_page().unwrap();
    assert_eq!(page3.data.len(), 50);
    assert!(!cursor.has_more());

    // 第四页（无数据）
    let page4 = cursor.next_page();
    assert!(page4.is_none());
}

#[test]
fn test_cursor_reset() {
    let db = create_test_db_with_nodes(250);
    let mut cursor = QueryCursor::new(&db, 100);

    // 读取两页
    cursor.next_page();
    cursor.next_page();
    assert_eq!(cursor.position(), 200);

    // 重置并重新读取
    cursor.reset();
    assert_eq!(cursor.position(), 0);

    let page = cursor.next_page().unwrap();
    assert_eq!(page.data.len(), 100);
    assert_eq!(page.page, 0);
}

#[test]
fn test_stream_query_as_iterator() {
    let db = create_test_db_with_nodes(250);
    let stream = StreamQuery::new(&db, 100);

    let mut batch_count = 0;
    let mut total_count = 0;

    for batch in stream {
        batch_count += 1;
        total_count += batch.data.len();
        println!(
            "Batch {}: {} items, progress: {:.1}%",
            batch.page,
            batch.data.len(),
            (batch.page * 100 + batch.data.len() * 100 / batch.page_size) as f64 / 100.0
        );
    }

    assert_eq!(batch_count, 3);
    assert_eq!(total_count, 250);
}

#[test]
fn test_stream_query_with_large_dataset() {
    let db = create_test_db_with_nodes(1000);
    let stream = StreamQuery::new(&db, 250);

    let mut batches = vec![];

    for batch in stream {
        batches.push(batch.data.len());
    }

    assert_eq!(batches, vec![250, 250, 250, 250]);
}

#[test]
fn test_stream_query_single_batch() {
    let db = create_test_db_with_nodes(50);
    let stream = StreamQuery::new(&db, 100);

    let mut batch_count = 0;

    for batch in stream {
        batch_count += 1;
        assert_eq!(batch.data.len(), 50);
        assert!(!batch.has_more);
        assert_eq!(batch.page, 0);
    }

    assert_eq!(batch_count, 1);
}

#[test]
fn test_stream_query_empty_database() {
    let db = GraphDatabase::new_in_memory();
    let stream = StreamQuery::new(&db, 100);

    let mut batch_count = 0;

    for _batch in stream {
        batch_count += 1;
    }

    assert_eq!(batch_count, 0);
}

#[test]
fn test_stream_query_progress_tracking() {
    let db = create_test_db_with_nodes(1000);
    let stream = StreamQuery::new(&db, 100);

    assert_eq!(stream.total(), 1000);
    assert_eq!(stream.remaining(), 1000);
    assert_eq!(stream.progress(), 0.0);
}

#[test]
fn test_page_result_consistency() {
    let db = create_test_db_with_nodes(250);

    // 获取所有页
    let mut all_nodes = Vec::new();
    let mut page = 0;

    loop {
        let result = query_paginated(&db, page, 100).unwrap();
        if result.data.is_empty() {
            break;
        }

        all_nodes.extend(result.data.clone());

        if !result.has_more {
            break;
        }

        page += 1;
    }

    assert_eq!(all_nodes.len(), 250);
    assert_eq!(page, 2); // 0, 1, 2 (三页)
}

#[test]
fn test_pagination_with_different_sizes() {
    let db = create_test_db_with_nodes(100);

    // 测试不同的页面大小
    let sizes = vec![1, 10, 25, 33, 50, 100];

    for size in sizes {
        let pages = (100 + size - 1) / size; // 计算总页数

        let mut total_count = 0;
        for page in 0..pages {
            let result = query_paginated(&db, page, size).unwrap();
            total_count += result.data.len();
        }

        assert_eq!(total_count, 100, "Failed for page size {}", size);
    }
}

#[test]
fn test_large_dataset_pagination() {
    let db = create_test_db_with_nodes(10000);

    let start = std::time::Instant::now();
    let page1 = query_paginated(&db, 0, 1000).unwrap();
    let elapsed = start.elapsed();

    assert_eq!(page1.data.len(), 1000);
    assert_eq!(page1.total, 10000);
    assert!(page1.has_more);

    println!(
        "Large dataset pagination (10000 nodes, page 1): {:?}",
        elapsed
    );

    // 性能断言：第一页应该在合理时间内返回
    assert!(elapsed.as_millis() < 100, "Pagination took too long: {:?}", elapsed);
}

#[test]
fn test_stream_query_performance() {
    let db = create_test_db_with_nodes(10000);

    let start = std::time::Instant::now();
    let stream = StreamQuery::new(&db, 1000);

    let mut total_processed = 0;
    for batch in stream {
        total_processed += batch.data.len();
    }

    let elapsed = start.elapsed();

    assert_eq!(total_processed, 10000);
    println!(
        "Stream query processed {} nodes in {:?}",
        total_processed, elapsed
    );

    // 性能断言：应该在合理时间内完成
    assert!(elapsed.as_secs() < 5, "Stream query took too long: {:?}", elapsed);
}

#[test]
fn test_cursor_vs_stream_consistency() {
    let db = create_test_db_with_nodes(500);

    // 使用游标
    let mut cursor = QueryCursor::new(&db, 100);
    let mut cursor_ids = Vec::new();

    while let Some(page) = cursor.next_page() {
        for node in page.data {
            cursor_ids.push(node.id);
        }
    }

    // 使用流式查询
    let stream = StreamQuery::new(&db, 100);
    let mut stream_ids = Vec::new();

    for batch in stream {
        for node in batch.data {
            stream_ids.push(node.id);
        }
    }

    // 两者应该返回相同的节点ID
    assert_eq!(cursor_ids, stream_ids);
}
