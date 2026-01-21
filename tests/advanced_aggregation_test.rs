// 高级聚合函数测试
// 测试 percentileCont, stDev, variance 函数

use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::query::Query;
use rs_graphdb::storage::StorageEngine;
use rs_graphdb::values::{Properties, Value};

// 辅助函数：创建带分数的节点
fn create_score_node(db: &mut GraphDatabase<impl StorageEngine>, name: &str, score: i64, age: i64) {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("score".to_string(), Value::Int(score));
    props.insert("age".to_string(), Value::Int(age));
    db.create_node(vec!["Student"], props);
}

#[test]
fn test_percentile_cont_median() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据：分数为 60, 70, 80, 90, 100
    create_score_node(&mut db, "Alice", 60, 20);
    create_score_node(&mut db, "Bob", 70, 21);
    create_score_node(&mut db, "Charlie", 80, 22);
    create_score_node(&mut db, "David", 90, 23);
    create_score_node(&mut db, "Eve", 100, 24);

    let query = Query::new(&db).from_label("Student");
    let median = Query::new(&db).from_label("Student").percentile_cont("score", 0.5);

    assert_eq!(median, Some(80.0)); // 中位数应该是 80
}

#[test]
fn test_percentile_cont_quartiles() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据：1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    for i in 1..=10 {
        create_score_node(&mut db, &format!("S{}", i), i, 20 + i);
    }

    // Q1 (25th percentile) 应该在 2.75 - 3.25 之间
    let q1 = Query::new(&db).from_label("Student").percentile_cont("score", 0.25);
    assert!(q1.is_some());
    assert!(q1.unwrap() > 2.0 && q1.unwrap() < 4.0);

    // Q2 (50th percentile / median) 应该是 5.5
    let q2 = Query::new(&db).from_label("Student").percentile_cont("score", 0.5);
    assert_eq!(q2, Some(5.5));

    // Q3 (75th percentile)
    let q3 = Query::new(&db).from_label("Student").percentile_cont("score", 0.75);
    assert!(q3.is_some());
    assert!(q3.unwrap() > 7.0 && q3.unwrap() < 9.0);
}

#[test]
fn test_percentile_cont_empty() {
    let db = GraphDatabase::new_in_memory();

    let query = Query::new(&db).from_label("Student");
    let median = Query::new(&db).from_label("Student").percentile_cont("score", 0.5);

    assert_eq!(median, None); // 没有数据时返回 None
}

#[test]
fn test_percentile_cont_single_value() {
    let mut db = GraphDatabase::new_in_memory();

    create_score_node(&mut db, "Alice", 80, 20);

    let query = Query::new(&db).from_label("Student");
    let median = Query::new(&db).from_label("Student").percentile_cont("score", 0.5);

    assert_eq!(median, Some(80.0)); // 只有一个值时，返回该值
}

#[test]
fn test_percentile_cont_invalid_percentile() {
    let mut db = GraphDatabase::new_in_memory();

    create_score_node(&mut db, "Alice", 80, 20);

    let query = Query::new(&db).from_label("Student");

    // 超出范围的百分位数
    assert_eq!(Query::new(&db).from_label("Student").percentile_cont("score", -0.1), None);
    assert_eq!(Query::new(&db).from_label("Student").percentile_cont("score", 1.1), None);
}

#[test]
fn test_percentile_cont_p95() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建100个节点，分数为 1-100
    for i in 1..=100 {
        create_score_node(&mut db, &format!("S{}", i), i, 20 + i);
    }

    let query = Query::new(&db).from_label("Student");
    let p95 = Query::new(&db).from_label("Student").percentile_cont("score", 0.95);

    // 95th percentile 应该接近 95
    assert!(p95.is_some());
    assert!((p95.unwrap() - 95.0).abs() < 1.0);
}

#[test]
fn test_stdev() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据：年龄 20, 25, 30, 35, 40
    // 平均值 = 30
    // 方差 = ((20-30)^2 + (25-30)^2 + (30-30)^2 + (35-30)^2 + (40-30)^2) / 4
    //      = (100 + 25 + 0 + 25 + 100) / 4 = 250 / 4 = 62.5
    // 标准差 = sqrt(62.5) ≈ 7.906
    create_score_node(&mut db, "Alice", 80, 20);
    create_score_node(&mut db, "Bob", 85, 25);
    create_score_node(&mut db, "Charlie", 90, 30);
    create_score_node(&mut db, "David", 95, 35);
    create_score_node(&mut db, "Eve", 100, 40);

    let query = Query::new(&db).from_label("Student");
    let stdev = Query::new(&db).from_label("Student").stdev("age");

    assert!(stdev.is_some());
    assert!((stdev.unwrap() - 7.906).abs() < 0.01);
}

#[test]
fn test_stdev_constant_values() {
    let mut db = GraphDatabase::new_in_memory();

    // 所有值都相同
    for i in 1..=5 {
        create_score_node(&mut db, &format!("S{}", i), 80, 30);
    }

    let query = Query::new(&db).from_label("Student");
    let stdev = Query::new(&db).from_label("Student").stdev("age");

    // 标准差应该是 0（所有值相同）
    assert_eq!(stdev, Some(0.0));
}

#[test]
fn test_stdev_empty() {
    let db = GraphDatabase::new_in_memory();

    let query = Query::new(&db).from_label("Student");
    let stdev = Query::new(&db).from_label("Student").stdev("age");

    assert_eq!(stdev, None); // 没有数据时返回 None
}

#[test]
fn test_stdev_single_value() {
    let mut db = GraphDatabase::new_in_memory();

    create_score_node(&mut db, "Alice", 80, 30);

    let query = Query::new(&db).from_label("Student");
    let stdev = Query::new(&db).from_label("Student").stdev("age");

    assert_eq!(stdev, None); // 只有一个值时返回 None
}

#[test]
fn test_variance() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据：年龄 20, 25, 30, 35, 40
    // 方差 = 62.5
    create_score_node(&mut db, "Alice", 80, 20);
    create_score_node(&mut db, "Bob", 85, 25);
    create_score_node(&mut db, "Charlie", 90, 30);
    create_score_node(&mut db, "David", 95, 35);
    create_score_node(&mut db, "Eve", 100, 40);

    let query = Query::new(&db).from_label("Student");
    let variance = Query::new(&db).from_label("Student").variance("age");

    assert!(variance.is_some());
    assert!((variance.unwrap() - 62.5).abs() < 0.01);
}

#[test]
fn test_variance_consistency_with_stdev() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    for i in 1..=10 {
        create_score_node(&mut db, &format!("S{}", i), 80 + i, 20 + i);
    }

    let query = Query::new(&db).from_label("Student");
    let stdev = Query::new(&db).from_label("Student").stdev("age");
    let variance = Query::new(&db).from_label("Student").variance("age");

    // 标准差应该是方差的平方根
    assert!(stdev.is_some() && variance.is_some());
    assert!((stdev.unwrap() - variance.unwrap().sqrt()).abs() < 0.001);
}

#[test]
fn test_variance_empty() {
    let db = GraphDatabase::new_in_memory();

    let query = Query::new(&db).from_label("Student");
    let variance = Query::new(&db).from_label("Student").variance("age");

    assert_eq!(variance, None); // 没有数据时返回 None
}

#[test]
fn test_aggregation_combined() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    for i in 1..=20 {
        create_score_node(&mut db, &format!("S{}", i), i * 5, 20 + i);
    }

    let query = Query::new(&db).from_label("Student");

    // 测试多个聚合函数
    let count = Query::new(&db).from_label("Student").count();
    let median = Query::new(&db).from_label("Student").percentile_cont("score", 0.5);
    let avg = Query::new(&db).from_label("Student").avg_int("score").unwrap();
    let stdev = Query::new(&db).from_label("Student").stdev("age");
    let variance = Query::new(&db).from_label("Student").variance("age");

    assert_eq!(count, 20);
    assert_eq!(median, Some(52.5)); // (50 + 55) / 2
    assert_eq!(avg, 52.5); // 平均值
    assert!(stdev.is_some());
    assert!(variance.is_some());
}

#[test]
fn test_aggregation_with_filter() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据 - 创建分数 >= 50 的学生
    for i in 5..=10 {
        create_score_node(&mut db, &format!("S{}", i), i * 10, 20 + i);
    }

    // 查询所有 Student 节点
    let query = Query::new(&db).from_label("Student");

    let median = Query::new(&db).from_label("Student").percentile_cont("score", 0.5);
    let count = query.count();

    assert_eq!(count, 6); // 有 6 个学生
    assert_eq!(median, Some(75.0)); // 50, 60, 70, 80, 90, 100 的中位数是 75
}

#[test]
fn test_percentile_cont_float_values() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建带浮点数的节点
    for i in 1..=5 {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text(format!("S{}", i)));
        props.insert("value".to_string(), Value::Float(i as f64 * 1.5));
        db.create_node(vec!["Data"], props);
    }

    let query = Query::new(&db).from_label("Data");
    let median = Query::new(&db).from_label("Data").percentile_cont("value", 0.5);

    // 1.5, 3.0, 4.5, 6.0, 7.5 的中位数是 4.5
    assert_eq!(median, Some(4.5));
}

#[test]
fn test_aggregation_edge_cases() {
    let mut db = GraphDatabase::new_in_memory();

    // 测试边界情况：两个值
    create_score_node(&mut db, "A", 80, 20);
    create_score_node(&mut db, "B", 90, 30);

    let query = Query::new(&db).from_label("Student");

    // 中位数应该是 (20 + 30) / 2 = 25
    let median = Query::new(&db).from_label("Student").percentile_cont("age", 0.5);
    assert_eq!(median, Some(25.0));

    // 方差 = ((20-25)^2 + (30-25)^2) / 1 = 50
    let variance = Query::new(&db).from_label("Student").variance("age");
    assert_eq!(variance, Some(50.0));

    // 标准差 = sqrt(50) ≈ 7.071
    let stdev = Query::new(&db).from_label("Student").stdev("age");
    assert!((stdev.unwrap() - 7.071).abs() < 0.01);
}
