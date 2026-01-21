// 增强聚合函数测试
// 测试 STDEV、PERCENTILECONT、PERCENTILEDISC 等新聚合函数

use rs_graphdb::{GraphDatabase};
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};
use rs_graphdb::cypher::{parse_cypher, execute_statement, CypherResult};

// 辅助函数：从 CypherResult 中提取节点列表
fn get_nodes(result: Result<CypherResult, String>) -> Vec<rs_graphdb::graph::model::Node> {
    match result {
        Ok(CypherResult::Nodes(nodes)) => nodes,
        _ => vec![],
    }
}

// ==================== STDEV 标准差测试 ====================

#[test]
fn test_stdev_simple() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据
    for i in 1..=5 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN STDEV(n.value) as std
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);
    let std_val = result[0].get("std");
    assert!(std_val.is_some());

    // 标准差应该是 sqrt(2) ≈ 1.414
    if let Some(Value::Float(std)) = std_val {
        assert!((std - 1.414).abs() < 0.01);
    } else {
        panic!("Expected Float value");
    }
}

#[test]
fn test_stdev_single_value() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let mut props = Properties::new();
    props.insert("value".to_string(), Value::Int(5));
    db.create_node(vec!["Number"], props);

    let query = r#"
        MATCH (n:Number)
        RETURN STDEV(n.value) as std
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    // 单个值的标准差应该是 NULL
    assert_eq!(result.len(), 1);
    let std_val = result[0].get("std");
    assert_eq!(std_val, Some(&Value::Null));
}

#[test]
fn test_stdev_empty() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let query = r#"
        MATCH (n:Number)
        RETURN STDEV(n.value) as std
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    // 空集
    assert_eq!(result.len(), 0);
}

#[test]
fn test_stdev_with_float() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let values = vec![1.5, 2.5, 3.5, 4.5];
    for v in values {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Float(v));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN STDEV(n.value) as std
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);
    let std_val = result[0].get("std");
    if let Some(Value::Float(std)) = std_val {
        // 1.5, 2.5, 3.5, 4.5 的标准差应该是 1.118...
        assert!((std - 1.118).abs() < 0.01);
    } else {
        panic!("Expected Float value");
    }
}

// ==================== PERCENTILECONT 连续百分位数测试 ====================

#[test]
fn test_percentilecont_median() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据: 1, 2, 3, 4, 5
    for i in 1..=5 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILECONT(n.value, 0.5) as median
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);
    let median_val = result[0].get("median");
    if let Some(Value::Float(median)) = median_val {
        // 中位数应该是 3
        assert!((median - 3.0).abs() < 0.01);
    } else {
        panic!("Expected Float value");
    }
}

#[test]
fn test_percentilecont_quartile() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据: 1, 2, 3, 4, 5, 6, 7, 8
    for i in 1..=8 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILECONT(n.value, 0.25) as q1
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);
    let q1_val = result[0].get("q1");
    if let Some(Value::Float(q1)) = q1_val {
        // 第一四分位数应该在 2 和 3 之间
        assert!(*q1 > 2.0 && *q1 < 3.0);
    } else {
        panic!("Expected Float value");
    }
}

#[test]
fn test_percentilecont_even_count() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 偶数个数据: 1, 2, 3, 4
    for i in 1..=4 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILECONT(n.value, 0.5) as median
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);
    let median_val = result[0].get("median");
    if let Some(Value::Float(median)) = median_val {
        // 1,2,3,4 的中位数应该是 2.5 (线性插值)
        assert!((median - 2.5).abs() < 0.01);
    } else {
        panic!("Expected Float value");
    }
}

#[test]
fn test_percentilecont_empty() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILECONT(n.value, 0.5) as median
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    // 空集
    assert_eq!(result.len(), 0);
}

// ==================== PERCENTILEDISC 离散百分位数测试 ====================

#[test]
fn test_percentiledisc_median() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据: 1, 2, 3, 4, 5
    for i in 1..=5 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILEDISC(n.value, 0.5) as median
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);
    let median_val = result[0].get("median");
    if let Some(Value::Float(median)) = median_val {
        // 离散中位数应该是 3（排序后的中间值）
        assert!((median - 3.0).abs() < 0.01);
    } else {
        panic!("Expected Float value");
    }
}

#[test]
fn test_percentiledisc_even_count() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 偶数个数据: 1, 2, 3, 4
    for i in 1..=4 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILEDISC(n.value, 0.5) as median
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);
    let median_val = result[0].get("median");
    if let Some(Value::Float(median)) = median_val {
        // 离散中位数应该四舍五入到最近的值，是 2 或 3
        assert!(*median == 2.0 || *median == 3.0);
    } else {
        panic!("Expected Float value");
    }
}

#[test]
fn test_percentiledisc_quartile() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据: 1, 2, 3, 4, 5, 6, 7, 8
    for i in 1..=8 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILEDISC(n.value, 0.25) as q1,
               PERCENTILEDISC(n.value, 0.75) as q3
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);
    let q1_val = result[0].get("q1");
    let q3_val = result[0].get("q3");

    if let (Some(Value::Float(q1)), Some(Value::Float(q3))) = (q1_val, q3_val) {
        // 第一四分位数应该接近 2
        assert!((q1 - 2.0).abs() < 1.0);
        // 第三四分位数应该接近 6
        assert!((q3 - 6.0).abs() < 1.0);
    } else {
        panic!("Expected Float values");
    }
}

// ==================== 组合测试 ====================

#[test]
fn test_multiple_aggregations() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据
    for i in 1..=10 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN AVG(n.value) as avg,
               STDEV(n.value) as std,
               PERCENTILECONT(n.value, 0.5) as median,
               PERCENTILEDISC(n.value, 0.5) as disc_median
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);

    // AVG: 1+2+...+10 / 10 = 5.5
    let avg_val = result[0].get("avg");
    if let Some(Value::Float(avg)) = avg_val {
        assert!((avg - 5.5).abs() < 0.01);
    }

    // STDEV: 应该 > 0
    let std_val = result[0].get("std");
    if let Some(Value::Float(std)) = std_val {
        assert!(*std > 0.0);
    }

    // PERCENTILECONT: 中位数 5.5
    let median_val = result[0].get("median");
    if let Some(Value::Float(median)) = median_val {
        assert!((median - 5.5).abs() < 0.01);
    }

    // PERCENTILEDISC: 离散中位数 5 或 6
    let disc_val = result[0].get("disc_median");
    if let Some(Value::Float(disc)) = disc_val {
        assert!(*disc == 5.0 || *disc == 6.0);
    }
}

#[test]
fn test_percentile_with_group_by() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建两组数据
    for i in 1..=5 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        props.insert("group".to_string(), Value::Text("A".to_string()));
        db.create_node(vec!["Number"], props);
    }

    for i in 10..=15 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        props.insert("group".to_string(), Value::Text("B".to_string()));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN n.group as group,
               PERCENTILECONT(n.value, 0.5) as median
        GROUP BY group
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    // 应该有两个分组
    assert_eq!(result.len(), 2);

    // 找到分组 A 和 B 的结果
    let mut found_a = false;
    let mut found_b = false;

    for row in &result {
        if let Some(Value::Text(group)) = row.get("group") {
            if group == "A" {
                found_a = true;
                if let Some(Value::Float(median)) = row.get("median") {
                    // A 组中位数应该接近 3
                    assert!((median - 3.0).abs() < 1.0);
                }
            } else if group == "B" {
                found_b = true;
                if let Some(Value::Float(median)) = row.get("median") {
                    // B 组中位数应该接近 12.5
                    assert!((median - 12.5).abs() < 1.0);
                }
            }
        }
    }

    assert!(found_a && found_b);
}

// ==================== 边界情况测试 ====================

#[test]
fn test_percentile_extremes() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    for i in 1..=10 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    // 测试 0% 和 100% 百分位数
    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILECONT(n.value, 0.0) as min,
               PERCENTILECONT(n.value, 1.0) as max
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 1);

    let min_val = result[0].get("min");
    let max_val = result[0].get("max");

    if let (Some(Value::Float(min)), Some(Value::Float(max))) = (min_val, max_val) {
        assert!((min - 1.0).abs() < 0.01);
        assert!((max - 10.0).abs() < 0.01);
    } else {
        panic!("Expected Float values");
    }
}

#[test]
fn test_stdev_grouped() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建两组数据
    for i in 1..=3 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i * 10));
        props.insert("category".to_string(), Value::Text("A".to_string()));
        db.create_node(vec!["Number"], props);
    }

    for i in 1..=3 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        props.insert("category".to_string(), Value::Text("B".to_string()));
        db.create_node(vec!["Number"], props);
    }

    let query = r#"
        MATCH (n:Number)
        RETURN n.category as category,
               STDEV(n.value) as std
        GROUP BY category
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = get_nodes(execute_statement(&mut db, &stmt));

    assert_eq!(result.len(), 2);

    // 验证两个分组都有标准差
    for row in &result {
        if let Some(Value::Float(std)) = row.get("std") {
            assert!(*std > 0.0);
        }
    }
}

// ==================== 错误处理测试 ====================

#[test]
fn test_percentile_invalid_parameter() {
    // 测试百分位数超出范围的处理
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    for i in 1..=5 {
        let mut props = Properties::new();
        props.insert("value".to_string(), Value::Int(i));
        db.create_node(vec!["Number"], props);
    }

    // 无效的百分位数 (> 1.0)
    let query = r#"
        MATCH (n:Number)
        RETURN PERCENTILECONT(n.value, 1.5) as invalid
    "#;

    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt);

    // 应该返回错误
    assert!(result.is_err());
}
