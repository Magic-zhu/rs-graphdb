// Cypher 聚合函数测试
// 测试 MIN/MAX/COLLECT/GROUP BY 功能

use rs_graphdb::cypher::{ast::*, parser};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};

// 辅助函数：创建测试节点
fn create_user(db: &mut GraphDatabase<MemStore>, name: &str, age: i64, city: &str) {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props.insert("city".to_string(), Value::Text(city.to_string()));
    db.create_node(vec!["User"], props);
}

// ==================== MIN/MAX 聚合测试 ====================

#[test]
fn test_parse_min_aggregation() {
    let queries = vec![
        "MATCH (u:User) RETURN MIN(u.age)",
        "MATCH (u:User) RETURN min(u.age)",
        "RETURN MIN(u.age)",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
        let stmt = result.unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                assert!(!q.return_clause.items.is_empty());
                // 验证包含聚合函数
                let has_agg = q.return_clause.items.iter().any(|item| {
                    matches!(item, ReturnItem::Aggregation(AggFunc::Min, _, _))
                });
                assert!(has_agg, "Expected MIN aggregation in: {}", query);
            }
            _ => panic!("Expected Query, got: {:?}", query),
        }
    }
}

#[test]
fn test_parse_max_aggregation() {
    let queries = vec![
        "MATCH (u:User) RETURN MAX(u.age)",
        "MATCH (u:User) RETURN max(u.age)",
        "RETURN MAX(u.age)",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
        let stmt = result.unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                let has_agg = q.return_clause.items.iter().any(|item| {
                    matches!(item, ReturnItem::Aggregation(AggFunc::Max, _, _))
                });
                assert!(has_agg, "Expected MAX aggregation in: {}", query);
            }
            _ => panic!("Expected Query"),
        }
    }
}

#[test]
fn test_parse_count_aggregation() {
    let queries = vec![
        "MATCH (u:User) RETURN COUNT(u.age)",
        "MATCH (u:User) RETURN count(u.age)",
        "RETURN COUNT(u)",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
    }
}

#[test]
fn test_parse_collect_aggregation() {
    let queries = vec![
        "MATCH (u:User) RETURN COLLECT(u.name)",
        "MATCH (u:User) RETURN collect(u.name)",
        "RETURN COLLECT(u.city)",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
        let stmt = result.unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                let has_agg = q.return_clause.items.iter().any(|item| {
                    matches!(item, ReturnItem::Aggregation(AggFunc::Collect, _, _))
                });
                assert!(has_agg, "Expected COLLECT aggregation in: {}", query);
            }
            _ => panic!("Expected Query"),
        }
    }
}

// ==================== GROUP BY 测试 ====================

#[test]
fn test_parse_group_by_single() {
    // 先测试最简单的 GROUP BY（没有 COUNT）
    let query = "MATCH (u:User) RETURN u GROUP BY u";
    let result = parser::parse_cypher(query);
    if !result.is_ok() {
        panic!("Failed to parse: {:?}", result);
    }

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert!(q.return_clause.group_by.is_some());
            let group_by = q.return_clause.group_by.as_ref().unwrap();
            assert_eq!(group_by.len(), 1);
            assert_eq!(group_by[0], "u");
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_parse_group_by_multiple() {
    let query = "MATCH (u:User) RETURN u.city, u.age, COUNT(*) GROUP BY u.city, u.age";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert!(q.return_clause.group_by.is_some());
            let group_by = q.return_clause.group_by.as_ref().unwrap();
            assert_eq!(group_by.len(), 2);
            assert_eq!(group_by[0], "u.city");
            assert_eq!(group_by[1], "u.age");
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_parse_group_by_with_order() {
    let query = "MATCH (u:User) RETURN u.city, COUNT(*) GROUP BY u.city ORDER BY COUNT(*) DESC";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert!(q.return_clause.group_by.is_some());
            assert!(q.return_clause.order_by.is_some());
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_parse_group_by_with_limit() {
    let query = "MATCH (u:User) RETURN u.city, COUNT(*) GROUP BY u.city LIMIT 10";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert!(q.return_clause.group_by.is_some());
            assert_eq!(q.return_clause.limit, Some(10));
        }
        _ => panic!("Expected Query"),
    }
}

// ==================== 复合查询测试 ====================

#[test]
fn test_min_max_with_group_by() {
    let query = "MATCH (u:User) RETURN u.city, MIN(u.age), MAX(u.age) GROUP BY u.city";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert!(q.return_clause.group_by.is_some());
            assert_eq!(q.return_clause.items.len(), 3); // u.city, MIN, MAX
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_collect_with_group_by() {
    let query = "MATCH (u:User) RETURN u.city, COLLECT(u.name) GROUP BY u.city";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert!(q.return_clause.group_by.is_some());
            assert_eq!(q.return_clause.items.len(), 2);
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_multiple_aggregations() {
    let query = "MATCH (u:User) RETURN COUNT(*), SUM(u.age), AVG(u.age), MIN(u.age), MAX(u.age)";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert_eq!(q.return_clause.items.len(), 5);
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_group_by_without_aggregation() {
    // GROUP BY 应该可以独立于聚合函数存在
    let query = "MATCH (u:User) RETURN u.city, u.name GROUP BY u.city, u.name";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert!(q.return_clause.group_by.is_some());
            let group_by = q.return_clause.group_by.as_ref().unwrap();
            assert_eq!(group_by.len(), 2);
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_count_with_var() {
    let queries = vec![
        "MATCH (u:User) RETURN COUNT(u)",
        "MATCH (u:User) RETURN count(u)",
        "RETURN COUNT(u.name)",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
    }
}

// ==================== 边界情况测试 ====================

#[test]
fn test_aggregation_with_where() {
    let query = "MATCH (u:User) WHERE u.age > 25 RETURN u.city, COUNT(*) GROUP BY u.city";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());
}

#[test]
fn test_aggregation_with_all_clauses() {
    let query = "MATCH (u:User) WHERE u.age > 25 RETURN u.city, MIN(u.age), MAX(u.age) GROUP BY u.city ORDER BY MIN(u.age) DESC LIMIT 5";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        CypherStatement::Query(q) => {
            assert!(q.where_clause.is_some());
            assert!(q.return_clause.group_by.is_some());
            assert!(q.return_clause.order_by.is_some());
            assert_eq!(q.return_clause.limit, Some(5));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn test_empty_group_by() {
    let query = "MATCH (u:User) RETURN COUNT(*) GROUP BY u.city";
    let result = parser::parse_cypher(query);
    assert!(result.is_ok());
}

#[test]
fn test_group_by_case_insensitive() {
    let queries = vec![
        "MATCH (u:User) RETURN u.city, COUNT(*) GROUP BY u.city",
        "MATCH (u:User) RETURN u.city, COUNT(*) group BY u.city",
        "MATCH (u:User) RETURN u.city, COUNT(*) Group By u.city",
        "match (u:User) return u.city, count(*) group by u.city",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
        let stmt = result.unwrap();
        match stmt {
            CypherStatement::Query(q) => {
                assert!(q.return_clause.group_by.is_some());
            }
            _ => panic!("Expected Query"),
        }
    }
}
