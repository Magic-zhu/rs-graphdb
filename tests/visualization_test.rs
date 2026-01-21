// 图可视化测试
// 测试图视图创建、布局算法、导出功能

use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::{StorageEngine, NodeId};
use rs_graphdb::values::{Properties, Value};
use rs_graphdb::visualization::{
    GraphView, VisNode, VisEdge, NodeStyle, EdgeStyle, GraphFormat,
    Layout, LayoutConfig, CircleLayout, ForceDirectedLayout, HierarchicalLayout,
    layout::HierarchicalDirection,
};

// 辅助函数：创建Person节点
fn create_person(db: &mut GraphDatabase<impl StorageEngine>, name: &str, age: i64) -> NodeId {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    db.create_node(vec!["Person"], props)
}

#[test]
fn test_graph_view_creation() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建简单的社交网络
    let alice = create_person(&mut db, "Alice", 30);
    let bob = create_person(&mut db, "Bob", 25);
    let charlie = create_person(&mut db, "Charlie", 35);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(bob, charlie, "KNOWS", Properties::new());

    // 创建图视图
    let graph_view = db.to_graph_view();

    assert_eq!(graph_view.node_count(), 3);
    assert_eq!(graph_view.edge_count(), 2);

    // 验证节点（不假设顺序）
    assert!(graph_view.nodes.iter().all(|n| n.labels[0] == "Person"));
    let names: Vec<_> = graph_view.nodes.iter().map(|n| n.display_name()).collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));

    // 验证边
    assert!(graph_view.edges.iter().all(|e| e.rel_type == "KNOWS"));
}

#[test]
fn test_subgraph_view() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建图
    let alice = create_person(&mut db, "Alice", 30);
    let bob = create_person(&mut db, "Bob", 25);
    let charlie = create_person(&mut db, "Charlie", 35);
    let david = create_person(&mut db, "David", 28);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(bob, charlie, "KNOWS", Properties::new());
    db.create_rel(charlie, david, "KNOWS", Properties::new());

    // 创建子图（只包含 Alice, Bob, Charlie）
    let subgraph = db.to_subgraph_view(&[alice, bob, charlie]);

    assert_eq!(subgraph.node_count(), 3);
    assert_eq!(subgraph.edge_count(), 2); // Alice->Bob, Bob->Charlie
}

#[test]
fn test_circle_layout() {
    let mut graph_view = GraphView::new();

    // 添加5个节点
    for i in 0..5 {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text(format!("Node{}", i)));
        let node = VisNode::new(i, vec!["Node".to_string()], props);
        graph_view.add_node(node);
    }

    // 应用圆形布局
    let mut layout = CircleLayout::with_default_config();
    layout.apply(&mut graph_view);

    // 验证所有节点都有位置
    for node in &graph_view.nodes {
        assert!(node.position.is_some(), "Node should have position after layout");
        let pos = node.position.as_ref().unwrap();
        assert!(pos.x > 0.0 && pos.x < 800.0, "X should be in bounds");
        assert!(pos.y > 0.0 && pos.y < 600.0, "Y should be in bounds");
    }

    // 验证元数据
    assert_eq!(graph_view.metadata.layout_algorithm, Some("Circle".to_string()));
}

#[test]
fn test_force_directed_layout() {
    let mut graph_view = GraphView::new();

    // 创建三角形图
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("A".to_string()));
    graph_view.add_node(VisNode::new(0, vec!["Node".to_string()], props.clone()));

    props.insert("name".to_string(), Value::Text("B".to_string()));
    graph_view.add_node(VisNode::new(1, vec!["Node".to_string()], props.clone()));

    props.insert("name".to_string(), Value::Text("C".to_string()));
    graph_view.add_node(VisNode::new(2, vec!["Node".to_string()], props.clone()));

    // 添加边形成三角形
    graph_view.add_edge(VisEdge::new(0, 1, "EDGE".to_string(), Properties::new()));
    graph_view.add_edge(VisEdge::new(1, 2, "EDGE".to_string(), Properties::new()));
    graph_view.add_edge(VisEdge::new(2, 0, "EDGE".to_string(), Properties::new()));

    // 应用力导向布局
    let config = LayoutConfig::new(400.0, 400.0).with_iterations(50);
    let mut layout = ForceDirectedLayout::new(config);
    layout.apply(&mut graph_view);

    // 验证所有节点都有位置
    for node in &graph_view.nodes {
        assert!(node.position.is_some());
    }

    // 验证元数据
    assert_eq!(graph_view.metadata.layout_algorithm, Some("ForceDirected".to_string()));
}

#[test]
fn test_hierarchical_layout_top_to_bottom() {
    let mut graph_view = GraphView::new();

    // 创建层次结构: 0 -> 1 -> 2
    let mut props = Properties::new();
    for i in 0..3 {
        props.insert("name".to_string(), Value::Text(format!("Node{}", i)));
        graph_view.add_node(VisNode::new(i, vec!["Node".to_string()], props.clone()));
    }

    graph_view.add_edge(VisEdge::new(0, 1, "PARENT".to_string(), Properties::new()));
    graph_view.add_edge(VisEdge::new(1, 2, "PARENT".to_string(), Properties::new()));

    // 应用层次布局（从上到下）
    let config = LayoutConfig::new(600.0, 400.0);
    let mut layout = HierarchicalLayout::new(config)
        .with_direction(HierarchicalDirection::TopToBottom);
    layout.apply(&mut graph_view);

    // 验证位置（Y坐标应该递增）
    let y0 = graph_view.nodes[0].position.as_ref().unwrap().y;
    let y1 = graph_view.nodes[1].position.as_ref().unwrap().y;
    let y2 = graph_view.nodes[2].position.as_ref().unwrap().y;

    assert!(y1 > y0, "Node 1 should be below Node 0");
    assert!(y2 > y1, "Node 2 should be below Node 1");

    // 验证元数据
    assert_eq!(graph_view.metadata.layout_algorithm, Some("Hierarchical".to_string()));
}

#[test]
fn test_hierarchical_layout_left_to_right() {
    let mut graph_view = GraphView::new();

    // 创建层次结构
    let mut props = Properties::new();
    for i in 0..3 {
        props.insert("name".to_string(), Value::Text(format!("Node{}", i)));
        graph_view.add_node(VisNode::new(i, vec!["Node".to_string()], props.clone()));
    }

    graph_view.add_edge(VisEdge::new(0, 1, "PARENT".to_string(), Properties::new()));
    graph_view.add_edge(VisEdge::new(1, 2, "PARENT".to_string(), Properties::new()));

    // 应用层次布局（从左到右）
    let config = LayoutConfig::new(600.0, 400.0);
    let mut layout = HierarchicalLayout::new(config)
        .with_direction(HierarchicalDirection::LeftToRight);
    layout.apply(&mut graph_view);

    // 验证位置（X坐标应该递增）
    let x0 = graph_view.nodes[0].position.as_ref().unwrap().x;
    let x1 = graph_view.nodes[1].position.as_ref().unwrap().x;
    let x2 = graph_view.nodes[2].position.as_ref().unwrap().x;

    assert!(x1 > x0, "Node 1 should be right of Node 0");
    assert!(x2 > x1, "Node 2 should be right of Node 1");
}

#[test]
fn test_json_export() {
    let mut graph_view = GraphView::new();

    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("TestNode".to_string()));
    props.insert("value".to_string(), Value::Int(42));

    let node = VisNode::new(1, vec!["Test".to_string()], props)
        .with_position(100.0, 200.0);
    graph_view.add_node(node);

    graph_view.add_edge(VisEdge::new(1, 2, "TEST_REL".to_string(), Properties::new()));

    // 导出为JSON
    let json_result = graph_view.export(GraphFormat::Json);
    assert!(json_result.is_ok());

    let json_str = json_result.unwrap();
    assert!(json_str.contains("\"nodes\""));
    assert!(json_str.contains("\"edges\""));
    assert!(json_str.contains("TestNode"));
    assert!(json_str.contains("TEST_REL"));
}

#[test]
fn test_dot_export() {
    let mut graph_view = GraphView::new();

    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("Alice".to_string()));

    let node = VisNode::new(1, vec!["Person".to_string()], props)
        .with_position(100.0, 200.0);
    graph_view.add_node(node);

    let mut edge_props = Properties::new();
    edge_props.insert("weight".to_string(), Value::Int(5));
    graph_view.add_edge(VisEdge::new(1, 2, "KNOWS".to_string(), edge_props));

    // 导出为DOT格式
    let dot_result = graph_view.export(GraphFormat::Dot);
    assert!(dot_result.is_ok());

    let dot_str = dot_result.unwrap();
    assert!(dot_str.contains("digraph G"));
    assert!(dot_str.contains("Alice"));
    assert!(dot_str.contains("KNOWS"));
    assert!(dot_str.contains("->"));
}

#[test]
fn test_node_style() {
    let style = NodeStyle::new()
        .with_color("#ff0000".to_string())
        .with_size(30.0)
        .with_shape("circle".to_string());

    assert_eq!(style.color, Some("#ff0000".to_string()));
    assert_eq!(style.size, Some(30.0));
    assert_eq!(style.shape, Some("circle".to_string()));
}

#[test]
fn test_edge_style() {
    let style = EdgeStyle::new()
        .with_color("#00ff00".to_string())
        .with_width(2.5);

    assert_eq!(style.color, Some("#00ff00".to_string()));
    assert_eq!(style.width, Some(2.5));
}

#[test]
fn test_vis_node_with_style() {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("StyledNode".to_string()));

    let style = NodeStyle::new()
        .with_color("#0000ff".to_string())
        .with_size(25.0);

    let node = VisNode::new(1, vec!["Test".to_string()], props)
        .with_style(style)
        .with_position(50.0, 100.0);

    assert!(node.style.is_some());
    assert_eq!(node.style.as_ref().unwrap().color, Some("#0000ff".to_string()));
    assert_eq!(node.position.as_ref().unwrap().x, 50.0);
}

#[test]
fn test_vis_edge_with_style() {
    let style = EdgeStyle::new()
        .with_color("#ff00ff".to_string())
        .with_width(3.0);

    let edge = VisEdge::new(1, 2, "REL".to_string(), Properties::new())
        .with_style(style)
        .with_id("edge123".to_string());

    assert!(edge.style.is_some());
    assert_eq!(edge.style.as_ref().unwrap().color, Some("#ff00ff".to_string()));
    assert_eq!(edge.id, Some("edge123".to_string()));
}

#[test]
fn test_empty_graph_view() {
    let graph_view = GraphView::new();
    assert_eq!(graph_view.node_count(), 0);
    assert_eq!(graph_view.edge_count(), 0);

    // 测试导出空图
    let json_result = graph_view.export(GraphFormat::Json);
    assert!(json_result.is_ok());

    let dot_result = graph_view.export(GraphFormat::Dot);
    assert!(dot_result.is_ok());
}

#[test]
fn test_graph_database_export() {
    let mut db = GraphDatabase::new_in_memory();

    let alice = create_person(&mut db, "Alice", 30);
    let bob = create_person(&mut db, "Bob", 25);
    db.create_rel(alice, bob, "KNOWS", Properties::new());

    // 测试JSON导出
    let json_result = db.export_graph(GraphFormat::Json);
    assert!(json_result.is_ok());
    let json_str = json_result.unwrap();
    assert!(json_str.contains("Alice"));
    assert!(json_str.contains("Bob"));
    assert!(json_str.contains("KNOWS"));

    // 测试DOT导出
    let dot_result = db.export_graph(GraphFormat::Dot);
    assert!(dot_result.is_ok());
    let dot_str = dot_result.unwrap();
    assert!(dot_str.contains("Alice"));
    assert!(dot_str.contains("Bob"));
}

#[test]
fn test_subgraph_export() {
    let mut db = GraphDatabase::new_in_memory();

    let alice = create_person(&mut db, "Alice", 30);
    let bob = create_person(&mut db, "Bob", 25);
    let charlie = create_person(&mut db, "Charlie", 35);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(bob, charlie, "KNOWS", Properties::new());

    // 导出子图（只包含 Alice 和 Bob）
    let json_result = db.export_subgraph(&[alice, bob], GraphFormat::Json);
    assert!(json_result.is_ok());

    let json_str = json_result.unwrap();
    assert!(json_str.contains("Alice"));
    assert!(json_str.contains("Bob"));
    // Charlie不应该出现在子图中
    assert!(!json_str.contains("Charlie"));
}

#[test]
fn test_graph_metadata() {
    let mut graph_view = GraphView::new();

    graph_view.metadata.title = Some("Test Graph".to_string());
    graph_view.metadata.created_at = Some("2025-01-21".to_string());

    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("Node1".to_string()));
    graph_view.add_node(VisNode::new(1, vec!["Test".to_string()], props));
    graph_view.add_edge(VisEdge::new(1, 2, "EDGE".to_string(), Properties::new()));

    assert_eq!(graph_view.metadata.node_count, 1);
    assert_eq!(graph_view.metadata.edge_count, 1);
    assert_eq!(graph_view.metadata.title, Some("Test Graph".to_string()));
}

#[test]
fn test_display_name_with_name_property() {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("Alice".to_string()));
    props.insert("age".to_string(), Value::Int(30));

    let node = VisNode::new(1, vec!["Person".to_string()], props);
    assert_eq!(node.display_name(), "Alice");
}

#[test]
fn test_display_name_without_name_property() {
    let props = Properties::new();
    let node = VisNode::new(123, vec!["Node".to_string()], props);
    assert_eq!(node.display_name(), "123");
}

#[test]
fn test_position_distance() {
    let pos1 = rs_graphdb::visualization::Position::new(0.0, 0.0);
    let pos2 = rs_graphdb::visualization::Position::new(3.0, 4.0);

    let distance = pos1.distance_to(&pos2);
    assert!((distance - 5.0).abs() < 0.001, "Distance should be 5.0");
}
