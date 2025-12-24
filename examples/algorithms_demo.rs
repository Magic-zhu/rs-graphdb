use rust_graphdb::{GraphDatabase, algorithms};
use rust_graphdb::values::{Properties, Value};

fn make_user(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

fn main() {
    println!("🦀 Rust Graph Database - Algorithms Demo\n");

    let mut db = GraphDatabase::new_in_memory();

    // 创建一个社交网络图
    let alice = db.create_node(vec!["Person"], make_user("Alice"));
    let bob = db.create_node(vec!["Person"], make_user("Bob"));
    let carol = db.create_node(vec!["Person"], make_user("Carol"));
    let dave = db.create_node(vec!["Person"], make_user("Dave"));
    let eve = db.create_node(vec!["Person"], make_user("Eve"));

    // 建立关系
    db.create_rel(alice, bob, "FRIEND", Properties::new());
    db.create_rel(alice, carol, "FRIEND", Properties::new());
    db.create_rel(bob, dave, "FRIEND", Properties::new());
    db.create_rel(carol, dave, "FRIEND", Properties::new());
    db.create_rel(dave, eve, "FRIEND", Properties::new());

    println!("📊 Graph created with 5 nodes and 5 relationships\n");

    // 1. 最短路径
    println!("1️⃣  Shortest Path (BFS):");
    if let Some(path) = algorithms::bfs_shortest_path(&db, alice, eve) {
        println!("   Path from Alice to Eve: {:?}", path);
        println!("   Length: {}", path.len() - 1);
    }
    println!();

    // 2. Dijkstra
    println!("2️⃣  Dijkstra Shortest Path:");
    if let Some((path, cost)) = algorithms::dijkstra(&db, alice, eve) {
        println!("   Path: {:?}", path);
        println!("   Cost: {}", cost);
    }
    println!();

    // 3. 度中心性
    println!("3️⃣  Degree Centrality:");
    let degree_cent = algorithms::degree_centrality(&db);
    let mut nodes: Vec<_> = degree_cent.iter().collect();
    nodes.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
    for (node_id, score) in nodes {
        let node = db.get_node(*node_id).unwrap();
        if let Some(Value::Text(name)) = node.props.get("name") {
            println!("   Node {}: {} - centrality: {:.3}", node_id, name, score);
        }
    }
    println!();

    // 4. 介数中心性
    println!("4️⃣  Betweenness Centrality:");
    let between_cent = algorithms::betweenness_centrality(&db);
    let mut nodes: Vec<_> = between_cent.iter().collect();
    nodes.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
    for (node_id, score) in nodes.iter().take(3) {
        let node = db.get_node(**node_id).unwrap();
        if let Some(Value::Text(name)) = node.props.get("name") {
            println!("   Node {}: {} - betweenness: {:.3}", node_id, name, score);
        }
    }
    println!();

    // 5. 连通分量
    println!("5️⃣  Connected Components:");
    let components = algorithms::connected_components(&db);
    let unique_components: std::collections::HashSet<_> = components.values().cloned().collect();
    println!("   Number of components: {}", unique_components.len());
    println!("   Component assignments: {:?}", components);
    println!();

    // 6. PageRank
    println!("6️⃣  PageRank:");
    let ranks = algorithms::pagerank(&db, 0.85, 20);
    let mut rank_vec: Vec<_> = ranks.iter().collect();
    rank_vec.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
    for (node_id, score) in rank_vec {
        let node = db.get_node(*node_id).unwrap();
        if let Some(Value::Text(name)) = node.props.get("name") {
            println!("   Node {}: {} - rank: {:.4}", node_id, name, score);
        }
    }
    println!();

    // 7. Louvain 社区检测
    println!("7️⃣  Louvain Communities:");
    let communities = algorithms::louvain(&db, 10);
    println!("   Community assignments: {:?}", communities);
}
