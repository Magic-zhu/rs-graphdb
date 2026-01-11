use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rs_graphdb::{GraphDatabase, values::{Properties, Value}};
use rs_graphdb::query::Query;

fn create_test_graph(size: usize) -> GraphDatabase<rs_graphdb::storage::mem_store::MemStore> {
    let mut db = GraphDatabase::new_in_memory();

    let mut nodes = Vec::new();
    for i in 0..size {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text(format!("User{}", i)));
        props.insert("age".to_string(), Value::Int((20 + (i % 50)) as i64));

        let node_id = db.create_node(vec!["User"], props);
        nodes.push(node_id);
    }

    // 创建一些关系
    for i in 0..size - 1 {
        db.create_rel(nodes[i], nodes[i + 1], "FRIEND", Properties::new());
    }

    db
}

fn bench_full_scan(c: &mut Criterion) {
    let db = create_test_graph(1000);

    c.bench_function("full_scan_1000_nodes", |b| {
        b.iter(|| {
            Query::new(black_box(&db))
                .from_label("User")
                .collect_nodes()
        });
    });
}

fn bench_indexed_query(c: &mut Criterion) {
    let db = create_test_graph(1000);

    c.bench_function("indexed_query_1000_nodes", |b| {
        b.iter(|| {
            Query::new(black_box(&db))
                .from_label_and_prop_eq("User", "name", "User500")
                .collect_nodes()
        });
    });
}

fn bench_traversal(c: &mut Criterion) {
    let db = create_test_graph(1000);

    c.bench_function("traversal_one_hop", |b| {
        b.iter(|| {
            Query::new(black_box(&db))
                .from_label_and_prop_eq("User", "name", "User0")
                .out("FRIEND")
                .collect_nodes()
        });
    });
}

fn bench_multi_hop_traversal(c: &mut Criterion) {
    let db = create_test_graph(100);

    c.bench_function("traversal_three_hops", |b| {
        b.iter(|| {
            Query::new(black_box(&db))
                .from_label_and_prop_eq("User", "name", "User0")
                .out("FRIEND")
                .out("FRIEND")
                .out("FRIEND")
                .collect_nodes()
        });
    });
}

criterion_group!(benches, bench_full_scan, bench_indexed_query, bench_traversal, bench_multi_hop_traversal);
criterion_main!(benches);
