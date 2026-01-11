use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rs_graphdb::{GraphDatabase};
use rs_graphdb::storage::sled_store::SledStore;
use rs_graphdb::values::{Properties, Value};
use std::path::PathBuf;

fn make_node_props(i: usize) -> (Vec<String>, Properties) {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(format!("User{}", i)));
    props.insert("age".to_string(), Value::Int((20 + (i % 50)) as i64));
    (vec!["User".to_string()], props)
}

fn make_rel_props(start: u64, end: u64) -> (u64, u64, String, Properties) {
    (start, end, "FRIEND".to_string(), Properties::new())
}

fn bench_single_vs_batch_nodes_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_creation_memory");

    for size in [100, 500, 1000, 5000].iter() {
        // 单个创建节点
        group.bench_with_input(BenchmarkId::new("single", size), size, |b, &size| {
            b.iter(|| {
                let mut db = GraphDatabase::new_in_memory();
                for i in 0..size {
                    let (labels, props) = make_node_props(i);
                    db.create_node(
                        labels.iter().map(|s| s.as_str()).collect(),
                        props
                    );
                }
                black_box(&db);
            });
        });

        // 批量创建节点
        group.bench_with_input(BenchmarkId::new("batch", size), size, |b, &size| {
            b.iter(|| {
                let mut db = GraphDatabase::new_in_memory();
                let nodes: Vec<_> = (0..size).map(|i| make_node_props(i)).collect();
                db.batch_create_nodes(black_box(nodes));
                black_box(&db);
            });
        });
    }

    group.finish();
}

fn bench_single_vs_batch_rels_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("rel_creation_memory");

    for size in [100, 500, 1000].iter() {
        // 单个创建关系
        group.bench_with_input(BenchmarkId::new("single", size), size, |b, &size| {
            b.iter(|| {
                let mut db = GraphDatabase::new_in_memory();

                // 先创建节点
                let mut nodes = Vec::new();
                for i in 0..=size {
                    let (labels, props) = make_node_props(i);
                    let id = db.create_node(
                        labels.iter().map(|s| s.as_str()).collect(),
                        props
                    );
                    nodes.push(id);
                }

                // 单个创建关系
                for i in 0..size {
                    db.create_rel(nodes[i], nodes[i + 1], "FRIEND", Properties::new());
                }

                black_box(&db);
            });
        });

        // 批量创建关系
        group.bench_with_input(BenchmarkId::new("batch", size), size, |b, &size| {
            b.iter(|| {
                let mut db = GraphDatabase::new_in_memory();

                // 先创建节点
                let mut nodes = Vec::new();
                for i in 0..=size {
                    let (labels, props) = make_node_props(i);
                    let id = db.create_node(
                        labels.iter().map(|s| s.as_str()).collect(),
                        props
                    );
                    nodes.push(id);
                }

                // 批量创建关系
                let rels: Vec<_> = (0..size)
                    .map(|i| make_rel_props(nodes[i], nodes[i + 1]))
                    .collect();
                db.batch_create_rels(black_box(rels));

                black_box(&db);
            });
        });
    }

    group.finish();
}

#[cfg(unix)]
fn bench_single_vs_batch_nodes_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_creation_sled");

    for size in [100, 500, 1000].iter() {
        // 单个创建节点
        group.bench_with_input(BenchmarkId::new("single", size), size, |b, &size| {
            b.iter(|| {
                let dir = PathBuf::from("/tmp/sled_bench_single");
                let _ = std::fs::remove_dir_all(&dir);

                let engine = SledStore::new(&dir).unwrap();
                let mut db = GraphDatabase::from_engine(engine);

                for i in 0..size {
                    let (labels, props) = make_node_props(i);
                    db.create_node(
                        labels.iter().map(|s| s.as_str()).collect(),
                        props
                    );
                }
                black_box(&db);

                let _ = std::fs::remove_dir_all(&dir);
            });
        });

        // 批量创建节点
        group.bench_with_input(BenchmarkId::new("batch", size), size, |b, &size| {
            b.iter(|| {
                let dir = PathBuf::from("/tmp/sled_bench_batch");
                let _ = std::fs::remove_dir_all(&dir);

                let engine = SledStore::new(&dir).unwrap();
                let mut db = GraphDatabase::from_engine(engine);

                let nodes: Vec<_> = (0..size).map(|i| make_node_props(i)).collect();
                db.batch_create_nodes(black_box(nodes));
                black_box(&db);

                let _ = std::fs::remove_dir_all(&dir);
            });
        });
    }

    group.finish();
}

#[cfg(windows)]
fn bench_single_vs_batch_nodes_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_creation_sled");

    for size in [100, 500, 1000].iter() {
        // 单个创建节点
        group.bench_with_input(BenchmarkId::new("single", size), size, |b, &size| {
            b.iter(|| {
                let dir = std::env::temp_dir().join(format!("sled_bench_single_{}", std::process::id()));
                let _ = std::fs::remove_dir_all(&dir);

                let engine = SledStore::new(&dir).unwrap();
                let mut db = GraphDatabase::from_engine(engine);

                for i in 0..size {
                    let (labels, props) = make_node_props(i);
                    db.create_node(
                        labels.iter().map(|s| s.as_str()).collect(),
                        props
                    );
                }
                black_box(&db);

                let _ = std::fs::remove_dir_all(&dir);
            });
        });

        // 批量创建节点
        group.bench_with_input(BenchmarkId::new("batch", size), size, |b, &size| {
            b.iter(|| {
                let dir = std::env::temp_dir().join(format!("sled_bench_batch_{}", std::process::id()));
                let _ = std::fs::remove_dir_all(&dir);

                let engine = SledStore::new(&dir).unwrap();
                let mut db = GraphDatabase::from_engine(engine);

                let nodes: Vec<_> = (0..size).map(|i| make_node_props(i)).collect();
                db.batch_create_nodes(black_box(nodes));
                black_box(&db);

                let _ = std::fs::remove_dir_all(&dir);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_vs_batch_nodes_memory,
    bench_single_vs_batch_rels_memory,
    bench_single_vs_batch_nodes_sled
);
criterion_main!(benches);
