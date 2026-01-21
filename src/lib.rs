pub mod values;
pub mod storage;
pub mod graph;
pub mod query;
pub mod query_engine;
pub mod query_stream;
pub mod index;
pub mod index_schema;
pub mod index_advanced;
pub mod index_composite;
pub mod index_persistent;
pub mod server;
pub mod cypher;
pub mod algorithms;
pub mod concurrent;
pub mod constraints;
pub mod service;
pub mod visualization;
pub mod transactions;

#[cfg(feature = "caching")]
pub mod cache;

#[cfg(feature = "grpc")]
pub mod grpc;

pub use crate::graph::db::GraphDatabase;
pub use crate::graph::{AsyncGraphDB, AsyncError};
pub use crate::storage::{NodeId, AsyncStorage};
pub use crate::concurrent::ConcurrentGraphDB;
pub use crate::query::Query;

// 导出约束模块
pub use crate::constraints::{
    Constraint, ConstraintType, ConstraintValidation, ConstraintManager,
};

// 导出查询引擎
pub use crate::query_engine::{
    QueryResult,
    QueryValue,
    QueryRow,
    QueryRows,
    QueryPath,
    QueryContext,
    PathQueryBuilder,
    MultiVarQueryExecutor,
    QueryOptimizer,
    AdvancedQueryBuilder,
    Direction as QueryDirection,
};

// 导出可视化模块
pub use crate::visualization::{
    GraphView, VisNode, VisEdge, NodeStyle, EdgeStyle, GraphMetadata, GraphFormat, Position,
    Layout, LayoutConfig, CircleLayout, ForceDirectedLayout, HierarchicalLayout,
    GraphExport, JsonExport, DotExport,
};

// 导出事务模块
pub use crate::transactions::{
    Transaction, TransactionManager, TransactionOp, TransactionResult, TransactionError,
    TransactionStatus, Snapshot, SnapshotManager, NodeData, RelData,
    IsolationLevel, TransactionConfig,
};

// 导出高级索引模块
pub use crate::index_advanced::{
    FullTextIndex, RangeIndex, OrderedFloat,
};

// 导出复合索引模块
pub use crate::index_composite::{
    CompositeIndexValue, CompositeIndexDef, CompositeIndex,
    CompositeIndexStats, CompositeIndexManager,
    ThreadSafeCompositeIndexManager,
};

// 导出流式查询模块
pub use crate::query_stream::{
    StreamError, StreamItem, QueryStream,
    BackpressureConfig, BackpressureHandler,
    StreamQueryBuilder, BatchProcessor, BatchFlushAction, StreamStats,
};
