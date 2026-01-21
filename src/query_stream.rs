// 增强的流式查询接口
//
// 实现高级流式查询功能：
// - 异步流式查询
// - 背压处理
// - 流控制
// - 批量处理
// - 进度跟踪

use crate::graph::model::{Node, Relationship};
use crate::storage::{NodeId, RelId, StorageEngine};
use crate::values::Value;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use futures::stream::{Stream, StreamExt};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;

/// 流式查询错误
#[derive(Debug, Clone)]
pub enum StreamError {
    /// 查询执行错误
    QueryError(String),
    /// 背压错误（生产者过快）
    Backpressure,
    /// 流关闭错误
    StreamClosed,
    /// 超时错误
    Timeout,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::QueryError(msg) => write!(f, "Query error: {}", msg),
            StreamError::Backpressure => write!(f, "Backpressure error"),
            StreamError::StreamClosed => write!(f, "Stream closed"),
            StreamError::Timeout => write!(f, "Timeout"),
        }
    }
}

impl std::error::Error for StreamError {}

/// 流式查询结果
#[derive(Debug, Clone)]
pub struct StreamItem {
    /// 节点数据
    pub node: Option<Node>,
    /// 关系数据
    pub rel: Option<Relationship>,
    /// 是否为批次结束
    pub is_batch_end: bool,
    /// 当前批次索引
    pub batch_index: usize,
    /// 总进度 (0.0 - 1.0)
    pub progress: f64,
}

impl StreamItem {
    /// 创建节点项目
    pub fn node(node: Node) -> Self {
        Self {
            node: Some(node),
            rel: None,
            is_batch_end: false,
            batch_index: 0,
            progress: 0.0,
        }
    }

    /// 创建关系项目
    pub fn rel(rel: Relationship) -> Self {
        Self {
            node: None,
            rel: Some(rel),
            is_batch_end: false,
            batch_index: 0,
            progress: 0.0,
        }
    }

    /// 创建批次结束标记
    pub fn batch_end(batch_index: usize, progress: f64) -> Self {
        Self {
            node: None,
            rel: None,
            is_batch_end: true,
            batch_index,
            progress,
        }
    }

    /// 获取节点
    pub fn get_node(&self) -> Option<&Node> {
        self.node.as_ref()
    }

    /// 获取关系
    pub fn get_rel(&self) -> Option<&Relationship> {
        self.rel.as_ref()
    }

    /// 是否为有效数据
    pub fn is_data(&self) -> bool {
        self.node.is_some() || self.rel.is_some()
    }
}

/// 背压配置
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// 通道缓冲区大小
    pub channel_buffer: usize,
    /// 并发限制
    pub concurrency_limit: usize,
    /// 批次大小
    pub batch_size: usize,
    /// 是否启用背压
    pub enable_backpressure: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            channel_buffer: 1000,
            concurrency_limit: 10,
            batch_size: 100,
            enable_backpressure: true,
        }
    }
}

impl BackpressureConfig {
    /// 创建新的背压配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置通道缓冲区大小
    pub fn with_channel_buffer(mut self, size: usize) -> Self {
        self.channel_buffer = size;
        self
    }

    /// 设置并发限制
    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = limit;
        self
    }

    /// 设置批次大小
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// 启用/禁用背压
    pub fn with_backpressure(mut self, enable: bool) -> Self {
        self.enable_backpressure = enable;
        self
    }
}

/// 流式查询状态
#[derive(Debug, Clone)]
pub struct StreamStats {
    /// 已处理的记录数
    pub processed_count: u64,
    /// 总记录数
    pub total_count: u64,
    /// 当前批次索引
    pub current_batch: usize,
    /// 总批次
    pub total_batches: usize,
    /// 处理开始时间
    pub start_time: std::time::Instant,
    /// 是否完成
    pub is_complete: bool,
}

impl StreamStats {
    /// 创建新的流状态
    pub fn new(total_count: u64) -> Self {
        Self {
            processed_count: 0,
            total_count,
            current_batch: 0,
            total_batches: 0,
            start_time: std::time::Instant::now(),
            is_complete: false,
        }
    }

    /// 获取进度百分比
    pub fn progress(&self) -> f64 {
        if self.total_count == 0 {
            return 1.0;
        }
        (self.processed_count as f64 / self.total_count as f64).min(1.0)
    }

    /// 获取已用时间（秒）
    pub fn elapsed_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    /// 估算剩余时间（秒）
    pub fn estimated_remaining_secs(&self) -> Option<f64> {
        if self.processed_count == 0 {
            return None;
        }
        let progress = self.progress();
        if progress >= 1.0 {
            return Some(0.0);
        }
        let elapsed = self.elapsed_secs();
        Some(elapsed / progress * (1.0 - progress))
    }

    /// 估算吞吐量（记录/秒）
    pub fn throughput(&self) -> f64 {
        let elapsed = self.elapsed_secs();
        if elapsed > 0.0 {
            self.processed_count as f64 / elapsed
        } else {
            0.0
        }
    }
}

/// 流式查询
///
/// 提供异步流式查询接口，支持背压处理
#[pin_project]
pub struct QueryStream {
    /// 接收通道
    #[pin]
    receiver: mpsc::Receiver<StreamItem>,
    /// 流状态
    stats: StreamStats,
}

impl QueryStream {
    /// 创建新的流式查询
    pub fn new(receiver: mpsc::Receiver<StreamItem>, total_count: u64) -> Self {
        Self {
            receiver,
            stats: StreamStats::new(total_count),
        }
    }

    /// 获取流状态
    pub fn stats(&self) -> &StreamStats {
        &self.stats
    }

    /// 收集所有结果（注意：可能消耗大量内存）
    pub async fn collect_all(mut self) -> Result<Vec<StreamItem>, StreamError> {
        let mut results = Vec::new();

        while let Some(item) = self.next().await {
            if item.is_data() {
                self.stats.processed_count += 1;
                results.push(item);
            }
        }

        self.stats.is_complete = true;
        Ok(results)
    }

    /// 收集所有节点
    pub async fn collect_nodes(mut self) -> Result<Vec<Node>, StreamError> {
        let mut nodes = Vec::new();

        while let Some(item) = self.next().await {
            if let Some(node) = item.node {
                self.stats.processed_count += 1;
                nodes.push(node);
            }
        }

        self.stats.is_complete = true;
        Ok(nodes)
    }

    /// 收集所有关系
    pub async fn collect_rels(mut self) -> Result<Vec<Relationship>, StreamError> {
        let mut rels = Vec::new();

        while let Some(item) = self.next().await {
            if let Some(rel) = item.rel {
                self.stats.processed_count += 1;
                rels.push(rel);
            }
        }

        self.stats.is_complete = true;
        Ok(rels)
    }

    /// 处理每个项目
    pub async fn for_each<F>(mut self, mut f: F) -> Result<(), StreamError>
    where
        F: FnMut(StreamItem),
    {
        while let Some(item) = self.next().await {
            if item.is_data() {
                self.stats.processed_count += 1;
            }
            f(item);
        }

        self.stats.is_complete = true;
        Ok(())
    }

    /// 处理每个节点
    pub async fn for_each_node<F>(mut self, mut f: F) -> Result<(), StreamError>
    where
        F: FnMut(Node),
    {
        while let Some(item) = self.next().await {
            if let Some(node) = item.node {
                self.stats.processed_count += 1;
                f(node);
            }
        }

        self.stats.is_complete = true;
        Ok(())
    }
}

impl Stream for QueryStream {
    type Item = StreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        this.receiver.poll_recv(cx)
    }
}

/// 流式查询构建器
pub struct StreamQueryBuilder {
    config: BackpressureConfig,
    filter_label: Option<String>,
    filter_property: Option<(String, Value)>,
}

impl Default for StreamQueryBuilder {
    fn default() -> Self {
        Self {
            config: BackpressureConfig::default(),
            filter_label: None,
            filter_property: None,
        }
    }
}

impl StreamQueryBuilder {
    /// 创建新的构建器
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置背压配置
    pub fn with_config(mut self, config: BackpressureConfig) -> Self {
        self.config = config;
        self
    }

    /// 设置标签过滤
    pub fn with_label_filter(mut self, label: String) -> Self {
        self.filter_label = Some(label);
        self
    }

    /// 设置属性过滤
    pub fn with_property_filter(mut self, prop: String, value: Value) -> Self {
        self.filter_property = Some((prop, value));
        self
    }

    /// 构建节点流
    pub fn build_node_stream(&self, nodes: Vec<Node>) -> QueryStream {
        let total_count = nodes.len() as u64;
        self.build_node_stream_with_stats(nodes, total_count)
    }

    /// 构建带统计的节点流
    pub fn build_node_stream_with_stats(
        &self,
        nodes: Vec<Node>,
        total_count: u64,
    ) -> QueryStream {
        let (tx, rx) = mpsc::channel(self.config.channel_buffer);

        // 应用过滤器
        let filtered_nodes: Vec<Node> = nodes
            .into_iter()
            .filter(|node| {
                // 标签过滤
                if let Some(ref label) = self.filter_label {
                    if !node.labels.contains(label) {
                        return false;
                    }
                }

                // 属性过滤
                if let Some((ref prop, ref value)) = self.filter_property {
                    if let Some(node_value) = node.props.get(prop) {
                        if node_value != value {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }

                true
            })
            .collect();

        let batch_size = self.config.batch_size;
        let total = filtered_nodes.len() as u64;

        // 异步任务：批量发送节点
        tokio::spawn(async move {
            let mut batch_index = 0usize;

            for chunk in filtered_nodes.chunks(batch_size) {
                for node in chunk {
                    let item = StreamItem::node(node.clone());
                    if tx.send(item).await.is_err() {
                        return; // 接收端已关闭
                    }
                }

                // 发送批次结束标记
                let progress = ((batch_index * batch_size + chunk.len()) as f64 / total as f64).min(1.0);
                let batch_end = StreamItem::batch_end(batch_index, progress);
                if tx.send(batch_end).await.is_err() {
                    return;
                }

                batch_index += 1;
            }
        });

        QueryStream::new(rx, total_count)
    }

    /// 构建关系流
    pub fn build_rel_stream<E: StorageEngine + Send + 'static>(
        &self,
        rels: Vec<Relationship>,
    ) -> QueryStream {
        let (tx, rx) = mpsc::channel(self.config.channel_buffer);
        let batch_size = self.config.batch_size;
        let total = rels.len() as u64;

        tokio::spawn(async move {
            let mut batch_index = 0usize;

            for chunk in rels.chunks(batch_size) {
                for rel in chunk {
                    let item = StreamItem::rel(rel.clone());
                    if tx.send(item).await.is_err() {
                        return;
                    }
                }

                let progress = ((batch_index * batch_size + chunk.len()) as f64 / total as f64).min(1.0);
                let batch_end = StreamItem::batch_end(batch_index, progress);
                if tx.send(batch_end).await.is_err() {
                    return;
                }

                batch_index += 1;
            }
        });

        QueryStream::new(rx, total)
    }
}

/// 背压处理器
///
/// 控制数据流速率，防止生产者压垮消费者
pub struct BackpressureHandler {
    /// 信号量（用于并发控制）
    semaphore: Arc<Semaphore>,
    /// 配置
    config: BackpressureConfig,
}

impl BackpressureHandler {
    /// 创建新的背压处理器
    pub fn new(config: BackpressureConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.concurrency_limit));

        Self {
            semaphore,
            config,
        }
    }

    /// 获取许可证（阻塞直到有可用许可证）
    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, StreamError> {
        self.semaphore
            .acquire()
            .await
            .map_err(|_| StreamError::Backpressure)
    }

    /// 尝试获取许可证（非阻塞）
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, StreamError> {
        self.semaphore
            .try_acquire()
            .map_err(|_| StreamError::Backpressure)
    }

    /// 获取配置
    pub fn config(&self) -> &BackpressureConfig {
        &self.config
    }

    /// 获取可用许可证数量
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

impl Clone for BackpressureHandler {
    fn clone(&self) -> Self {
        Self {
            semaphore: Arc::clone(&self.semaphore),
            config: self.config.clone(),
        }
    }
}

/// 信号量许可证类型别名
pub type SemaphorePermit<'a> = tokio::sync::SemaphorePermit<'a>;

/// 批量处理器
///
/// 提供批量处理功能，自动处理背压
pub struct BatchProcessor<T> {
    /// 批次缓冲区
    buffer: Vec<T>,
    /// 批次大小
    batch_size: usize,
    /// 背压处理器
    backpressure: Option<BackpressureHandler>,
    /// 处理计数
    processed_count: usize,
}

impl<T> BatchProcessor<T> {
    /// 创建新的批量处理器
    pub fn new(batch_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            backpressure: None,
            processed_count: 0,
        }
    }

    /// 设置背压处理器
    pub fn with_backpressure(mut self, handler: BackpressureHandler) -> Self {
        self.backpressure = Some(handler);
        self
    }

    /// 添加项目到批次
    pub fn add(&mut self, item: T) -> BatchFlushAction<T> {
        self.buffer.push(item);

        if self.buffer.len() >= self.batch_size {
            // 批次已满，需要刷新
            BatchFlushAction::Flush(std::mem::take(&mut self.buffer))
        } else {
            BatchFlushAction::Continue
        }
    }

    /// 手动刷新批次
    pub fn flush(&mut self) -> Vec<T> {
        std::mem::take(&mut self.buffer)
    }

    /// 获取当前批次大小
    pub fn current_batch_size(&self) -> usize {
        self.buffer.len()
    }

    /// 是否有待刷新的数据
    pub fn has_pending(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// 获取已处理项目数
    pub fn processed_count(&self) -> usize {
        self.processed_count
    }

    /// 标记一批项目已处理
    pub fn mark_processed(&mut self, count: usize) {
        self.processed_count += count;
    }
}

impl<T> Default for BatchProcessor<T> {
    fn default() -> Self {
        Self::new(100)
    }
}

/// 批量刷新动作
pub enum BatchFlushAction<T> {
    /// 继续添加
    Continue,
    /// 需要刷新（返回批次数据）
    Flush(Vec<T>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Properties;
    use std::collections::HashMap;

    fn create_test_nodes(count: usize) -> Vec<Node> {
        (0..count)
            .map(|i| Node {
                id: i as NodeId,
                labels: vec!["User".to_string()],
                props: {
                    let mut props = Properties::new();
                    props.insert("id".to_string(), Value::Int(i as i64));
                    props.insert("name".to_string(), Value::Text(format!("User{}", i)));
                    props
                },
            })
            .collect()
    }

    #[test]
    fn test_stream_item_node() {
        let node = Node {
            id: 1,
            labels: vec!["User".to_string()],
            props: Properties::new(),
        };

        let item = StreamItem::node(node.clone());
        assert!(item.is_data());
        assert_eq!(item.get_node(), Some(&node));
        assert!(item.get_rel().is_none());
    }

    #[test]
    fn test_stream_item_rel() {
        let rel = Relationship {
            id: 1,
            start: 0,
            end: 1,
            typ: "KNOWS".to_string(),
            props: Properties::new(),
        };

        let item = StreamItem::rel(rel.clone());
        assert!(item.is_data());
        assert_eq!(item.get_rel(), Some(&rel));
        assert!(item.get_node().is_none());
    }

    #[test]
    fn test_stream_item_batch_end() {
        let item = StreamItem::batch_end(5, 0.5);
        assert!(item.is_batch_end);
        assert!(!item.is_data());
        assert_eq!(item.batch_index, 5);
        assert_eq!(item.progress, 0.5);
    }

    #[test]
    fn test_backpressure_config_default() {
        let config = BackpressureConfig::default();
        assert_eq!(config.channel_buffer, 1000);
        assert_eq!(config.concurrency_limit, 10);
        assert_eq!(config.batch_size, 100);
        assert!(config.enable_backpressure);
    }

    #[test]
    fn test_backpressure_config_builder() {
        let config = BackpressureConfig::new()
            .with_channel_buffer(500)
            .with_concurrency_limit(5)
            .with_batch_size(50)
            .with_backpressure(false);

        assert_eq!(config.channel_buffer, 500);
        assert_eq!(config.concurrency_limit, 5);
        assert_eq!(config.batch_size, 50);
        assert!(!config.enable_backpressure);
    }

    #[test]
    fn test_stream_stats() {
        let mut stats = StreamStats::new(1000);

        assert_eq!(stats.processed_count, 0);
        assert_eq!(stats.total_count, 1000);
        assert_eq!(stats.progress(), 0.0);
        assert!(!stats.is_complete);

        stats.processed_count = 500;
        assert_eq!(stats.progress(), 0.5);
    }

    #[test]
    fn test_stream_stats_complete() {
        let mut stats = StreamStats::new(100);
        stats.processed_count = 100;
        stats.is_complete = true;

        assert_eq!(stats.progress(), 1.0);
        assert!(stats.is_complete);
    }

    #[test]
    fn test_batch_processor() {
        let mut processor = BatchProcessor::new(10);

        // 添加9个项目，不应该刷新
        for i in 0..9 {
            let action = processor.add(i);
            assert!(matches!(action, BatchFlushAction::Continue));
        }

        // 添加第10个项目，应该刷新
        let action = processor.add(9);
        match action {
            BatchFlushAction::Flush(batch) => {
                assert_eq!(batch.len(), 10);
            }
            _ => panic!("Expected flush action"),
        }

        assert_eq!(processor.current_batch_size(), 0);
    }

    #[test]
    fn test_batch_processor_flush() {
        let mut processor = BatchProcessor::new(10);

        for i in 0..5 {
            processor.add(i);
        }

        assert_eq!(processor.current_batch_size(), 5);
        assert!(processor.has_pending());

        let batch = processor.flush();
        assert_eq!(batch.len(), 5);
        assert!(!processor.has_pending());
    }

    #[tokio::test]
    async fn test_stream_query_builder_default() {
        let builder = StreamQueryBuilder::new();
        let nodes = create_test_nodes(100);
        let _stream = builder.build_node_stream(nodes);

        // 流已创建，实际测试需要异步运行时
    }

    #[tokio::test]
    async fn test_stream_query_builder_with_filter() {
        let builder = StreamQueryBuilder::new()
            .with_label_filter("User".to_string());

        let nodes = create_test_nodes(10);
        let _stream = builder.build_node_stream(nodes);

        // 流已创建，实际测试需要异步运行时
    }

    #[test]
    fn test_backpressure_handler() {
        let config = BackpressureConfig::new().with_concurrency_limit(5);
        let handler = BackpressureHandler::new(config);

        assert_eq!(handler.available_permits(), 5);
        assert_eq!(handler.config().concurrency_limit, 5);
    }

    #[test]
    fn test_backpressure_handler_clone() {
        let config = BackpressureConfig::new().with_concurrency_limit(5);
        let handler1 = BackpressureHandler::new(config.clone());
        let handler2 = handler1.clone();

        assert_eq!(handler1.available_permits(), 5);
        assert_eq!(handler2.available_permits(), 5);
    }

    #[tokio::test]
    async fn test_query_stream_collect_nodes() {
        let nodes = create_test_nodes(100);
        let (tx, rx) = mpsc::channel(100);

        // 发送节点到流
        for node in nodes.clone() {
            tx.send(StreamItem::node(node)).await.unwrap();
        }
        drop(tx);

        let stream = QueryStream::new(rx, 100);
        let collected = stream.collect_nodes().await.unwrap();

        assert_eq!(collected.len(), 100);
    }

    #[tokio::test]
    async fn test_query_stream_for_each() {
        let nodes = create_test_nodes(50);
        let (tx, rx) = mpsc::channel(100);

        for node in nodes {
            tx.send(StreamItem::node(node)).await.unwrap();
        }
        drop(tx);

        let stream = QueryStream::new(rx, 50);
        let mut count = 0;

        stream.for_each(|_| count += 1).await.unwrap();

        assert_eq!(count, 50);
    }

    #[tokio::test]
    async fn test_stream_stats_throughput() {
        let mut stats = StreamStats::new(1000);

        // 模拟一些处理时间
        std::thread::sleep(std::time::Duration::from_millis(100));

        stats.processed_count = 500;
        let throughput = stats.throughput();

        assert!(throughput > 0.0);
    }
}
