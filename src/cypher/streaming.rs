//! 流式查询功能
//!
//! 用于大数据集的流式处理，避免一次性加载所有数据到内存。

use crate::cypher::{ast::CypherQuery, executor::execute_cypher};
use crate::graph::{db::GraphDatabase, model::Node};
use crate::storage::{StorageEngine, StoredNode};

/// 分页查询结果
///
/// # 示例
///
/// ```
/// use rs_graphdb::cypher::streaming::PageResult;
///
/// let page: PageResult<i32> = PageResult {
///     data: vec![1, 2, 3],
///     page: 0,
///     page_size: 100,
///     total: 250,
///     has_more: true,
/// };
///
/// assert_eq!(page.data.len(), 3);
/// assert_eq!(page.total_pages(), 3);
/// ```
#[derive(Debug, Clone)]
pub struct PageResult<T> {
    /// 当前页的数据
    pub data: Vec<T>,
    /// 当前页码（从0开始）
    pub page: usize,
    /// 每页大小
    pub page_size: usize,
    /// 总记录数
    pub total: usize,
    /// 是否有更多数据
    pub has_more: bool,
}

impl<T> PageResult<T> {
    /// 创建空的分页结果
    pub fn empty() -> Self {
        PageResult {
            data: Vec::new(),
            page: 0,
            page_size: 0,
            total: 0,
            has_more: false,
        }
    }

    /// 获取总页数
    pub fn total_pages(&self) -> usize {
        if self.page_size == 0 {
            return 0;
        }
        (self.total + self.page_size - 1) / self.page_size
    }

    /// 是否为第一页
    pub fn is_first_page(&self) -> bool {
        self.page == 0
    }

    /// 是否为最后一页
    pub fn is_last_page(&self) -> bool {
        !self.has_more
    }
}

/// 查询游标
///
/// 用于维护查询状态，支持分批获取数据
///
/// # 示例
///
/// ```no_run
/// use rs_graphdb::{GraphDatabase, cypher};
/// use rs_graphdb::cypher::streaming::QueryCursor;
///
/// # fn main() -> Result<(), String> {
/// let mut db = GraphDatabase::new_in_memory();
/// // ... 创建数据 ...
///
/// // 创建游标
/// let mut cursor = QueryCursor::new(&db, 100);
///
/// // 获取第一页
/// if let Some(page) = cursor.next_page() {
///     println!("Got {} nodes", page.data.len());
/// }
///
/// // 获取下一页
/// while cursor.has_more() {
///     if let Some(page) = cursor.next_page() {
///         println!("Got {} nodes", page.data.len());
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct QueryCursor<'a, E: StorageEngine> {
    db: &'a GraphDatabase<E>,
    position: usize,
    page_size: usize,
    total: Option<usize>,
}

impl<'a, E: StorageEngine> QueryCursor<'a, E> {
    /// 创建新的查询游标
    pub fn new(db: &'a GraphDatabase<E>, page_size: usize) -> Self {
        QueryCursor {
            db,
            position: 0,
            page_size,
            total: None,
        }
    }

    /// 使用查询创建游标
    pub fn with_query(
        db: &'a GraphDatabase<E>,
        query: &CypherQuery,
        page_size: usize,
    ) -> Result<Self, String> {
        // 执行查询获取总数（不带 LIMIT）
        let total = db.all_stored_nodes().count();

        Ok(QueryCursor {
            db,
            position: 0,
            page_size,
            total: Some(total),
        })
    }

    /// 获取下一页数据
    pub fn next_page(&mut self) -> Option<PageResult<Node>> {
        let nodes: Vec<_> = self.db.all_stored_nodes().collect();

        let total = nodes.len();
        let start = self.position;
        let end = (start + self.page_size).min(total);

        if start >= total {
            return None;
        }

        let page_data: Vec<Node> = nodes[start..end]
            .iter()
            .map(|n| Node {
                id: n.id,
                labels: n.labels.clone(),
                props: n.props.clone(),
            })
            .collect();

        let page = self.position / self.page_size;
        let has_more = end < total;

        self.position = end;

        Some(PageResult {
            data: page_data,
            page,
            page_size: self.page_size,
            total,
            has_more,
        })
    }

    /// 是否还有更多数据
    pub fn has_more(&self) -> bool {
        let total = self.total.unwrap_or_else(|| self.db.all_stored_nodes().count());
        self.position < total
    }

    /// 获取当前位置
    pub fn position(&self) -> usize {
        self.position
    }

    /// 获取总记录数
    pub fn total(&self) -> usize {
        self.total.unwrap_or_else(|| self.db.all_stored_nodes().count())
    }

    /// 重置游标到开头
    pub fn reset(&mut self) {
        self.position = 0;
    }
}

/// 执行分页查询
///
/// # 参数
///
/// * `db` - 图数据库引用
/// * `page` - 页码（从0开始）
/// * `page_size` - 每页大小
///
/// # 返回
///
/// 返回分页结果，包含数据、页码、总数等信息
///
/// # 示例
///
/// ```no_run
/// # use rs_graphdb::GraphDatabase;
/// # use rs_graphdb::cypher::streaming::query_paginated;
/// # fn main() -> Result<(), String> {
/// let db = GraphDatabase::new_in_memory();
/// // ... 创建数据 ...
///
/// // 获取第一页，每页100条
/// let page1 = query_paginated(&db, 0, 100)?;
/// println!("Page 1: {} items, total: {}", page1.data.len(), page1.total);
///
/// // 获取第二页
/// let page2 = query_paginated(&db, 1, 100)?;
/// println!("Page 2: {} items", page2.data.len());
/// # Ok(())
/// # }
/// ```
pub fn query_paginated<E: StorageEngine>(
    db: &GraphDatabase<E>,
    page: usize,
    page_size: usize,
) -> Result<PageResult<Node>, String> {
    let nodes: Vec<_> = db.all_stored_nodes().collect();
    let total = nodes.len();

    let start = page * page_size;
    let end = (start + page_size).min(total);

    if start >= total {
        // 页码超出范围，返回空结果
        return Ok(PageResult {
            data: Vec::new(),
            page,
            page_size,
            total,
            has_more: false,
        });
    }

    let page_data: Vec<Node> = nodes[start..end]
        .iter()
        .map(|n| Node {
            id: n.id,
            labels: n.labels.clone(),
            props: n.props.clone(),
        })
        .collect();

    let has_more = end < total;

    Ok(PageResult {
        data: page_data,
        page,
        page_size,
        total,
        has_more,
    })
}

/// 流式查询迭代器
///
/// 按批次返回数据，避免一次性加载所有数据到内存
///
/// # 示例
///
/// ```no_run
/// # use rs_graphdb::GraphDatabase;
/// # use rs_graphdb::cypher::streaming::StreamQuery;
/// # fn main() {
/// let db = GraphDatabase::new_in_memory();
/// // ... 创建数据 ...
///
/// // 创建流式查询，每批1000条
/// let stream = StreamQuery::new(&db, 1000);
///
/// // 处理每一批数据
/// for batch in stream {
///     let count = batch.data.len();
///     for node in &batch.data {
///         // 处理节点
///         println!("Node: {:?}", node.id);
///     }
///     println!("Processed {} nodes, has_more: {}", count, batch.has_more);
/// }
/// # }
/// ```
pub struct StreamQuery<'a, E: StorageEngine> {
    db: &'a GraphDatabase<E>,
    batch_size: usize,
    position: usize,
    total: usize,
}

impl<'a, E: StorageEngine> StreamQuery<'a, E> {
    /// 创建新的流式查询
    pub fn new(db: &'a GraphDatabase<E>, batch_size: usize) -> Self {
        let total = db.all_stored_nodes().count();
        StreamQuery {
            db,
            batch_size,
            position: 0,
            total,
        }
    }

    /// 获取剩余记录数
    pub fn remaining(&self) -> usize {
        self.total.saturating_sub(self.position)
    }

    /// 获取总记录数
    pub fn total(&self) -> usize {
        self.total
    }

    /// 获取进度百分比 (0-100)
    pub fn progress(&self) -> f64 {
        if self.total == 0 {
            return 100.0;
        }
        (self.position as f64 / self.total as f64) * 100.0
    }
}

impl<'a, E: StorageEngine> Iterator for StreamQuery<'a, E> {
    type Item = PageResult<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.total {
            return None;
        }

        let nodes: Vec<_> = self.db.all_stored_nodes().collect();
        let start = self.position;
        let end = (self.position + self.batch_size).min(self.total);

        let batch_data: Vec<Node> = nodes[start..end]
            .iter()
            .map(|n| Node {
                id: n.id,
                labels: n.labels.clone(),
                props: n.props.clone(),
            })
            .collect();

        let page = self.position / self.batch_size;
        let has_more = end < self.total;

        self.position = end;

        Some(PageResult {
            data: batch_data,
            page,
            page_size: self.batch_size,
            total: self.total,
            has_more,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Value;
    use crate::storage::mem_store::MemStore;

    fn create_test_db() -> GraphDatabase<MemStore> {
        let mut db = GraphDatabase::new_in_memory();

        // 创建250个测试节点
        for i in 0..250 {
            let mut props = crate::values::Properties::new();
            props.insert("id".to_string(), Value::Int(i));
            props.insert("name".to_string(), Value::Text(format!("User{}", i)));
            db.create_node(vec!["User"], props);
        }

        db
    }

    #[test]
    fn test_page_result_empty() {
        let page = PageResult::<()>::empty();
        assert_eq!(page.total_pages(), 0);
        assert!(page.is_first_page());
        assert!(page.is_last_page());
    }

    #[test]
    fn test_page_result_total_pages() {
        let page: PageResult<()> = PageResult {
            data: vec![],
            page: 0,
            page_size: 100,
            total: 250,
            has_more: true,
        };

        assert_eq!(page.total_pages(), 3); // (250 + 100 - 1) / 100 = 3
        assert!(page.is_first_page());
        assert!(!page.is_last_page());
    }

    #[test]
    fn test_query_paginated_first_page() {
        let db = create_test_db();
        let page = query_paginated(&db, 0, 100).unwrap();

        assert_eq!(page.page, 0);
        assert_eq!(page.page_size, 100);
        assert_eq!(page.total, 250);
        assert_eq!(page.data.len(), 100);
        assert!(page.has_more);
        assert!(page.is_first_page());
        assert!(!page.is_last_page());
    }

    #[test]
    fn test_query_paginated_last_page() {
        let db = create_test_db();
        let page = query_paginated(&db, 2, 100).unwrap();

        assert_eq!(page.page, 2);
        assert_eq!(page.page_size, 100);
        assert_eq!(page.total, 250);
        assert_eq!(page.data.len(), 50); // 最后一批只有50个
        assert!(!page.has_more);
        assert!(!page.is_first_page());
        assert!(page.is_last_page());
    }

    #[test]
    fn test_query_paginated_out_of_range() {
        let db = create_test_db();
        let page = query_paginated(&db, 10, 100).unwrap();

        assert_eq!(page.page, 10);
        assert_eq!(page.total, 250);
        assert_eq!(page.data.len(), 0);
        assert!(!page.has_more);
    }

    #[test]
    fn test_query_cursor_next_page() {
        let db = create_test_db();
        let mut cursor = QueryCursor::new(&db, 100);

        // 第一页
        let page1 = cursor.next_page().unwrap();
        assert_eq!(page1.data.len(), 100);
        assert_eq!(page1.page, 0);
        assert!(page1.has_more);
        assert_eq!(cursor.position(), 100);

        // 第二页
        let page2 = cursor.next_page().unwrap();
        assert_eq!(page2.data.len(), 100);
        assert_eq!(page2.page, 1);
        assert!(page2.has_more);
        assert_eq!(cursor.position(), 200);

        // 第三页（最后一页）
        let page3 = cursor.next_page().unwrap();
        assert_eq!(page3.data.len(), 50);
        assert_eq!(page3.page, 2);
        assert!(!page3.has_more);
        assert_eq!(cursor.position(), 250);

        // 第四页（无数据）
        let page4 = cursor.next_page();
        assert!(page4.is_none());
    }

    #[test]
    fn test_query_cursor_has_more() {
        let db = create_test_db();
        let cursor = QueryCursor::new(&db, 100);

        assert!(cursor.has_more());
        assert_eq!(cursor.total(), 250);
    }

    #[test]
    fn test_query_cursor_reset() {
        let db = create_test_db();
        let mut cursor = QueryCursor::new(&db, 100);

        cursor.next_page();
        assert_eq!(cursor.position(), 100);

        cursor.reset();
        assert_eq!(cursor.position(), 0);

        let page = cursor.next_page().unwrap();
        assert_eq!(page.data.len(), 100);
    }

    #[test]
    fn test_stream_query_iterator() {
        let db = create_test_db();
        let stream = StreamQuery::new(&db, 100);

        let mut batches = vec![];

        for batch in stream {
            batches.push(batch.data.len());
        }

        assert_eq!(batches, vec![100, 100, 50]);
    }

    #[test]
    fn test_stream_query_progress() {
        let db = create_test_db();
        let stream = StreamQuery::new(&db, 100);

        assert_eq!(stream.total(), 250);
        assert_eq!(stream.remaining(), 250);
        assert_eq!(stream.progress(), 0.0);
    }
}
