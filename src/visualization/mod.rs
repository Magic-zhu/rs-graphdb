// 图可视化模块
//
// 提供图数据的可视化和布局功能，包括：
// - 图数据序列化（JSON格式）
// - Graphviz DOT格式导出
// - 多种布局算法（圆形、力导向、层次布局）

pub mod layout;
pub mod export;

pub use layout::{
    Layout, LayoutConfig, CircleLayout, ForceDirectedLayout, HierarchicalLayout,
    LayoutNode, LayoutEdge,
};
pub use export::{
    GraphExport, JsonExport, DotExport,
};

use crate::storage::NodeId;
use crate::values::Properties;
use serde::{Deserialize, Serialize};

/// 可视化的图视图
///
/// 包含用于可视化的节点和边，以及可选的布局信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphView {
    /// 可视化的节点
    pub nodes: Vec<VisNode>,
    /// 可视化的边
    pub edges: Vec<VisEdge>,
    /// 图的元数据
    pub metadata: GraphMetadata,
}

impl GraphView {
    /// 创建一个新的空图视图
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            metadata: GraphMetadata::default(),
        }
    }

    /// 添加节点
    pub fn add_node(&mut self, node: VisNode) {
        self.nodes.push(node);
        self.metadata.node_count = self.nodes.len();
    }

    /// 添加边
    pub fn add_edge(&mut self, edge: VisEdge) {
        self.edges.push(edge);
        self.metadata.edge_count = self.edges.len();
    }

    /// 应用布局
    pub fn apply_layout(&mut self, layout: &mut impl Layout) {
        layout.apply(self);
    }

    /// 导出为指定格式
    pub fn export(&self, format: GraphFormat) -> Result<String, String> {
        match format {
            GraphFormat::Json => JsonExport::export(self),
            GraphFormat::Dot => DotExport::export(self),
        }
    }

    /// 获取节点数量
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// 获取边数量
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

impl Default for GraphView {
    fn default() -> Self {
        Self::new()
    }
}

/// 可视化的节点
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisNode {
    /// 节点ID
    pub id: NodeId,
    /// 节点标签
    pub labels: Vec<String>,
    /// 节点属性
    pub properties: Properties,
    /// 可选的布局位置
    pub position: Option<Position>,
    /// 可选的显示样式
    pub style: Option<NodeStyle>,
}

impl VisNode {
    /// 创建一个新的可视化节点
    pub fn new(id: NodeId, labels: Vec<String>, properties: Properties) -> Self {
        Self {
            id,
            labels,
            properties,
            position: None,
            style: None,
        }
    }

    /// 设置位置
    pub fn with_position(mut self, x: f64, y: f64) -> Self {
        self.position = Some(Position { x, y });
        self
    }

    /// 设置样式
    pub fn with_style(mut self, style: NodeStyle) -> Self {
        self.style = Some(style);
        self
    }

    /// 获取主标签（第一个标签）
    pub fn primary_label(&self) -> Option<&str> {
        self.labels.first().map(|s| s.as_str())
    }

    /// 获取显示名称（优先使用name属性，否则使用ID）
    pub fn display_name(&self) -> String {
        self.properties
            .get("name")
            .and_then(|v| if let crate::values::Value::Text(s) = v { Some(s.clone()) } else { None })
            .unwrap_or_else(|| format!("{}", self.id))
    }
}

/// 可视化的边
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisEdge {
    /// 边ID
    pub id: Option<String>,
    /// 起始节点ID
    pub source: NodeId,
    /// 目标节点ID
    pub target: NodeId,
    /// 关系类型
    pub rel_type: String,
    /// 边属性
    pub properties: Properties,
    /// 可选的显示样式
    pub style: Option<EdgeStyle>,
}

impl VisEdge {
    /// 创建一个新的可视化边
    pub fn new(source: NodeId, target: NodeId, rel_type: String, properties: Properties) -> Self {
        Self {
            id: None,
            source,
            target,
            rel_type,
            properties,
            style: None,
        }
    }

    /// 设置ID
    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// 设置样式
    pub fn with_style(mut self, style: EdgeStyle) -> Self {
        self.style = Some(style);
        self
    }
}

/// 节点样式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStyle {
    /// 背景颜色
    pub color: Option<String>,
    /// 边框颜色
    pub border_color: Option<String>,
    /// 边框宽度
    pub border_width: Option<f64>,
    /// 节点大小
    pub size: Option<f64>,
    /// 节点形状（circle, rect, ellipse等）
    pub shape: Option<String>,
    /// 标签字体大小
    pub font_size: Option<f64>,
}

impl NodeStyle {
    /// 创建默认样式
    pub fn new() -> Self {
        Self {
            color: None,
            border_color: None,
            border_width: None,
            size: None,
            shape: None,
            font_size: None,
        }
    }

    /// 设置颜色
    pub fn with_color(mut self, color: String) -> Self {
        self.color = Some(color);
        self
    }

    /// 设置大小
    pub fn with_size(mut self, size: f64) -> Self {
        self.size = Some(size);
        self
    }

    /// 设置形状
    pub fn with_shape(mut self, shape: String) -> Self {
        self.shape = Some(shape);
        self
    }
}

impl Default for NodeStyle {
    fn default() -> Self {
        Self::new()
    }
}

/// 边样式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeStyle {
    /// 边颜色
    pub color: Option<String>,
    /// 边宽度
    pub width: Option<f64>,
    /// 边样式（solid, dashed, dotted等）
    pub style: Option<String>,
    /// 标签字体大小
    pub font_size: Option<f64>,
}

impl EdgeStyle {
    /// 创建默认样式
    pub fn new() -> Self {
        Self {
            color: None,
            width: None,
            style: None,
            font_size: None,
        }
    }

    /// 设置颜色
    pub fn with_color(mut self, color: String) -> Self {
        self.color = Some(color);
        self
    }

    /// 设置宽度
    pub fn with_width(mut self, width: f64) -> Self {
        self.width = Some(width);
        self
    }
}

impl Default for EdgeStyle {
    fn default() -> Self {
        Self::new()
    }
}

/// 图元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphMetadata {
    /// 节点数量
    pub node_count: usize,
    /// 边数量
    pub edge_count: usize,
    /// 图标题
    pub title: Option<String>,
    /// 创建时间
    pub created_at: Option<String>,
    /// 布局算法
    pub layout_algorithm: Option<String>,
}

impl Default for GraphMetadata {
    fn default() -> Self {
        Self {
            node_count: 0,
            edge_count: 0,
            title: None,
            created_at: None,
            layout_algorithm: None,
        }
    }
}

/// 图导出格式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphFormat {
    /// JSON格式
    Json,
    /// Graphviz DOT格式
    Dot,
}

/// 位置坐标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

impl Position {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    /// 计算到另一个位置的距离
    pub fn distance_to(&self, other: &Position) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }
}
