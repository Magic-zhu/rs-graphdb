// 图布局算法
//
// 提供多种图布局算法：
// - 圆形布局
// - 力导向布局
// - 层次布局

use crate::visualization::{GraphView, VisNode, VisEdge, Position};
use std::collections::HashMap;

/// 布局配置
#[derive(Debug, Clone)]
pub struct LayoutConfig {
    /// 画布宽度
    pub width: f64,
    /// 画布高度
    pub height: f64,
    /// 节点间距
    pub node_spacing: f64,
    /// 迭代次数（用于力导向布局）
    pub iterations: usize,
}

impl Default for LayoutConfig {
    fn default() -> Self {
        Self {
            width: 800.0,
            height: 600.0,
            node_spacing: 50.0,
            iterations: 100,
        }
    }
}

impl LayoutConfig {
    pub fn new(width: f64, height: f64) -> Self {
        Self {
            width,
            height,
            ..Default::default()
        }
    }

    pub fn with_node_spacing(mut self, spacing: f64) -> Self {
        self.node_spacing = spacing;
        self
    }

    pub fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = iterations;
        self
    }
}

/// 布局算法trait
pub trait Layout {
    /// 应用布局到图
    fn apply(&mut self, graph: &mut GraphView);

    /// 获取布局名称
    fn name(&self) -> &str {
        "unknown"
    }
}

/// 圆形布局
///
/// 将节点排列成一个圆形
#[derive(Debug)]
pub struct CircleLayout {
    config: LayoutConfig,
}

impl CircleLayout {
    pub fn new(config: LayoutConfig) -> Self {
        Self { config }
    }

    pub fn with_default_config() -> Self {
        Self::new(LayoutConfig::default())
    }
}

impl Layout for CircleLayout {
    fn apply(&mut self, graph: &mut GraphView) {
        let node_count = graph.node_count();
        if node_count == 0 {
            return;
        }

        let center_x = self.config.width / 2.0;
        let center_y = self.config.height / 2.0;
        let radius = self.config.node_spacing * node_count as f64 / (2.0 * std::f64::consts::PI);

        for (i, node) in graph.nodes.iter_mut().enumerate() {
            let angle = 2.0 * std::f64::consts::PI * i as f64 / node_count as f64;
            let x = center_x + radius * angle.cos();
            let y = center_y + radius * angle.sin();
            node.position = Some(Position::new(x, y));
        }

        graph.metadata.layout_algorithm = Some("Circle".to_string());
    }

    fn name(&self) -> &str {
        "Circle"
    }
}

/// 力导向布局
///
/// 使用力导向算法排列节点，模拟物理力场
#[derive(Debug)]
pub struct ForceDirectedLayout {
    config: LayoutConfig,
    /// 斥力常数
    repulsion: f64,
    /// 弹簧常数
    spring_length: f64,
    /// 弹簧刚度
    spring_k: f64,
    /// 阻尼系数
    damping: f64,
}

impl ForceDirectedLayout {
    pub fn new(config: LayoutConfig) -> Self {
        Self {
            config,
            repulsion: 1000.0,
            spring_length: 100.0,
            spring_k: 0.05,
            damping: 0.9,
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(LayoutConfig::default())
    }

    /// 设置斥力常数
    pub fn with_repulsion(mut self, repulsion: f64) -> Self {
        self.repulsion = repulsion;
        self
    }

    /// 设置弹簧长度
    pub fn with_spring_length(mut self, length: f64) -> Self {
        self.spring_length = length;
        self
    }

    /// 设置弹簧刚度
    pub fn with_spring_k(mut self, k: f64) -> Self {
        self.spring_k = k;
        self
    }
}

impl Layout for ForceDirectedLayout {
    fn apply(&mut self, graph: &mut GraphView) {
        let node_count = graph.node_count();
        if node_count == 0 {
            return;
        }

        // 初始化位置（随机分布）
        let mut positions: HashMap<crate::storage::NodeId, Position> = HashMap::new();
        let mut velocities: HashMap<crate::storage::NodeId, (f64, f64)> = HashMap::new();

        use rand::Rng;
        let mut rng = rand::thread_rng();

        for (i, node) in graph.nodes.iter().enumerate() {
            let x = self.config.width * 0.2 + rng.gen::<f64>() * self.config.width * 0.6;
            let y = self.config.height * 0.2 + rng.gen::<f64>() * self.config.height * 0.6;
            positions.insert(node.id, Position::new(x, y));
            velocities.insert(node.id, (0.0, 0.0));
        }

        // 迭代计算力
        for _ in 0..self.config.iterations {
            let mut forces: HashMap<crate::storage::NodeId, (f64, f64)> = HashMap::new();

            // 计算斥力（节点之间互相排斥）
            for (i, node_a) in graph.nodes.iter().enumerate() {
                let mut fx = 0.0;
                let mut fy = 0.0;

                for (j, node_b) in graph.nodes.iter().enumerate() {
                    if i == j {
                        continue;
                    }

                    let pos_a = positions.get(&node_a.id).unwrap();
                    let pos_b = positions.get(&node_b.id).unwrap();

                    let dx = pos_a.x - pos_b.x;
                    let dy = pos_a.y - pos_b.y;
                    let dist_sq = dx * dx + dy * dy;
                    let dist = dist_sq.sqrt().max(1.0);

                    let force = self.repulsion / dist_sq;
                    fx += force * dx / dist;
                    fy += force * dy / dist;
                }

                forces.insert(node_a.id, (fx, fy));
            }

            // 计算弹簧力（连接的节点互相吸引）
            for edge in &graph.edges {
                let pos_source = positions.get(&edge.source).unwrap();
                let pos_target = positions.get(&edge.target).unwrap();

                let dx = pos_target.x - pos_source.x;
                let dy = pos_target.y - pos_source.y;
                let dist = (dx * dx + dy * dy).sqrt().max(1.0);

                let force = self.spring_k * (dist - self.spring_length);
                let fx = force * dx / dist;
                let fy = force * dy / dist;

                if let Some((f_x, f_y)) = forces.get_mut(&edge.source) {
                    *f_x += fx;
                    *f_y += fy;
                }

                if let Some((f_x, f_y)) = forces.get_mut(&edge.target) {
                    *f_x -= fx;
                    *f_y -= fy;
                }
            }

            // 应用力并更新位置
            for node in &graph.nodes {
                if let (Some(force), Some(vel), Some(pos)) = (
                    forces.get(&node.id),
                    velocities.get_mut(&node.id),
                    positions.get_mut(&node.id)
                ) {
                    // 更新速度（带阻尼）
                    vel.0 = (vel.0 + force.0) * self.damping;
                    vel.1 = (vel.1 + force.1) * self.damping;

                    // 更新位置
                    pos.x += vel.0;
                    pos.y += vel.1;

                    // 边界约束
                    pos.x = pos.x.max(10.0).min(self.config.width - 10.0);
                    pos.y = pos.y.max(10.0).min(self.config.height - 10.0);
                }
            }
        }

        // 应用位置
        for node in graph.nodes.iter_mut() {
            node.position = positions.get(&node.id).cloned();
        }

        graph.metadata.layout_algorithm = Some("ForceDirected".to_string());
    }

    fn name(&self) -> &str {
        "ForceDirected"
    }
}

/// 层次布局
///
/// 按照层次结构排列节点（从上到下或从左到右）
#[derive(Debug)]
pub struct HierarchicalLayout {
    config: LayoutConfig,
    /// 层次方向
    direction: HierarchicalDirection,
    /// 层内间距
    layer_spacing: f64,
    /// 节点间距
    node_spacing: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HierarchicalDirection {
    /// 从上到下
    TopToBottom,
    /// 从左到右
    LeftToRight,
}

impl HierarchicalLayout {
    pub fn new(config: LayoutConfig) -> Self {
        Self {
            config,
            direction: HierarchicalDirection::TopToBottom,
            layer_spacing: 100.0,
            node_spacing: 80.0,
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(LayoutConfig::default())
    }

    /// 设置层次方向
    pub fn with_direction(mut self, direction: HierarchicalDirection) -> Self {
        self.direction = direction;
        self
    }

    /// 设置层间距
    pub fn with_layer_spacing(mut self, spacing: f64) -> Self {
        self.layer_spacing = spacing;
        self
    }

    /// 计算节点的层次（使用BFS）
    fn calculate_layers(&self, graph: &GraphView) -> HashMap<crate::storage::NodeId, usize> {
        let mut layers: HashMap<crate::storage::NodeId, usize> = HashMap::new();
        let mut visited = std::collections::HashSet::new();

        // 构建邻接表
        let mut adj: HashMap<crate::storage::NodeId, Vec<crate::storage::NodeId>> = HashMap::new();
        let mut reverse_adj: HashMap<crate::storage::NodeId, Vec<crate::storage::NodeId>> = HashMap::new();

        for node in &graph.nodes {
            adj.entry(node.id).or_default();
            reverse_adj.entry(node.id).or_default();
        }

        for edge in &graph.edges {
            adj.entry(edge.source).or_default().push(edge.target);
            reverse_adj.entry(edge.target).or_default().push(edge.source);
        }

        // 找到根节点（没有入边的节点）
        let mut roots: Vec<crate::storage::NodeId> = graph.nodes
            .iter()
            .filter(|n| reverse_adj.get(&n.id).map_or(true, |v| v.is_empty()))
            .map(|n| n.id)
            .collect();

        // 如果没有根节点，使用所有节点
        if roots.is_empty() {
            roots = graph.nodes.iter().map(|n| n.id).collect();
        }

        // BFS计算层次
        for root in roots {
            if visited.contains(&root) {
                continue;
            }

            let mut queue = std::collections::VecDeque::new();
            queue.push_back((root, 0));
            visited.insert(root);

            while let Some((node_id, layer)) = queue.pop_front() {
                layers.entry(node_id).or_insert(layer);

                if let Some(neighbors) = adj.get(&node_id) {
                    for &neighbor in neighbors {
                        if !visited.contains(&neighbor) {
                            visited.insert(neighbor);
                            queue.push_back((neighbor, layer + 1));
                        }
                    }
                }
            }
        }

        // 处理未访问的节点（孤立节点）
        for node in &graph.nodes {
            if !visited.contains(&node.id) {
                layers.insert(node.id, 0);
            }
        }

        layers
    }
}

impl Layout for HierarchicalLayout {
    fn apply(&mut self, graph: &mut GraphView) {
        if graph.node_count() == 0 {
            return;
        }

        let layers = self.calculate_layers(graph);

        // 按层分组节点
        let mut layer_nodes: HashMap<usize, Vec<crate::storage::NodeId>> = HashMap::new();
        for (node_id, layer) in &layers {
            layer_nodes.entry(*layer).or_default().push(*node_id);
        }

        // 计算每层的位置
        let max_layer = layer_nodes.keys().cloned().max().unwrap_or(0);

        for (layer, nodes) in layer_nodes {
            let layer_size = nodes.len() as f64;

            for (i, node_id) in nodes.iter().enumerate() {
                let pos = if self.direction == HierarchicalDirection::TopToBottom {
                    let x = self.config.width / 2.0
                        + (i as f64 - layer_size / 2.0) * self.node_spacing;
                    let y = 50.0 + layer as f64 * self.layer_spacing;
                    Position::new(x, y)
                } else {
                    let x = 50.0 + layer as f64 * self.layer_spacing;
                    let y = self.config.height / 2.0
                        + (i as f64 - layer_size / 2.0) * self.node_spacing;
                    Position::new(x, y)
                };

                if let Some(node) = graph.nodes.iter_mut().find(|n| n.id == *node_id) {
                    node.position = Some(pos);
                }
            }
        }

        graph.metadata.layout_algorithm = Some("Hierarchical".to_string());
    }

    fn name(&self) -> &str {
        "Hierarchical"
    }
}

/// 用于布局的临时节点结构
#[derive(Debug, Clone)]
pub struct LayoutNode {
    pub id: crate::storage::NodeId,
    pub position: Position,
}

/// 用于布局的临时边结构
#[derive(Debug, Clone)]
pub struct LayoutEdge {
    pub source: crate::storage::NodeId,
    pub target: crate::storage::NodeId,
}
