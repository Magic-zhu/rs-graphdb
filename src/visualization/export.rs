// 图导出功能
//
// 提供多种图导出格式：
// - JSON格式（用于前端可视化库）
// - Graphviz DOT格式

use crate::visualization::{GraphView, GraphFormat, VisNode, VisEdge};
use serde_json;

/// 图导出trait
pub trait GraphExport {
    /// 导出图为字符串
    fn export(graph: &GraphView) -> Result<String, String>;
}

/// JSON导出
///
/// 导出为JSON格式，可用于D3.js、Cytoscape.js等前端库
pub struct JsonExport;

impl GraphExport for JsonExport {
    fn export(graph: &GraphView) -> Result<String, String> {
        // 导出为Cytoscape.js兼容格式
        let cytoscape_format = serde_json::json!({
            "data": [
                {
                    "nodes": graph.nodes.iter().map(|node| {
                        let mut obj = serde_json::json!({
                            "data": {
                                "id": node.id,
                                "labels": node.labels,
                            }
                        });

                        // 添加属性
                        if let Some(obj_data) = obj.get_mut("data") {
                            if let Some(data_obj) = obj_data.as_object_mut() {
                                for (key, value) in &node.properties {
                                    data_obj.insert(key.clone(), serde_json::to_value(value).unwrap_or(serde_json::Value::Null));
                                }
                            }
                        }

                        // 添加位置
                        if let Some(pos) = &node.position {
                            if let Some(obj_pos) = obj.get_mut("data") {
                                if let Some(data_obj) = obj_pos.as_object_mut() {
                                    data_obj.insert("_x".to_string(), serde_json::json!(pos.x));
                                    data_obj.insert("_y".to_string(), serde_json::json!(pos.y));
                                }
                            }
                        }

                        // 添加样式
                        if let Some(style) = &node.style {
                            if let Some(obj_style) = obj.get_mut("data") {
                                if let Some(data_obj) = obj_style.as_object_mut() {
                                    if let Some(color) = &style.color {
                                        data_obj.insert("_color".to_string(), serde_json::json!(color));
                                    }
                                    if let Some(size) = style.size {
                                        data_obj.insert("_size".to_string(), serde_json::json!(size));
                                    }
                                }
                            }
                        }

                        obj
                    }).collect::<Vec<_>>()
                },
                {
                    "edges": graph.edges.iter().map(|edge| {
                        let mut obj = serde_json::json!({
                            "data": {
                                "id": edge.id.clone().unwrap_or_else(|| format!("{}_{}_{}", edge.source, edge.rel_type, edge.target)),
                                "source": edge.source,
                                "target": edge.target,
                                "label": edge.rel_type,
                            }
                        });

                        // 添加属性
                        if let Some(obj_data) = obj.get_mut("data") {
                            if let Some(data_obj) = obj_data.as_object_mut() {
                                for (key, value) in &edge.properties {
                                    data_obj.insert(key.clone(), serde_json::to_value(value).unwrap_or(serde_json::Value::Null));
                                }
                            }
                        }

                        obj
                    }).collect::<Vec<_>>()
                }
            ],
            "metadata": graph.metadata
        });

        serde_json::to_string_pretty(&cytoscape_format)
            .map_err(|e| format!("JSON serialization error: {}", e))
    }
}

/// Graphviz DOT导出
///
/// 导出为Graphviz DOT格式，可用于生成图片
pub struct DotExport;

impl GraphExport for DotExport {
    fn export(graph: &GraphView) -> Result<String, String> {
        let mut dot = String::new();

        // 图类型
        dot.push_str("digraph G {\n");

        // 全局设置
        dot.push_str("  node [shape=box, style=rounded];\n");
        dot.push_str("  rankdir=TB;\n");

        // 添加元数据作为注释
        if let Some(title) = &graph.metadata.title {
            dot.push_str(&format!("  // Title: {}\n", title));
        }
        if let Some(layout) = &graph.metadata.layout_algorithm {
            dot.push_str(&format!("  // Layout: {}\n", layout));
        }
        dot.push_str(&format!("  // Nodes: {}, Edges: {}\n", graph.metadata.node_count, graph.metadata.edge_count));
        dot.push_str("\n");

        // 添加节点
        for node in &graph.nodes {
            let label = node.display_name();
            dot.push_str(&format!("  \"{}\"", node.id));

            let mut attrs = Vec::new();

            // 标签
            attrs.push(format!("label=\"{}\"", escape_dot_string(&label)));

            // 颜色
            if let Some(style) = &node.style {
                if let Some(color) = &style.color {
                    attrs.push(format!("fillcolor=\"{}\", style=filled", color));
                }
            }

            // 形状
            if let Some(style) = &node.style {
                if let Some(shape) = &style.shape {
                    attrs.push(format!("shape={}", shape));
                }
            }

            // 位置（如果已计算）
            if let Some(pos) = &node.position {
                attrs.push(format!("pos=\"{},{}\"", pos.x, pos.y));
            }

            if !attrs.is_empty() {
                dot.push_str(&format!(" [{}]", attrs.join(", ")));
            }

            dot.push_str(";\n");
        }

        dot.push_str("\n");

        // 添加边
        for edge in &graph.edges {
            dot.push_str(&format!("  \"{}\" -> \"{}\"", edge.source, edge.target));

            let mut attrs = Vec::new();

            // 标签
            attrs.push(format!("label=\"{}\"", escape_dot_string(&edge.rel_type)));

            // 颜色
            if let Some(style) = &edge.style {
                if let Some(color) = &style.color {
                    attrs.push(format!("color=\"{}\"", color));
                }
            }

            // 样式
            if let Some(style) = &edge.style {
                if let Some(style_str) = &style.style {
                    attrs.push(format!("style={}", style_str));
                }
            }

            if !attrs.is_empty() {
                dot.push_str(&format!(" [{}]", attrs.join(", ")));
            }

            dot.push_str(";\n");
        }

        dot.push_str("}\n");

        Ok(dot)
    }
}

/// 转义DOT字符串
fn escape_dot_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
