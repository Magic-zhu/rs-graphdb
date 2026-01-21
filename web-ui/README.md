# rs-graphdb Web UI

基于 Vue 3 + Pinia + Tailwind CSS 的图数据库可视化管理界面，参考 Neo4j Browser 设计。

## 技术栈

- **Vue 3** - 渐进式 JavaScript 框架
- **Pinia** - Vue 状态管理库
- **Tailwind CSS** - 实用优先的 CSS 框架
- **Vite** - 下一代前端构建工具
- **TypeScript** - JavaScript 的超集
- **@antv/g6** - 图可视化库

## 功能特性

### 查询功能
- **查询编辑器** - 支持 Cypher/Gremlin 风格查询
- **查询历史** - 自动保存最近 100 条查询记录
- **收藏夹系统** - 保存常用查询，支持标签分类
- **命令系统** - 支持 `:sysinfo`, `:queries`, `:stats` 等命令

### 数据管理
- **节点管理** - 创建、查看、删除节点
- **关系管理** - 创建、查看、删除关系
- **属性编辑** - JSON 格式编辑节点/关系属性

### 可视化功能
- **多种布局** - 力导向、环形、网格、同心圆、辐射、随机
- **样式定制** - 节点大小、颜色方案、边样式
- **物理引擎** - 可开关的物理模拟
- **过滤功能** - 按节点度数过滤
- **导出功能** - 导出为 PNG、SVG、JSON、CSV

### 系统监控
- **数据库统计** - 节点数、关系数、标签类型
- **系统信息** - 存储大小、ID 分配、页面缓存、事务信息
- **实时状态** - 连接状态、数据库状态

## 开发

### 安装依赖

```bash
cd web-ui
npm install
```

### 启动开发服务器

```bash
npm run dev
```

开发服务器将在 `http://localhost:5173` 启动。

API 请求将被代理到 `http://127.0.0.1:3000`（Rust 后端）。

### 类型检查

```bash
npm run type-check
```

## 构建

构建生产版本，输出到 `../static/` 目录：

```bash
npm run build
```

## 项目结构

```
web-ui/
├── src/
│   ├── api/                  # API 服务层
│   │   └── index.ts          # API 客户端
│   ├── components/           # Vue 组件
│   │   ├── QueryEditor.vue   # 查询编辑器
│   │   ├── SystemInfo.vue    # 系统信息面板
│   │   ├── ExportDialog.vue  # 导出对话框
│   │   ├── VisualizationControls.vue  # 可视化控制
│   │   ├── GraphView.vue     # 图可视化
│   │   ├── NodeDetails.vue   # 节点详情
│   │   ├── PanelSection.vue  # 面板区块
│   │   └── StatRow.vue       # 统计行
│   ├── stores/               # Pinia 状态管理
│   │   ├── graph.ts          # 图数据 store
│   │   ├── visualization.ts  # 可视化 store
│   │   ├── queryHistory.ts   # 查询历史 store
│   │   ├── favorites.ts      # 收藏夹 store
│   │   └── commands.ts       # 命令系统
│   ├── types/                # TypeScript 类型定义
│   │   ├── graph.ts          # 图数据类型
│   │   └── query.ts          # 查询相关类型
│   ├── utils/                # 工具函数
│   │   └── export.ts         # 导出工具
│   ├── views/                # 页面组件
│   │   └── HomeView.vue      # 主页面
│   ├── App.vue               # 根组件
│   ├── main.ts               # 应用入口
│   └── style.css             # 全局样式
├── index.html                # HTML 模板
├── vite.config.ts            # Vite 配置
├── tailwind.config.js        # Tailwind CSS 配置
├── tsconfig.json             # TypeScript 配置
└── package.json              # 项目配置

```

## 与 Rust 项目集成

1. 构建 Web UI：
   ```bash
   cd web-ui
   npm run build
   ```

2. 构建的文件将输出到 `static/` 目录

3. 启动 Rust 服务器：
   ```bash
   cargo run --example demo_server
   ```

4. 访问 `http://127.0.0.1:3000/ui` 查看界面
