# rs-graphdb Web UI

基于 Vue 3 + Pinia + Tailwind CSS 的图数据库可视化管理界面。

## 技术栈

- **Vue 3** - 渐进式 JavaScript 框架
- **Pinia** - Vue 状态管理库
- **Tailwind CSS** - 实用优先的 CSS 框架
- **Vite** - 下一代前端构建工具
- **TypeScript** - JavaScript 的超集
- **vis-network** - 网络可视化库

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
│   ├── api/           # API 服务层
│   ├── assets/        # 静态资源
│   ├── components/    # Vue 组件
│   ├── router/        # 路由配置
│   ├── stores/        # Pinia 状态管理
│   ├── types/         # TypeScript 类型定义
│   ├── views/         # 页面组件
│   ├── App.vue        # 根组件
│   ├── main.ts        # 应用入口
│   └── style.css      # 全局样式
├── index.html         # HTML 模板
├── vite.config.ts     # Vite 配置
├── tailwind.config.js # Tailwind CSS 配置
├── tsconfig.json      # TypeScript 配置
└── package.json       # 项目配置

```

## 功能特性

### 仪表盘
- 数据库统计概览
- 标签和关系类型列表
- 快捷操作按钮

### 节点管理
- 创建节点（支持 JSON 属性）
- 节点列表浏览
- 节点搜索

### 关系管理
- 创建关系（指定起止节点）
- 关系列表查看

### 查询功能
- 按标签查询
- 按属性精确查询
- 全局模糊搜索

### 图可视化
- 交互式网络图
- 节点详情面板
- 邻居节点展开
- 物理引擎切换
- 节点定位和聚焦

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
