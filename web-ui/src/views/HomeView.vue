<template>
  <div class="flex flex-1 min-h-0">
    <!-- Left Sidebar -->
    <aside class="w-80 bg-gray-900 border-r border-gray-800 flex flex-col shrink-0">
      <!-- Tabs -->
      <div class="flex border-b border-gray-800">
        <button
          v-for="tab in tabs"
          :key="tab.key"
          @click="activeTab = tab.key"
          :class="[
            'flex-1 px-4 py-3 text-sm font-medium transition-colors',
            activeTab === tab.key
              ? 'text-primary-400 border-b-2 border-primary-500'
              : 'text-gray-500 hover:text-gray-300',
          ]"
        >
          {{ tab.label }}
        </button>
      </div>

      <!-- Tab Content -->
      <div class="flex-1 overflow-y-auto p-4">
        <!-- Query Tab -->
        <div v-if="activeTab === 'query'" class="space-y-4">
          <CodeMirrorEditor @execute="handleExecuteQuery" />
        </div>

        <!-- Dashboard Tab -->
        <div v-if="activeTab === 'dashboard'" class="space-y-4">
          <PanelSection title="数据库统计">
            <StatRow label="节点总数" :value="graphStore.stats.node_count" />
            <StatRow label="关系总数" :value="graphStore.stats.rel_count" />
            <StatRow label="标签类型" :value="graphStore.stats.labels.length" />
            <StatRow label="关系类型" :value="graphStore.stats.rel_types.length" />
          </PanelSection>

          <PanelSection title="快捷操作">
            <button @click="loadFullGraph" class="btn btn-primary w-full mb-2">
              加载完整图
            </button>
            <button @click="clearVisualization" class="btn btn-secondary w-full mb-2">
              清空可视化
            </button>
            <button @click="refreshStats" class="btn btn-secondary w-full">
              刷新统计
            </button>
          </PanelSection>

          <PanelSection title="标签列表">
            <div class="flex flex-wrap gap-2">
              <span
                v-for="label in graphStore.stats.labels"
                :key="label"
                class="px-2 py-1 bg-gray-800 text-gray-300 text-xs rounded-full"
              >
                {{ label }}
              </span>
            </div>
          </PanelSection>

          <PanelSection title="关系类型">
            <div class="flex flex-wrap gap-2">
              <span
                v-for="type in graphStore.stats.rel_types"
                :key="type"
                class="px-2 py-1 bg-gray-800 text-gray-300 text-xs rounded-full"
              >
                {{ type }}
              </span>
            </div>
          </PanelSection>
        </div>

        <!-- System Info Tab -->
        <div v-if="activeTab === 'system'" class="space-y-4">
          <SystemInfo @execute-command="handleExecuteCommand" />
        </div>

        <!-- Nodes Tab -->
        <div v-if="activeTab === 'nodes'" class="space-y-4">
          <PanelSection title="创建节点">
            <form @submit.prevent="handleCreateNode" class="space-y-3">
              <div>
                <label class="block text-sm text-gray-400 mb-1">标签</label>
                <input
                  v-model="nodeForm.label"
                  type="text"
                  class="input"
                  placeholder="例如: User, Person"
                  required
                />
              </div>
              <div>
                <label class="block text-sm text-gray-400 mb-1">属性 (JSON)</label>
                <textarea
                  v-model="nodeForm.properties"
                  class="input"
                  rows="4"
                  placeholder='{"name": "Alice", "age": 30}'
                />
              </div>
              <button type="submit" class="btn btn-primary w-full">创建节点</button>
            </form>
            <div v-if="nodeForm.message" :class="['status-message', nodeForm.messageType]">
              {{ nodeForm.message }}
            </div>
          </PanelSection>

          <PanelSection title="节点列表">
            <input
              v-model="nodeSearch"
              type="text"
              class="input mb-3"
              placeholder="搜索节点..."
            />
            <div class="space-y-1 max-h-64 overflow-y-auto">
              <div
                v-for="node in filteredNodes"
                :key="node.id"
                @click="selectNode(node.id)"
                class="flex justify-between items-center p-2 rounded hover:bg-gray-800 cursor-pointer transition-colors"
              >
                <span class="text-gray-500">#{{ node.id }}</span>
                <span class="text-gray-300 text-sm">{{ node.labels.join(', ') }}</span>
              </div>
              <p v-if="filteredNodes.length === 0" class="text-sm text-gray-500 text-center py-4">
                暂无节点
              </p>
            </div>
          </PanelSection>
        </div>

        <!-- Relations Tab -->
        <div v-if="activeTab === 'relations'" class="space-y-4">
          <PanelSection title="创建关系">
            <form @submit.prevent="handleCreateRel" class="space-y-3">
              <div>
                <label class="block text-sm text-gray-400 mb-1">起始节点 ID</label>
                <input
                  v-model="relForm.start"
                  type="number"
                  class="input"
                  placeholder="起始节点 ID"
                  required
                />
              </div>
              <div>
                <label class="block text-sm text-gray-400 mb-1">目标节点 ID</label>
                <input
                  v-model="relForm.end"
                  type="number"
                  class="input"
                  placeholder="目标节点 ID"
                  required
                />
              </div>
              <div>
                <label class="block text-sm text-gray-400 mb-1">关系类型</label>
                <input
                  v-model="relForm.type"
                  type="text"
                  class="input"
                  placeholder="例如: FRIEND, KNOWS"
                  required
                />
              </div>
              <div>
                <label class="block text-sm text-gray-400 mb-1">属性 (JSON, 可选)</label>
                <textarea
                  v-model="relForm.properties"
                  class="input"
                  rows="2"
                  placeholder='{"since": "2020"}'
                />
              </div>
              <button type="submit" class="btn btn-primary w-full">创建关系</button>
            </form>
            <div v-if="relForm.message" :class="['status-message', relForm.messageType]">
              {{ relForm.message }}
            </div>
          </PanelSection>

          <PanelSection title="关系列表">
            <div class="space-y-1 max-h-64 overflow-y-auto">
              <div
                v-for="rel in graphStore.rels.slice(0, 50)"
                :key="rel.id"
                class="flex justify-between items-center p-2 rounded hover:bg-gray-800 text-sm"
              >
                <span class="text-gray-500">#{{ rel.id }}</span>
                <span class="text-gray-300">{{ rel.typ }}</span>
                <span class="text-gray-400">{{ rel.start }} → {{ rel.end }}</span>
              </div>
              <p v-if="graphStore.rels.length === 0" class="text-sm text-gray-500 text-center py-4">
                暂无关系
              </p>
            </div>
          </PanelSection>
        </div>
      </div>
    </aside>

    <!-- Main Content -->
    <main class="flex-1 flex flex-col relative min-w-0">
      <!-- Toolbar -->
      <div class="h-12 bg-gray-900 border-b border-gray-800 flex items-center px-4 gap-2 shrink-0">
        <input
          v-model.number="focusNodeId"
          type="number"
          class="input max-w-xs mb-0"
          placeholder="输入节点 ID 跳转..."
        />
        <button @click="() => handleFocusNode()" class="btn btn-primary">定位</button>
        <button @click="handleFitGraph" class="btn btn-secondary">适应视图</button>
        <button @click="togglePhysics" class="btn btn-secondary">
          {{ visStore.physicsEnabled ? '禁用物理' : '启用物理' }}
        </button>
        <div class="ml-auto flex items-center gap-2">
          <button
            @click="showVisualizationControls = !showVisualizationControls"
            :class="[
              'px-3 py-1.5 text-sm rounded transition-colors',
              showVisualizationControls
                ? 'bg-primary-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
            ]"
          >
            可视化设置
          </button>
          <button
            @click="showExportDialog = true"
            class="px-3 py-1.5 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 transition-colors"
          >
            导出
          </button>
        </div>
        <span v-if="visStore.selectedNodeId" class="text-sm text-gray-500">
          选中: 节点 #{{ visStore.selectedNodeId }}
        </span>
      </div>

      <!-- Graph View & Visualization Controls -->
      <div class="flex-1 flex relative min-h-0 min-w-0">
        <!-- Graph Container -->
        <div class="flex-1 relative min-h-0 min-w-0">
          <!-- Graph View -->
          <GraphView
            v-show="resultViewMode === 'graph'"
            ref="graphViewRef"
            @closeDetails="visStore.selectNode(null)"
            @expandNeighbors="handleExpandNeighbors"
          />
          <NodeDetails
            v-show="resultViewMode === 'graph'"
            :visible="visStore.selectedNodeId !== null"
            :nodeId="visStore.selectedNodeId"
            @close="visStore.selectNode(null)"
            @focus="handleFocusNode"
            @showNeighbors="handleShowNeighbors"
          />

          <!-- Table View -->
          <TableView
            v-show="resultViewMode === 'table'"
            :viewMode="resultViewMode"
            :data="queryResults"
            :loading="queryLoading"
            :error="queryError"
            @change-view="resultViewMode = $event"
            @focus-node="handleFocusNode"
          />
        </div>

        <!-- Visualization Controls Panel -->
        <VisualizationControls
          v-if="showVisualizationControls && resultViewMode === 'graph'"
          @fit-view="handleFitGraph"
          @zoom-in="handleZoomIn"
          @zoom-out="handleZoomOut"
          @reset-view="handleResetView"
          @show-export="showExportDialog = true"
          @layout-change="handleLayoutChange"
          @style-change="handleStyleChange"
        />
      </div>
    </main>

    <!-- Export Dialog -->
    <ExportDialog v-model:visible="showExportDialog" @close="showExportDialog = false" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useGraphStore } from '@/stores/graph'
import { useVisualizationStore } from '@/stores/visualization'
import { useCommandStore } from '@/stores/commands'
import GraphView from '@/components/GraphView.vue'
import NodeDetails from '@/components/NodeDetails.vue'
import PanelSection from '@/components/PanelSection.vue'
import StatRow from '@/components/StatRow.vue'
import CodeMirrorEditor from '@/components/CodeMirrorEditor.vue'
import SystemInfo from '@/components/SystemInfo.vue'
import VisualizationControls from '@/components/VisualizationControls.vue'
import ExportDialog from '@/components/ExportDialog.vue'
import TableView from '@/components/TableView.vue'

const graphStore = useGraphStore()
const visStore = useVisualizationStore()
const { executeCommand, isCommand } = useCommandStore()

const graphViewRef = ref<InstanceType<typeof GraphView>>()
const showVisualizationControls = ref(false)
const showExportDialog = ref(false)
const resultViewMode = ref<'graph' | 'table'>('graph')
const queryResults = ref<any[]>([])
const queryLoading = ref(false)
const queryError = ref('')

// Tabs
const tabs = [
  { key: 'query', label: '查询' },
  { key: 'dashboard', label: '仪表盘' },
  { key: 'system', label: '系统' },
  { key: 'nodes', label: '节点' },
  { key: 'relations', label: '关系' },
]
const activeTab = ref('query')

// Node form
const nodeForm = ref({
  label: '',
  properties: '',
  message: '',
  messageType: 'success' as 'success' | 'error',
})

const nodeSearch = ref('')

// Relation form
const relForm = ref({
  start: 0,
  end: 0,
  type: '',
  properties: '',
  message: '',
  messageType: 'success' as 'success' | 'error',
})

// Focus node
const focusNodeId = ref<number | null>(null)

// Computed
const filteredNodes = computed(() => {
  if (!nodeSearch.value) return graphStore.nodes.slice(0, 50)
  const search = nodeSearch.value.toLowerCase()
  return graphStore.nodes.filter(
    (n) =>
      n.labels.some((l) => l.toLowerCase().includes(search)) ||
      n.id.toString().includes(search)
  )
})

// Methods
async function refreshStats() {
  try {
    await Promise.all([
      graphStore.fetchStats(),
      graphStore.fetchLabels(),
      graphStore.fetchRelTypes(),
    ])
  } catch (err) {
    console.error('Failed to refresh stats:', err)
  }
}

async function loadFullGraph() {
  try {
    await Promise.all([graphStore.fetchNodes(), graphStore.fetchRels()])

    const visNodes = graphStore.nodes.map((n) => ({
      id: n.id,
      label: (n.properties.name as string) || `${n.labels[0]} #${n.id}`,
      title: `ID: ${n.id}\nLabels: ${n.labels.join(', ')}`,
    }))

    const visEdges = graphStore.rels.map((r) => ({
      id: r.id.toString(),
      from: r.start,
      to: r.end,
      label: r.typ,
    }))

    visStore.setNodes(visNodes)
    visStore.setEdges(visEdges)
  } catch (err) {
    console.error('Failed to load graph:', err)
  }
}

function clearVisualization() {
  visStore.clear()
}

async function handleCreateNode() {
  const props = parseJson(nodeForm.value.properties)
  if (props === null) {
    nodeForm.value.message = 'JSON 格式错误'
    nodeForm.value.messageType = 'error'
    return
  }

  try {
    const id = await graphStore.createNode([nodeForm.value.label], props)
    nodeForm.value.message = `节点创建成功! ID: ${id}`
    nodeForm.value.messageType = 'success'

    visStore.addNode({
      id,
      label: props.name || `${nodeForm.value.label} #${id}`,
      title: `ID: ${id}\nLabel: ${nodeForm.value.label}`,
    })

    nodeForm.value.label = ''
    nodeForm.value.properties = ''
  } catch (err) {
    nodeForm.value.message = '创建失败: ' + (err instanceof Error ? err.message : '未知错误')
    nodeForm.value.messageType = 'error'
  }
}

async function handleCreateRel() {
  const props = parseJson(relForm.value.properties) || {}

  try {
    const id = await graphStore.createRel(
      relForm.value.start,
      relForm.value.end,
      relForm.value.type,
      props
    )
    relForm.value.message = `关系创建成功! ID: ${id}`
    relForm.value.messageType = 'success'

    visStore.addEdge({
      id: id.toString(),
      from: relForm.value.start,
      to: relForm.value.end,
      label: relForm.value.type,
    })

    relForm.value.start = 0
    relForm.value.end = 0
    relForm.value.type = ''
    relForm.value.properties = ''
  } catch (err) {
    relForm.value.message = '创建失败: ' + (err instanceof Error ? err.message : '未知错误')
    relForm.value.messageType = 'error'
  }
}

async function handleExecuteQuery(query: string) {
  // Check if it's a command
  if (isCommand(query)) {
    const result = await executeCommand(query)
    if (!result.success) {
      alert(result.message)
    }
    return
  }

  // Execute query
  queryLoading.value = true
  queryError.value = ''

  try {
    // Parse query to extract label and property/value
    const labelMatch = query.match(/FROM\s+(\w+)/i)
    const propMatch = query.match(/WHERE\s+(\w+)\s*=\s*['"]([^'"]+)['"]/i)

    if (labelMatch) {
      const label = labelMatch[1]
      const property = propMatch ? propMatch[1] : undefined
      const value = propMatch ? propMatch[2] : undefined

      const results = await graphStore.queryByLabel(label, property, value)
      queryResults.value = results
      resultViewMode.value = 'graph'

      // Also update visualization
      const visNodes = results.map((n: any) => ({
        id: n.id,
        label: (n.properties.name as string) || `${n.labels[0]} #${n.id}`,
        title: `ID: ${n.id}\nLabels: ${n.labels.join(', ')}`,
      }))

      const visEdges: any[] = []
      for (const node of results) {
        const neighbors = await graphStore.fetchNeighbors(node.id)
        neighbors.outgoing.forEach((targetId: number) => {
          visEdges.push({
            id: `${node.id}-${targetId}`,
            from: node.id,
            to: targetId,
            label: 'CONNECTS',
          })
        })
      }

      visStore.setNodes(visNodes)
      visStore.setEdges(visEdges)
    } else {
      queryError.value = '无法解析查询。请使用格式: FROM <Label> [WHERE <property> = <value>]'
    }
  } catch (err) {
    queryError.value = err instanceof Error ? err.message : '查询失败'
  } finally {
    queryLoading.value = false
  }
}

function handleExecuteCommand(command: string) {
  handleExecuteQuery(command)
}

function selectNode(id: number) {
  visStore.selectNode(id)
  handleFocusNode(id)
}

function handleFocusNode(id?: number) {
  const nodeId = id || focusNodeId.value
  if (nodeId) {
    visStore.selectNode(nodeId)
    graphViewRef.value?.focus(nodeId)
  }
}

function handleFitGraph() {
  graphViewRef.value?.fit()
}

function togglePhysics() {
  visStore.togglePhysics()
}

function handleShowNeighbors(_id: number) {
  // Already handled in NodeDetails component
}

async function handleExpandNeighbors(nodeId: number) {
  try {
    const neighbors = await graphStore.fetchNeighbors(nodeId)

    // 获取现有节点 ID 集合
    const existingNodeIds = new Set(visStore.nodes.keys())

    // 添加出边邻居节点
    for (const targetId of neighbors.outgoing) {
      const targetIdNum = typeof targetId === 'string' ? parseInt(targetId, 10) : targetId
      if (!existingNodeIds.has(targetIdNum)) {
        const node = await graphStore.fetchNode(targetIdNum)
        if (node) {
          visStore.addNode({
            id: node.id,
            label: (node.properties.name as string) || `${node.labels[0]} #${node.id}`,
            title: `ID: ${node.id}\nLabels: ${node.labels.join(', ')}`,
          })
        }
      }

      // 添加边
      visStore.addEdge({
        id: `${nodeId}-${targetIdNum}`,
        from: nodeId,
        to: targetIdNum,
        label: 'CONNECTS',
      })
    }

    // 添加入边邻居节点
    for (const sourceId of neighbors.incoming) {
      const sourceIdNum = typeof sourceId === 'string' ? parseInt(sourceId, 10) : sourceId
      if (!existingNodeIds.has(sourceIdNum)) {
        const node = await graphStore.fetchNode(sourceIdNum)
        if (node) {
          visStore.addNode({
            id: node.id,
            label: (node.properties.name as string) || `${node.labels[0]} #${node.id}`,
            title: `ID: ${node.id}\nLabels: ${node.labels.join(', ')}`,
          })
        }
      }

      // 添加边
      visStore.addEdge({
        id: `${sourceIdNum}-${nodeId}`,
        from: sourceIdNum,
        to: nodeId,
        label: 'CONNECTS',
      })
    }
  } catch (err) {
    console.error('Failed to expand neighbors:', err)
  }
}

function handleZoomIn() {
  // TODO: Implement zoom in
}

function handleZoomOut() {
  // TODO: Implement zoom out
}

function handleResetView() {
  visStore.clear()
  handleFitGraph()
}

function handleLayoutChange(layout: string) {
  // TODO: Apply layout change
  console.log('Layout changed to:', layout)
}

function handleStyleChange(style: any) {
  // TODO: Apply style changes
  console.log('Style changed:', style)
}

function parseJson(str: string): Record<string, any> | null {
  if (!str) return {}
  try {
    return JSON.parse(str)
  } catch {
    return null
  }
}

onMounted(() => {
  refreshStats()
})
</script>

<style scoped>
.status-message {
  @apply p-2 rounded-lg text-xs;
}

.status-message.success {
  @apply bg-green-500/10 text-green-400 border border-green-500/30;
}

.status-message.error {
  @apply bg-red-500/10 text-red-400 border border-red-500/30;
}
</style>
