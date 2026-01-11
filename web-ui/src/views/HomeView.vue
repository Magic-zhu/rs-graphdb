<template>
  <div class="flex h-full">
    <!-- Sidebar -->
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

        <!-- Query Tab -->
        <div v-if="activeTab === 'query'" class="space-y-4">
          <PanelSection title="按标签查询">
            <div class="space-y-3">
              <select v-model="queryLabel" class="input">
                <option value="">选择标签...</option>
                <option v-for="label in graphStore.labels" :key="label" :value="label">
                  {{ label }}
                </option>
              </select>
              <button @click="handleQueryByLabel" class="btn btn-primary w-full">查询</button>
              <div v-if="queryLabelMessage" :class="['status-message', queryLabelMessageType]">
                {{ queryLabelMessage }}
              </div>
            </div>
          </PanelSection>

          <PanelSection title="按属性查询">
            <div class="space-y-3">
              <select v-model="queryPropLabel" class="input">
                <option value="">选择标签...</option>
                <option v-for="label in graphStore.labels" :key="label" :value="label">
                  {{ label }}
                </option>
              </select>
              <input
                v-model="queryPropName"
                type="text"
                class="input"
                placeholder="属性名"
              />
              <input
                v-model="queryPropValue"
                type="text"
                class="input"
                placeholder="属性值"
              />
              <button @click="handleQueryByProperty" class="btn btn-primary w-full">查询</button>
              <div v-if="queryPropMessage" :class="['status-message', queryPropMessageType]">
                {{ queryPropMessage }}
              </div>
            </div>
          </PanelSection>

          <PanelSection title="全局搜索">
            <div class="space-y-3">
              <input
                v-model="globalSearchQuery"
                type="text"
                class="input"
                placeholder="搜索节点标签或属性..."
              />
              <button @click="handleGlobalSearch" class="btn btn-primary w-full">搜索</button>
              <div v-if="searchMessage" :class="['status-message', searchMessageType]">
                {{ searchMessage }}
              </div>
            </div>
          </PanelSection>
        </div>
      </div>
    </aside>

    <!-- Graph Container -->
    <main class="flex-1 flex flex-col relative">
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
        <span v-if="visStore.selectedNodeId" class="ml-auto text-sm text-gray-500">
          选中: 节点 #{{ visStore.selectedNodeId }}
        </span>
      </div>

      <!-- Graph View -->
      <div class="flex-1 relative">
        <GraphView ref="graphViewRef" @closeDetails="visStore.selectNode(null)" />
        <NodeDetails
          :visible="visStore.selectedNodeId !== null"
          :nodeId="visStore.selectedNodeId"
          @close="visStore.selectNode(null)"
          @focus="handleFocusNode"
          @showNeighbors="handleShowNeighbors"
        />
      </div>
    </main>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useGraphStore } from '@/stores/graph'
import { useVisualizationStore } from '@/stores/visualization'
import GraphView from '@/components/GraphView.vue'
import NodeDetails from '@/components/NodeDetails.vue'

const graphStore = useGraphStore()
const visStore = useVisualizationStore()
const graphViewRef = ref<InstanceType<typeof GraphView>>()

// Tabs
const tabs = [
  { key: 'dashboard', label: '仪表盘' },
  { key: 'nodes', label: '节点' },
  { key: 'relations', label: '关系' },
  { key: 'query', label: '查询' },
]
const activeTab = ref('dashboard')

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

// Query
const queryLabel = ref('')
const queryLabelMessage = ref('')
const queryLabelMessageType = ref<'success' | 'error'>('success')

const queryPropLabel = ref('')
const queryPropName = ref('')
const queryPropValue = ref('')
const queryPropMessage = ref('')
const queryPropMessageType = ref<'success' | 'error'>('success')

const globalSearchQuery = ref('')
const searchMessage = ref('')
const searchMessageType = ref<'success' | 'error'>('success')

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

    showMessage('search', 'success', `已加载 ${visNodes.length} 个节点和 ${visEdges.length} 条关系`)
  } catch (err) {
    showMessage('search', 'error', '加载失败: ' + (err instanceof Error ? err.message : '未知错误'))
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

async function handleQueryByLabel() {
  if (!queryLabel.value) {
    showMessage('queryLabel', 'error', '请选择标签')
    return
  }

  try {
    const results = await graphStore.queryByLabel(queryLabel.value)
    showMessage('queryLabel', 'success', `找到 ${results.length} 个节点`)
    visualizeResults(results)
  } catch (err) {
    showMessage('queryLabel', 'error', '查询失败: ' + (err instanceof Error ? err.message : '未知错误'))
  }
}

async function handleQueryByProperty() {
  if (!queryPropLabel.value || !queryPropName.value || !queryPropValue.value) {
    showMessage('queryProp', 'error', '请填写完整')
    return
  }

  try {
    const results = await graphStore.queryByProperty(
      queryPropLabel.value,
      queryPropName.value,
      queryPropValue.value
    )
    showMessage('queryProp', 'success', `找到 ${results.length} 个节点`)
    visualizeResults(results)
  } catch (err) {
    showMessage('queryProp', 'error', '查询失败: ' + (err instanceof Error ? err.message : '未知错误'))
  }
}

async function handleGlobalSearch() {
  if (!globalSearchQuery.value) {
    showMessage('search', 'error', '请输入搜索内容')
    return
  }

  try {
    const results = await graphStore.searchNodes(globalSearchQuery.value)
    showMessage('search', 'success', `找到 ${results.length} 个匹配节点`)
    visualizeResults(results)
  } catch (err) {
    showMessage('search', 'error', '搜索失败: ' + (err instanceof Error ? err.message : '未知错误'))
  }
}

function visualizeResults(results: any[]) {
  visStore.clear()
  const nodes = results.map((n) => ({
    id: n.id,
    label: (n.properties.name as string) || `${n.labels[0]} #${n.id}`,
    title: `ID: ${n.id}\nLabels: ${n.labels.join(', ')}`,
  }))
  visStore.setNodes(nodes)
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

function showMessage(
  target: 'queryLabel' | 'queryProp' | 'search',
  type: 'success' | 'error',
  message: string
) {
  if (target === 'queryLabel') {
    queryLabelMessage.value = message
    queryLabelMessageType.value = type
  } else if (target === 'queryProp') {
    queryPropMessage.value = message
    queryPropMessageType.value = type
  } else {
    searchMessage.value = message
    searchMessageType.value = type
  }
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

<script lang="ts">
const PanelSection = {
  props: ['title'],
  template: `
    <div class="panel">
      <h3 class="text-sm font-semibold text-gray-300 mb-3">{{ title }}</h3>
      <slot />
    </div>
  `,
}

const StatRow = {
  props: ['label', 'value'],
  template: `
    <div class="flex justify-between items-center py-1 text-sm">
      <span class="text-gray-500">{{ label }}</span>
      <span class="text-primary-400 font-semibold">{{ value }}</span>
    </div>
  `,
}
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
