<template>
  <div
    v-if="visible && nodeData"
    class="absolute top-16 right-4 w-80 bg-gray-900 border border-gray-700 rounded-xl shadow-2xl z-10 overflow-hidden"
  >
    <!-- Header -->
    <div class="flex items-center justify-between p-4 border-b border-gray-700">
      <h3 class="font-semibold text-gray-200">
        {{ isEditing ? '编辑节点' : '节点详情' }}
      </h3>
      <div class="flex items-center gap-2">
        <button
          v-if="!isEditing"
          @click="startEditing"
          class="text-gray-500 hover:text-primary-400 transition-colors"
          title="编辑节点"
        >
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
          </svg>
        </button>
        <button
          @click="close"
          class="text-gray-500 hover:text-gray-300 transition-colors"
        >
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>
    </div>

    <!-- Content - View Mode -->
    <div v-if="!isEditing" class="p-4 space-y-3 max-h-96 overflow-y-auto">
      <DetailRow label="ID" :value="nodeData.id.toString()" />
      <DetailRow label="标签" :value="nodeData.labels.join(', ')" />

      <div class="border-t border-gray-700 pt-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">属性</h4>
        <div v-if="hasProperties" class="space-y-2">
          <DetailRow
            v-for="(value, key) in nodeData.properties"
            :key="key"
            :label="key"
            :value="String(value)"
          />
        </div>
        <p v-else class="text-sm text-gray-500">无属性</p>
      </div>

      <div class="border-t border-gray-700 pt-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">连接</h4>
        <DetailRow label="出边" :value="neighbors?.outgoing.length?.toString() || '0'" />
        <DetailRow label="入边" :value="neighbors?.incoming.length?.toString() || '0'" />
      </div>
    </div>

    <!-- Content - Edit Mode -->
    <div v-else class="p-4 space-y-3 max-h-96 overflow-y-auto">
      <div>
        <label class="block text-sm text-gray-400 mb-1">ID (只读)</label>
        <input
          :value="nodeData.id"
          type="text"
          class="input"
          disabled
        />
      </div>

      <div>
        <label class="block text-sm text-gray-400 mb-1">标签 (逗号分隔)</label>
        <input
          v-model="editForm.labels"
          type="text"
          class="input"
          placeholder="User, Person"
        />
      </div>

      <div>
        <div class="flex items-center justify-between mb-2">
          <label class="text-sm text-gray-400">属性</label>
          <button
            @click="addProperty"
            class="text-xs text-primary-400 hover:text-primary-300"
          >
            + 添加属性
          </button>
        </div>
        <div class="space-y-2">
          <div
            v-for="(prop, index) in editForm.properties"
            :key="index"
            class="flex gap-2 items-center"
          >
            <input
              v-model="prop.key"
              type="text"
              class="input flex-1"
              placeholder="属性名"
            />
            <input
              v-model="prop.value"
              type="text"
              class="input flex-1"
              placeholder="属性值"
            />
            <button
              @click="removeProperty(index)"
              class="text-red-400 hover:text-red-300 px-2"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
          <p v-if="editForm.properties.length === 0" class="text-xs text-gray-500">
            点击上方按钮添加属性
          </p>
        </div>
      </div>

      <div v-if="updateMessage" :class="['text-sm p-2 rounded', updateMessageType === 'success' ? 'bg-green-500/10 text-green-400' : 'bg-red-500/10 text-red-400']">
        {{ updateMessage }}
      </div>
    </div>

    <!-- Actions - View Mode -->
    <div v-if="!isEditing" class="p-4 border-t border-gray-700 space-y-2">
      <button @click="focusNode" class="btn btn-primary w-full">
        定位节点
      </button>
      <div class="grid grid-cols-2 gap-2">
        <button @click="showNeighbors" class="btn btn-secondary">
          显示邻居
        </button>
        <button @click="deleteNode" class="btn btn-danger">
          删除节点
        </button>
      </div>
    </div>

    <!-- Actions - Edit Mode -->
    <div v-else class="p-4 border-t border-gray-700 space-y-2">
      <div class="grid grid-cols-2 gap-2">
        <button @click="cancelEditing" class="btn btn-secondary">
          取消
        </button>
        <button @click="saveNode" class="btn btn-primary" :disabled="isSaving">
          {{ isSaving ? '保存中...' : '保存' }}
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useVisualizationStore } from '@/stores/visualization'
import { useGraphStore } from '@/stores/graph'
import api from '@/api'

const props = defineProps<{
  visible: boolean
  nodeId: number | null
}>()

const emit = defineEmits(['close', 'focus', 'showNeighbors'])

const visStore = useVisualizationStore()
const graphStore = useGraphStore()

const nodeData = ref<any>(null)
const neighbors = ref<{ outgoing: number[]; incoming: number[] } | null>(null)

// Edit state
const isEditing = ref(false)
const isSaving = ref(false)
const updateMessage = ref('')
const updateMessageType = ref<'success' | 'error'>('success')

const editForm = ref({
  labels: '',
  properties: [] as Array<{ key: string; value: string }>,
})

const hasProperties = computed(() => {
  return nodeData.value && Object.keys(nodeData.value.properties).length > 0
})

watch(
  () => props.nodeId,
  async (id) => {
    if (id && props.visible) {
      try {
        const [node, neighborsData] = await Promise.all([
          api.getNode(id),
          api.getNodeNeighbors(id),
        ])
        nodeData.value = node
        neighbors.value = neighborsData
      } catch (err) {
        console.error('Failed to fetch node details:', err)
      }
    }
  },
  { immediate: true }
)

function close() {
  emit('close')
}

function focusNode() {
  if (props.nodeId) {
    emit('focus', props.nodeId)
  }
}

async function showNeighbors() {
  if (!props.nodeId || !neighbors.value) return

  const allNeighbors = [...neighbors.value.outgoing, ...neighbors.value.incoming]

  // Add missing nodes to visualization
  for (const neighborId of allNeighbors) {
    if (!visStore.getNode(neighborId)) {
      try {
        const node = await api.getNode(neighborId)
        const nodeName = node.properties.name
        visStore.addNode({
          id: node.id,
          label: (typeof nodeName === 'string' ? nodeName : null) || `${node.labels[0]} #${node.id}`,
          title: `ID: ${node.id}\nLabels: ${node.labels.join(', ')}`,
        })
      } catch (err) {
        console.error('Failed to fetch neighbor:', err)
      }
    }
  }

  // Add edges for outgoing relationships
  for (const targetId of neighbors.value.outgoing) {
    const edgeId = `${props.nodeId}-${targetId}`
    if (!visStore.getEdge(edgeId)) {
      visStore.addEdge({
        id: edgeId,
        from: props.nodeId,
        to: targetId,
        label: 'RELATED',
      })
    }
  }

  emit('showNeighbors', props.nodeId)
}

async function deleteNode() {
  if (!props.nodeId) return
  if (!confirm(`确定要删除节点 #${props.nodeId} 吗?`)) return

  try {
    await graphStore.deleteNode(props.nodeId)
    visStore.removeNode(props.nodeId)
    emit('close')
  } catch (err) {
    console.error('Failed to delete node:', err)
    alert('删除失败: ' + (err instanceof Error ? err.message : '未知错误'))
  }
}

// Edit functions
function startEditing() {
  if (!nodeData.value) return

  editForm.value.labels = nodeData.value.labels.join(', ')
  editForm.value.properties = Object.entries(nodeData.value.properties).map(([key, value]) => ({
    key,
    value: String(value),
  }))

  updateMessage.value = ''
  isEditing.value = true
}

function cancelEditing() {
  isEditing.value = false
  updateMessage.value = ''
  editForm.value = {
    labels: '',
    properties: [],
  }
}

function addProperty() {
  editForm.value.properties.push({ key: '', value: '' })
}

function removeProperty(index: number) {
  editForm.value.properties.splice(index, 1)
}

async function saveNode() {
  if (!props.nodeId) return

  isSaving.value = true
  updateMessage.value = ''

  try {
    // Parse labels
    const labels = editForm.value.labels
      .split(',')
      .map(l => l.trim())
      .filter(l => l.length > 0)

    // Parse properties
    const properties: Record<string, string | number | boolean> = {}
    for (const prop of editForm.value.properties) {
      if (prop.key.trim()) {
        properties[prop.key.trim()] = parsePropertyValue(prop.value)
      }
    }

    // Update node via API
    await api.updateNode(props.nodeId, { properties })

    // Update local node data
    nodeData.value = {
      ...nodeData.value,
      labels,
      properties,
    }

    // Update visualization label
    const visNode = visStore.getNode(props.nodeId)
    if (visNode) {
      const newName = properties.name
      visStore.updateNode(props.nodeId, {
        label: (typeof newName === 'string' ? newName : null) || `${labels[0]} #${props.nodeId}`,
        title: `ID: ${props.nodeId}\nLabels: ${labels.join(', ')}`,
      })
    }

    updateMessage.value = '节点更新成功!'
    updateMessageType.value = 'success'

    // Exit edit mode after short delay
    setTimeout(() => {
      isEditing.value = false
    }, 1000)
  } catch (err) {
    console.error('Failed to update node:', err)
    updateMessage.value = '更新失败: ' + (err instanceof Error ? err.message : '未知错误')
    updateMessageType.value = 'error'
  } finally {
    isSaving.value = false
  }
}

function parsePropertyValue(value: string): string | number | boolean {
  // Try to parse as number
  if (/^\d+$/.test(value)) {
    return parseInt(value, 10)
  }
  if (/^\d+\.\d+$/.test(value)) {
    return parseFloat(value)
  }

  // Try to parse as boolean
  if (value.toLowerCase() === 'true') return true
  if (value.toLowerCase() === 'false') return false

  // Return as string
  return value
}
</script>

<script lang="ts">
const DetailRow = {
  props: ['label', 'value'],
  template: `
    <div class="flex justify-between items-center text-sm">
      <span class="text-gray-500">{{ label }}</span>
      <span class="text-gray-200 font-medium">{{ value }}</span>
    </div>
  `,
}
</script>
