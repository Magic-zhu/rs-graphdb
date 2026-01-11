<template>
  <div
    v-if="visible && nodeData"
    class="absolute top-16 right-4 w-80 bg-gray-900 border border-gray-700 rounded-xl shadow-2xl z-10 overflow-hidden"
  >
    <!-- Header -->
    <div class="flex items-center justify-between p-4 border-b border-gray-700">
      <h3 class="font-semibold text-gray-200">节点详情</h3>
      <button
        @click="close"
        class="text-gray-500 hover:text-gray-300 transition-colors"
      >
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>

    <!-- Content -->
    <div class="p-4 space-y-3 max-h-96 overflow-y-auto">
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

    <!-- Actions -->
    <div class="p-4 border-t border-gray-700 space-y-2">
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
