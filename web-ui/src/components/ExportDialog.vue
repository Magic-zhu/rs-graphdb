<template>
  <div v-if="visible" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
    <div class="bg-gray-800 rounded-lg p-6 max-w-md w-full mx-4">
      <h3 class="text-lg font-semibold text-white mb-4">导出数据</h3>

      <div class="space-y-4">
        <!-- Export Type -->
        <div>
          <label class="block text-sm text-gray-300 mb-2">导出类型</label>
          <div class="grid grid-cols-2 gap-2">
            <button
              v-for="type in exportTypes"
              :key="type.value"
              @click="selectedType = type.value as 'graph' | 'query'"
              :class="[
                'px-4 py-3 rounded-lg text-sm font-medium transition-colors',
                selectedType === type.value
                  ? 'bg-primary-600 text-white'
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
              ]"
            >
              <div class="flex items-center justify-center gap-2">
                <span v-html="type.icon"></span>
                <span>{{ type.label }}</span>
              </div>
            </button>
          </div>
        </div>

        <!-- Format Selection -->
        <div>
          <label class="block text-sm text-gray-300 mb-2">文件格式</label>
          <div class="flex gap-2">
            <button
              v-for="format in availableFormats"
              :key="format"
              @click="selectedFormat = format as 'png' | 'svg' | 'json' | 'csv'"
              :class="[
                'px-3 py-2 rounded text-sm font-medium transition-colors',
                selectedFormat === format
                  ? 'bg-primary-600 text-white'
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
              ]"
            >
              {{ format.toUpperCase() }}
            </button>
          </div>
        </div>

        <!-- Filename -->
        <div>
          <label class="block text-sm text-gray-300 mb-2">文件名</label>
          <input
            v-model="filename"
            type="text"
            class="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-gray-200 text-sm"
            placeholder="输入文件名..."
          />
        </div>

        <!-- Options -->
        <div v-if="selectedType === 'graph'" class="space-y-2">
          <label class="flex items-center gap-2 text-sm text-gray-300">
            <input
              v-model="includeStyles"
              type="checkbox"
              class="w-4 h-4 rounded border-gray-600 bg-gray-700 text-primary-600 focus:ring-primary-500"
            />
            <span>包含样式信息</span>
          </label>
        </div>

        <!-- Preview -->
        <div class="bg-gray-700 rounded-lg p-3">
          <div class="text-xs text-gray-400 mb-1">预览</div>
          <div class="text-sm text-gray-200">
            {{ filename }}.{{ selectedFormat }}
            <span v-if="selectedType === 'graph' && selectedFormat === 'csv'">-nodes.csv, {{ filename }}-edges.csv</span>
          </div>
        </div>

        <!-- Actions -->
        <div class="flex gap-2">
          <button
            @click="handleExport"
            :disabled="exporting"
            class="flex-1 px-4 py-2 bg-primary-600 text-white font-medium rounded-lg hover:bg-primary-700 disabled:bg-gray-700 disabled:text-gray-500 disabled:cursor-not-allowed transition-colors"
          >
            {{ exporting ? '导出中...' : '导出' }}
          </button>
          <button
            @click="$emit('close')"
            class="flex-1 px-4 py-2 bg-gray-600 text-white font-medium rounded-lg hover:bg-gray-700 transition-colors"
          >
            取消
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { exportGraphImage, exportGraphData, exportGraphCSV } from '@/utils/export'
import { useVisualizationStore } from '@/stores/visualization'
import { useGraphStore } from '@/stores/graph'

interface Props {
  visible: boolean
}

defineProps<Props>()
const emit = defineEmits<{
  close: []
}>()

const visStore = useVisualizationStore()
const graphStore = useGraphStore()

const selectedType = ref<'graph' | 'query'>('graph')
const selectedFormat = ref<'png' | 'svg' | 'json' | 'csv'>('json')
const filename = ref(`graphdb-export-${new Date().toISOString().slice(0, 10)}`)
const includeStyles = ref(true)
const exporting = ref(false)

const exportTypes = [
  {
    value: 'graph',
    label: '图数据',
    icon: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>',
  },
  {
    value: 'query',
    label: '查询结果',
    icon: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" /></svg>',
  },
]

const availableFormats = computed(() => {
  if (selectedType.value === 'graph') {
    return ['png', 'svg', 'json', 'csv']
  }
  return ['json', 'csv']
})

async function handleExport() {
  if (exporting.value) return

  exporting.value = true

  try {
    if (selectedType.value === 'graph') {
      await exportGraph()
    } else {
      await exportQueryResults()
    }

    emit('close')
  } catch (error) {
    console.error('Export failed:', error)
    alert(`导出失败: ${error instanceof Error ? error.message : '未知错误'}`)
  } finally {
    exporting.value = false
  }
}

async function exportGraph() {
  const nodes = Array.from(visStore.nodes.values())
  const edges = Array.from(visStore.edges.values())

  if (nodes.length === 0) {
    throw new Error('没有可导出的图数据')
  }

  const format = selectedFormat.value

  if (format === 'png' || format === 'svg') {
    // Find the graph container
    const container = document.querySelector('.graph-container') as HTMLElement
    if (!container) {
      throw new Error('找不到图容器')
    }
    await exportGraphImage(container, { format, filename: filename.value })
  } else if (format === 'json') {
    exportGraphData(nodes, edges, { filename: filename.value, includeStyles: includeStyles.value })
  } else if (format === 'csv') {
    exportGraphCSV(nodes, edges, { filename: filename.value })
  }
}

async function exportQueryResults() {
  // This would use the current query results
  // For now, export all nodes as query results
  const nodes = graphStore.nodes
  const columns = ['id', 'labels', 'properties']

  const format = selectedFormat.value

  if (format === 'json') {
    const json = JSON.stringify(
      {
        exportDate: new Date().toISOString(),
        rowCount: nodes.length,
        columns,
        rows: nodes,
      },
      null,
      2
    )
    downloadFile(json, `${filename.value}.json`, 'application/json')
  } else {
    // CSV
    const headers = columns.join(',')
    const rows = nodes.map((node) => {
      const labels = node.labels.join(';')
      const props = JSON.stringify(node.properties).replace(/"/g, '""')
      return `${node.id},"${labels}","${props}"`
    })
    const csv = [headers, ...rows].join('\n')
    downloadFile(csv, `${filename.value}.csv`, 'text/csv')
  }
}

function downloadFile(content: string, filename: string, mimeType: string) {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.style.display = 'none'
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}
</script>
