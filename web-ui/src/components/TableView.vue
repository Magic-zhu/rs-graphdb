<template>
  <div class="table-view">
    <!-- View Toggle -->
    <div class="flex items-center justify-between mb-4">
      <div class="flex items-center gap-2">
        <button
          @click="$emit('change-view', 'graph')"
          :class="[
            'px-3 py-1.5 text-sm rounded transition-colors',
            viewMode === 'graph'
              ? 'bg-primary-600 text-white'
              : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
          ]"
          title="图形视图"
        >
          <svg class="w-4 h-4 inline mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
          </svg>
          图形
        </button>
        <button
          @click="$emit('change-view', 'table')"
          :class="[
            'px-3 py-1.5 text-sm rounded transition-colors',
            viewMode === 'table'
              ? 'bg-primary-600 text-white'
              : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
          ]"
          title="表格视图"
        >
          <svg class="w-4 h-4 inline mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
          </svg>
          表格
        </button>
      </div>

      <!-- Export Buttons -->
      <div class="flex items-center gap-2">
        <button
          @click="exportCSV"
          class="px-3 py-1.5 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 transition-colors"
          title="导出 CSV"
        >
          CSV
        </button>
        <button
          @click="exportJSON"
          class="px-3 py-1.5 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 transition-colors"
          title="导出 JSON"
        >
          JSON
        </button>
      </div>
    </div>

    <!-- Table Content -->
    <div class="table-container">
      <div v-if="loading" class="flex items-center justify-center h-64">
        <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
      </div>

      <div v-else-if="error" class="bg-red-500/10 border border-red-500/30 rounded-lg p-4">
        <p class="text-red-400 text-sm">{{ error }}</p>
      </div>

      <div v-else-if="rows.length === 0" class="text-center py-8">
        <svg class="w-16 h-16 mx-auto text-gray-600 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
        </svg>
        <p class="text-gray-500">暂无数据</p>
        <p class="text-gray-600 text-sm mt-1">执行查询以查看结果</p>
      </div>

      <div v-else class="overflow-auto">
        <!-- Summary Info -->
        <div class="flex items-center justify-between mb-3 px-2">
          <span class="text-sm text-gray-400">共 {{ rows.length }} 条结果</span>
          <input
            v-model="searchQuery"
            type="text"
            class="px-3 py-1 bg-gray-800 border border-gray-700 rounded text-sm text-gray-200 focus:outline-none focus:border-primary-500"
            placeholder="搜索..."
            style="width: 200px"
          />
        </div>

        <!-- Data Table -->
        <table class="w-full text-sm">
          <thead class="bg-gray-800 sticky top-0">
            <tr>
              <th
                v-for="column in columns"
                :key="column"
                @click="sortBy(column)"
                class="px-4 py-2 text-left text-gray-400 font-medium cursor-pointer hover:bg-gray-700 select-none"
              >
                <div class="flex items-center gap-1">
                  <span>{{ column }}</span>
                  <span v-if="sortColumn === column" class="text-primary-400">
                    {{ sortOrder === 'asc' ? '↑' : '↓' }}
                  </span>
                </div>
              </th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-800">
            <tr
              v-for="(row, index) in paginatedRows"
              :key="index"
              class="hover:bg-gray-800/50 transition-colors"
            >
              <td
                v-for="column in columns"
                :key="column"
                class="px-4 py-2 text-gray-300 max-w-xs truncate"
                :title="formatCellValue(row[column])"
              >
                <span
                  v-if="isNodeId(row[column])"
                  @click="$emit('focus-node', row[column])"
                  class="text-primary-400 hover:text-primary-300 cursor-pointer underline"
                >
                  {{ row[column] }}
                </span>
                <span v-else>{{ formatCellValue(row[column]) }}</span>
              </td>
            </tr>
          </tbody>
        </table>

        <!-- Pagination -->
        <div class="flex items-center justify-between mt-4 px-2">
          <button
            @click="previousPage"
            :disabled="currentPage === 1"
            class="px-3 py-1 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 disabled:bg-gray-800 disabled:text-gray-600 disabled:cursor-not-allowed transition-colors"
          >
            上一页
          </button>
          <span class="text-sm text-gray-400">
            第 {{ currentPage }} / {{ totalPages }} 页
          </span>
          <button
            @click="nextPage"
            :disabled="currentPage === totalPages"
            class="px-3 py-1 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 disabled:bg-gray-800 disabled:text-gray-600 disabled:cursor-not-allowed transition-colors"
          >
            下一页
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'

const props = defineProps<{
  viewMode: 'graph' | 'table'
  data?: any[]
  loading?: boolean
  error?: string
}>()

defineEmits<{
  'change-view': [mode: 'graph' | 'table']
  'focus-node': [id: number]
}>()

// State
const searchQuery = ref('')
const sortColumn = ref<string>('')
const sortOrder = ref<'asc' | 'desc'>('asc')
const currentPage = ref(1)
const pageSize = 50

// Extract columns from data
const columns = computed(() => {
  if (!props.data || props.data.length === 0) return []

  // Get all unique keys from all rows
  const keys = new Set<string>()
  props.data.forEach((row) => {
    Object.keys(row).forEach((key) => keys.add(key))
  })

  // Sort keys: put id first, then labels, then others alphabetically
  const sortedKeys = Array.from(keys).sort((a, b) => {
    if (a === 'id') return -1
    if (b === 'id') return 1
    if (a === 'labels') return -1
    if (b === 'labels') return 1
    return a.localeCompare(b)
  })

  return sortedKeys
})

// Extract rows from data
const rows = computed(() => {
  if (!props.data) return []

  return props.data.map((item) => {
    const row: Record<string, any> = {}

    if (item.id !== undefined) row.id = item.id
    if (item.labels) row.labels = Array.isArray(item.labels) ? item.labels.join(', ') : item.labels
    if (item.properties) {
      Object.entries(item.properties).forEach(([key, value]) => {
        row[key] = value
      })
    } else {
      Object.entries(item).forEach(([key, value]) => {
        if (key !== 'id' && key !== 'labels') {
          row[key] = value
        }
      })
    }

    return row
  })
})

// Filter rows by search query
const filteredRows = computed(() => {
  if (!searchQuery.value) return rows.value

  const query = searchQuery.value.toLowerCase()
  return rows.value.filter((row) => {
    return Object.values(row).some((value) => {
      const str = String(value).toLowerCase()
      return str.includes(query)
    })
  })
})

// Sort rows
const sortedRows = computed(() => {
  if (!sortColumn.value) return filteredRows.value

  return [...filteredRows.value].sort((a, b) => {
    const aVal = a[sortColumn.value]
    const bVal = b[sortColumn.value]

    if (aVal === bVal) return 0
    if (aVal === null || aVal === undefined) return 1
    if (bVal === null || bVal === undefined) return -1

    const comparison = String(aVal).localeCompare(String(bVal))
    return sortOrder.value === 'asc' ? comparison : -comparison
  })
})

// Pagination
const totalPages = computed(() => Math.ceil(sortedRows.value.length / pageSize))

const paginatedRows = computed(() => {
  const start = (currentPage.value - 1) * pageSize
  const end = start + pageSize
  return sortedRows.value.slice(start, end)
})

// Reset to page 1 when data changes
watch(() => props.data, () => {
  currentPage.value = 1
  sortColumn.value = ''
  sortOrder.value = 'asc'
  searchQuery.value = ''
})

// Methods
function sortBy(column: string) {
  if (sortColumn.value === column) {
    sortOrder.value = sortOrder.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortColumn.value = column
    sortOrder.value = 'asc'
  }
}

function previousPage() {
  if (currentPage.value > 1) {
    currentPage.value--
  }
}

function nextPage() {
  if (currentPage.value < totalPages.value) {
    currentPage.value++
  }
}

function formatCellValue(value: any): string {
  if (value === null || value === undefined) return ''
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function isNodeId(value: any): boolean {
  // Check if value looks like a node ID (number in id column)
  return typeof value === 'number' && sortColumn.value === 'id'
}

function exportCSV() {
  if (rows.value.length === 0) return

  const headers = columns.value.join(',')
  const csvRows = rows.value.map((row) => {
    return columns.value
      .map((col) => {
        const value = formatCellValue(row[col])
        // Escape quotes and wrap in quotes if contains comma
        if (value.includes(',') || value.includes('"') || value.includes('\n')) {
          return `"${value.replace(/"/g, '""')}"`
        }
        return value
      })
      .join(',')
  })

  const csv = [headers, ...csvRows].join('\n')
  downloadFile(csv, 'query-results.csv', 'text/csv')
}

function exportJSON() {
  if (rows.value.length === 0) return

  const json = JSON.stringify(rows.value, null, 2)
  downloadFile(json, 'query-results.json', 'application/json')
}

function downloadFile(content: string, filename: string, mimeType: string) {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}
</script>

<style scoped>
.table-view {
  @apply h-full flex flex-col;
}

.table-container {
  @apply flex-1 overflow-auto bg-gray-900/50 rounded-lg;
  min-height: 300px;
}

table {
  border-collapse: collapse;
}

th,
td {
  white-space: nowrap;
}

::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

::-webkit-scrollbar-track {
  @apply bg-gray-800;
}

::-webkit-scrollbar-thumb {
  @apply bg-gray-600 rounded;
}

::-webkit-scrollbar-thumb:hover {
  @apply bg-gray-500;
}
</style>
