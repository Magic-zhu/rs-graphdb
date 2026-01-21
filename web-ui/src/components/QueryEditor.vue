<template>
  <div class="query-editor">
    <div class="flex items-center justify-between mb-2">
      <h3 class="text-sm font-medium text-gray-300">查询编辑器</h3>
      <div class="flex items-center gap-2">
        <button
          @click="showHistory = !showHistory"
          :class="[
            'px-2 py-1 text-xs rounded transition-colors',
            showHistory ? 'bg-primary-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
          ]"
          title="查询历史"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        </button>
        <button
          @click="showFavorites = !showFavorites"
          :class="[
            'px-2 py-1 text-xs rounded transition-colors',
            showFavorites ? 'bg-yellow-600 text-white' : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
          ]"
          title="收藏夹"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11.049 2.927c.3-.921 1.603-.921 1.902 0l1.519 4.674a1 1 0 00.95.69h4.915c.969 0 1.371 1.24.588 1.81l-3.976 2.888a1 1 0 00-.363 1.118l1.518 4.674c.3.922-.755 1.688-1.538 1.118l-3.976-2.888a1 1 0 00-1.176 0l-3.976 2.888c-.783.57-1.838-.197-1.538-1.118l1.518-4.674a1 1 0 00-.363-1.118l-3.976-2.888c-.784-.57-.38-1.81.588-1.81h4.914a1 1 0 00.951-.69l1.519-4.674z" />
          </svg>
        </button>
      </div>
    </div>

    <!-- Query Editor -->
    <div class="relative mb-2">
      <textarea
        ref="textareaRef"
        v-model="query"
        class="w-full h-32 bg-gray-800 border border-gray-700 rounded-lg p-3 text-gray-200 text-sm font-mono resize-none focus:outline-none focus:border-primary-500"
        placeholder="输入查询或命令 (例如: :sysinfo, :help)"
        @keydown.ctrl.enter="executeQuery"
        @keydown.meta.enter="executeQuery"
      />
      <div class="absolute bottom-2 right-2 flex items-center gap-2">
        <span class="text-xs text-gray-500">Ctrl+Enter 执行</span>
      </div>
    </div>

    <!-- Action Buttons -->
    <div class="flex items-center gap-2 mb-2">
      <button
        @click="executeQuery"
        :disabled="loading || !query.trim()"
        class="flex-1 px-4 py-2 bg-primary-600 text-white text-sm font-medium rounded-lg hover:bg-primary-700 disabled:bg-gray-700 disabled:text-gray-500 disabled:cursor-not-allowed transition-colors"
      >
        <span v-if="loading">执行中...</span>
        <span v-else>执行查询</span>
      </button>
      <button
        @click="saveAsFavorite"
        :disabled="!query.trim()"
        class="px-3 py-2 bg-gray-700 text-gray-300 text-sm font-medium rounded-lg hover:bg-gray-600 disabled:bg-gray-800 disabled:text-gray-600 disabled:cursor-not-allowed transition-colors"
        title="保存为收藏"
      >
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />
        </svg>
      </button>
      <button
        @click="clearQuery"
        class="px-3 py-2 bg-gray-700 text-gray-300 text-sm font-medium rounded-lg hover:bg-gray-600 transition-colors"
        title="清空"
      >
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>

    <!-- Status Message -->
    <div v-if="message" :class="['mb-2 p-2 rounded-lg text-xs', messageClass]">
      {{ message }}
    </div>

    <!-- Query History Panel -->
    <div v-if="showHistory" class="mb-2 p-3 bg-gray-800 rounded-lg border border-gray-700">
      <div class="flex items-center justify-between mb-2">
        <h4 class="text-sm font-medium text-gray-300">查询历史</h4>
        <button
          @click="queryHistoryStore.clearHistory()"
          class="text-xs text-red-400 hover:text-red-300"
        >
          清空历史
        </button>
      </div>
      <div class="max-h-48 overflow-y-auto space-y-1">
        <div
          v-for="entry in queryHistoryStore.recentHistory"
          :key="entry.id"
          @click="loadQuery(entry.query)"
          class="p-2 bg-gray-700 rounded cursor-pointer hover:bg-gray-600 transition-colors"
        >
          <div class="flex items-center justify-between mb-1">
            <span class="text-xs text-gray-400">{{ formatTime(entry.timestamp) }}</span>
            <span
              :class="[
                'text-xs px-1.5 py-0.5 rounded',
                entry.success ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400',
              ]"
            >
              {{ entry.success ? '成功' : '失败' }}
            </span>
          </div>
          <div class="text-sm text-gray-200 font-mono truncate">{{ entry.query }}</div>
          <div v-if="entry.resultCount !== undefined" class="text-xs text-gray-400 mt-1">
            结果: {{ entry.resultCount }} 条
            <span v-if="entry.executionTime">· {{ entry.executionTime }}ms</span>
          </div>
        </div>
        <p v-if="queryHistoryStore.historyCount === 0" class="text-sm text-gray-500 text-center py-4">
          暂无查询历史
        </p>
      </div>
    </div>

    <!-- Favorites Panel -->
    <div v-if="showFavorites" class="p-3 bg-gray-800 rounded-lg border border-gray-700">
      <div class="flex items-center justify-between mb-2">
        <h4 class="text-sm font-medium text-gray-300">收藏夹</h4>
        <div class="flex items-center gap-2">
          <button
            @click="showImportExport = true"
            class="text-xs text-primary-400 hover:text-primary-300"
          >
            导入/导出
          </button>
        </div>
      </div>

      <!-- Tag Filter -->
      <div v-if="favoritesStore.allTags.length > 0" class="mb-2 flex flex-wrap gap-1">
        <button
          @click="selectedTag = null"
          :class="[
            'px-2 py-1 text-xs rounded-full transition-colors',
            selectedTag === null
              ? 'bg-primary-600 text-white'
              : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
          ]"
        >
          全部
        </button>
        <button
          v-for="tag in favoritesStore.allTags"
          :key="tag"
          @click="selectedTag = tag"
          :class="[
            'px-2 py-1 text-xs rounded-full transition-colors',
            selectedTag === tag
              ? 'bg-primary-600 text-white'
              : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
          ]"
        >
          {{ tag }}
        </button>
      </div>

      <div class="max-h-48 overflow-y-auto space-y-1">
        <div
          v-for="fav in filteredFavorites"
          :key="fav.id"
          class="p-2 bg-gray-700 rounded hover:bg-gray-600 transition-colors"
        >
          <div class="flex items-center justify-between mb-1">
            <h5 class="text-sm font-medium text-gray-200">{{ fav.name }}</h5>
            <div class="flex items-center gap-1">
              <button
                @click="loadQuery(fav.query)"
                class="p-1 text-primary-400 hover:text-primary-300"
                title="加载查询"
              >
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </button>
              <button
                @click="deleteFavorite(fav.id)"
                class="p-1 text-red-400 hover:text-red-300"
                title="删除"
              >
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                </svg>
              </button>
            </div>
          </div>
          <p v-if="fav.description" class="text-xs text-gray-400 mb-1">{{ fav.description }}</p>
          <div class="text-xs text-gray-500 font-mono truncate">{{ fav.query }}</div>
          <div v-if="fav.tags.length > 0" class="mt-1 flex flex-wrap gap-1">
            <span
              v-for="tag in fav.tags"
              :key="tag"
              class="px-1.5 py-0.5 bg-gray-600 text-gray-300 text-xs rounded"
            >
              {{ tag }}
            </span>
          </div>
        </div>
        <p v-if="filteredFavorites.length === 0" class="text-sm text-gray-500 text-center py-4">
          暂无收藏
        </p>
      </div>
    </div>

    <!-- Import/Export Modal -->
    <div v-if="showImportExport" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div class="bg-gray-800 rounded-lg p-4 max-w-lg w-full mx-4">
        <h3 class="text-lg font-medium text-white mb-4">导入/导出收藏夹</h3>
        <div class="space-y-3">
          <button
            @click="exportFavorites"
            class="w-full px-4 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors"
          >
            导出收藏夹
          </button>
          <div>
            <label class="block text-sm text-gray-300 mb-1">导入 JSON</label>
            <textarea
              v-model="importJson"
              class="w-full h-32 bg-gray-700 border border-gray-600 rounded-lg p-2 text-gray-200 text-sm"
              placeholder="粘贴导出的 JSON..."
            />
          </div>
          <div class="flex gap-2">
            <button
              @click="importFavorites"
              class="flex-1 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
            >
              导入
            </button>
            <button
              @click="showImportExport = false"
              class="flex-1 px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors"
            >
              取消
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Save Favorite Modal -->
    <div v-if="showSaveFavorite" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div class="bg-gray-800 rounded-lg p-4 max-w-md w-full mx-4">
        <h3 class="text-lg font-medium text-white mb-4">保存为收藏</h3>
        <div class="space-y-3">
          <div>
            <label class="block text-sm text-gray-300 mb-1">名称</label>
            <input
              v-model="favoriteForm.name"
              type="text"
              class="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-gray-200 text-sm"
              placeholder="输入名称..."
            />
          </div>
          <div>
            <label class="block text-sm text-gray-300 mb-1">描述 (可选)</label>
            <input
              v-model="favoriteForm.description"
              type="text"
              class="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-gray-200 text-sm"
              placeholder="输入描述..."
            />
          </div>
          <div>
            <label class="block text-sm text-gray-300 mb-1">标签 (逗号分隔)</label>
            <input
              v-model="favoriteForm.tags"
              type="text"
              class="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-gray-200 text-sm"
              placeholder="例如: basic, nodes"
            />
          </div>
          <div class="flex gap-2">
            <button
              @click="confirmSaveFavorite"
              class="flex-1 px-4 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors"
            >
              保存
            </button>
            <button
              @click="showSaveFavorite = false"
              class="flex-1 px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors"
            >
              取消
            </button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useQueryHistoryStore } from '@/stores/queryHistory'
import { useFavoritesStore } from '@/stores/favorites'

const emit = defineEmits<{
  execute: [query: string]
}>()

const queryHistoryStore = useQueryHistoryStore()
const favoritesStore = useFavoritesStore()

const query = ref('')
const textareaRef = ref<HTMLTextAreaElement>()
const loading = ref(false)
const message = ref('')
const messageClass = ref('')
const showHistory = ref(false)
const showFavorites = ref(false)
const showImportExport = ref(false)
const showSaveFavorite = ref(false)
const selectedTag = ref<string | null>(null)
const importJson = ref('')

const favoriteForm = ref({
  name: '',
  description: '',
  tags: '',
})

const filteredFavorites = computed(() => {
  if (selectedTag.value) {
    return favoritesStore.favorites.filter((f) => f.tags.includes(selectedTag.value!))
  }
  return favoritesStore.favorites
})

async function executeQuery() {
  if (!query.value.trim() || loading.value) return

  const queryText = query.value.trim()
  loading.value = true
  message.value = ''

  const startTime = performance.now()

  try {
    emit('execute', queryText)

    const executionTime = Math.round(performance.now() - startTime)
    showMessage('查询执行成功', 'success')

    queryHistoryStore.addQuery(queryText, true, undefined, executionTime)
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : '未知错误'
    showMessage(`查询失败: ${errorMessage}`, 'error')
    queryHistoryStore.addQuery(queryText, false, undefined, undefined, errorMessage)
  } finally {
    loading.value = false
  }
}

function clearQuery() {
  query.value = ''
  message.value = ''
  textareaRef.value?.focus()
}

function loadQuery(q: string) {
  query.value = q
  textareaRef.value?.focus()
}

function saveAsFavorite() {
  if (!query.value.trim()) return
  favoriteForm.value = {
    name: '',
    description: '',
    tags: '',
  }
  showSaveFavorite.value = true
}

function confirmSaveFavorite() {
  if (!favoriteForm.value.name.trim()) {
    showMessage('请输入名称', 'error')
    return
  }

  const tags = favoriteForm.value.tags
    .split(',')
    .map((t) => t.trim())
    .filter((t) => t)

  favoritesStore.addFavorite(
    favoriteForm.value.name,
    query.value,
    favoriteForm.value.description,
    tags
  )

  showSaveFavorite.value = false
  showMessage('收藏已保存', 'success')
}

function deleteFavorite(id: string) {
  if (confirm('确定要删除这个收藏吗？')) {
    favoritesStore.removeFavorite(id)
    showMessage('收藏已删除', 'success')
  }
}

function exportFavorites() {
  const json = favoritesStore.exportFavorites()
  const blob = new Blob([json], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `graphdb-favorites-${new Date().toISOString().slice(0, 10)}.json`
  a.click()
  URL.revokeObjectURL(url)
  showMessage('收藏已导出', 'success')
}

function importFavorites() {
  if (!importJson.value.trim()) {
    showMessage('请输入 JSON', 'error')
    return
  }

  const result = favoritesStore.importFavorites(importJson.value)
  if (result.success) {
    showImportExport.value = false
    importJson.value = ''
    showMessage(result.message, 'success')
  } else {
    showMessage(result.message, 'error')
  }
}

function showMessage(text: string, type: 'success' | 'error') {
  message.value = text
  messageClass.value = type === 'success'
    ? 'bg-green-500/10 text-green-400 border border-green-500/30'
    : 'bg-red-500/10 text-red-400 border border-red-500/30'
  setTimeout(() => {
    message.value = ''
  }, 5000)
}

function formatTime(date: Date): string {
  const now = new Date()
  const diff = now.getTime() - date.getTime()
  const minutes = Math.floor(diff / 60000)
  const hours = Math.floor(diff / 3600000)
  const days = Math.floor(diff / 86400000)

  if (minutes < 1) return '刚刚'
  if (minutes < 60) return `${minutes}分钟前`
  if (hours < 24) return `${hours}小时前`
  return `${days}天前`
}
</script>

<style scoped>
.query-editor textarea {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
}
</style>
