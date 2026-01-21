<template>
  <div class="system-info-panel">
    <div class="flex items-center justify-between mb-4">
      <h3 class="text-lg font-semibold text-white">系统信息</h3>
      <button
        @click="refreshInfo"
        :disabled="loading"
        class="p-2 text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
        title="刷新"
      >
        <svg
          :class="['w-5 h-5', loading && 'animate-spin']"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
          />
        </svg>
      </button>
    </div>

    <div class="space-y-4">
      <!-- Kernel Version -->
      <div class="bg-gray-800 rounded-lg p-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">内核版本</h4>
        <p class="text-white font-mono text-sm">{{ systemInfo.kernelVersion }}</p>
      </div>

      <!-- Store Size -->
      <div class="bg-gray-800 rounded-lg p-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">存储大小</h4>
        <p class="text-white text-sm">{{ systemInfo.storeSize }}</p>
      </div>

      <!-- ID Allocation -->
      <div class="bg-gray-800 rounded-lg p-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">ID 分配</h4>
        <div class="grid grid-cols-2 gap-2 text-sm">
          <div>
            <span class="text-gray-500">节点 ID:</span>
            <span class="text-white ml-2">{{ systemInfo.idAllocation.nodeIds.toLocaleString() }}</span>
          </div>
          <div>
            <span class="text-gray-500">关系 ID:</span>
            <span class="text-white ml-2">{{ systemInfo.idAllocation.relIds.toLocaleString() }}</span>
          </div>
        </div>
      </div>

      <!-- Page Cache -->
      <div class="bg-gray-800 rounded-lg p-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">页面缓存</h4>
        <div class="space-y-1 text-sm">
          <div class="flex justify-between">
            <span class="text-gray-500">命中:</span>
            <span class="text-green-400">{{ systemInfo.pageCache.hits.toLocaleString() }}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-gray-500">未命中:</span>
            <span class="text-red-400">{{ systemInfo.pageCache.misses.toLocaleString() }}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-gray-500">命中率:</span>
            <span class="text-primary-400">{{ systemInfo.pageCache.ratio }}</span>
          </div>
        </div>
      </div>

      <!-- Transactions -->
      <div class="bg-gray-800 rounded-lg p-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">事务</h4>
        <div class="space-y-1 text-sm">
          <div class="flex justify-between">
            <span class="text-gray-500">最后关闭:</span>
            <span class="text-white">{{ formatTimestamp(systemInfo.transactions.lastClosed) }}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-gray-500">打开的事务:</span>
            <span class="text-white">{{ systemInfo.transactions.open }}</span>
          </div>
        </div>
      </div>

      <!-- Databases -->
      <div class="bg-gray-800 rounded-lg p-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">数据库</h4>
        <div class="space-y-2">
          <div
            v-for="db in systemInfo.databases"
            :key="db.name"
            class="flex items-center justify-between"
          >
            <span class="text-white text-sm">{{ db.name }}</span>
            <span
              :class="[
                'px-2 py-0.5 text-xs rounded-full',
                db.status === 'online'
                  ? 'bg-green-500/20 text-green-400'
                  : 'bg-red-500/20 text-red-400',
              ]"
            >
              {{ db.status }}
            </span>
          </div>
        </div>
      </div>

      <!-- Uptime -->
      <div class="bg-gray-800 rounded-lg p-3">
        <h4 class="text-sm font-medium text-gray-400 mb-2">运行时间</h4>
        <p class="text-white text-sm">{{ systemInfo.uptime }}</p>
      </div>

      <!-- Quick Commands -->
      <div class="bg-gray-800 rounded-lg p-3">
        <h4 class="text-sm font-medium text-gray-400 mb-3">快捷命令</h4>
        <div class="grid grid-cols-2 gap-2">
          <button
            @click="$emit('execute-command', ':sysinfo')"
            class="px-3 py-2 bg-gray-700 text-gray-300 text-xs rounded hover:bg-gray-600 transition-colors text-left"
          >
            :sysinfo
          </button>
          <button
            @click="$emit('execute-command', ':queries')"
            class="px-3 py-2 bg-gray-700 text-gray-300 text-xs rounded hover:bg-gray-600 transition-colors text-left"
          >
            :queries
          </button>
          <button
            @click="$emit('execute-command', ':stats')"
            class="px-3 py-2 bg-gray-700 text-gray-300 text-xs rounded hover:bg-gray-600 transition-colors text-left"
          >
            :stats
          </button>
          <button
            @click="$emit('execute-command', ':clear')"
            class="px-3 py-2 bg-gray-700 text-gray-300 text-xs rounded hover:bg-gray-600 transition-colors text-left"
          >
            :clear
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useCommandStore } from '@/stores/commands'

defineEmits<{
  'execute-command': [command: string]
}>()

const { systemInfo, fetchSystemInfo } = useCommandStore()
const loading = ref(false)

async function refreshInfo() {
  loading.value = true
  try {
    await fetchSystemInfo()
  } finally {
    loading.value = false
  }
}

function formatTimestamp(timestamp: string): string {
  try {
    const date = new Date(timestamp)
    const now = new Date()
    const diff = now.getTime() - date.getTime()
    const seconds = Math.floor(diff / 1000)
    const minutes = Math.floor(seconds / 60)
    const hours = Math.floor(minutes / 60)

    if (seconds < 60) return `${seconds}秒前`
    if (minutes < 60) return `${minutes}分钟前`
    if (hours < 24) return `${hours}小时前`
    return date.toLocaleDateString()
  } catch {
    return timestamp
  }
}

onMounted(() => {
  fetchSystemInfo()
})
</script>
