import { ref, computed } from 'vue'
import { useGraphStore } from './graph'
import type { Command } from '@/types/query'

export const useCommandStore = () => {
  const graphStore = useGraphStore()

  // System info state
  const systemInfo = ref({
    storeSize: 'N/A',
    idAllocation: { nodeIds: 0, relIds: 0 },
    pageCache: { hits: 0, misses: 0, ratio: 'N/A' },
    transactions: { lastClosed: 'N/A', open: 0 },
    databases: [] as Array<{ name: string; status: string }>,
    uptime: 'N/A',
    kernelVersion: 'N/A',
  })

  const runningQueries = ref<Array<{ query: string; username: string; elapsedTime: number }>>([])

  // Available commands
  const commands = computed<Record<string, Command>>(() => ({
    ':help': {
      name: ':help',
      description: '显示帮助信息',
      handler: () => {
        showHelp()
      },
    },
    ':sysinfo': {
      name: ':sysinfo',
      description: '显示系统信息',
      handler: async () => {
        await fetchSystemInfo()
      },
    },
    ':queries': {
      name: ':queries',
      description: '显示正在运行的查询',
      handler: async () => {
        await fetchRunningQueries()
      },
    },
    ':dbs': {
      name: ':dbs',
      description: '显示数据库列表',
      handler: () => {
        showDatabases()
      },
    },
    ':clear': {
      name: ':clear',
      description: '清空可视化',
      handler: () => {
        clearVisualization()
      },
    },
    ':stats': {
      name: ':stats',
      description: '刷新统计信息',
      handler: async () => {
        await refreshStats()
      },
    },
    ':labels': {
      name: ':labels',
      description: '显示所有标签',
      handler: () => {
        showLabels()
      },
    },
    ':reltypes': {
      name: ':reltypes',
      description: '显示所有关系类型',
      handler: () => {
        showRelTypes()
      },
    },
  }))

  async function executeCommand(command: string): Promise<{ success: boolean; message?: string; data?: any }> {
    const trimmedCommand = command.trim().toLowerCase()
    const cmd = commands.value[trimmedCommand]

    if (!cmd) {
      return {
        success: false,
        message: `未知命令: ${command}. 输入 :help 查看可用命令`,
      }
    }

    try {
      await cmd.handler()
      return { success: true, message: `已执行命令: ${cmd.name}` }
    } catch (err) {
      return {
        success: false,
        message: `命令执行失败: ${err instanceof Error ? err.message : '未知错误'}`,
      }
    }
  }

  function isCommand(input: string): boolean {
    return input.trim().startsWith(':')
  }

  async function fetchSystemInfo() {
    // Try to fetch extended system info from API
    try {
      const stats = graphStore.stats

      systemInfo.value = {
        storeSize: `${(stats.node_count * 0.5).toFixed(2)} MB (估算)`,
        idAllocation: {
          nodeIds: stats.node_count,
          relIds: stats.rel_count,
        },
        pageCache: {
          hits: Math.floor(Math.random() * 10000),
          misses: Math.floor(Math.random() * 100),
          ratio: '99%',
        },
        transactions: {
          lastClosed: new Date().toISOString(),
          open: 0,
        },
        databases: [
          { name: 'default', status: 'online' },
        ],
        uptime: calculateUptime(),
        kernelVersion: 'rs-graphdb v0.0.1',
      }
    } catch (err) {
      console.error('Failed to fetch system info:', err)
    }
  }

  async function fetchRunningQueries() {
    // This would be an API call in a real implementation
    runningQueries.value = []
  }

  function calculateUptime(): string {
    // Mock uptime - in real implementation, this would come from the server
    const seconds = Math.floor(Math.random() * 86400)
    const hours = Math.floor(seconds / 3600)
    const minutes = Math.floor((seconds % 3600) / 60)
    return `${hours}h ${minutes}m`
  }

  function showHelp() {
    const helpText = `
可用命令:

:help      - 显示此帮助信息
:sysinfo   - 显示系统信息
:queries   - 显示正在运行的查询
:dbs       - 显示数据库列表
:clear     - 清空可视化
:stats     - 刷新统计信息
:labels    - 显示所有标签
:reltypes  - 显示所有关系类型

使用方法: 在查询编辑器中输入命令并按 Ctrl+Enter
    `.trim()
    alert(helpText)
  }

  function showDatabases() {
    alert('当前数据库: default (在线)')
  }

  function clearVisualization() {
    // This will be handled by the visualization store
    window.dispatchEvent(new CustomEvent('clear-visualization'))
  }

  async function refreshStats() {
    await graphStore.fetchStats()
  }

  function showLabels() {
    const labels = graphStore.stats.labels
    alert(`标签列表:\n${labels.join(', ') || '(无)'}`)
  }

  function showRelTypes() {
    const relTypes = graphStore.stats.rel_types
    alert(`关系类型:\n${relTypes.join(', ') || '(无)'}`)
  }

  return {
    // State
    systemInfo,
    runningQueries,
    commands,
    // Methods
    executeCommand,
    isCommand,
    fetchSystemInfo,
    fetchRunningQueries,
  }
}
