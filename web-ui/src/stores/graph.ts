import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import api, { type NodeResponse, type RelResponse, type DatabaseStats } from '@/api'

export const useGraphStore = defineStore('graph', () => {
  // State
  const stats = ref<DatabaseStats>({
    node_count: 0,
    rel_count: 0,
    labels: [],
    rel_types: [],
  })

  const nodes = ref<NodeResponse[]>([])
  const rels = ref<RelResponse[]>([])
  const labels = ref<string[]>([])
  const relTypes = ref<string[]>([])
  const loading = ref(false)
  const error = ref<string | null>(null)

  // Computed
  const nodeCount = computed(() => stats.value.node_count)
  const relCount = computed(() => stats.value.rel_count)

  // Actions
  async function fetchStats() {
    try {
      stats.value = await api.getStats()
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to fetch stats'
      throw err
    }
  }

  async function fetchNodes() {
    try {
      loading.value = true
      nodes.value = await api.getAllNodes()
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to fetch nodes'
      throw err
    } finally {
      loading.value = false
    }
  }

  async function fetchRels() {
    try {
      loading.value = true
      rels.value = await api.getAllRels()
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to fetch relations'
      throw err
    } finally {
      loading.value = false
    }
  }

  async function fetchLabels() {
    try {
      labels.value = await api.getLabels()
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to fetch labels'
      throw err
    }
  }

  async function fetchRelTypes() {
    try {
      relTypes.value = await api.getRelTypes()
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to fetch relation types'
      throw err
    }
  }

  async function createNode(labels: string[], properties: Record<string, any>) {
    try {
      const result = await api.createNode({ labels, properties })
      await fetchStats()
      return result.id
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to create node'
      throw err
    }
  }

  async function createRel(start: number, end: number, relType: string, properties: Record<string, any>) {
    try {
      const result = await api.createRel({ start, end, rel_type: relType, properties })
      await fetchStats()
      return result.id
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to create relation'
      throw err
    }
  }

  async function deleteNode(id: number) {
    try {
      await api.deleteNode(id)
      await fetchStats()
      nodes.value = nodes.value.filter(n => n.id !== id)
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to delete node'
      throw err
    }
  }

  async function deleteRel(id: number) {
    try {
      await api.deleteRel(id)
      await fetchStats()
      rels.value = rels.value.filter(r => r.id !== id)
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Failed to delete relation'
      throw err
    }
  }

  async function queryByLabel(label: string) {
    try {
      return await api.query({ label })
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Query failed'
      throw err
    }
  }

  async function queryByProperty(label: string, property: string, value: string) {
    try {
      return await api.query({ label, property, value })
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Query failed'
      throw err
    }
  }

  async function searchNodes(query: string) {
    try {
      return await api.search({ query })
    } catch (err) {
      error.value = err instanceof Error ? err.message : 'Search failed'
      throw err
    }
  }

  function clearError() {
    error.value = null
  }

  return {
    // State
    stats,
    nodes,
    rels,
    labels,
    relTypes,
    loading,
    error,
    // Computed
    nodeCount,
    relCount,
    // Actions
    fetchStats,
    fetchNodes,
    fetchRels,
    fetchLabels,
    fetchRelTypes,
    createNode,
    createRel,
    deleteNode,
    deleteRel,
    queryByLabel,
    queryByProperty,
    searchNodes,
    clearError,
  }
})
