import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { QueryHistory } from '@/types/query'

const STORAGE_KEY = 'graphdb_query_history'
const MAX_HISTORY = 100

export const useQueryHistoryStore = defineStore('queryHistory', () => {
  // State
  const history = ref<QueryHistory[]>([])
  const loading = ref(false)

  // Computed
  const recentHistory = computed(() => history.value.slice(0, 20))
  const historyCount = computed(() => history.value.length)

  // Actions
  function loadFromStorage() {
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      if (stored) {
        const parsed = JSON.parse(stored)
        history.value = parsed.map((h: any) => ({
          ...h,
          timestamp: new Date(h.timestamp),
        }))
      }
    } catch (err) {
      console.error('Failed to load query history:', err)
      history.value = []
    }
  }

  function saveToStorage() {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(history.value))
    } catch (err) {
      console.error('Failed to save query history:', err)
    }
  }

  function addQuery(
    query: string,
    success: boolean,
    resultCount?: number,
    executionTime?: number,
    errorMessage?: string
  ) {
    const entry: QueryHistory = {
      id: `qh_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      query: query.trim(),
      timestamp: new Date(),
      resultCount,
      executionTime,
      success,
      errorMessage,
    }

    // Don't add duplicate queries
    const lastEntry = history.value[0]
    if (lastEntry && lastEntry.query === entry.query) {
      return
    }

    history.value.unshift(entry)

    // Keep only MAX_HISTORY entries
    if (history.value.length > MAX_HISTORY) {
      history.value = history.value.slice(0, MAX_HISTORY)
    }

    saveToStorage()
  }

  function removeQuery(id: string) {
    history.value = history.value.filter((h) => h.id !== id)
    saveToStorage()
  }

  function clearHistory() {
    history.value = []
    saveToStorage()
  }

  function searchHistory(searchTerm: string): QueryHistory[] {
    if (!searchTerm) return history.value
    const term = searchTerm.toLowerCase()
    return history.value.filter((h) => h.query.toLowerCase().includes(term))
  }

  // Initialize
  loadFromStorage()

  return {
    // State
    history,
    loading,
    // Computed
    recentHistory,
    historyCount,
    // Actions
    addQuery,
    removeQuery,
    clearHistory,
    searchHistory,
    loadFromStorage,
    saveToStorage,
  }
})
