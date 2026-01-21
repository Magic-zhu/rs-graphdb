import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useQueryHistoryStore } from './queryHistory'

describe('QueryHistory Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    // Clear localStorage
    localStorage.clear()
    vi.clearAllMocks()
  })

  describe('Add Query', () => {
    it('should add a successful query to history', () => {
      const store = useQueryHistoryStore()
      const query = 'MATCH (n) RETURN n LIMIT 10'

      store.addQuery(query, true, 10, 150)

      expect(store.history).toHaveLength(1)
      expect(store.history[0].query).toBe(query)
      expect(store.history[0].success).toBe(true)
      expect(store.history[0].resultCount).toBe(10)
      expect(store.history[0].executionTime).toBe(150)
    })

    it('should add a failed query to history', () => {
      const store = useQueryHistoryStore()
      const query = 'INVALID QUERY'

      store.addQuery(query, false, undefined, undefined, 'Syntax error')

      expect(store.history).toHaveLength(1)
      expect(store.history[0].success).toBe(false)
      expect(store.history[0].errorMessage).toBe('Syntax error')
    })

    it('should not add duplicate consecutive queries', () => {
      const store = useQueryHistoryStore()
      const query = 'MATCH (n) RETURN n'

      store.addQuery(query, true, 5, 100)
      store.addQuery(query, true, 5, 100)

      expect(store.history).toHaveLength(1)
    })

    it('should limit history to MAX_HISTORY entries', () => {
      const store = useQueryHistoryStore()

      // Add more than MAX_HISTORY (100) queries
      for (let i = 0; i < 105; i++) {
        store.addQuery(`MATCH (n) RETURN n LIMIT ${i}`, true, i, 100)
      }

      expect(store.history).toHaveLength(100)
      expect(store.history[0].query).toContain('104') // Most recent
    })
  })

  describe('Remove Query', () => {
    it('should remove a query from history', () => {
      const store = useQueryHistoryStore()
      store.addQuery('QUERY 1', true)
      store.addQuery('QUERY 2', true)

      const idToRemove = store.history[0].id
      store.removeQuery(idToRemove)

      expect(store.history).toHaveLength(1)
      expect(store.history[0].query).toBe('QUERY 1')
    })
  })

  describe('Clear History', () => {
    it('should clear all history', () => {
      const store = useQueryHistoryStore()
      store.addQuery('QUERY 1', true)
      store.addQuery('QUERY 2', true)

      store.clearHistory()

      expect(store.history).toHaveLength(0)
    })
  })

  describe('Search History', () => {
    it('should search history by query text', () => {
      const store = useQueryHistoryStore()
      store.addQuery('MATCH (u:User) RETURN u', true)
      store.addQuery('MATCH (p:Post) RETURN p', true)
      store.addQuery('MATCH (c:Comment) RETURN c', true)

      const results = store.searchHistory('User')

      expect(results).toHaveLength(1)
      expect(results[0].query).toContain('User')
    })

    it('should return all history when search term is empty', () => {
      const store = useQueryHistoryStore()
      store.addQuery('QUERY 1', true)
      store.addQuery('QUERY 2', true)

      const results = store.searchHistory('')

      expect(results).toHaveLength(2)
    })
  })

  describe('Recent History', () => {
    it('should return only recent 20 queries', () => {
      const store = useQueryHistoryStore()

      for (let i = 0; i < 25; i++) {
        store.addQuery(`QUERY ${i}`, true)
      }

      expect(store.historyCount).toBe(25)
      expect(store.recentHistory).toHaveLength(20)
    })
  })

  describe('Persistence', () => {
    it('should save history to localStorage', () => {
      const store = useQueryHistoryStore()
      const setItemSpy = vi.spyOn(localStorage, 'setItem')

      store.addQuery('TEST QUERY', true)

      expect(setItemSpy).toHaveBeenCalled()
    })

    it('should load history from localStorage on init', () => {
      const savedHistory = [
        {
          id: 'test-id',
          query: 'SAVED QUERY',
          timestamp: new Date().toISOString(),
          success: true,
        },
      ]
      vi.spyOn(localStorage, 'getItem').mockReturnValue(JSON.stringify(savedHistory))

      const store = useQueryHistoryStore()

      expect(store.history).toHaveLength(1)
      expect(store.history[0].query).toBe('SAVED QUERY')
    })
  })
})
