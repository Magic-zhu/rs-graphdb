import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useGraphStore } from './graph'
import { api } from '@/api'

// Mock API module
vi.mock('@/api', () => ({
  api: {
    getStats: vi.fn(),
    getLabels: vi.fn(),
    getRelTypes: vi.fn(),
    getAllNodes: vi.fn(),
    getAllRels: vi.fn(),
    createNode: vi.fn(),
    createRel: vi.fn(),
    deleteNode: vi.fn(),
    deleteRel: vi.fn(),
    query: vi.fn(),
    search: vi.fn(),
  },
}))

describe('Graph Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  describe('Stats', () => {
    it('should fetch stats successfully', async () => {
      const mockStats = {
        node_count: 100,
        rel_count: 200,
        labels: ['User', 'Post'],
        rel_types: ['FOLLOWS', 'LIKES'],
      }
      vi.mocked(api.getStats).mockResolvedValue(mockStats)

      const store = useGraphStore()
      await store.fetchStats()

      expect(store.stats).toEqual(mockStats)
      expect(store.nodeCount).toBe(100)
      expect(store.relCount).toBe(200)
    })

    it('should handle fetch stats error', async () => {
      vi.mocked(api.getStats).mockRejectedValue(new Error('Network error'))

      const store = useGraphStore()
      await expect(store.fetchStats()).rejects.toThrow('Network error')
      expect(store.error).toBe('Failed to fetch stats')
    })
  })

  describe('Nodes', () => {
    it('should fetch all nodes', async () => {
      const mockNodes = [
        { id: 1, labels: ['User'], properties: { name: 'Alice' } },
        { id: 2, labels: ['User'], properties: { name: 'Bob' } },
      ]
      vi.mocked(api.getAllNodes).mockResolvedValue(mockNodes)

      const store = useGraphStore()
      await store.fetchNodes()

      expect(store.nodes).toEqual(mockNodes)
      expect(store.loading).toBe(false)
    })

    it('should create a new node', async () => {
      const mockResponse = { id: 3 }
      vi.mocked(api.createNode).mockResolvedValue(mockResponse)
      vi.mocked(api.getStats).mockResolvedValue({
        node_count: 101,
        rel_count: 200,
        labels: ['User'],
        rel_types: [],
      })

      const store = useGraphStore()
      const nodeId = await store.createNode(['User'], { name: 'Charlie' })

      expect(nodeId).toBe(3)
      expect(api.createNode).toHaveBeenCalledWith({
        labels: ['User'],
        properties: { name: 'Charlie' },
      })
    })
  })

  describe('Relationships', () => {
    it('should fetch all relationships', async () => {
      const mockRels = [
        { id: 1, start: 1, end: 2, typ: 'FOLLOWS', properties: {} },
        { id: 2, start: 2, end: 1, typ: 'FOLLOWS', properties: {} },
      ]
      vi.mocked(api.getAllRels).mockResolvedValue(mockRels)

      const store = useGraphStore()
      await store.fetchRels()

      expect(store.rels).toEqual(mockRels)
    })

    it('should create a new relationship', async () => {
      const mockResponse = { id: 3 }
      vi.mocked(api.createRel).mockResolvedValue(mockResponse)
      vi.mocked(api.getStats).mockResolvedValue({
        node_count: 100,
        rel_count: 201,
        labels: [],
        rel_types: ['FOLLOWS'],
      })

      const store = useGraphStore()
      const relId = await store.createRel(1, 2, 'FOLLOWS', { since: '2024' })

      expect(relId).toBe(3)
      expect(api.createRel).toHaveBeenCalledWith({
        start: 1,
        end: 2,
        rel_type: 'FOLLOWS',
        properties: { since: '2024' },
      })
    })
  })

  describe('Query', () => {
    it('should query by label', async () => {
      const mockResults = [
        { id: 1, labels: ['User'], properties: { name: 'Alice' } },
      ]
      vi.mocked(api.query).mockResolvedValue(mockResults)

      const store = useGraphStore()
      const results = await store.queryByLabel('User')

      expect(results).toEqual(mockResults)
      expect(api.query).toHaveBeenCalledWith({ label: 'User' })
    })

    it('should search nodes', async () => {
      const mockResults = [
        { id: 1, labels: ['User'], properties: { name: 'Alice' } },
      ]
      vi.mocked(api.search).mockResolvedValue(mockResults)

      const store = useGraphStore()
      const results = await store.searchNodes('Alice')

      expect(results).toEqual(mockResults)
      expect(api.search).toHaveBeenCalledWith({ query: 'Alice' })
    })
  })

  describe('Delete Operations', () => {
    it('should delete a node', async () => {
      vi.mocked(api.deleteNode).mockResolvedValue(undefined)
      vi.mocked(api.getStats).mockResolvedValue({
        node_count: 99,
        rel_count: 200,
        labels: ['User'],
        rel_types: [],
      })

      const store = useGraphStore()
      store.nodes = [
        { id: 1, labels: ['User'], properties: { name: 'Alice' } },
        { id: 2, labels: ['User'], properties: { name: 'Bob' } },
      ]

      await store.deleteNode(1)

      expect(api.deleteNode).toHaveBeenCalledWith(1)
      expect(store.nodes).toHaveLength(1)
      expect(store.nodes[0].id).toBe(2)
    })

    it('should delete a relationship', async () => {
      vi.mocked(api.deleteRel).mockResolvedValue(undefined)
      vi.mocked(api.getStats).mockResolvedValue({
        node_count: 100,
        rel_count: 199,
        labels: [],
        rel_types: ['FOLLOWS'],
      })

      const store = useGraphStore()
      store.rels = [
        { id: 1, start: 1, end: 2, typ: 'FOLLOWS', properties: {} },
        { id: 2, start: 2, end: 1, typ: 'FOLLOWS', properties: {} },
      ]

      await store.deleteRel(1)

      expect(api.deleteRel).toHaveBeenCalledWith(1)
      expect(store.rels).toHaveLength(1)
      expect(store.rels[0].id).toBe(2)
    })
  })
})
