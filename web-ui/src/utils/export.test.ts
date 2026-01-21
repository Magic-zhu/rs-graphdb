import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  exportGraphData,
  exportGraphCSV,
  exportQueryResults,
  copyToClipboard,
} from './export'

describe('Export Utils', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Mock URL.createObjectURL and related APIs
    global.URL.createObjectURL = vi.fn(() => 'blob:mock-url')
    global.URL.revokeObjectURL = vi.fn()
    // Mock document methods
    vi.spyOn(document, 'createElement').mockReturnValue({
      href: '',
      download: '',
      style: {},
      click: vi.fn(),
    } as any)
    vi.spyOn(document.body, 'appendChild').mockReturnValue({} as any)
    vi.spyOn(document.body, 'removeChild').mockReturnValue({} as any)
  })

  describe('exportGraphData', () => {
    it('should export graph data as JSON', () => {
      const nodes = [
        { id: '1', data: { label: 'Node 1' }, style: {} },
        { id: '2', data: { label: 'Node 2' }, style: {} },
      ]
      const edges = [
        { id: 'e1', source: '1', target: '2', data: { label: 'CONNECTS' }, style: {} },
      ]

      exportGraphData(nodes, edges, { filename: 'test' })

      expect(document.createElement).toHaveBeenCalledWith('a')
    })

    it('should export without styles when includeStyles is false', () => {
      const nodes = [
        { id: '1', data: { label: 'Node 1' }, style: { fill: 'red' } },
      ]
      const edges = [
        { id: 'e1', source: '1', target: '2', data: { label: 'CONNECTS' }, style: { stroke: 'blue' } },
      ]

      exportGraphData(nodes, edges, { filename: 'test', includeStyles: false })

      expect(document.createElement).toHaveBeenCalled()
    })
  })

  describe('exportGraphCSV', () => {
    it('should export both nodes and edges as CSV', () => {
      const nodes = [
        { id: '1', data: { label: 'Node 1' } },
        { id: '2', data: { label: 'Node 2' } },
      ]
      const edges = [
        { id: 'e1', source: '1', target: '2', data: { label: 'CONNECTS' } },
      ]

      exportGraphCSV(nodes, edges, { filename: 'test' })

      // Should create two download links (nodes and edges)
      expect(document.createElement).toHaveBeenCalled()
    })
  })

  describe('exportQueryResults', () => {
    it('should export as JSON', () => {
      const results = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]
      const columns = ['id', 'name']

      exportQueryResults(results, columns, { format: 'json', filename: 'test' })

      expect(document.createElement).toHaveBeenCalled()
    })

    it('should export as CSV', () => {
      const results = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]
      const columns = ['id', 'name']

      exportQueryResults(results, columns, { format: 'csv', filename: 'test' })

      expect(document.createElement).toHaveBeenCalled()
    })
  })

  describe('copyToClipboard', () => {
    it('should copy string to clipboard', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(global.navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      })

      await copyToClipboard('test text')

      expect(mockWriteText).toHaveBeenCalledWith('test text')
    })

    it('should copy object as JSON to clipboard', async () => {
      const mockWriteText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(global.navigator, 'clipboard', {
        value: { writeText: mockWriteText },
        writable: true,
        configurable: true,
      })

      const data = { key: 'value' }
      await copyToClipboard(data)

      expect(mockWriteText).toHaveBeenCalledWith(JSON.stringify(data, null, 2))
    })

    it('should handle clipboard errors', async () => {
      Object.defineProperty(global.navigator, 'clipboard', {
        value: { writeText: vi.fn().mockRejectedValue(new Error('Permission denied')) },
        writable: true,
        configurable: true,
      })

      await expect(copyToClipboard('test')).rejects.toThrow('Failed to copy to clipboard')
    })

    it('should throw error when clipboard API is not available', async () => {
      Object.defineProperty(global.navigator, 'clipboard', {
        value: undefined,
        writable: true,
        configurable: true,
      })

      await expect(copyToClipboard('test')).rejects.toThrow('Failed to copy to clipboard')
    })
  })
})
