import { describe, it, expect, beforeEach, vi } from 'vitest'
import { useCommandStore } from './commands'
import { useGraphStore } from './graph'

// Mock graph store
vi.mock('./graph', () => ({
  useGraphStore: vi.fn(),
}))

describe('Command Store', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Reset mock implementations
    vi.mocked(useGraphStore).mockReturnValue({
      stats: {
        node_count: 100,
        rel_count: 200,
        labels: ['User', 'Post'],
        rel_types: ['FOLLOWS', 'LIKES'],
      },
      fetchStats: vi.fn().mockResolvedValue(undefined),
    } as any)
  })

  describe('Command Recognition', () => {
    it('should recognize valid commands', () => {
      const { isCommand } = useCommandStore()

      expect(isCommand(':help')).toBe(true)
      expect(isCommand(':sysinfo')).toBe(true)
      expect(isCommand(':queries')).toBe(true)
      expect(isCommand(':dbs')).toBe(true)
      expect(isCommand(':clear')).toBe(true)
      expect(isCommand(':stats')).toBe(true)
      expect(isCommand(':labels')).toBe(true)
      expect(isCommand(':reltypes')).toBe(true)
    })

    it('should not recognize invalid commands', () => {
      const { isCommand } = useCommandStore()

      expect(isCommand(':invalid')).toBe(false)
      expect(isCommand('MATCH (n) RETURN n')).toBe(false)
      expect(isCommand('')).toBe(false)
      expect(isCommand('help')).toBe(false) // Missing colon
      expect(isCommand(': INVALID')).toBe(true) // Space after colon is still a command
    })

    it('should be case insensitive', () => {
      const { isCommand } = useCommandStore()

      expect(isCommand(':HELP')).toBe(true)
      expect(isCommand(':SysInfo')).toBe(true)
      expect(isCommand(':STATS')).toBe(true)
    })
  })

  describe('Command Execution', () => {
    it('should execute :help command', async () => {
      const mockAlert = vi.spyOn(window, 'alert').mockImplementation(() => {})
      const { executeCommand } = useCommandStore()

      const result = await executeCommand(':help')

      expect(result.success).toBe(true)
      expect(mockAlert).toHaveBeenCalled()
      expect(mockAlert.mock.calls[0][0]).toContain('可用命令')
    })

    it('should execute :stats command', async () => {
      const mockFetchStats = vi.fn().mockResolvedValue(undefined)
      vi.mocked(useGraphStore).mockReturnValue({
        stats: { node_count: 100, rel_count: 200, labels: [], rel_types: [] },
        fetchStats: mockFetchStats,
      } as any)

      const { executeCommand } = useCommandStore()

      const result = await executeCommand(':stats')

      expect(result.success).toBe(true)
      expect(mockFetchStats).toHaveBeenCalled()
    })

    it('should execute :labels command', async () => {
      const mockAlert = vi.spyOn(window, 'alert').mockImplementation(() => {})
      vi.mocked(useGraphStore).mockReturnValue({
        stats: {
          node_count: 100,
          rel_count: 200,
          labels: ['User', 'Post', 'Comment'],
          rel_types: [],
        },
        fetchStats: vi.fn(),
      } as any)

      const { executeCommand } = useCommandStore()

      const result = await executeCommand(':labels')

      expect(result.success).toBe(true)
      expect(mockAlert).toHaveBeenCalled()
      const alertContent = mockAlert.mock.calls[0][0]
      expect(alertContent).toContain('User')
      expect(alertContent).toContain('Post')
      expect(alertContent).toContain('Comment')
    })

    it('should execute :reltypes command', async () => {
      const mockAlert = vi.spyOn(window, 'alert').mockImplementation(() => {})
      vi.mocked(useGraphStore).mockReturnValue({
        stats: {
          node_count: 100,
          rel_count: 200,
          labels: [],
          rel_types: ['FOLLOWS', 'LIKES', 'MENTIONS'],
        },
        fetchStats: vi.fn(),
      } as any)

      const { executeCommand } = useCommandStore()

      const result = await executeCommand(':reltypes')

      expect(result.success).toBe(true)
      expect(mockAlert).toHaveBeenCalled()
      const alertContent = mockAlert.mock.calls[0][0]
      expect(alertContent).toContain('FOLLOWS')
      expect(alertContent).toContain('LIKES')
      expect(alertContent).toContain('MENTIONS')
    })

    it('should handle unknown command', async () => {
      const { executeCommand } = useCommandStore()

      const result = await executeCommand(':unknown')

      expect(result.success).toBe(false)
      expect(result.message).toContain('未知命令')
    })

    it('should handle command execution errors', async () => {
      const mockFetchStats = vi.fn().mockRejectedValue(new Error('Network error'))
      vi.mocked(useGraphStore).mockReturnValue({
        stats: { node_count: 100, rel_count: 200, labels: [], rel_types: [] },
        fetchStats: mockFetchStats,
      } as any)

      const { executeCommand } = useCommandStore()

      const result = await executeCommand(':stats')

      expect(result.success).toBe(false)
      expect(result.message).toContain('命令执行失败')
    })
  })

  describe('System Info', () => {
    it('should fetch system info', async () => {
      const { fetchSystemInfo, systemInfo } = useCommandStore()

      await fetchSystemInfo()

      expect(systemInfo.value.kernelVersion).toContain('rs-graphdb')
      expect(systemInfo.value.storeSize).toBeDefined()
      expect(systemInfo.value.databases).toBeDefined()
    })

    it('should calculate uptime correctly', async () => {
      const { fetchSystemInfo, systemInfo } = useCommandStore()

      await fetchSystemInfo()

      expect(systemInfo.value.uptime).toMatch(/\d+h \d+m/)
    })
  })

  describe('Custom Events', () => {
    it('should dispatch clear-visualization event for :clear command', async () => {
      const mockDispatchEvent = vi.spyOn(window, 'dispatchEvent').mockImplementation(() => true)
      const { executeCommand } = useCommandStore()

      const result = await executeCommand(':clear')

      expect(result.success).toBe(true)
      expect(mockDispatchEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'clear-visualization',
        })
      )
    })
  })
})
