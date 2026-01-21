// Query related types
export interface QueryHistory {
  id: string
  query: string
  timestamp: Date
  resultCount?: number
  executionTime?: number
  success: boolean
  errorMessage?: string
}

export interface FavoriteQuery {
  id: string
  name: string
  query: string
  description?: string
  tags: string[]
  createdAt: Date
  updatedAt: Date
}

export interface QueryResult {
  nodes: any[]
  relationships: any[]
  executionTime: number
  rowCount: number
}

export interface Command {
  name: string
  description: string
  handler: () => void | Promise<void>
}

export const COMMANDS: Record<string, Command> = {} as Record<string, Command>
