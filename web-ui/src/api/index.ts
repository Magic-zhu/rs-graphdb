// API types
export interface Node {
  id: number
  labels: string[]
  properties: Record<string, string | number | boolean>
}

export interface Relation {
  id: number
  start: number
  end: number
  typ: string
  properties: Record<string, string | number | boolean>
}

export interface NodeResponse {
  id: number
  labels: string[]
  properties: Record<string, string | number | boolean>
}

export interface RelResponse {
  id: number
  start: number
  end: number
  typ: string
  properties: Record<string, string | number | boolean>
}

export interface DatabaseStats {
  node_count: number
  rel_count: number
  labels: string[]
  rel_types: string[]
}

export interface NeighborsResponse {
  outgoing: number[]
  incoming: number[]
}

export interface CreateNodeRequest {
  labels: string[]
  properties: Record<string, string | number | boolean>
}

export interface CreateNodeResponse {
  id: number
}

export interface CreateRelRequest {
  start: number
  end: number
  rel_type: string
  properties: Record<string, string | number | boolean>
}

export interface CreateRelResponse {
  id: number
}

export interface QueryRequest {
  label: string
  property?: string
  value?: string
  out_rel?: string
  in_rel?: string
}

export interface SearchRequest {
  query: string
}

export interface UpdateNodeRequest {
  properties: Record<string, string | number | boolean>
}

export interface BatchCreateNodesRequest {
  nodes: Array<[string[], Record<string, string | number | boolean>]>
}

export interface BatchCreateNodesResponse {
  ids: number[]
}

export interface BatchCreateRelsRequest {
  rels: Array<[number, number, string, Record<string, string | number | boolean>]>
}

export interface BatchCreateRelsResponse {
  ids: number[]
}

// API client
const API_BASE = import.meta.env.DEV ? '/api' : ''

async function request<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const url = `${API_BASE}${endpoint}`
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  })

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`)
  }

  return response.json()
}

export const api = {
  // Stats
  getStats: () => request<DatabaseStats>('/stats'),

  // Labels
  getLabels: () => request<string[]>('/labels'),
  getRelTypes: () => request<string[]>('/rel-types'),

  // Nodes
  getAllNodes: () => request<NodeResponse[]>('/nodes'),
  getNode: (id: number) => request<NodeResponse>(`/nodes/${id}`),
  createNode: (data: CreateNodeRequest) =>
    request<CreateNodeResponse>('/nodes', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
  updateNode: (id: number, data: UpdateNodeRequest) =>
    request(`/nodes/${id}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    }),
  deleteNode: (id: number) =>
    request(`/nodes/${id}`, { method: 'DELETE' }),
  getNodeNeighbors: (id: number) => request<NeighborsResponse>(`/nodes/${id}/neighbors`),

  // Relations
  getAllRels: () => request<RelResponse[]>('/rels'),
  getRel: (id: number) => request<RelResponse>(`/rels/${id}`),
  createRel: (data: CreateRelRequest) =>
    request<CreateRelResponse>('/rels', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
  deleteRel: (id: number) =>
    request(`/rels/${id}`, { method: 'DELETE' }),

  // Query
  query: (data: QueryRequest) =>
    request<NodeResponse[]>('/query', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  // Search
  search: (data: SearchRequest) =>
    request<NodeResponse[]>('/search', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  // Batch operations
  batchCreateNodes: (data: BatchCreateNodesRequest) =>
    request<BatchCreateNodesResponse>('/batch/nodes', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
  batchCreateRels: (data: BatchCreateRelsRequest) =>
    request<BatchCreateRelsResponse>('/batch/rels', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
}

export default api
