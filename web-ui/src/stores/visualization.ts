import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export interface VisNode {
  id: number
  label: string
  title?: string
  color?: string
  x?: number
  y?: number
}

export interface VisEdge {
  id: string
  from: number
  to: number
  label?: string
  color?: string
}

export const useVisualizationStore = defineStore('visualization', () => {
  // State
  const nodes = ref<Map<number, VisNode>>(new Map())
  const edges = ref<Map<string, VisEdge>>(new Map())
  const selectedNodeId = ref<number | null>(null)
  const physicsEnabled = ref(true)

  // Computed
  const nodeCount = computed(() => nodes.value.size)
  const edgeCount = computed(() => edges.value.size)
  const selectedNode = computed(() => {
    if (selectedNodeId.value === null) return null
    return nodes.value.get(selectedNodeId.value) || null
  })

  // Actions
  function setNode(node: VisNode) {
    nodes.value.set(node.id, node)
  }

  function setNodes(newNodes: VisNode[]) {
    const map = new Map<number, VisNode>()
    newNodes.forEach(node => map.set(node.id, node))
    nodes.value = map
  }

  function addNode(node: VisNode) {
    nodes.value.set(node.id, node)
  }

  function removeNode(id: number) {
    nodes.value.delete(id)
    // Remove all edges connected to this node
    const edgesToRemove: string[] = []
    edges.value.forEach((edge, key) => {
      if (edge.from === id || edge.to === id) {
        edgesToRemove.push(key)
      }
    })
    edgesToRemove.forEach(key => edges.value.delete(key))
  }

  function setEdge(edge: VisEdge) {
    edges.value.set(edge.id, edge)
  }

  function setEdges(newEdges: VisEdge[]) {
    const map = new Map<string, VisEdge>()
    newEdges.forEach(edge => map.set(edge.id, edge))
    edges.value = map
  }

  function addEdge(edge: VisEdge) {
    edges.value.set(edge.id, edge)
  }

  function removeEdge(id: string) {
    edges.value.delete(id)
  }

  function clear() {
    nodes.value.clear()
    edges.value.clear()
    selectedNodeId.value = null
  }

  function selectNode(id: number | null) {
    selectedNodeId.value = id
  }

  function togglePhysics() {
    physicsEnabled.value = !physicsEnabled.value
  }

  function getNode(id: number) {
    return nodes.value.get(id)
  }

  function getEdge(id: string) {
    return edges.value.get(id)
  }

  function getAllNodes() {
    return Array.from(nodes.value.values())
  }

  function getAllEdges() {
    return Array.from(edges.value.values())
  }

  return {
    // State
    nodes,
    edges,
    selectedNodeId,
    physicsEnabled,
    // Computed
    nodeCount,
    edgeCount,
    selectedNode,
    // Actions
    setNode,
    setNodes,
    addNode,
    removeNode,
    setEdge,
    setEdges,
    addEdge,
    removeEdge,
    clear,
    selectNode,
    togglePhysics,
    getNode,
    getEdge,
    getAllNodes,
    getAllEdges,
  }
})
