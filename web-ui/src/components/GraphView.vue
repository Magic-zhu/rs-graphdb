<template>
  <div ref="containerRef" class="w-full h-full absolute inset-0 bg-gray-950 graph-container"></div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import { Graph } from '@antv/g6'
import { useVisualizationStore } from '@/stores/visualization'

const emit = defineEmits<{
  'closeDetails': []
  'expandNeighbors': [nodeId: number]
}>()

const containerRef = ref<HTMLElement>()
const visStore = useVisualizationStore()

let graph: Graph | null = null
let resizeObserver: ResizeObserver | null = null

// 获取容器尺寸
function getContainerSize() {
  if (!containerRef.value) return { width: 800, height: 600 }
  return {
    width: containerRef.value.clientWidth,
    height: containerRef.value.clientHeight,
  }
}

// 调整图表尺寸
function resizeGraph() {
  if (!graph || !containerRef.value) return
  const { width, height } = getContainerSize()
  console.log('Resizing graph to:', { width, height })
  graph.resize(width, height)
}

// 设置节点状态
function setNodeState(id: string, state: string, value: boolean) {
  if (!graph) return
  try {
    const allNodeData = graph.getNodeData() as any
    const nodeData = Array.isArray(allNodeData)
      ? allNodeData.find((n: any) => n.id === id)
      : allNodeData

    if (nodeData) {
      const states = value
        ? [...(nodeData.states || []), state]
        : (nodeData.states || []).filter((s: string) => s !== state)
      graph.updateNodeData([{ id, style: { states } }])
    }
  } catch (error) {
    console.error('Error setting node state:', error)
  }
}

// 设置边状态
function setEdgeState(id: string, state: string, value: boolean) {
  if (!graph) return
  try {
    const allEdgeData = graph.getEdgeData() as any
    const edgeData = Array.isArray(allEdgeData)
      ? allEdgeData.find((e: any) => e.id === id)
      : allEdgeData

    if (edgeData) {
      const states = value
        ? [...(edgeData.states || []), state]
        : (edgeData.states || []).filter((s: string) => s !== state)
      graph.updateEdgeData([{ id, style: { states } }])
    }
  } catch (error) {
    console.error('Error setting edge state:', error)
  }
}

// 更新图表数据
async function updateGraph() {
  if (!graph) return

  const nodesArray = Array.from(visStore.nodes.values()).map((n) => ({
    id: n.id.toString(),
    data: {
      label: n.label,
      title: n.title,
    },
    style: n.color ? { fill: n.color } : {},
  }))

  const edgesArray = Array.from(visStore.edges.values()).map((e) => ({
    id: e.id,
    source: e.from.toString(),
    target: e.to.toString(),
    data: {
      label: e.label,
    },
    style: e.color ? { stroke: e.color } : {},
  }))

  console.log('Updating graph:', { nodes: nodesArray.length, edges: edgesArray.length })
  console.log('Edges data:', edgesArray.slice(0, 5))

  try {
    // 清空现有数据
    graph.clear()

    // 先添加节点数据
    if (nodesArray.length > 0) {
      graph.addNodeData(nodesArray)
    }

    // 渲染节点
    await graph.draw()

    // 等待一帧确保节点完全渲染
    await new Promise(resolve => requestAnimationFrame(resolve))

    // 再添加边数据
    if (edgesArray.length > 0) {
      graph.addEdgeData(edgesArray)
    }

    // 最终渲染
    await graph.draw()

    // 调试：检查渲染后的数据
    const renderedNodes = graph.getNodeData()
    const renderedEdges = graph.getEdgeData()
    console.log('Rendered:', {
      nodes: Array.isArray(renderedNodes) ? renderedNodes.length : 0,
      edges: Array.isArray(renderedEdges) ? renderedEdges.length : 0,
    })

    // 重新设置选中状态
    if (visStore.selectedNodeId !== null) {
      setNodeState(visStore.selectedNodeId.toString(), 'selected', true)
    }
  } catch (error) {
    console.error('Error updating graph:', error)
    console.error('Error details:', error)
  }
}

onMounted(() => {
  if (!containerRef.value) return

  const { width, height } = getContainerSize()
  console.log('Initializing graph with size:', { width, height })

  // 创建 G6 图实例
  const GraphConstructor = Graph as any
  graph = new GraphConstructor({
    container: containerRef.value,
    width,
    height,
    padding: 20,
    node: {
      style: {
        size: 35,
        labelText: (d: any) => d.data?.label || '',
        labelFill: '#e5e7eb',
        labelFontSize: 12,
        labelMaxWidth: 150,
        fill: '#1d4ed8',
        stroke: '#3b82f6',
        strokeWidth: 2,
      },
      state: {
        hover: {
          halo: true,
          haloLineWidth: 8,
          haloStroke: '#3b82f6',
          haloOpacity: 0.3,
        },
        selected: {
          fill: '#2563eb',
          stroke: '#3b82f6',
          haloLineWidth: 8,
          haloStroke: '#3b82f6',
          haloOpacity: 0.5,
        },
      },
    },
    edge: {
      style: {
        stroke: '#6b7280',
        lineWidth: 2,
        endArrow: true,
        endArrowType: 'triangle',
        endArrowSize: 12,
        labelText: (d: any) => d.data?.label || '',
        labelFill: '#9ca3af',
        labelFontSize: 11,
        labelBackground: true,
        labelBackgroundFill: '#1f2937',
        labelBackgroundRadius: 4,
        labelPadding: [2, 4],
        labelMaxWidth: 100,
      },
      state: {
        hover: {
          stroke: '#3b82f6',
          lineWidth: 3,
        },
        selected: {
          stroke: '#3b82f6',
          lineWidth: 3,
        },
      },
    },
    layout: {
      type: 'force',
      preventOverlap: true,
      nodeSpacing: 50,
      linkDistance: 150,
    },
    behaviors: [
      {
        type: 'zoom-canvas',
        enableOptimize: true,
        optimizeZoom: 0.01,
      },
      {
        type: 'drag-canvas',
      },
      {
        type: 'drag-element',
      },
    ],
  })

  console.log('Graph initialized')

  if (graph) {
    // 监听节点点击事件
    graph.on('node:click', (event: any) => {
      const nodeId = event.itemId as string
      console.log('Node clicked:', nodeId)
      visStore.selectNode(parseInt(nodeId, 10))
    })

    // 监听节点双击事件 - 展开邻居
    graph.on('node:dblclick', (event: any) => {
      const nodeId = event.itemId as string
      console.log('Node double clicked:', nodeId)
      emit('expandNeighbors', parseInt(nodeId, 10))
    })

    // 监听画布点击事件（取消选择）
    graph.on('canvas:click', () => {
      visStore.selectNode(null)
    })

    // 监听节点悬停事件
    graph.on('node:mouseenter', (event: any) => {
      setNodeState(event.itemId, 'hover', true)
    })

    graph.on('node:mouseleave', (event: any) => {
      setNodeState(event.itemId, 'hover', false)
    })

    // 监听边悬停事件
    graph.on('edge:mouseenter', (event: any) => {
      setEdgeState(event.itemId, 'hover', true)
    })

    graph.on('edge:mouseleave', (event: any) => {
      setEdgeState(event.itemId, 'hover', false)
    })
  }

  // 监听 store 变化并更新图表
  watch(
    () => [visStore.nodes, visStore.edges] as const,
    () => {
      updateGraph()
    },
    { deep: true }
  )

  // 监听选中的节点
  watch(
    () => visStore.selectedNodeId,
    (id) => {
      if (!graph) return

      // 清除所有选中状态
      const nodeData = graph.getNodeData()
      const edgeData = graph.getEdgeData()

      if (Array.isArray(nodeData)) {
        nodeData.forEach((node: any) => {
          setNodeState(node.id, 'selected', false)
        })
      }
      if (Array.isArray(edgeData)) {
        edgeData.forEach((edge: any) => {
          setEdgeState(edge.id, 'selected', false)
        })
      }

      // 设置新选中的节点
      if (id !== null) {
        const idStr = id.toString()
        setNodeState(idStr, 'selected', true)
        // 聚焦到选中的节点
        graph.focusElement(idStr, true, {
          duration: 300,
          easing: 'ease-cubic',
        })
      }
    }
  )

  // 使用 ResizeObserver 监听容器尺寸变化
  resizeObserver = new ResizeObserver(() => {
    resizeGraph()
  })
  if (containerRef.value) {
    resizeObserver.observe(containerRef.value)
  }
})

onUnmounted(() => {
  if (resizeObserver) {
    resizeObserver.disconnect()
    resizeObserver = null
  }
  if (graph) {
    graph.destroy()
    graph = null
  }
})

// 暴露方法
defineExpose({
  fit: () => {
    if (graph) {
      graph.fitView()
    }
  },
  focus: (id: number) => {
    if (graph) {
      visStore.selectNode(id)
    }
  },
})
</script>

<style scoped>
.graph-container {
  min-height: 100%;
}
</style>
