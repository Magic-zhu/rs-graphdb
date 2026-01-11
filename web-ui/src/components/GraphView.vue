<template>
  <div ref="containerRef" class="w-full h-full bg-gray-950"></div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import { Network } from 'vis-network/standalone'
import { useVisualizationStore } from '@/stores/visualization'

interface VisNetworkData {
  nodes: any[]
  edges: any[]
}

interface ClickParams {
  nodes: number[]
}

const containerRef = ref<HTMLElement>()
const visStore = useVisualizationStore()

let network: Network | null = null

const options = {
  nodes: {
    shape: 'dot',
    size: 25,
    font: { size: 14, color: '#e5e7eb' },
    borderWidth: 2,
    color: {
      border: '#3b82f6',
      background: '#1d4ed8',
      highlight: { border: '#3b82f6', background: '#2563eb' },
    },
  },
  edges: {
    width: 2,
    color: { color: '#4b5563', highlight: '#3b82f6' },
    arrows: { to: { enabled: true, scaleFactor: 0.6 } },
    font: { size: 12, color: '#9ca3af', align: 'top' },
  },
  physics: {
    enabled: true,
    barnesHut: { gravitationalConstant: -12000, springLength: 200 },
  },
}

onMounted(() => {
  if (!containerRef.value) return

  const data: VisNetworkData = {
    nodes: [],
    edges: [],
  }

  network = new Network(containerRef.value, data, options)

  // Handle node click
  network.on('click', (params: ClickParams) => {
    if (params.nodes.length > 0) {
      visStore.selectNode(params.nodes[0])
    } else {
      visStore.selectNode(null)
    }
  })

  // Watch for store changes and update network
  watch(
    () => [visStore.nodes, visStore.edges] as const,
    ([nodesMap, edgesMap]) => {
      if (!network) return

      const nodesArray = Array.from(nodesMap.values()).map((n) => ({
        id: n.id,
        label: n.label,
        title: n.title,
      }))

      const edgesArray = Array.from(edgesMap.values()).map((e) => ({
        id: e.id,
        from: e.from,
        to: e.to,
        label: e.label,
      }))

      network.setData({ nodes: nodesArray, edges: edgesArray })
    },
    { deep: true }
  )

  watch(
    () => visStore.physicsEnabled,
    (enabled) => {
      if (network) {
        network.setOptions({ physics: { enabled } })
      }
    }
  )
})

onUnmounted(() => {
  if (network) {
    network.destroy()
    network = null
  }
})

// Expose methods
defineExpose({
  fit: () => network?.fit(),
  focus: (id: number) => {
    network?.selectNodes([id])
    network?.focus(id, { animation: true, scale: 1.2 })
  },
})
</script>
