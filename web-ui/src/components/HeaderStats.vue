<template>
  <div class="flex items-center gap-6 text-sm">
    <div class="flex items-center gap-2">
      <span class="text-gray-500">节点</span>
      <span class="text-primary-400 font-semibold">{{ stats.node_count }}</span>
    </div>
    <div class="flex items-center gap-2">
      <span class="text-gray-500">关系</span>
      <span class="text-primary-400 font-semibold">{{ stats.rel_count }}</span>
    </div>
    <div class="flex items-center gap-2">
      <span class="text-gray-500">标签</span>
      <span class="text-primary-400 font-semibold">{{ stats.labels.length }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { useGraphStore } from '@/stores/graph'

const graphStore = useGraphStore()

const stats = computed(() => graphStore.stats)

onMounted(() => {
  graphStore.fetchStats()
})
</script>
