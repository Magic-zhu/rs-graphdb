<template>
  <div class="panel space-y-3">
    <h3 class="text-sm font-semibold text-gray-300">{{ title }}</h3>
    <slot />
  </div>
</template>

<script setup lang="ts">
defineProps<{
  title: string
}>()
</script>
