<template>
  <div class="flex justify-between items-center py-1 text-sm">
    <span class="text-gray-500">{{ label }}</span>
    <span class="text-primary-400 font-semibold">{{ value }}</span>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  label: string
  value: number | string
}>()
</script>
