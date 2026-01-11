<template>
  <div class="h-screen flex flex-col overflow-hidden">
    <!-- Header -->
    <header class="h-14 bg-gray-900 border-b border-gray-800 flex items-center justify-between px-6 shrink-0">
      <div class="flex items-center gap-3">
        <div class="w-8 h-8 bg-gradient-to-br from-primary-500 to-primary-700 rounded-lg flex items-center justify-center text-white font-bold text-sm">
          RG
        </div>
        <h1 class="text-lg font-semibold text-primary-400">rs-graphdb</h1>
        <span class="px-2 py-0.5 bg-green-600 text-white text-xs font-medium rounded-full">v0.0.1</span>
      </div>

      <HeaderStats />
    </header>

    <!-- Main Content -->
    <main class="flex-1 flex overflow-hidden">
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
import { RouterView } from 'vue-router'
import HeaderStats from '@/components/HeaderStats.vue'
</script>
