<template>
  <div class="visualization-controls bg-gray-900 border-l border-gray-800 flex flex-col">
    <!-- Header -->
    <div class="p-4 border-b border-gray-800">
      <h3 class="text-sm font-semibold text-gray-300">可视化控制</h3>
    </div>

    <!-- Scrollable Content -->
    <div class="flex-1 overflow-y-auto p-4 space-y-4">
      <!-- Layout Section -->
      <PanelSection title="布局算法">
        <div class="grid grid-cols-2 gap-2">
          <button
            v-for="layout in layouts"
            :key="layout.value"
            @click="changeLayout(layout.value)"
            :class="[
              'px-3 py-2 rounded text-xs font-medium transition-colors',
              currentLayout === layout.value
                ? 'bg-primary-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
            ]"
          >
            {{ layout.label }}
          </button>
        </div>
      </PanelSection>

      <!-- Physics Controls -->
      <PanelSection title="物理引擎">
        <div class="space-y-3">
          <button
            @click="togglePhysics"
            :class="[
              'w-full px-3 py-2 rounded text-sm font-medium transition-colors',
              physicsEnabled
                ? 'bg-green-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
            ]"
          >
            {{ physicsEnabled ? '物理引擎: 启用' : '物理引擎: 禁用' }}
          </button>

          <div v-if="physicsEnabled && currentLayout === 'force'" class="space-y-2">
            <div>
              <label class="text-xs text-gray-400">节点间距</label>
              <input
                v-model.number="layoutConfig.nodeSpacing"
                type="range"
                min="20"
                max="200"
                class="w-full"
                @input="updateLayout"
              />
              <span class="text-xs text-gray-500">{{ layoutConfig.nodeSpacing }}px</span>
            </div>
            <div>
              <label class="text-xs text-gray-400">边长度</label>
              <input
                v-model.number="layoutConfig.linkDistance"
                type="range"
                min="50"
                max="300"
                class="w-full"
                @input="updateLayout"
              />
              <span class="text-xs text-gray-500">{{ layoutConfig.linkDistance }}px</span>
            </div>
          </div>
        </div>
      </PanelSection>

      <!-- Node Styles -->
      <PanelSection title="节点样式">
        <div class="space-y-3">
          <div>
            <label class="text-xs text-gray-400 block mb-1">节点大小</label>
            <input
              v-model.number="nodeStyle.size"
              type="range"
              min="10"
              max="100"
              class="w-full"
              @input="updateNodeStyle"
            />
            <span class="text-xs text-gray-500">{{ nodeStyle.size }}px</span>
          </div>

          <div>
            <label class="text-xs text-gray-400 block mb-2">颜色方案</label>
            <div class="grid grid-cols-3 gap-1">
              <button
                v-for="scheme in colorSchemes"
                :key="scheme.name"
                @click="setColorScheme(scheme)"
                :class="[
                  'px-2 py-1 rounded text-xs transition-colors',
                  nodeStyle.colorScheme === scheme.name
                    ? 'bg-primary-600 text-white'
                    : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
                ]"
              >
                {{ scheme.label }}
              </button>
            </div>
          </div>

          <div>
            <label class="text-xs text-gray-400 block mb-2">按标签着色</label>
            <div class="flex flex-wrap gap-1">
              <button
                @click="colorByLabel = null"
                :class="[
                  'px-2 py-1 text-xs rounded-full transition-colors',
                  colorByLabel === null
                    ? 'bg-primary-600 text-white'
                    : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
                ]"
              >
                全部
              </button>
              <button
                v-for="label in labels"
                :key="label"
                @click="colorByLabel = label"
                :class="[
                  'px-2 py-1 text-xs rounded-full transition-colors',
                  colorByLabel === label
                    ? 'bg-primary-600 text-white'
                    : 'bg-gray-700 text-gray-300 hover:bg-gray-600',
                ]"
              >
                {{ label }}
              </button>
            </div>
          </div>
        </div>
      </PanelSection>

      <!-- Edge Styles -->
      <PanelSection title="边样式">
        <div class="space-y-3">
          <div>
            <label class="text-xs text-gray-400 block mb-1">边粗细</label>
            <input
              v-model.number="edgeStyle.lineWidth"
              type="range"
              min="1"
              max="10"
              class="w-full"
              @input="updateEdgeStyle"
            />
            <span class="text-xs text-gray-500">{{ edgeStyle.lineWidth }}px</span>
          </div>

          <div>
            <label class="text-xs text-gray-400 block mb-1">透明度</label>
            <input
              v-model.number="edgeStyle.opacity"
              type="range"
              min="10"
              max="100"
              class="w-full"
              @input="updateEdgeStyle"
            />
            <span class="text-xs text-gray-500">{{ edgeStyle.opacity }}%</span>
          </div>

          <label class="flex items-center gap-2 text-sm text-gray-300">
            <input
              v-model="edgeStyle.showArrows"
              type="checkbox"
              class="w-4 h-4 rounded border-gray-600 bg-gray-700 text-primary-600"
              @change="updateEdgeStyle"
            />
            <span>显示箭头</span>
          </label>

          <label class="flex items-center gap-2 text-sm text-gray-300">
            <input
              v-model="edgeStyle.showLabels"
              type="checkbox"
              class="w-4 h-4 rounded border-gray-600 bg-gray-700 text-primary-600"
              @change="updateEdgeStyle"
            />
            <span>显示标签</span>
          </label>
        </div>
      </PanelSection>

      <!-- View Controls -->
      <PanelSection title="视图控制">
        <div class="space-y-2">
          <button
            @click="$emit('fit-view')"
            class="w-full px-3 py-2 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 transition-colors"
          >
            适应视图
          </button>
          <button
            @click="$emit('zoom-in')"
            class="w-full px-3 py-2 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 transition-colors"
          >
            放大
          </button>
          <button
            @click="$emit('zoom-out')"
            class="w-full px-3 py-2 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 transition-colors"
          >
            缩小
          </button>
          <button
            @click="$emit('reset-view')"
            class="w-full px-3 py-2 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 transition-colors"
          >
            重置视图
          </button>
        </div>
      </PanelSection>

      <!-- Filter -->
      <PanelSection title="过滤">
        <div class="space-y-3">
          <div>
            <label class="text-xs text-gray-400 block mb-1">最小节点度</label>
            <input
              v-model.number="minDegree"
              type="range"
              min="0"
              max="10"
              class="w-full"
              @change="applyFilter"
            />
            <span class="text-xs text-gray-500">≥ {{ minDegree }} 连接</span>
          </div>

          <button
            v-if="hasFilter"
            @click="clearFilter"
            class="w-full px-3 py-2 bg-red-600/20 text-red-400 text-sm rounded hover:bg-red-600/30 transition-colors"
          >
            清除过滤
          </button>
        </div>
      </PanelSection>

      <!-- Export -->
      <PanelSection title="导出">
        <button
          @click="$emit('show-export')"
          class="w-full px-3 py-2 bg-gray-700 text-gray-300 text-sm rounded hover:bg-gray-600 transition-colors flex items-center justify-center gap-2"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"
            />
          </svg>
          导出图数据
        </button>
      </PanelSection>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useGraphStore } from '@/stores/graph'
import PanelSection from './PanelSection.vue'

const emit = defineEmits<{
  'fit-view': []
  'zoom-in': []
  'zoom-out': []
  'reset-view': []
  'show-export': []
  'layout-change': [layout: string]
  'style-change': [style: any]
}>()

const graphStore = useGraphStore()

// Layout
const currentLayout = ref('force')
const layouts = [
  { value: 'force', label: '力导向' },
  { value: 'circular', label: '环形' },
  { value: 'grid', label: '网格' },
  { value: 'concentric', label: '同心圆' },
  { value: 'radial', label: '辐射' },
  { value: 'random', label: '随机' },
]

const layoutConfig = ref({
  nodeSpacing: 50,
  linkDistance: 150,
  preventOverlap: true,
})

// Physics
const physicsEnabled = ref(true)

// Node style
const nodeStyle = ref({
  size: 35,
  colorScheme: 'default',
})

const colorSchemes = [
  { name: 'default', label: '默认', colors: ['#1d4ed8', '#dc2626', '#16a34a', '#d97706', '#9333ea'] },
  { name: 'pastel', label: '柔和', colors: ['#93c5fd', '#fca5a5', '#86efac', '#fcd34d', '#c4b5fd'] },
  { name: 'vibrant', label: '鲜艳', colors: ['#00d4ff', '#ff006e', '#06d6a0', '#ffd166', '#8338ec'] },
  { name: 'grayscale', label: '灰度', colors: ['#f8fafc', '#e2e8f0', '#cbd5e1', '#94a3b8', '#64748b'] },
]

const colorByLabel = ref<string | null>(null)

// Edge style
const edgeStyle = ref({
  lineWidth: 2,
  opacity: 100,
  showArrows: true,
  showLabels: true,
})

// Filter
const minDegree = ref(0)
const hasFilter = computed(() => minDegree.value > 0)

// Labels
const labels = computed(() => graphStore.stats.labels)

// Methods
function changeLayout(layout: string) {
  currentLayout.value = layout
  emit('layout-change', layout)
}

function togglePhysics() {
  physicsEnabled.value = !physicsEnabled.value
  emit('style-change', { physicsEnabled: physicsEnabled.value })
}

function updateLayout() {
  emit('style-change', { layout: currentLayout.value, config: layoutConfig.value })
}

function updateNodeStyle() {
  emit('style-change', { node: nodeStyle.value })
}

function updateEdgeStyle() {
  emit('style-change', { edge: edgeStyle.value })
}

function setColorScheme(scheme: typeof colorSchemes[0]) {
  nodeStyle.value.colorScheme = scheme.name
  emit('style-change', { colorScheme: scheme })
}

function applyFilter() {
  emit('style-change', { filter: { minDegree: minDegree.value } })
}

function clearFilter() {
  minDegree.value = 0
  emit('style-change', { filter: null })
}
</script>

<style scoped>
.visualization-controls {
  width: 280px;
}
</style>
