import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { FavoriteQuery } from '@/types/query'

const STORAGE_KEY = 'graphdb_favorites'

export const useFavoritesStore = defineStore('favorites', () => {
  // State
  const favorites = ref<FavoriteQuery[]>([])
  const loading = ref(false)

  // Computed
  const favoritesByTag = computed(() => {
    const tags = new Map<string, FavoriteQuery[]>()
    favorites.value.forEach((fav) => {
      fav.tags.forEach((tag) => {
        if (!tags.has(tag)) {
          tags.set(tag, [])
        }
        tags.get(tag)!.push(fav)
      })
    })
    return tags
  })

  const allTags = computed(() => {
    const tags = new Set<string>()
    favorites.value.forEach((fav) => {
      fav.tags.forEach((tag) => tags.add(tag))
    })
    return Array.from(tags).sort()
  })

  // Actions
  function loadFromStorage() {
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      if (stored) {
        const parsed = JSON.parse(stored)
        favorites.value = parsed.map((f: any) => ({
          ...f,
          createdAt: new Date(f.createdAt),
          updatedAt: new Date(f.updatedAt),
        }))
      }
    } catch (err) {
      console.error('Failed to load favorites:', err)
      favorites.value = []
    }
  }

  function saveToStorage() {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(favorites.value))
    } catch (err) {
      console.error('Failed to save favorites:', err)
    }
  }

  function addFavorite(
    name: string,
    query: string,
    description?: string,
    tags: string[] = []
  ) {
    const favorite: FavoriteQuery = {
      id: `fav_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      name: name.trim(),
      query: query.trim(),
      description: description?.trim(),
      tags,
      createdAt: new Date(),
      updatedAt: new Date(),
    }

    favorites.value.unshift(favorite)
    saveToStorage()
    return favorite.id
  }

  function updateFavorite(
    id: string,
    updates: Partial<Pick<FavoriteQuery, 'name' | 'query' | 'description' | 'tags'>>
  ) {
    const index = favorites.value.findIndex((f) => f.id === id)
    if (index !== -1) {
      favorites.value[index] = {
        ...favorites.value[index],
        ...updates,
        updatedAt: new Date(),
      }
      saveToStorage()
    }
  }

  function removeFavorite(id: string) {
    favorites.value = favorites.value.filter((f) => f.id !== id)
    saveToStorage()
  }

  function getFavorite(id: string): FavoriteQuery | undefined {
    return favorites.value.find((f) => f.id === id)
  }

  function searchFavorites(searchTerm: string): FavoriteQuery[] {
    if (!searchTerm) return favorites.value
    const term = searchTerm.toLowerCase()
    return favorites.value.filter(
      (f) =>
        f.name.toLowerCase().includes(term) ||
        f.query.toLowerCase().includes(term) ||
        f.description?.toLowerCase().includes(term) ||
        f.tags.some((t) => t.toLowerCase().includes(term))
    )
  }

  function exportFavorites(): string {
    return JSON.stringify(favorites.value, null, 2)
  }

  function importFavorites(json: string): { success: boolean; message: string } {
    try {
      const imported = JSON.parse(json)
      if (!Array.isArray(imported)) {
        return { success: false, message: 'Invalid format: expected array' }
      }

      // Validate structure
      const validFavorites = imported.filter(
        (f: any) =>
          typeof f.name === 'string' &&
          typeof f.query === 'string' &&
          f.name.trim() &&
          f.query.trim()
      )

      if (validFavorites.length === 0) {
        return { success: false, message: 'No valid favorites found' }
      }

      // Add new favorites
      validFavorites.forEach((f: any) => {
        const newFav: FavoriteQuery = {
          id: `fav_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          name: f.name.trim(),
          query: f.query.trim(),
          description: f.description?.trim(),
          tags: Array.isArray(f.tags) ? f.tags : [],
          createdAt: new Date(),
          updatedAt: new Date(),
        }
        favorites.value.unshift(newFav)
      })

      saveToStorage()
      return { success: true, message: `Imported ${validFavorites.length} favorites` }
    } catch (err) {
      return { success: false, message: 'Invalid JSON format' }
    }
  }

  // Add default favorites
  function addDefaultFavorites() {
    if (favorites.value.length === 0) {
      const defaults: Omit<FavoriteQuery, 'id' | 'createdAt' | 'updatedAt'>[] = [
        {
          name: 'Get All Nodes',
          query: '',
          description: 'Retrieve all nodes from the database',
          tags: ['basic', 'nodes'],
        },
        {
          name: 'Count by Label',
          query: '',
          description: 'Count nodes grouped by their labels',
          tags: ['basic', 'statistics'],
        },
        {
          name: 'Find Relationships',
          query: '',
          description: 'Find relationships between nodes',
          tags: ['basic', 'relationships'],
        },
      ]

      defaults.forEach((d) => {
        favorites.value.push({
          ...d,
          id: `fav_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          createdAt: new Date(),
          updatedAt: new Date(),
        })
      })

      saveToStorage()
    }
  }

  // Initialize
  loadFromStorage()
  addDefaultFavorites()

  return {
    // State
    favorites,
    loading,
    // Computed
    favoritesByTag,
    allTags,
    // Actions
    addFavorite,
    updateFavorite,
    removeFavorite,
    getFavorite,
    searchFavorites,
    exportFavorites,
    importFavorites,
    loadFromStorage,
    saveToStorage,
  }
})
