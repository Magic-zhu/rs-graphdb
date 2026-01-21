import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useFavoritesStore } from './favorites'

describe('Favorites Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    localStorage.clear()
    vi.clearAllMocks()
  })

  describe('Add Favorite', () => {
    it('should add a new favorite', () => {
      const store = useFavoritesStore()

      const id = store.addFavorite(
        'Get All Users',
        'MATCH (u:User) RETURN u',
        'Fetch all user nodes',
        ['basic', 'users']
      )

      expect(store.favorites).toHaveLength(1) // Default favorites are added
      expect(id).toBeDefined()
      expect(store.favorites[store.favorites.length - 1].name).toBe('Get All Users')
      expect(store.favorites[store.favorites.length - 1].tags).toEqual(['basic', 'users'])
    })

    it('should trim whitespace from favorite name and query', () => {
      const store = useFavoritesStore()

      store.addFavorite(
        '  Test Name  ',
        '  MATCH (n) RETURN n  ',
        undefined,
        []
      )

      const lastFav = store.favorites[store.favorites.length - 1]
      expect(lastFav.name).toBe('Test Name')
      expect(lastFav.query).toBe('MATCH (n) RETURN n')
    })
  })

  describe('Update Favorite', () => {
    it('should update an existing favorite', () => {
      const store = useFavoritesStore()
      const id = store.addFavorite('Original Name', 'MATCH (n) RETURN n')

      store.updateFavorite(id, {
        name: 'Updated Name',
        description: 'Updated description',
      })

      const fav = store.getFavorite(id)
      expect(fav?.name).toBe('Updated Name')
      expect(fav?.description).toBe('Updated description')
      expect(fav?.query).toBe('MATCH (n) RETURN n') // Query unchanged
    })

    it('should update tags', () => {
      const store = useFavoritesStore()
      const id = store.addFavorite('Test', 'MATCH (n) RETURN n', undefined, ['tag1'])

      store.updateFavorite(id, { tags: ['tag2', 'tag3'] })

      const fav = store.getFavorite(id)
      expect(fav?.tags).toEqual(['tag2', 'tag3'])
    })
  })

  describe('Remove Favorite', () => {
    it('should remove a favorite', () => {
      const store = useFavoritesStore()
      const id = store.addFavorite('To Remove', 'MATCH (n) RETURN n')

      expect(store.favorites.some(f => f.name === 'To Remove')).toBe(true)

      store.removeFavorite(id)

      expect(store.favorites.some(f => f.name === 'To Remove')).toBe(false)
    })
  })

  describe('Get Favorite', () => {
    it('should return a favorite by id', () => {
      const store = useFavoritesStore()
      const id = store.addFavorite('Test Query', 'MATCH (n) RETURN n')

      const fav = store.getFavorite(id)

      expect(fav).toBeDefined()
      expect(fav?.name).toBe('Test Query')
    })

    it('should return undefined for non-existent id', () => {
      const store = useFavoritesStore()

      const fav = store.getFavorite('non-existent')

      expect(fav).toBeUndefined()
    })
  })

  describe('Search Favorites', () => {
    beforeEach(() => {
      const store = useFavoritesStore()
      // Clear default favorites
      store.favorites = []
      store.addFavorite('Get Users', 'MATCH (u:User) RETURN u', 'User query', ['users'])
      store.addFavorite('Get Posts', 'MATCH (p:Post) RETURN p', 'Post query', ['posts'])
      store.addFavorite('Count Nodes', 'MATCH (n) RETURN count(n)', 'Count query', ['stats'])
    })

    it('should search by name', () => {
      const store = useFavoritesStore()
      const results = store.searchFavorites('Users')

      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Get Users')
    })

    it('should search by query content', () => {
      const store = useFavoritesStore()
      const results = store.searchFavorites('Post')

      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Get Posts')
    })

    it('should search by tag', () => {
      const store = useFavoritesStore()
      const results = store.searchFavorites('stats')

      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Count Nodes')
    })

    it('should return all favorites when search is empty', () => {
      const store = useFavoritesStore()
      const results = store.searchFavorites('')

      expect(results).toHaveLength(3)
    })
  })

  describe('Tags', () => {
    it('should extract all unique tags', () => {
      const store = useFavoritesStore()
      store.favorites = []
      store.addFavorite('Query 1', 'MATCH (n) RETURN n', undefined, ['tag1', 'tag2'])
      store.addFavorite('Query 2', 'MATCH (n) RETURN n', undefined, ['tag2', 'tag3'])
      store.addFavorite('Query 3', 'MATCH (n) RETURN n', undefined, ['tag1'])

      const tags = store.allTags

      expect(tags).toEqual(['tag1', 'tag2', 'tag3'])
    })

    it('should group favorites by tag', () => {
      const store = useFavoritesStore()
      store.favorites = []
      store.addFavorite('Query 1', 'MATCH (n) RETURN n', undefined, ['tag1'])
      store.addFavorite('Query 2', 'MATCH (n) RETURN n', undefined, ['tag1', 'tag2'])

      const grouped = store.favoritesByTag

      expect(grouped.get('tag1')).toHaveLength(2)
      expect(grouped.get('tag2')).toHaveLength(1)
    })
  })

  describe('Import/Export', () => {
    it('should export favorites as JSON', () => {
      const store = useFavoritesStore()
      store.addFavorite('Export Test', 'MATCH (n) RETURN n')

      const json = store.exportFavorites()
      const parsed = JSON.parse(json)

      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed.some((f: any) => f.name === 'Export Test')).toBe(true)
    })

    it('should import favorites from JSON', () => {
      const store = useFavoritesStore()
      const importData = JSON.stringify([
        {
          name: 'Imported Query',
          query: 'MATCH (n) RETURN n',
          description: 'Test import',
          tags: ['test'],
        },
      ])

      const result = store.importFavorites(importData)

      expect(result.success).toBe(true)
      expect(store.favorites.some(f => f.name === 'Imported Query')).toBe(true)
    })

    it('should handle invalid import JSON', () => {
      const store = useFavoritesStore()

      const result = store.importFavorites('invalid json')

      expect(result.success).toBe(false)
      expect(result.message).toContain('Invalid JSON')
    })

    it('should handle invalid import format', () => {
      const store = useFavoritesStore()

      const result = store.importFavorites(JSON.stringify({ not: 'an array' }))

      expect(result.success).toBe(false)
      expect(result.message).toContain('expected array')
    })
  })

  describe('Persistence', () => {
    it('should save to localStorage', () => {
      const store = useFavoritesStore()
      const setItemSpy = vi.spyOn(localStorage, 'setItem')

      store.addFavorite('Persistence Test', 'MATCH (n) RETURN n')

      expect(setItemSpy).toHaveBeenCalled()
    })

    it('should load from localStorage on init', () => {
      const savedData = [
        {
          id: 'import-test',
          name: 'Loaded Favorite',
          query: 'MATCH (n) RETURN n',
          description: 'Test',
          tags: [],
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        },
      ]
      vi.spyOn(localStorage, 'getItem').mockReturnValue(JSON.stringify(savedData))

      const store = useFavoritesStore()

      expect(store.favorites.some(f => f.name === 'Loaded Favorite')).toBe(true)
    })
  })

  describe('Default Favorites', () => {
    it('should add default favorites when store is empty', () => {
      vi.spyOn(localStorage, 'getItem').mockReturnValue(null)

      const store = useFavoritesStore()

      expect(store.favorites.length).toBeGreaterThan(0)
      expect(store.favorites.some(f => f.name === 'Get All Nodes')).toBe(true)
    })

    it('should not add defaults when favorites exist', () => {
      const existing = [
        {
          id: 'existing',
          name: 'Existing Favorite',
          query: 'MATCH (n) RETURN n',
          tags: [],
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        },
      ]
      vi.spyOn(localStorage, 'getItem').mockReturnValue(JSON.stringify(existing))

      const store = useFavoritesStore()

      expect(store.favorites.length).toBe(1)
      expect(store.favorites[0].name).toBe('Existing Favorite')
    })
  })
})
