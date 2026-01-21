import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import StatRow from './StatRow.vue'

describe('StatRow Component', () => {
  it('should render label and value', () => {
    const wrapper = mount(StatRow, {
      props: {
        label: 'Test Label',
        value: 42,
      },
    })

    expect(wrapper.text()).toContain('Test Label')
    expect(wrapper.text()).toContain('42')
  })

  it('should format numeric values', () => {
    const wrapper = mount(StatRow, {
      props: {
        label: 'Count',
        value: 1234567,
      },
    })

    expect(wrapper.text()).toContain('1,234,567')
  })

  it('should handle zero values', () => {
    const wrapper = mount(StatRow, {
      props: {
        label: 'Empty',
        value: 0,
      },
    })

    expect(wrapper.text()).toContain('0')
  })

  it('should apply proper styling', () => {
    const wrapper = mount(StatRow, {
      props: {
        label: 'Test',
        value: 1,
      },
    })

    const rows = wrapper.findAll('div')
    const labelRow = rows.find(r => r.text().includes('Test'))
    rows.find(r => r.text().includes('1'))

    expect(labelRow?.classes()).toContain('flex')
    expect(labelRow?.classes()).toContain('justify-between')
  })
})
