import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import PanelSection from './PanelSection.vue'

describe('PanelSection Component', () => {
  it('should render title correctly', () => {
    const wrapper = mount(PanelSection, {
      props: {
        title: 'Test Section',
      },
      slots: {
        default: '<p>Section content</p>',
      },
    })

    expect(wrapper.text()).toContain('Test Section')
    expect(wrapper.text()).toContain('Section content')
  })

  it('should render slot content', () => {
    const wrapper = mount(PanelSection, {
      props: {
        title: 'Test',
      },
      slots: {
        default: '<div class="custom-content">Custom Content</div>',
      },
    })

    expect(wrapper.find('.custom-content').exists()).toBe(true)
    expect(wrapper.find('.custom-content').text()).toBe('Custom Content')
  })

  it('should apply proper CSS classes', () => {
    const wrapper = mount(PanelSection, {
      props: {
        title: 'Test',
      },
    })

    // Check that the component has the expected structure
    expect(wrapper.find('h4').exists()).toBe(true)
    expect(wrapper.find('h4').classes()).toContain('text-sm')
    expect(wrapper.find('h4').classes()).toContain('font-medium')
    expect(wrapper.find('h4').classes()).toContain('text-gray-400')
  })
})
