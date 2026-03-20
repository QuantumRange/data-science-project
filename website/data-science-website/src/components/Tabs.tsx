import React from 'react'
import { Tab, TabGroup, TabList, TabPanel, TabPanels } from '@headlessui/react'

export type TabsProps = {
  children: React.ReactNode
  defaultIndex?: number
  onChange?: (index: number) => void
  manual?: boolean
  vertical?: boolean
}

/**
 * HeadlessUI Tabs wrapper that works in Astro files.
 *
 * ## The Problem
 * Astro cannot pass React children that return `null` (marker components).
 * A pattern like `<TabsItem title="...">` won't work because Astro serializes
 * children during SSR, and components returning `null` produce nothing to pass.
 *
 * ## The Solution
 * Use data attributes on regular HTML elements. Astro preserves these attributes
 * when passing children to React, so we can extract tab configuration from them.
 *
 * ## Usage
 * ```astro
 * <Tabs client:load>
 *   <div data-title="Tab 1">Content for tab 1</div>
 *   <div data-title="Tab 2">Content for tab 2</div>
 *   <div data-title="Disabled" data-disabled>This tab is disabled</div>
 * </Tabs>
 * ```
 *
 * ## Attributes
 * - `data-title` - The text shown in the tab button (required)
 * - `data-disabled` - If present, the tab will be disabled
 */
export function Tabs({ children, defaultIndex = 0, onChange, manual, vertical }: TabsProps) {
  // Convert children to array for iteration
  const childArray = React.Children.toArray(children)

  // Extract tab configuration from each child's data attributes.
  // Each child element becomes both a tab button (using data-title) and a panel (using its content).
  const tabItems = childArray.map((child) => {
    if (React.isValidElement(child)) {
      // Access the child's props to read data attributes
      const props = child.props as Record<string, unknown>

      return {
        // data-title becomes the tab button text
        title: props['data-title'] as React.ReactNode ?? 'Tab',
        // data-disabled presence (any value or no value) disables the tab
        disabled: props['data-disabled'] !== undefined,
        // The entire child element becomes the panel content
        content: child,
      }
    }
    // Fallback for non-element children (text nodes, etc.)
    return { title: 'Tab', disabled: false, content: child }
  })

  if (tabItems.length === 0) {
    return null
  }

  // Render HeadlessUI TabGroup with extracted configuration
  return (
    <TabGroup defaultIndex={defaultIndex} onChange={onChange} manual={manual} vertical={vertical}>
      <TabList className="flex gap-2 border-b border-gray-200">
        {tabItems.map((item, index) => (
          <Tab
            key={index}
            disabled={item.disabled}
            className="px-4 py-2 text-sm font-medium focus:outline-none data-[selected]:border-b-2 data-[selected]:border-blue-500 data-[selected]:text-blue-600 data-[hover]:bg-gray-100 data-[disabled]:opacity-50"
          >
            {item.title}
          </Tab>
        ))}
      </TabList>
      <TabPanels className="mt-4">
        {tabItems.map((item, index) => (
          <TabPanel key={index} className="p-4">
            {item.content}
          </TabPanel>
        ))}
      </TabPanels>
    </TabGroup>
  )
}
