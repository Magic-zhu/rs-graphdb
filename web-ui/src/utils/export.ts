export interface ExportOptions {
  format: 'png' | 'svg' | 'json' | 'csv'
  filename?: string
  includeStyles?: boolean
}

/**
 * Export graph visualization as image
 */
export async function exportGraphImage(
  container: HTMLElement,
  options: Partial<ExportOptions> = {}
): Promise<void> {
  const { format = 'png', filename = `graph-export-${Date.now()}` } = options

  try {
    // For SVG export
    if (format === 'svg') {
      const svgData = await exportAsSVG(container)
      downloadFile(svgData, `${filename}.svg`, 'image/svg+xml')
      return
    }

    // For PNG export - use html2canvas if available, otherwise fallback
    const canvas = await exportAsCanvas(container)
    const dataUrl = canvas.toDataURL('image/png')
    downloadDataUrl(dataUrl, `${filename}.png`)
  } catch (error) {
    console.error('Export failed:', error)
    throw new Error(`Failed to export as ${format.toUpperCase()}`)
  }
}

/**
 * Export graph data as JSON
 */
export function exportGraphData(
  nodes: any[],
  edges: any[],
  options: Partial<ExportOptions> = {}
): void {
  const { filename = `graph-data-${Date.now()}`, includeStyles = true } = options

  const data = {
    exportDate: new Date().toISOString(),
    nodeCount: nodes.length,
    edgeCount: edges.length,
    nodes: includeStyles ? nodes : nodes.map(({ id, data }) => ({ id, data })),
    edges: includeStyles ? edges : edges.map(({ id, source, target, data }) => ({ id, source, target, data })),
  }

  const json = JSON.stringify(data, null, 2)
  downloadFile(json, `${filename}.json`, 'application/json')
}

/**
 * Export graph data as CSV
 */
export function exportGraphCSV(
  nodes: any[],
  edges: any[],
  options: Partial<ExportOptions> = {}
): void {
  const { filename = `graph-data-${Date.now()}` } = options

  // Export nodes CSV
  const nodesCsv = convertToCSV(nodes.map(n => ({
    id: n.id,
    label: n.data?.label || '',
    ...n.data
  })))

  // Export edges CSV
  const edgesCsv = convertToCSV(edges.map(e => ({
    id: e.id,
    source: e.source,
    target: e.target,
    label: e.data?.label || '',
    ...e.data
  })))

  // Download both files
  downloadFile(nodesCsv, `${filename}-nodes.csv`, 'text/csv')
  setTimeout(() => {
    downloadFile(edgesCsv, `${filename}-edges.csv`, 'text/csv')
  }, 100)
}

/**
 * Export query results
 */
export function exportQueryResults(
  results: any[],
  columns: string[],
  options: Partial<ExportOptions> = {}
): void {
  const { format = 'csv', filename = `query-results-${Date.now()}` } = options

  if (format === 'json') {
    const json = JSON.stringify({
      exportDate: new Date().toISOString(),
      rowCount: results.length,
      columns,
      rows: results,
    }, null, 2)
    downloadFile(json, `${filename}.json`, 'application/json')
  } else {
    const csv = convertToCSV(results, columns)
    downloadFile(csv, `${filename}.csv`, 'text/csv')
  }
}

// Helper functions

async function exportAsSVG(container: HTMLElement): Promise<string> {
  const svgElement = container.querySelector('svg')
  if (!svgElement) {
    throw new Error('No SVG element found in container')
  }

  // Clone the SVG
  const clone = svgElement.cloneNode(true) as SVGElement
  clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg')

  // Get styles
  const styles = Array.from(document.querySelectorAll('style'))
    .map(style => style.innerHTML)
    .join('\n')

  // Add styles to SVG
  const styleElement = document.createElement('style')
  styleElement.textContent = styles
  clone.prepend(styleElement)

  return new XMLSerializer().serializeToString(clone)
}

async function exportAsCanvas(container: HTMLElement): Promise<HTMLCanvasElement> {
  // Try to use html2canvas if available
  if (typeof (window as any).html2canvas === 'function') {
    return await (window as any).html2canvas(container, {
      backgroundColor: '#1f2937',
      scale: 2,
    })
  }

  // Fallback: create canvas from SVG
  const svgElement = container.querySelector('svg') as SVGElement
  if (!svgElement) {
    throw new Error('No SVG element found')
  }

  const svgData = new XMLSerializer().serializeToString(svgElement)
  const svgBlob = new Blob([svgData], { type: 'image/svg+xml;charset=utf-8' })
  const url = URL.createObjectURL(svgBlob)

  return new Promise((resolve, reject) => {
    const img = new Image()
    img.onload = () => {
      const canvas = document.createElement('canvas')
      const bbox = svgElement.getBoundingClientRect()
      canvas.width = bbox.width * 2
      canvas.height = bbox.height * 2

      const ctx = canvas.getContext('2d')
      if (!ctx) {
        reject(new Error('Failed to get canvas context'))
        return
      }

      ctx.scale(2, 2)
      ctx.fillStyle = '#1f2937'
      ctx.fillRect(0, 0, canvas.width, canvas.height)
      ctx.drawImage(img, 0, 0)

      URL.revokeObjectURL(url)
      resolve(canvas)
    }

    img.onerror = () => {
      URL.revokeObjectURL(url)
      reject(new Error('Failed to load SVG as image'))
    }

    img.src = url
  })
}

function convertToCSV(data: any[], columns?: string[]): string {
  if (data.length === 0) return ''

  // Determine columns
  const cols = columns || Object.keys(data[0])

  // Create header
  const header = cols.join(',')

  // Create rows
  const rows = data.map(row =>
    cols.map(col => {
      const value = row[col]
      // Escape quotes and wrap in quotes if contains comma or quote
      if (value === null || value === undefined) return ''
      const strValue = String(value)
      if (strValue.includes(',') || strValue.includes('"') || strValue.includes('\n')) {
        return `"${strValue.replace(/"/g, '""')}"`
      }
      return strValue
    }).join(',')
  )

  return [header, ...rows].join('\n')
}

function downloadFile(content: string, filename: string, mimeType: string): void {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  downloadDataUrl(url, filename)
  URL.revokeObjectURL(url)
}

function downloadDataUrl(dataUrl: string, filename: string): void {
  const a = document.createElement('a')
  a.href = dataUrl
  a.download = filename
  a.style.display = 'none'
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
}

/**
 * Copy data to clipboard
 */
export async function copyToClipboard(data: any): Promise<void> {
  try {
    const text = typeof data === 'string' ? data : JSON.stringify(data, null, 2)
    await navigator.clipboard.writeText(text)
  } catch (error) {
    console.error('Failed to copy to clipboard:', error)
    throw new Error('Failed to copy to clipboard')
  }
}
