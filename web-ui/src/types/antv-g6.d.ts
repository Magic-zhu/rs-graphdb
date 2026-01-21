declare module '@antv/g6' {
  export interface Graph {
    on(event: string, callback: (...args: any[]) => void): void
    off(event: string, callback?: (...args: any[]) => void): void
    addNodeData(data: any[]): void
    addEdgeData(data: any[]): void
    updateNodeData(data: any[]): void
    updateEdgeData(data: any[]): void
    getNodeData(): any[] | any
    getEdgeData(): any[] | any
    clear(): void
    draw(): Promise<void>
    destroy(): void
    resize(width: number, height: number): void
    fitView(): void
    focusElement(id: string, animate?: boolean, options?: any): void
  }

  export function Graph(config: any): Graph
}

declare module '*?raw' {
  const content: string
  export default content
}

declare module '*.svg' {
  const content: string
  export default content
}

declare module '*.png' {
  const content: string
  export default content
}

declare module '*.jpg' {
  const content: string
  export default content
}
