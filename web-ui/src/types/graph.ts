export interface Node {
  id: number
  label: string
  title?: string
  color?: string
  x?: number
  y?: number
  size?: number
}

export interface Edge {
  id: string
  from: number
  to: number
  label?: string
  color?: string
  width?: number
}

export interface NetworkOptions {
  nodes: {
    shape: string
    size: number
    font: {
      size: number
      color: string
    }
    borderWidth: number
    color: {
      border: string
      background: string
      highlight: {
        border: string
        background: string
      }
    }
  }
  edges: {
    width: number
    color: {
      color: string
      highlight: string
    }
    arrows: {
      to: {
        enabled: boolean
        scaleFactor: number
      }
    }
    font: {
      size: number
      color: string
      align: string
    }
  }
  physics: {
    enabled: boolean
    barnesHut: {
      gravitationalConstant: number
      springLength: number
    }
  }
}
