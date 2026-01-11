/// <reference types="vite/client" />

declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent<{}, {}, any>
  export default component
}

declare module 'vis-network/standalone' {
  export { Network }
  export type { Node, Edge, Options } from 'vis-network'
}

declare module 'vis-data/standalone' {
  export { DataSet, DataView }
  export type { DataInterface } from 'vis-data'
}
