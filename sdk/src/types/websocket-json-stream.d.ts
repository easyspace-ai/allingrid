declare module '@teamwork/websocket-json-stream' {
  import { EventEmitter } from 'node:events'
  
  interface WebSocketJSONStreamOptions {
    highWaterMark?: number
  }
  
  export default class WebSocketJSONStream extends EventEmitter {
    constructor(ws: WebSocket, options?: WebSocketJSONStreamOptions)
    write(data: any): void
    end(): void
    destroy(): void
  }
}
