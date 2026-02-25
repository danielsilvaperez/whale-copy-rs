import { Socket } from "node:net";

export interface RpcRequest {
  method: string;
  params?: unknown;
  actor_chat_id?: string;
  request_id?: string;
}

export interface RpcResponse {
  ok: boolean;
  result?: unknown;
  error?: string;
  request_id?: string;
}

export class EngineRpcClient {
  constructor(
    private readonly socketPath: string,
    private readonly timeoutMs: number,
  ) {}

  async request(method: string, params: unknown = {}, actorChatId?: string): Promise<unknown> {
    const request: RpcRequest = {
      method,
      params,
      actor_chat_id: actorChatId,
      request_id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
    };

    const response = await this.sendRaw(request);
    if (!response.ok) {
      throw new Error(response.error || `RPC ${method} failed`);
    }

    return response.result;
  }

  private sendRaw(payload: RpcRequest): Promise<RpcResponse> {
    return new Promise((resolve, reject) => {
      const socket = new Socket();
      let settled = false;
      let buffer = "";

      const done = (fn: () => void): void => {
        if (settled) {
          return;
        }
        settled = true;
        socket.destroy();
        fn();
      };

      const timer = setTimeout(() => {
        done(() => reject(new Error(`RPC timeout after ${this.timeoutMs}ms`)));
      }, this.timeoutMs);

      socket.setNoDelay(true);
      socket.connect(this.socketPath, () => {
        const line = `${JSON.stringify(payload)}\n`;
        socket.write(line);
      });

      socket.on("data", (chunk) => {
        buffer += chunk.toString("utf8");
        const lineEnd = buffer.indexOf("\n");
        if (lineEnd < 0) {
          return;
        }

        const line = buffer.slice(0, lineEnd).trim();
        if (!line) {
          return;
        }

        clearTimeout(timer);
        done(() => {
          try {
            resolve(JSON.parse(line) as RpcResponse);
          } catch (error) {
            reject(new Error(`Failed to parse RPC response: ${(error as Error).message}`));
          }
        });
      });

      socket.on("error", (error) => {
        clearTimeout(timer);
        done(() => reject(error));
      });

      socket.on("close", () => {
        if (settled) {
          return;
        }
        clearTimeout(timer);
        done(() => reject(new Error("RPC socket closed before response")));
      });
    });
  }
}
