import { strict as assert } from "node:assert";
import { existsSync, unlinkSync } from "node:fs";
import { createServer, Server, Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { EngineRpcClient } from "../src/rpcClient.js";

function tempSocketPath(): string {
  return join(tmpdir(), `rpc-test-${Date.now()}-${Math.random().toString(36).slice(2)}.sock`);
}

function cleanupSocket(path: string): void {
  if (existsSync(path)) {
    try {
      unlinkSync(path);
    } catch {
      // ignore cleanup errors
    }
  }
}

/** Create a server and return it along with a cleanup fn that destroys all sockets. */
function makeServer(handler: (socket: Socket) => void): { server: Server; closeAll: () => Promise<void> } {
  const openSockets = new Set<Socket>();
  const server = createServer((socket) => {
    openSockets.add(socket);
    socket.on("close", () => openSockets.delete(socket));
    handler(socket);
  });
  const closeAll = (): Promise<void> =>
    new Promise<void>((resolve) => {
      for (const s of openSockets) s.destroy();
      server.close(() => resolve());
    });
  return { server, closeAll };
}

test("request resolves with result on ok=true response", async () => {
  const socketPath = tempSocketPath();
  const { server, closeAll } = makeServer((socket) => {
    let buf = "";
    socket.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      const idx = buf.indexOf("\n");
      if (idx < 0) return;
      const line = buf.slice(0, idx).trim();
      const req = JSON.parse(line) as { request_id?: string };
      socket.write(JSON.stringify({ ok: true, result: { pong: true }, request_id: req.request_id }) + "\n");
    });
  });

  await new Promise<void>((resolve) => server.listen(socketPath, resolve));

  try {
    const client = new EngineRpcClient(socketPath, 2_000);
    const result = await client.request("ping", {});
    assert.deepEqual(result, { pong: true });
  } finally {
    await closeAll();
    cleanupSocket(socketPath);
  }
});

test("request rejects with error message when ok=false", async () => {
  const socketPath = tempSocketPath();
  const { server, closeAll } = makeServer((socket) => {
    let buf = "";
    socket.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      if (buf.indexOf("\n") < 0) return;
      socket.write(JSON.stringify({ ok: false, error: "something went wrong" }) + "\n");
    });
  });

  await new Promise<void>((resolve) => server.listen(socketPath, resolve));

  try {
    const client = new EngineRpcClient(socketPath, 2_000);
    await assert.rejects(() => client.request("ping", {}), /something went wrong/);
  } finally {
    await closeAll();
    cleanupSocket(socketPath);
  }
});

test("request rejects with timeout message after timeoutMs", async () => {
  const socketPath = tempSocketPath();
  // Server accepts but never responds
  const { server, closeAll } = makeServer((_socket) => {
    // intentionally do nothing — no response
  });

  await new Promise<void>((resolve) => server.listen(socketPath, resolve));

  try {
    const client = new EngineRpcClient(socketPath, 150);
    await assert.rejects(() => client.request("ping", {}), /timeout/i);
  } finally {
    await closeAll();
    cleanupSocket(socketPath);
  }
});

test("request rejects when socket closes before response", async () => {
  const socketPath = tempSocketPath();
  // Server immediately destroys the connection — client gets either "closed" or EPIPE
  const { server, closeAll } = makeServer((socket) => {
    socket.destroy();
  });

  await new Promise<void>((resolve) => server.listen(socketPath, resolve));

  try {
    const client = new EngineRpcClient(socketPath, 2_000);
    // The error may be "RPC socket closed before response" or "write EPIPE" depending on timing
    await assert.rejects(() => client.request("ping", {}), /(closed|EPIPE)/i);
  } finally {
    await closeAll();
    cleanupSocket(socketPath);
  }
});

test("request includes actor_chat_id in outgoing message", async () => {
  const socketPath = tempSocketPath();
  let receivedActorChatId: string | undefined;

  const { server, closeAll } = makeServer((socket) => {
    let buf = "";
    socket.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      const idx = buf.indexOf("\n");
      if (idx < 0) return;
      const line = buf.slice(0, idx).trim();
      const req = JSON.parse(line) as { actor_chat_id?: string; request_id?: string };
      receivedActorChatId = req.actor_chat_id;
      socket.write(JSON.stringify({ ok: true, result: {}, request_id: req.request_id }) + "\n");
    });
  });

  await new Promise<void>((resolve) => server.listen(socketPath, resolve));

  try {
    const client = new EngineRpcClient(socketPath, 2_000);
    await client.request("ping", {}, "chat-42");
    assert.equal(receivedActorChatId, "chat-42");
  } finally {
    await closeAll();
    cleanupSocket(socketPath);
  }
});

test("request generates unique request_ids", async () => {
  const socketPath = tempSocketPath();
  const requestIds: string[] = [];

  const { server, closeAll } = makeServer((socket) => {
    let buf = "";
    socket.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      let idx: number;
      while ((idx = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, idx).trim();
        buf = buf.slice(idx + 1);
        if (!line) continue;
        const req = JSON.parse(line) as { request_id?: string };
        if (req.request_id) requestIds.push(req.request_id);
        socket.write(JSON.stringify({ ok: true, result: {}, request_id: req.request_id }) + "\n");
      }
    });
  });

  await new Promise<void>((resolve) => server.listen(socketPath, resolve));

  try {
    const client = new EngineRpcClient(socketPath, 2_000);
    // Each request uses a new socket connection
    await client.request("ping", {});
    await client.request("ping", {});
    await client.request("ping", {});

    assert.equal(requestIds.length, 3);
    assert.equal(new Set(requestIds).size, 3);
  } finally {
    await closeAll();
    cleanupSocket(socketPath);
  }
});
