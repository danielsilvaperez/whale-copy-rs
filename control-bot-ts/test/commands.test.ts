import { strict as assert } from "node:assert";
import test from "node:test";

import { handleCommand } from "../src/commands.js";
import { EngineRpcClient } from "../src/rpcClient.js";

function makeRpc(handler: (method: string, params: unknown) => unknown): EngineRpcClient {
  return {
    request: async (method: string, params: unknown) => handler(method, params),
  } as unknown as EngineRpcClient;
}

const chatId = "123";
const opts = { defaultLogLimit: 30 };

test("/multiplier with no arg returns usage", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/multiplier", chatId, rpc, opts);
  assert.ok(result.includes("Usage:"));
});

test("/multiplier with negative number returns usage", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/multiplier -1", chatId, rpc, opts);
  assert.ok(result.includes("Usage:"));
});

test("/multiplier with 2.5 calls set_multiplier RPC", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "set_multiplier");
    assert.deepEqual(params, { multiplier: 2.5 });
    called = true;
    return { multiplier: 2.5 };
  });
  await handleCommand("/multiplier 2.5", chatId, rpc, opts);
  assert.ok(called);
});

test("/equity with 0 returns usage", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/equity 0", chatId, rpc, opts);
  assert.ok(result.includes("Usage:"));
});

test("/equity with 5000 calls set_follower_equity RPC", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "set_follower_equity");
    assert.deepEqual(params, { follower_equity_usd: 5000 });
    called = true;
    return { follower_equity_usd: 5000 };
  });
  await handleCommand("/equity 5000", chatId, rpc, opts);
  assert.ok(called);
});

test("/copy_sells with 'on' calls set_copy_sells { copy_sells: true }", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "set_copy_sells");
    assert.deepEqual(params, { copy_sells: true });
    called = true;
    return { copy_sells: true };
  });
  await handleCommand("/copy_sells on", chatId, rpc, opts);
  assert.ok(called);
});

test("/copy_sells with 'off' calls set_copy_sells { copy_sells: false }", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "set_copy_sells");
    assert.deepEqual(params, { copy_sells: false });
    called = true;
    return { copy_sells: false };
  });
  await handleCommand("/copy_sells off", chatId, rpc, opts);
  assert.ok(called);
});

test("/copy_sells with 'maybe' returns usage", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/copy_sells maybe", chatId, rpc, opts);
  assert.ok(result.includes("Usage:"));
});

test("/mode with no arg returns usage", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/mode", chatId, rpc, opts);
  assert.ok(result.includes("Usage:"));
});

test("/mode 'dry_run' calls set_mode RPC", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "set_mode");
    assert.deepEqual(params, { mode: "dry_run" });
    called = true;
    return { mode: "dry_run" };
  });
  await handleCommand("/mode dry_run", chatId, rpc, opts);
  assert.ok(called);
});

test("/risk with no arg returns usage", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/risk", chatId, rpc, opts);
  assert.ok(result.includes("Usage:"));
});

test("/pause calls pause_trading RPC", async () => {
  let called = false;
  const rpc = makeRpc((method) => {
    assert.equal(method, "pause_trading");
    called = true;
    return {};
  });
  await handleCommand("/pause", chatId, rpc, opts);
  assert.ok(called);
});

test("/resume calls resume_trading RPC", async () => {
  let called = false;
  const rpc = makeRpc((method) => {
    assert.equal(method, "resume_trading");
    called = true;
    return {};
  });
  await handleCommand("/resume", chatId, rpc, opts);
  assert.ok(called);
});

test("/stopbot calls pause_trading RPC", async () => {
  let called = false;
  const rpc = makeRpc((method) => {
    assert.equal(method, "pause_trading");
    called = true;
    return {};
  });
  await handleCommand("/stopbot", chatId, rpc, opts);
  assert.ok(called);
});

test("/startbot calls resume_trading RPC", async () => {
  let called = false;
  const rpc = makeRpc((method) => {
    assert.equal(method, "resume_trading");
    called = true;
    return {};
  });
  await handleCommand("/startbot", chatId, rpc, opts);
  assert.ok(called);
});

test("/panic calls panic_close RPC", async () => {
  let called = false;
  const rpc = makeRpc((method) => {
    assert.equal(method, "panic_close");
    called = true;
    return {};
  });
  await handleCommand("/panic", chatId, rpc, opts);
  assert.ok(called);
});

test("/logs with no arg uses defaultLogLimit", async () => {
  let capturedParams: unknown;
  const rpc = makeRpc((_method, params) => {
    capturedParams = params;
    return { events: [] };
  });
  await handleCommand("/logs", chatId, rpc, opts);
  assert.deepEqual(capturedParams, { limit: 30 });
});

test("/logs with 200 caps at 100", async () => {
  let capturedParams: unknown;
  const rpc = makeRpc((_method, params) => {
    capturedParams = params;
    return { events: [] };
  });
  await handleCommand("/logs 200", chatId, rpc, opts);
  assert.deepEqual(capturedParams, { limit: 100 });
});

test("/wallet_add with no arg returns usage", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/wallet_add", chatId, rpc, opts);
  assert.ok(result.includes("Usage:"));
});

test("/wallet_add calls add_wallet RPC", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "add_wallet");
    assert.deepEqual(params, { wallet: "0xabcd" });
    called = true;
    return { inserted: true };
  });
  await handleCommand("/wallet_add 0xabcd", chatId, rpc, opts);
  assert.ok(called);
});

test("/wallet_remove calls remove_wallet RPC", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "remove_wallet");
    assert.deepEqual(params, { wallet: "0xabcd" });
    called = true;
    return { removed: true };
  });
  await handleCommand("/wallet_remove 0xabcd", chatId, rpc, opts);
  assert.ok(called);
});

test("/approve calls ack_suggestion RPC", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "ack_suggestion");
    assert.deepEqual(params, { id: "suggestion-123" });
    called = true;
    return { approved: true };
  });
  await handleCommand("/approve suggestion-123", chatId, rpc, opts);
  assert.ok(called);
});

test("/reject calls reject_suggestion RPC", async () => {
  let called = false;
  const rpc = makeRpc((method, params) => {
    assert.equal(method, "reject_suggestion");
    assert.deepEqual(params, { id: "suggestion-456" });
    called = true;
    return { rejected: true };
  });
  await handleCommand("/reject suggestion-456", chatId, rpc, opts);
  assert.ok(called);
});

test("/help returns text with all commands", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/help", chatId, rpc, opts);
  assert.ok(result.includes("/status"));
  assert.ok(result.includes("/pause"));
  assert.ok(result.includes("/resume"));
});

test("/start returns help text", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/start", chatId, rpc, opts);
  assert.ok(result.includes("/status"));
});

test("unknown command returns error message", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("/notarealcommand", chatId, rpc, opts);
  assert.ok(result.includes("Unknown command"));
});

test("non-command text returns slash command hint", async () => {
  const rpc = makeRpc(() => {
    throw new Error("should not be called");
  });
  const result = await handleCommand("hello world", chatId, rpc, opts);
  assert.ok(result.toLowerCase().includes("slash command") || result.includes("/help"));
});

test("formatStatus handles missing health fields gracefully", async () => {
  const rpc = makeRpc(() => {
    return {}; // empty object — no health, settings, or tracked_wallets
  });
  const result = await handleCommand("/status", chatId, rpc, opts);
  assert.ok(result.includes("Whale Copy Platform Status"));
  assert.ok(result.includes("unknown"));
});

test("formatWallets shows 'No tracked wallets' on empty array", async () => {
  const rpc = makeRpc(() => {
    return { wallets: [] };
  });
  const result = await handleCommand("/wallet_list", chatId, rpc, opts);
  assert.equal(result, "No tracked wallets");
});

test("formatSuggestions shows 'No pending suggestions' on empty", async () => {
  const rpc = makeRpc(() => {
    return { pending: [] };
  });
  const result = await handleCommand("/suggestions", chatId, rpc, opts);
  assert.equal(result, "No pending suggestions");
});

test("formatLogs truncates payload at 200 chars", async () => {
  const longPayload = { data: "x".repeat(300) };
  const rpc = makeRpc(() => {
    return {
      events: [
        {
          ts: "2026-02-20T12:00:00Z",
          class: "info",
          event_type: "test_event",
          payload: longPayload,
        },
      ],
    };
  });
  const result = await handleCommand("/logs", chatId, rpc, opts);
  const fullPayloadStr = JSON.stringify(longPayload);
  // Full payload is longer than 200 chars
  assert.ok(fullPayloadStr.length > 200);
  // The result should contain the first 200 chars of the payload
  assert.ok(result.includes(fullPayloadStr.slice(0, 200)));
  // But not the full payload
  assert.ok(!result.includes(fullPayloadStr));
});
