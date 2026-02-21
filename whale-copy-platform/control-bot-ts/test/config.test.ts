import { strict as assert } from "node:assert";
import test from "node:test";

import { loadConfig } from "../src/config.js";

// Keys touched across config tests — save and restore to avoid cross-test pollution
const CONFIG_ENV_KEYS = [
  "TELEGRAM_BOT_TOKEN",
  "TELEGRAM_ALLOWED_CHAT_IDS",
  "TELEGRAM_CHAT_ID",
  "ENGINE_SOCKET_PATH",
  "RPC_TIMEOUT_MS",
  "TELEGRAM_POLL_TIMEOUT_SEC",
  "TELEGRAM_EVENT_POLL_MS",
  "TELEGRAM_HEARTBEAT_MS",
  "COMMAND_LOG_LIMIT_DEFAULT",
] as const;

function saveEnv(): Record<string, string | undefined> {
  const saved: Record<string, string | undefined> = {};
  for (const key of CONFIG_ENV_KEYS) {
    saved[key] = process.env[key];
  }
  return saved;
}

function restoreEnv(saved: Record<string, string | undefined>): void {
  for (const key of CONFIG_ENV_KEYS) {
    const val = saved[key];
    if (val === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = val;
    }
  }
}

function setMinimalEnv(): void {
  process.env["TELEGRAM_BOT_TOKEN"] = "test-bot-token";
  process.env["TELEGRAM_ALLOWED_CHAT_IDS"] = "123,456";
  delete process.env["TELEGRAM_CHAT_ID"];
}

test("loadConfig throws if TELEGRAM_BOT_TOKEN missing", () => {
  const saved = saveEnv();
  try {
    delete process.env["TELEGRAM_BOT_TOKEN"];
    delete process.env["TELEGRAM_ALLOWED_CHAT_IDS"];
    delete process.env["TELEGRAM_CHAT_ID"];
    assert.throws(() => loadConfig(), /TELEGRAM_BOT_TOKEN/);
  } finally {
    restoreEnv(saved);
  }
});

test("loadConfig throws if no allowed chat IDs configured", () => {
  const saved = saveEnv();
  try {
    process.env["TELEGRAM_BOT_TOKEN"] = "test-token";
    // Provide only commas — after split+filter the set will be empty
    process.env["TELEGRAM_ALLOWED_CHAT_IDS"] = "  ,  ,  ";
    delete process.env["TELEGRAM_CHAT_ID"];
    assert.throws(() => loadConfig(), /No allowed chat IDs/);
  } finally {
    restoreEnv(saved);
  }
});

test("loadConfig parses comma-separated TELEGRAM_ALLOWED_CHAT_IDS", () => {
  const saved = saveEnv();
  try {
    setMinimalEnv();
    process.env["TELEGRAM_ALLOWED_CHAT_IDS"] = "111,222,333";
    const config = loadConfig();
    assert.ok(config.allowedChatIds.has("111"));
    assert.ok(config.allowedChatIds.has("222"));
    assert.ok(config.allowedChatIds.has("333"));
    assert.equal(config.allowedChatIds.size, 3);
  } finally {
    restoreEnv(saved);
  }
});

test("loadConfig trims whitespace from chat IDs", () => {
  const saved = saveEnv();
  try {
    setMinimalEnv();
    process.env["TELEGRAM_ALLOWED_CHAT_IDS"] = " 100 , 200 , 300 ";
    const config = loadConfig();
    assert.ok(config.allowedChatIds.has("100"));
    assert.ok(config.allowedChatIds.has("200"));
    assert.ok(config.allowedChatIds.has("300"));
  } finally {
    restoreEnv(saved);
  }
});

test("parseIntOr falls back to default on non-numeric input", () => {
  const saved = saveEnv();
  try {
    setMinimalEnv();
    process.env["RPC_TIMEOUT_MS"] = "not-a-number";
    const config = loadConfig();
    assert.equal(config.rpcTimeoutMs, 2500); // default
  } finally {
    restoreEnv(saved);
  }
});

test("parseIntOr uses provided integer value", () => {
  const saved = saveEnv();
  try {
    setMinimalEnv();
    process.env["RPC_TIMEOUT_MS"] = "4000";
    const config = loadConfig();
    assert.equal(config.rpcTimeoutMs, 4000);
  } finally {
    restoreEnv(saved);
  }
});

test("engineSocketPath defaults to /tmp/whale-copy-engine.sock", () => {
  const saved = saveEnv();
  try {
    setMinimalEnv();
    delete process.env["ENGINE_SOCKET_PATH"];
    const config = loadConfig();
    assert.equal(config.engineSocketPath, "/tmp/whale-copy-engine.sock");
  } finally {
    restoreEnv(saved);
  }
});
