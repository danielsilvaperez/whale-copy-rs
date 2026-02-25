export interface ControlBotConfig {
  telegramBotToken: string;
  allowedChatIds: Set<string>;
  engineSocketPath: string;
  pollTimeoutSec: number;
  notificationPollMs: number;
  heartbeatMs: number;
  rpcTimeoutMs: number;
  commandLogLimitDefault: number;
}

function parseIntOr(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function loadConfig(): ControlBotConfig {
  const telegramBotToken = process.env.TELEGRAM_BOT_TOKEN?.trim() ?? "";
  if (!telegramBotToken) {
    throw new Error("TELEGRAM_BOT_TOKEN is required");
  }

  const allowedRaw = process.env.TELEGRAM_ALLOWED_CHAT_IDS?.trim() ?? process.env.TELEGRAM_CHAT_ID?.trim() ?? "";
  if (!allowedRaw) {
    throw new Error("TELEGRAM_ALLOWED_CHAT_IDS or TELEGRAM_CHAT_ID is required");
  }

  const allowedChatIds = new Set(
    allowedRaw
      .split(",")
      .map((v) => v.trim())
      .filter((v) => v.length > 0),
  );

  if (allowedChatIds.size === 0) {
    throw new Error("No allowed chat IDs configured");
  }

  return {
    telegramBotToken,
    allowedChatIds,
    engineSocketPath: process.env.ENGINE_SOCKET_PATH?.trim() || "/tmp/whale-copy-engine.sock",
    pollTimeoutSec: parseIntOr("TELEGRAM_POLL_TIMEOUT_SEC", 45),
    notificationPollMs: parseIntOr("TELEGRAM_EVENT_POLL_MS", 3500),
    heartbeatMs: parseIntOr("TELEGRAM_HEARTBEAT_MS", 5000),
    rpcTimeoutMs: parseIntOr("RPC_TIMEOUT_MS", 2500),
    commandLogLimitDefault: parseIntOr("COMMAND_LOG_LIMIT_DEFAULT", 30),
  };
}
