import "dotenv/config";

import { loadConfig } from "./config.js";
import { handleCommand } from "./commands.js";
import { EngineRpcClient } from "./rpcClient.js";
import { TelegramApi, TelegramUpdate } from "./telegram.js";

interface EngineEvent {
  id: string;
  ts: string;
  class: string;
  event_type: string;
  payload: Record<string, unknown>;
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

function logJson(level: "info" | "warn" | "error", event: string, payload: Record<string, unknown>): void {
  const line = {
    ts: new Date().toISOString(),
    level,
    event,
    ...payload,
  };
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(line));
}

function eventMessage(event: EngineEvent): string | null {
  if (!["critical", "trade", "risk", "rotation"].includes(event.class)) {
    return null;
  }

  return [
    `[${event.class.toUpperCase()}] ${event.event_type}`,
    `ts: ${event.ts}`,
    JSON.stringify(event.payload),
  ].join("\n");
}

async function processUpdates(
  updates: TelegramUpdate[],
  telegram: TelegramApi,
  rpc: EngineRpcClient,
  allowedChatIds: Set<string>,
  defaultLogLimit: number,
): Promise<void> {
  for (const update of updates) {
    const message = update.message;
    if (!message?.text) {
      continue;
    }

    const chatId = String(message.chat.id);
    if (!allowedChatIds.has(chatId)) {
      logJson("warn", "telegram_unauthorized_chat", { chatId });
      continue;
    }

    try {
      const reply = await handleCommand(message.text, chatId, rpc, {
        defaultLogLimit,
      });
      const trimmed = reply.length > 3800 ? `${reply.slice(0, 3800)}\n...truncated` : reply;
      await telegram.sendMessage(chatId, trimmed);
    } catch (error) {
      const errorMsg = (error as Error).message;
      const safeMessage = errorMsg.length > 200 ? errorMsg.slice(0, 200) : errorMsg;
      const messageText = `Command failed: ${safeMessage}`;
      await telegram.sendMessage(chatId, messageText);
      logJson("error", "command_failed", {
        chatId,
        command: message.text,
        error: errorMsg,
      });
    }
  }
}

async function run(): Promise<void> {
  const config = loadConfig();
  const telegram = new TelegramApi(config.telegramBotToken);
  const rpc = new EngineRpcClient(config.engineSocketPath, config.rpcTimeoutMs);

  const primaryChatId = [...config.allowedChatIds][0];
  if (!primaryChatId) {
    throw new Error("No allowed chat IDs configured - cannot start bot");
  }
  let offset = 0;
  let notifierPrimed = false;
  const seenEventIds = new Set<string>();

  let startupMessage = "whale-copy control bot started";
  try {
    const status = await rpc.request("get_status", {});
    const s = status as { health?: { mode?: string; paused?: boolean; tracked_wallet_count?: number } };
    const h = s.health ?? {};
    startupMessage = [
      "whale-copy control bot started",
      `engine mode: ${h.mode ?? "unknown"}`,
      `paused: ${String(h.paused ?? "unknown")}`,
      `tracked wallets: ${h.tracked_wallet_count ?? "unknown"}`,
    ].join("\n");
  } catch {
    startupMessage += " (engine not reachable yet)";
  }
  await telegram.sendMessage(primaryChatId, startupMessage);

  setInterval(async () => {
    try {
      await rpc.request("report_heartbeat", {
        service: "telegram",
        status: "ok",
        details: {
          pid: process.pid,
          uptime_sec: Math.round(process.uptime()),
          update_offset: offset,
        },
      });
    } catch (error) {
      logJson("warn", "telegram_heartbeat_failed", { error: (error as Error).message });
    }
  }, config.heartbeatMs);

  setInterval(async () => {
    try {
      const raw = await rpc.request("tail_events", { limit: 120 });
      const events = ((raw as { events?: EngineEvent[] }).events ?? []).filter((event) => event?.id);

      if (!notifierPrimed) {
        for (const event of events) {
          seenEventIds.add(event.id);
        }
        notifierPrimed = true;
        return;
      }

      for (const event of events) {
        if (seenEventIds.has(event.id)) {
          continue;
        }
        seenEventIds.add(event.id);

        const formatted = eventMessage(event);
        if (!formatted) {
          continue;
        }

        await telegram.sendMessage(primaryChatId, formatted.slice(0, 3800));
      }

      if (seenEventIds.size > 5000) {
        const recent = new Set(events.map((event) => event.id));
        seenEventIds.clear();
        for (const id of recent) {
          seenEventIds.add(id);
        }
      }
    } catch (error) {
      logJson("warn", "telegram_event_poll_failed", { error: (error as Error).message });
    }
  }, config.notificationPollMs);

  while (true) {
    try {
      const updates = await telegram.getUpdates(offset, config.pollTimeoutSec);
      if (updates.length === 0) {
        continue;
      }

      await processUpdates(
        updates,
        telegram,
        rpc,
        config.allowedChatIds,
        config.commandLogLimitDefault,
      );

      const highestUpdate = updates[updates.length - 1]?.update_id;
      if (typeof highestUpdate === "number") {
        offset = highestUpdate + 1;
      }
    } catch (error) {
      logJson("error", "telegram_poll_failed", { error: (error as Error).message });
      await sleep(1_250);
    }
  }
}

run().catch((error) => {
  logJson("error", "fatal", { error: (error as Error).message });
  process.exit(1);
});
