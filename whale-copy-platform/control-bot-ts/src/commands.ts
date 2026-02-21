import { parseCapsArgs, parseCommandText } from "./commandParser.js";
import { EngineRpcClient } from "./rpcClient.js";

interface CommandOptions {
  defaultLogLimit: number;
}

interface EngineStatusResult {
  health?: {
    mode?: string;
    risk_profile?: string;
    paused?: boolean;
    tracked_wallet_count?: number;
    pending_suggestion_count?: number;
    daily_notional_usd?: number;
    follower_equity_usd?: number;
    recent_event_count?: number;
    last_heartbeat_at?: string;
  };
  settings?: {
    multiplier?: number;
    caps?: {
      max_copy_notional_per_trade?: number;
      max_market_exposure_usd?: number;
      max_daily_notional_usd?: number;
      max_open_positions?: number;
    };
  };
  tracked_wallets?: string[];
}

function asObject<T>(value: unknown): T {
  return value as T;
}

function formatStatus(result: EngineStatusResult): string {
  const health = result.health ?? {};
  const settings = result.settings ?? {};
  const caps = settings.caps ?? {};
  const wallets = result.tracked_wallets ?? [];

  return [
    "Whale Copy Platform Status",
    `mode: ${health.mode ?? "unknown"}`,
    `paused: ${String(health.paused ?? false)}`,
    `risk_profile: ${health.risk_profile ?? "unknown"}`,
    `multiplier: ${settings.multiplier ?? "unknown"}`,
    `follower_equity_usd: ${health.follower_equity_usd ?? "unknown"}`,
    `daily_notional_usd: ${health.daily_notional_usd ?? 0}`,
    `tracked_wallets: ${wallets.length}`,
    `pending_suggestions: ${health.pending_suggestion_count ?? 0}`,
    `heartbeat: ${health.last_heartbeat_at ?? "unknown"}`,
    "caps:",
    `  max_copy_notional_per_trade=${caps.max_copy_notional_per_trade ?? "n/a"}`,
    `  max_market_exposure_usd=${caps.max_market_exposure_usd ?? "n/a"}`,
    `  max_daily_notional_usd=${caps.max_daily_notional_usd ?? "n/a"}`,
    `  max_open_positions=${caps.max_open_positions ?? "n/a"}`,
  ].join("\n");
}

function formatWallets(value: unknown): string {
  const wallets = asObject<{ wallets?: string[] }>(value).wallets ?? [];
  if (wallets.length === 0) {
    return "No tracked wallets";
  }

  return ["Tracked wallets:", ...wallets.map((wallet, index) => `${index + 1}. ${wallet}`)].join("\n");
}

function formatSuggestions(value: unknown): string {
  const pending = asObject<{ pending?: Array<{ id: string; wallet: string; score: number; reason: string }> }>(value).pending ?? [];

  if (pending.length === 0) {
    return "No pending suggestions";
  }

  const lines = ["Pending suggestions:"];
  for (const suggestion of pending) {
    lines.push(`${suggestion.id} | ${suggestion.wallet} | score=${suggestion.score.toFixed(2)}`);
    lines.push(`reason: ${suggestion.reason}`);
  }
  return lines.join("\n");
}

function formatLogs(value: unknown): string {
  const events = asObject<{ events?: Array<{ ts: string; class: string; event_type: string; payload: Record<string, unknown> }> }>(value).events ?? [];

  if (events.length === 0) {
    return "No events available";
  }

  const lines = ["Recent events:"];
  for (const event of events) {
    const payload = JSON.stringify(event.payload);
    lines.push(`${event.ts} | ${event.class} | ${event.event_type}`);
    lines.push(payload.slice(0, 200));
  }

  return lines.join("\n");
}

function helpText(): string {
  return [
    "Commands:",
    "/status",
    "/health",
    "/startbot",
    "/stopbot",
    "/pause",
    "/resume",
    "/panic",
    "/mode <dry_run|live>",
    "/risk <conservative|balanced|aggressive>",
    "/multiplier <number>",
    "/caps key=value [key=value ...]",
    "/wallet_add <address>",
    "/wallet_remove <address>",
    "/wallet_list",
    "/suggestions",
    "/approve <suggestion_id>",
    "/reject <suggestion_id>",
    "/logs [limit]",
  ].join("\n");
}

export async function handleCommand(
  text: string,
  chatId: string,
  rpc: EngineRpcClient,
  options: CommandOptions,
): Promise<string> {
  const parsed = parseCommandText(text);
  if (!parsed) {
    return "Expected a slash command. Try /help";
  }

  const { name, args } = parsed;

  switch (name) {
    case "help":
    case "start":
      return helpText();
    case "status": {
      const value = await rpc.request("get_status", {}, chatId);
      return formatStatus(asObject<EngineStatusResult>(value));
    }
    case "health": {
      const ping = await rpc.request("ping", {}, chatId);
      const status = await rpc.request("get_status", {}, chatId);
      return [`ping: ${JSON.stringify(ping)}`, "", formatStatus(asObject<EngineStatusResult>(status))].join("\n");
    }
    case "startbot":
    case "resume": {
      await rpc.request("resume_trading", {}, chatId);
      return "Trading resumed";
    }
    case "stopbot":
    case "pause": {
      await rpc.request("pause_trading", {}, chatId);
      return "Trading paused";
    }
    case "panic": {
      await rpc.request("panic_close", {}, chatId);
      return "Panic close triggered; trading paused";
    }
    case "mode": {
      const mode = args[0];
      if (!mode) {
        return "Usage: /mode <dry_run|live>";
      }
      const value = await rpc.request("set_mode", { mode }, chatId);
      return `Mode updated: ${JSON.stringify(value)}`;
    }
    case "risk": {
      const risk_profile = args[0];
      if (!risk_profile) {
        return "Usage: /risk <conservative|balanced|aggressive>";
      }
      const value = await rpc.request("set_risk_profile", { risk_profile }, chatId);
      return `Risk profile updated: ${JSON.stringify(value)}`;
    }
    case "multiplier": {
      const multiplier = Number(args[0]);
      if (!Number.isFinite(multiplier) || multiplier <= 0) {
        return "Usage: /multiplier <positive-number>";
      }
      const value = await rpc.request("set_multiplier", { multiplier }, chatId);
      return `Multiplier updated: ${JSON.stringify(value)}`;
    }
    case "caps": {
      const caps = parseCapsArgs(args);
      if (Object.keys(caps).length === 0) {
        return "Usage: /caps max_copy_notional_per_trade=1500 max_market_exposure_usd=7500 max_daily_notional_usd=45000 max_open_positions=25";
      }
      const value = await rpc.request("set_caps", caps, chatId);
      return `Caps updated: ${JSON.stringify(value)}`;
    }
    case "wallet_add": {
      const wallet = args[0];
      if (!wallet) {
        return "Usage: /wallet_add <0x...>";
      }
      const value = await rpc.request("add_wallet", { wallet }, chatId);
      return `Wallet add result: ${JSON.stringify(value)}`;
    }
    case "wallet_remove": {
      const wallet = args[0];
      if (!wallet) {
        return "Usage: /wallet_remove <0x...>";
      }
      const value = await rpc.request("remove_wallet", { wallet }, chatId);
      return `Wallet remove result: ${JSON.stringify(value)}`;
    }
    case "wallet_list": {
      const value = await rpc.request("list_wallets", {}, chatId);
      return formatWallets(value);
    }
    case "suggestions": {
      const value = await rpc.request("list_suggestions", {}, chatId);
      return formatSuggestions(value);
    }
    case "approve": {
      const id = args[0];
      if (!id) {
        return "Usage: /approve <suggestion-id>";
      }
      const value = await rpc.request("ack_suggestion", { id }, chatId);
      return `Suggestion approved: ${JSON.stringify(value)}`;
    }
    case "reject": {
      const id = args[0];
      if (!id) {
        return "Usage: /reject <suggestion-id>";
      }
      const value = await rpc.request("reject_suggestion", { id }, chatId);
      return `Suggestion rejected: ${JSON.stringify(value)}`;
    }
    case "logs": {
      const limitRaw = Number(args[0]);
      const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 100) : options.defaultLogLimit;
      const value = await rpc.request("tail_events", { limit }, chatId);
      return formatLogs(value);
    }
    default:
      return `Unknown command: /${name}. Try /help`;
  }
}
