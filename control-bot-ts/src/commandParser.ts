export interface ParsedCommand {
  name: string;
  args: string[];
}

const VALID_CAP_KEYS = new Set([
  "max_copy_notional_per_trade",
  "max_market_exposure_usd",
  "max_daily_notional_usd",
  "max_open_positions",
]);

export function parseCommandText(text: string): ParsedCommand | null {
  const trimmed = text.trim();
  if (!trimmed.startsWith("/")) {
    return null;
  }

  const parts = trimmed.split(/\s+/).filter(Boolean);
  if (parts.length === 0) {
    return null;
  }

  const rawCommand = parts[0].slice(1);
  if (!rawCommand) {
    return null;
  }

  const commandWithoutBot = rawCommand.split("@")[0];
  return {
    name: commandWithoutBot.toLowerCase(),
    args: parts.slice(1),
  };
}

export function parseCapsArgs(args: string[]): Record<string, number> {
  const caps: Record<string, number> = {};

  for (const token of args) {
    const [key, rawValue] = token.split("=", 2);
    if (!key || rawValue === undefined) {
      continue;
    }

    const normalizedKey = key.trim();
    if (!VALID_CAP_KEYS.has(normalizedKey)) {
      continue;
    }

    const numeric = Number(rawValue);
    if (!Number.isFinite(numeric) || numeric <= 0) {
      continue;
    }

    caps[normalizedKey] = numeric;
  }

  return caps;
}
