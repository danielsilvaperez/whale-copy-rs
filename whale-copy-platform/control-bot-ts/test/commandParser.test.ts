import { strict as assert } from "node:assert";
import test from "node:test";

import { parseCapsArgs, parseCommandText } from "../src/commandParser.js";

test("parseCommandText handles bot suffix and args", () => {
  const parsed = parseCommandText("/wallet_add@mybot 0xabc");
  assert.ok(parsed);
  assert.equal(parsed?.name, "wallet_add");
  assert.deepEqual(parsed?.args, ["0xabc"]);
});

test("parseCommandText rejects non-commands", () => {
  assert.equal(parseCommandText("status"), null);
  assert.equal(parseCommandText("   "), null);
});

test("parseCapsArgs keeps only supported positive numeric keys", () => {
  const caps = parseCapsArgs([
    "max_copy_notional_per_trade=1500",
    "max_open_positions=25",
    "unknown_key=10",
    "max_daily_notional_usd=-1",
  ]);

  assert.deepEqual(caps, {
    max_copy_notional_per_trade: 1500,
    max_open_positions: 25,
  });
});
