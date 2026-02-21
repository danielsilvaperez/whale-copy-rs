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

test("parseCommandText returns null for empty string", () => {
  assert.equal(parseCommandText(""), null);
});

test("parseCommandText returns null for slash only", () => {
  assert.equal(parseCommandText("/"), null);
});

test("parseCommandText lowercases command name", () => {
  const parsed = parseCommandText("/STATUS");
  assert.ok(parsed);
  assert.equal(parsed?.name, "status");
});

test("parseCommandText preserves arg casing", () => {
  const parsed = parseCommandText("/command FooBar BAZ");
  assert.ok(parsed);
  assert.deepEqual(parsed?.args, ["FooBar", "BAZ"]);
});

test("parseCapsArgs ignores zero values", () => {
  const caps = parseCapsArgs(["max_open_positions=0"]);
  assert.deepEqual(caps, {});
});

test("parseCapsArgs ignores malformed key=value (no equals)", () => {
  const caps = parseCapsArgs(["max_open_positionsnoequalssign"]);
  assert.deepEqual(caps, {});
});

test("parseCapsArgs handles multiple equals (takes first value)", () => {
  const caps = parseCapsArgs(["max_open_positions=25=extra"]);
  assert.deepEqual(caps, { max_open_positions: 25 });
});
