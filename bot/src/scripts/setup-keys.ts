/**
 * Setup Keys - Derive Polymarket CLOB API credentials
 *
 * Uses createOrDeriveApiKey() which deterministically derives credentials
 * from your wallet signature. Safe to run multiple times — returns the
 * same credentials each time.
 *
 * Usage:
 *   npx tsx src/scripts/setup-keys.ts
 *
 * Reads PRIVATE_KEY from ../.env automatically.
 */

import { readFileSync } from "fs";
import { resolve } from "path";
import { ClobClient } from "@polymarket/clob-client";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";

const CLOB_HOST = "https://clob.polymarket.com";
const CHAIN_ID = 137;

function loadEnv() {
  const envPath = resolve(__dirname, "../../../.env");
  try {
    const content = readFileSync(envPath, "utf-8");
    for (const line of content.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = trimmed.indexOf("=");
      if (eq === -1) continue;
      const key = trimmed.slice(0, eq).trim();
      const val = trimmed.slice(eq + 1).trim();
      if (!process.env[key]) process.env[key] = val;
    }
  } catch {
    // .env not found, rely on existing env vars
  }
}

async function main() {
  loadEnv();
  const privateKey = process.env.PRIVATE_KEY;
  if (!privateKey) {
    console.error("ERROR: PRIVATE_KEY not found in .env or environment");
    process.exit(1);
  }

  const key = privateKey.startsWith("0x")
    ? (privateKey as `0x${string}`)
    : (`0x${privateKey}` as `0x${string}`);

  const account = privateKeyToAccount(key);
  const walletClient = createWalletClient({
    account,
    chain: polygon,
    transport: http(),
  });

  console.log(`Wallet address: ${account.address}`);
  console.log("Deriving API credentials...\n");

  // Create client without creds to derive them
  const client = new ClobClient(CLOB_HOST, CHAIN_ID, walletClient);
  const creds = await client.createOrDeriveApiKey();

  console.log("=== Polymarket CLOB API Credentials ===");
  console.log(`API_KEY=${creds.key}`);
  console.log(`API_SECRET=${creds.secret}`);
  console.log(`API_PASSPHRASE=${creds.passphrase}`);
  console.log("\nSave these to your .env file.");
}

main().catch((err) => {
  console.error("Failed:", err);
  process.exit(1);
});
