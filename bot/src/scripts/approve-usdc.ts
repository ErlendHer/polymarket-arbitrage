/**
 * Approve USDC.e - Approve spending on Polymarket contracts
 *
 * Approves USDC.e for:
 *   1. CTF Exchange (for BUY orders)
 *   2. Neg Risk CTF Exchange (for neg-risk markets)
 *   3. Conditional Tokens framework (for SELL orders)
 *
 * Usage:
 *   npx tsx src/scripts/approve-usdc.ts
 *
 * Reads PRIVATE_KEY from ../.env automatically.
 */

import { readFileSync } from "fs";
import { resolve } from "path";
import {
  createWalletClient,
  createPublicClient,
  http,
  parseAbi,
  maxUint256,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";

const USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174" as const;
const CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E" as const;
const NEG_RISK_EXCHANGE =
  "0xC5d563A36AE78145C45a50134d48A1215220f80a" as const;
const CONDITIONAL_TOKENS =
  "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045" as const;

const erc20Abi = parseAbi([
  "function approve(address spender, uint256 amount) returns (bool)",
  "function allowance(address owner, address spender) view returns (uint256)",
]);

const erc1155Abi = parseAbi([
  "function setApprovalForAll(address operator, bool approved)",
  "function isApprovedForAll(address account, address operator) view returns (bool)",
]);

interface ApprovalTarget {
  name: string;
  address: `0x${string}`;
  type: "erc20" | "erc1155";
}

const TARGETS: ApprovalTarget[] = [
  { name: "CTF Exchange", address: CTF_EXCHANGE, type: "erc20" },
  { name: "Neg Risk CTF Exchange", address: NEG_RISK_EXCHANGE, type: "erc20" },
  {
    name: "CTF Exchange (Conditional Tokens)",
    address: CTF_EXCHANGE,
    type: "erc1155",
  },
  {
    name: "Neg Risk CTF Exchange (Conditional Tokens)",
    address: NEG_RISK_EXCHANGE,
    type: "erc1155",
  },
];

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
  const publicClient = createPublicClient({
    chain: polygon,
    transport: http(),
  });

  console.log(`Wallet: ${account.address}\n`);

  for (const target of TARGETS) {
    if (target.type === "erc20") {
      // Check current allowance
      const allowance = await publicClient.readContract({
        address: USDC_E,
        abi: erc20Abi,
        functionName: "allowance",
        args: [account.address, target.address],
      });

      if (allowance >= maxUint256 / 2n) {
        console.log(`✓ ${target.name}: USDC.e already approved`);
        continue;
      }

      console.log(`→ ${target.name}: Approving USDC.e...`);
      const hash = await walletClient.writeContract({
        address: USDC_E,
        abi: erc20Abi,
        functionName: "approve",
        args: [target.address, maxUint256],
      });
      console.log(`  tx: ${hash}`);
      const receipt = await publicClient.waitForTransactionReceipt({ hash });
      console.log(
        `  ${receipt.status === "success" ? "✓ Confirmed" : "✗ Failed"} (block ${receipt.blockNumber})`
      );
    } else {
      // ERC1155 setApprovalForAll on Conditional Tokens contract
      const approved = await publicClient.readContract({
        address: CONDITIONAL_TOKENS,
        abi: erc1155Abi,
        functionName: "isApprovedForAll",
        args: [account.address, target.address],
      });

      if (approved) {
        console.log(
          `✓ ${target.name}: Conditional Tokens already approved`
        );
        continue;
      }

      console.log(`→ ${target.name}: Approving Conditional Tokens...`);
      const hash = await walletClient.writeContract({
        address: CONDITIONAL_TOKENS,
        abi: erc1155Abi,
        functionName: "setApprovalForAll",
        args: [target.address, true],
      });
      console.log(`  tx: ${hash}`);
      const receipt = await publicClient.waitForTransactionReceipt({ hash });
      console.log(
        `  ${receipt.status === "success" ? "✓ Confirmed" : "✗ Failed"} (block ${receipt.blockNumber})`
      );
    }
  }

  console.log("\nDone! All approvals set.");
}

main().catch((err) => {
  console.error("Failed:", err);
  process.exit(1);
});
