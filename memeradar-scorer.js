// memeradar-scorer.js
// Webhook receiver + paper-trading pipeline for new pump.fun launches,
// Cupsey-style: tight dev-holding and legitimacy filters, immediate entry
// instead of waiting for a dip (speed over conviction), and fast,
// disciplined exits rather than letting winners run. Four layers:
//
//   1. Is the coin good?      -> composite scoring (holder concentration,
//                                 deployer history, liquidity safety, dev
//                                 holding %, dex-paid signal, narrative,
//                                 smart money)
//   2. When should I buy it?  -> immediate by default; a pullback-wait
//                                 mode is still available via ENTRY_STYLE
//   3. Place the buy          -> simulated entry at the real market price
//                                 via Jupiter's quote API (no funds move)
//   4. Watch the position     -> fast, tight exits
//
// IMPORTANT CAVEAT: the actual edge behind high-frequency sniper traders
// is largely raw latency — being in the same block as the launch. This
// script polls on an interval and cannot replicate that.
//
// Setup:
//   npm install
//   cp .env.example .env   (fill in your API keys)
//   node memeradar-scorer.js
//
// Then register a Helius webhook pointing at this server's /new-launch URL
// (see the README comment at the bottom of this file).

import express from 'express';
import fetch from 'node-fetch';
import fs from 'fs';
import 'dotenv/config';

const app = express();
app.use(express.json());

const BIRDEYE_API_KEY = process.env.BIRDEYE_API_KEY;
const PUMP_FUN_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

// ---- Layer 1 weights — a starting point, not tuned on real outcomes yet. ----
const WEIGHTS = {
  holderConcentration: 0.20,
  deployerHistory: 0.20,
  liquiditySafety: 0.15,
  devHolding: 0.15,
  dexPaid: 0.10,
  narrative: 0.15,
  smartMoney: 0.05,
};

// ---- Config for layers 2 to 4 — tune freely ----
const SIM_CONFIG = {
  BUY_THRESHOLD: 60,

  POSITION_SIZE_USDC: 100,

  ENTRY_STYLE: 'immediate',        // 'immediate' (Cupsey-style) or 'pullback' (wait for a dip)
  PULLBACK_PCT_MIN: 8,
  CONFIRMATION_TICKS: 2,
  MAX_ENTRY_WAIT_MINUTES: 15,
  ENTRY_SAMPLE_INTERVAL_SECONDS: 15,
  PRICE_HISTORY_MAX_SAMPLES: 60,

  MAX_DEV_HOLDING_PCT: 5,

  // A past launch by this deployer that cleared this market cap counted as
  // "survived" rather than an instant dump. Tunable — real migration off
  // the bonding curve happens around $69k, this is a much lower bar meant
  // to separate "got some real buying interest" from background noise.
  DEPLOYER_SURVIVAL_MCAP_USD: 10000,

  TAKE_PROFIT_PCT: 20,
  STOP_LOSS_PCT: 12,
  MAX_HOLD_MINUTES: 10,
  CHECK_INTERVAL_SECONDS: 15,

  PROBE_USDC_AMOUNT: 10,
};

const STATE_FILE = './paper-trades.json';
let watchlist = [];
let openPositions = [];
let closedTrades = [];

function loadState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    watchlist = parsed.watchlist || [];
    openPositions = parsed.openPositions || [];
    closedTrades = parsed.closedTrades || [];
  } catch {
    watchlist = [];
    openPositions = [];
    closedTrades = [];
  }
}

function saveState() {
  fs.writeFileSync(STATE_FILE, JSON.stringify({ watchlist, openPositions, closedTrades }, null, 2));
}

loadState();

// =====================================================================
// LAYER 1: is the coin good?
// =====================================================================

async function getHolderAndDevData(mint, deployerWallet) {
  try {
    const res = await fetch(
      `https://public-api.birdeye.so/defi/token_holder?address=${mint}`,
      { headers: { 'X-API-KEY': BIRDEYE_API_KEY } }
    );
    const data = await res.json();
    const holders = data?.data?.items;
    if (!holders || holders.length === 0) {
      return {
        holderConcentration: { score: null, raw: 'no holder data yet' },
        devHolding: { score: null, raw: 'no holder data yet' },
      };
    }

    const totalSupply = holders.reduce((sum, h) => sum + h.amount, 0);
    const top10 = holders.slice(0, 10).reduce((sum, h) => sum + h.amount, 0);
    const concentrationPct = totalSupply ? (top10 / totalSupply) * 100 : 100;
    const concentrationScore = Math.max(0, 100 - concentrationPct);

    const devHolder = holders.find(h => h.owner === deployerWallet);
    const devHoldingPct = devHolder && totalSupply ? (devHolder.amount / totalSupply) * 100 : 0;
    const devScore = devHoldingPct <= SIM_CONFIG.MAX_DEV_HOLDING_PCT
      ? 100
      : Math.max(0, 100 - (devHoldingPct - SIM_CONFIG.MAX_DEV_HOLDING_PCT) * 5);

    return {
      holderConcentration: { score: concentrationScore, raw: { concentrationPct } },
      devHolding: { score: devScore, raw: { devHoldingPct } },
    };
  } catch (err) {
    return {
      holderConcentration: { score: null, raw: `error: ${err.message}` },
      devHolding: { score: null, raw: `error: ${err.message}` },
    };
  }
}

// Single RugCheck fetch, shared by liquiditySafety and deployerHistory below
// so we don't hit the API twice for one launch.
async function getRugCheckReport(mint) {
  try {
    const res = await fetch(`https://api.rugcheck.xyz/v1/tokens/${mint}/report`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

function deriveLiquiditySafety(report) {
  if (!report) return { score: null, raw: 'rugcheck fetch failed' };
  if (report.rugged === true) return { score: 0, raw: { rugged: true } };

  const market = Array.isArray(report.markets) ? report.markets[0] : null;
  const lpLockedPct = market?.lp?.lpLockedPct ?? 0;

  // Scale smoothly: fully locked costs nothing, fully unlocked costs 50.
  let score = 100 - (100 - lpLockedPct) * 0.5;
  if (report.mintAuthority) score -= 25;   // deployer can still mint more supply
  if (report.freezeAuthority) score -= 25; // deployer can freeze wallets

  return {
    score: Math.max(0, score),
    raw: { lpLockedPct, mintAuthority: report.mintAuthority, freezeAuthority: report.freezeAuthority },
  };
}

// Uses RugCheck's creatorTokens field — the deployer's own past launches,
// each with the market cap it reached — instead of guessing from raw
// Helius transaction history.
function deriveDeployerHistory(report) {
  const tokens = report?.creatorTokens;
  if (!Array.isArray(tokens) || tokens.length === 0) {
    // No history isn't a point for or against — neutral score.
    return { score: 50, raw: { totalPastLaunches: 0 } };
  }

  const threshold = SIM_CONFIG.DEPLOYER_SURVIVAL_MCAP_USD;
  const survived = tokens.filter(t => (t.marketCap || 0) >= threshold).length;
  const survivalRate = survived / tokens.length;

  return {
    score: survivalRate * 100,
    raw: { totalPastLaunches: tokens.length, survived, survivalMcapThresholdUSD: threshold },
  };
}

// Cupsey-style "dex paid" signal. Best-effort against DexScreener's orders
// endpoint — a token with no orders yet (true for anything seconds old)
// returns a non-JSON 404 page rather than an empty array, so that's
// treated as "not paid" instead of an error.
async function getDexPaid(mint) {
  try {
    const res = await fetch(`https://api.dexscreener.com/orders/v1/solana/${mint}`);

    if (!res.ok) return { score: 0, raw: { paid: false, note: `http ${res.status}` } };

    const contentType = res.headers.get('content-type') || '';
    if (!contentType.includes('application/json')) {
      return { score: 0, raw: { paid: false, note: 'non-JSON response' } };
    }

    const data = await res.json();
    const paid = Array.isArray(data) && data.some(order => order.status === 'approved');
    return { score: paid ? 100 : 0, raw: { paid } };
  } catch (err) {
    return { score: 0, raw: `error: ${err.message}` };
  }
}

async function getNarrativeScore(tokenName, tokenSymbol) {
  // Wire this into whatever MemeRadar already uses for AI web lookups.
  return { score: 50, raw: 'not yet wired to MemeRadar narrative logic' };
}

async function getSmartMoneyScore(mint) {
  // Needs a labeled-wallet source (Arkham, Nansen). Stubbed until you pick one.
  return { score: 0, raw: 'not yet wired to a labeled-wallet source' };
}

function computeCompositeScore(components) {
  let total = 0;
  let weightUsed = 0;
  for (const [key, weight] of Object.entries(WEIGHTS)) {
    const s = components[key]?.score;
    if (s !== null && s !== undefined) {
      total += s * weight;
      weightUsed += weight;
    }
  }
  return weightUsed > 0 ? Math.round(total / weightUsed) : null;
}

// =====================================================================
// Shared: price sampling via Jupiter (quote only — no funds ever move)
// =====================================================================
async function getApproxPrice(mint) {
  try {
    const amountBaseUnits = SIM_CONFIG.PROBE_USDC_AMOUNT * 1_000_000;
    const res = await fetch(
      `https://quote-api.jup.ag/v6/quote?inputMint=${USDC_MINT}&outputMint=${mint}&amount=${amountBaseUnits}&slippageBps=100`
    );
    const data = await res.json();
    const outAmount = Number(data?.outAmount);
    if (!outAmount) return null;
    return SIM_CONFIG.PROBE_USDC_AMOUNT / outAmount;
  } catch {
    return null;
  }
}

// =====================================================================
// LAYER 2: when should I buy it?
// =====================================================================

function addToWatchlist(mint, tokenName, tokenSymbol, compositeScore) {
  watchlist.push({
    mint, tokenName, tokenSymbol, compositeScore,
    addedTime: new Date().toISOString(),
    priceSamples: [],
  });
  saveState();
  console.log(`Watching ${tokenSymbol} (score ${compositeScore}) for a pullback entry...`);
}

function removeFromWatchlist(mint) {
  watchlist = watchlist.filter(w => w.mint !== mint);
}

async function evaluateEntryTiming(item) {
  const price = await getApproxPrice(item.mint);
  const minutesWaiting = (Date.now() - new Date(item.addedTime).getTime()) / 60000;

  if (price === null) {
    if (minutesWaiting >= SIM_CONFIG.MAX_ENTRY_WAIT_MINUTES) {
      removeFromWatchlist(item.mint);
      saveState();
      console.log(`Dropped ${item.tokenSymbol} — never got a tradable price route.`);
    }
    return;
  }

  item.priceSamples.push({ time: Date.now(), price });
  if (item.priceSamples.length > SIM_CONFIG.PRICE_HISTORY_MAX_SAMPLES) item.priceSamples.shift();

  const localHigh = Math.max(...item.priceSamples.map(s => s.price));
  const pullbackPct = ((localHigh - price) / localHigh) * 100;

  const recent = item.priceSamples.slice(-(SIM_CONFIG.CONFIRMATION_TICKS + 1));
  let bouncing = recent.length === SIM_CONFIG.CONFIRMATION_TICKS + 1;
  if (bouncing) {
    for (let i = 1; i < recent.length; i++) {
      if (recent[i].price <= recent[i - 1].price) { bouncing = false; break; }
    }
  }

  if (pullbackPct >= SIM_CONFIG.PULLBACK_PCT_MIN && bouncing) {
    await triggerEntry(item, price, 'pullback_entry');
  } else if (minutesWaiting >= SIM_CONFIG.MAX_ENTRY_WAIT_MINUTES) {
    await triggerEntry(item, price, 'timeout_entry');
  } else {
    saveState();
  }
}

async function triggerEntry(item, entryPrice, entryReason) {
  removeFromWatchlist(item.mint);
  await simulateBuy(item.mint, item.tokenName, item.tokenSymbol, item.compositeScore, entryPrice, entryReason);
}

// =====================================================================
// LAYER 3: place the buy (simulated)
// =====================================================================

async function simulateBuy(mint, tokenName, tokenSymbol, compositeScore, entryPrice, entryReason = 'immediate') {
  const price = entryPrice ?? await getApproxPrice(mint);
  if (!price) {
    console.log(`Skipped simulated buy for ${tokenSymbol} — no Jupiter route/liquidity.`);
    return;
  }
  const position = {
    mint, tokenName, tokenSymbol, compositeScore, entryReason,
    entryPrice: price,
    entryTime: new Date().toISOString(),
    positionSizeUSDC: SIM_CONFIG.POSITION_SIZE_USDC,
  };
  openPositions.push(position);
  saveState();
  console.log(`Simulated BUY: ${tokenSymbol} (${mint}) — ${entryReason}, score ${compositeScore}`);
}

// =====================================================================
// LAYER 4: watch the position — fast, disciplined exits
// =====================================================================

async function evaluatePosition(position) {
  const currentPrice = await getApproxPrice(position.mint);

  if (!currentPrice) {
    closePosition(position, { exitReason: 'liquidity_gone', pnlPct: -100 });
    return;
  }

  const pnlPct = ((currentPrice - position.entryPrice) / position.entryPrice) * 100;
  const heldMinutes = (Date.now() - new Date(position.entryTime).getTime()) / 60000;

  if (pnlPct >= SIM_CONFIG.TAKE_PROFIT_PCT) {
    closePosition(position, { exitReason: 'take_profit', pnlPct });
  } else if (pnlPct <= -SIM_CONFIG.STOP_LOSS_PCT) {
    closePosition(position, { exitReason: 'stop_loss', pnlPct });
  } else if (heldMinutes >= SIM_CONFIG.MAX_HOLD_MINUTES) {
    closePosition(position, { exitReason: 'max_hold_time', pnlPct });
  }
}

function closePosition(position, { exitReason, pnlPct }) {
  const pnlUSDC = position.positionSizeUSDC * (pnlPct / 100);
  const closedTrade = {
    ...position,
    exitTime: new Date().toISOString(),
    exitReason,
    pnlPct: Math.round(pnlPct * 100) / 100,
    pnlUSDC: Math.round(pnlUSDC * 100) / 100,
  };
  closedTrades.push(closedTrade);
  openPositions = openPositions.filter(p => p.mint !== position.mint);
  saveState();
  console.log(`Simulated SELL: ${position.tokenSymbol} — ${exitReason}, ${closedTrade.pnlPct}% (${closedTrade.pnlUSDC} USDC)`);
}

function computeSummary() {
  const totalClosed = closedTrades.length;
  const wins = closedTrades.filter(t => t.pnlUSDC > 0).length;
  const losses = closedTrades.filter(t => t.pnlUSDC <= 0).length;
  const totalRealizedPnlUSDC = closedTrades.reduce((sum, t) => sum + t.pnlUSDC, 0);
  const winRatePct = totalClosed > 0 ? Math.round((wins / totalClosed) * 1000) / 10 : null;

  return {
    watchlistCount: watchlist.length,
    openPositionCount: openPositions.length,
    totalClosedTrades: totalClosed,
    wins, losses, winRatePct,
    totalRealizedPnlUSDC: Math.round(totalRealizedPnlUSDC * 100) / 100,
  };
}

// ---- Background loops ----
setInterval(async () => {
  for (const item of [...watchlist]) {
    await evaluateEntryTiming(item);
  }
}, SIM_CONFIG.ENTRY_SAMPLE_INTERVAL_SECONDS * 1000);

setInterval(async () => {
  for (const position of [...openPositions]) {
    await evaluatePosition(position);
  }
}, SIM_CONFIG.CHECK_INTERVAL_SECONDS * 1000);

// ---- Webhook endpoint: new launch detected (triggers layer 1) ----
app.post('/new-launch', async (req, res) => {
  try {
    const event = Array.isArray(req.body) ? req.body[0] : req.body;
    const mint = event?.tokenTransfers?.[0]?.mint;
    const deployerWallet = event?.feePayer;
    const tokenName = event?.events?.token?.name || 'unknown';
    const tokenSymbol = event?.events?.token?.symbol || 'unknown';

    if (!mint || !deployerWallet) {
      return res.status(400).json({ error: 'missing mint or deployer in payload' });
    }

    const [holderAndDev, rugCheckReport, dexPaid, narrative, smartMoney] =
      await Promise.all([
        getHolderAndDevData(mint, deployerWallet),
        getRugCheckReport(mint),
        getDexPaid(mint),
        getNarrativeScore(tokenName, tokenSymbol),
        getSmartMoneyScore(mint),
      ]);

    const components = {
      holderConcentration: holderAndDev.holderConcentration,
      devHolding: holderAndDev.devHolding,
      deployerHistory: deriveDeployerHistory(rugCheckReport),
      liquiditySafety: deriveLiquiditySafety(rugCheckReport),
      dexPaid,
      narrative,
      smartMoney,
    };
    const compositeScore = computeCompositeScore(components);

    console.log(JSON.stringify({ mint, tokenName, tokenSymbol, deployerWallet, compositeScore, components }, null, 2));

    if (compositeScore !== null && compositeScore >= SIM_CONFIG.BUY_THRESHOLD) {
      if (SIM_CONFIG.ENTRY_STYLE === 'immediate') {
        await simulateBuy(mint, tokenName, tokenSymbol, compositeScore, undefined, 'immediate');
      } else {
        addToWatchlist(mint, tokenName, tokenSymbol, compositeScore);
      }
    }

    res.status(200).json({ ok: true, compositeScore });
  } catch (err) {
    console.error('Error scoring launch:', err);
    res.status(500).json({ error: 'internal error' });
  }
});

// ---- View the full pipeline ----
app.get('/portfolio', (req, res) => {
  res.json({ summary: computeSummary(), watchlist, openPositions, closedTrades });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`MemeRadar scorer + simulator listening on port ${PORT}`));

/* ----------------------------------------------------------------------
README

1. API keys: Birdeye (birdeye.so) is the only one this script calls at
   runtime now. RugCheck, Jupiter, and DexScreener need no key. Helius is
   still used to register the webhook from your Helius dashboard, but the
   script itself no longer needs the Helius API key for anything else —
   deployerHistory now comes from RugCheck's creatorTokens field instead.

2. Deploy with a stable public URL and a long-running process.

3. Register the Helius webhook once deployed:

   POST https://api.helius.xyz/v0/webhooks?api-key=YOUR_KEY
   {
     "webhookURL": "https://your-deployed-url.com/new-launch",
     "transactionTypes": ["CREATE"],
     "accountAddresses": ["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"],
     "webhookType": "enhanced"
   }

4. Two fixes made after reviewing real logs:
   - liquiditySafety was checking a field (liquidity.locked) that doesn't
     exist in RugCheck's actual schema. It now reads lpLockedPct from the
     real markets[].lp object.
   - dexPaid was crashing on a non-JSON 404 response for brand-new tokens
     with no orders yet. It now treats that as "not paid" instead of an
     error.

5. deployerHistory now uses RugCheck's own creatorTokens list — the
   deployer's past launches with their market caps — rather than a Helius
   transaction-history stub. A past launch counts as "survived" if it
   cleared DEPLOYER_SURVIVAL_MCAP_USD (default $10,000, well under full
   bonding-curve migration at ~$69k, meant to separate real traction from
   background noise). Tune that threshold as you see real outcomes.

6. Cupsey-style adjustments: devHolding and dexPaid scoring inputs,
   ENTRY_STYLE defaults to 'immediate', and exits are tighter/faster
   (TAKE_PROFIT_PCT, STOP_LOSS_PCT, MAX_HOLD_MINUTES). None of this
   replicates the latency edge real snipers rely on — this approximates
   the selection and exit philosophy on a polling interval, not sub-block
   execution.

7. Still stubbed: getNarrativeScore, getSmartMoneyScore.

8. GET /portfolio shows the watchlist, open positions, closed trades, and
   running win rate / total P&L. State persists to paper-trades.json.
---------------------------------------------------------------------- */
