// memeradar-scorer.js
// Webhook receiver + paper-trading pipeline for new pump.fun launches,
// structured as four layers of judgment:
//
//   1. Is the coin good?      -> composite scoring (holders, deployer
//                                 history, liquidity, narrative, smart money)
//   2. When should I buy it?  -> entry-timing layer: watches price action
//                                 for a pullback-and-bounce (a local low)
//                                 rather than buying the instant it scores well
//   3. Place the buy          -> simulated entry at the real market price
//                                 via Jupiter's quote API (no funds move)
//   4. Watch the position     -> monitors for take profit / stop loss /
//                                 max hold time / loss of liquidity
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

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const BIRDEYE_API_KEY = process.env.BIRDEYE_API_KEY;
const PUMP_FUN_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

// ---- Layer 1 weights — a starting point, not tuned on real outcomes yet. ----
const WEIGHTS = {
  holderConcentration: 0.25,
  deployerHistory: 0.25,
  liquiditySafety: 0.20,
  narrative: 0.20,
  smartMoney: 0.10,
};

// ---- Config for layers 2 to 4 — tune freely ----
const SIM_CONFIG = {
  BUY_THRESHOLD: 65,              // composite score needed to enter the watchlist at all
  POSITION_SIZE_USDC: 100,        // hypothetical size per trade

  // Layer 2: entry timing
  PULLBACK_PCT_MIN: 8,            // minimum dip from the local high, since being added, to count as "a low"
  CONFIRMATION_TICKS: 2,          // consecutive rising samples required to call it a bounce, not a crash
  MAX_ENTRY_WAIT_MINUTES: 15,     // give up waiting for a pullback and just buy at market after this long
  ENTRY_SAMPLE_INTERVAL_SECONDS: 15,
  PRICE_HISTORY_MAX_SAMPLES: 60,

  // Layer 4: position monitoring
  TAKE_PROFIT_PCT: 50,
  STOP_LOSS_PCT: 25,
  MAX_HOLD_MINUTES: 180,
  CHECK_INTERVAL_SECONDS: 30,

  PROBE_USDC_AMOUNT: 10,          // fixed USDC amount used to sample price via Jupiter
};

const STATE_FILE = './paper-trades.json';
let watchlist = [];     // layer 2: coins that scored well, waiting for an entry signal
let openPositions = []; // layer 3/4: simulated buys currently being watched
let closedTrades = [];  // finished trades

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

async function getHolderConcentration(mint) {
  try {
    const res = await fetch(
      `https://public-api.birdeye.so/defi/token_holder?address=${mint}`,
      { headers: { 'X-API-KEY': BIRDEYE_API_KEY } }
    );
    const data = await res.json();
    const holders = data?.data?.items;
    if (!holders || holders.length === 0) return { score: null, raw: 'no holder data yet' };

    const totalSupply = holders.reduce((sum, h) => sum + h.amount, 0);
    const top10 = holders.slice(0, 10).reduce((sum, h) => sum + h.amount, 0);
    const concentrationPct = totalSupply ? (top10 / totalSupply) * 100 : 100;
    const score = Math.max(0, 100 - concentrationPct);
    return { score, raw: { concentrationPct } };
  } catch (err) {
    return { score: null, raw: `error: ${err.message}` };
  }
}

async function getDeployerHistory(deployerWallet) {
  try {
    const res = await fetch(
      `https://api.helius.xyz/v0/addresses/${deployerWallet}/transactions?api-key=${HELIUS_API_KEY}`
    );
    const txs = await res.json();
    const pastLaunches = (txs || []).filter(tx =>
      tx.instructions?.some(ix => ix.programId === PUMP_FUN_PROGRAM_ID && ix.type === 'CREATE')
    );
    if (pastLaunches.length === 0) return { score: 50, raw: { totalPastLaunches: 0 } };

    const survivedCount = await countSurvivedLaunches(pastLaunches);
    const survivalRate = survivedCount / pastLaunches.length;
    return { score: survivalRate * 100, raw: { totalPastLaunches: pastLaunches.length, survived: survivedCount } };
  } catch (err) {
    return { score: null, raw: `error: ${err.message}` };
  }
}

async function countSurvivedLaunches(pastLaunches) {
  // Placeholder: check each past mint's current liquidity/holder count.
  return 0;
}

async function getLiquiditySafety(mint) {
  try {
    const res = await fetch(`https://api.rugcheck.xyz/v1/tokens/${mint}/report`);
    const data = await res.json();
    let score = 100;
    if (!data?.liquidity?.locked) score -= 50;
    if (data?.mintAuthority) score -= 25;
    if (data?.freezeAuthority) score -= 25;
    return { score: Math.max(0, score), raw: data };
  } catch (err) {
    return { score: null, raw: `error: ${err.message}` };
  }
}

async function getNarrativeScore(tokenName, tokenSymbol) {
  // Wire this into whatever MemeRadar already uses for AI web lookups /
  // account-mention tracking. Stubbed so the pipeline runs end to end.
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
// Price is derived as (fixed USDC input) / (tokens returned), so it's a
// genuine price in USDC per base-unit of the token — consistent over time
// for a given mint, which is all layers 2 and 4 need. It won't exactly
// match a live sell quote since real exit slippage is usually worse than
// this buy-side approximation, especially on thin meme coin liquidity.
async function getApproxPrice(mint) {
  try {
    const amountBaseUnits = SIM_CONFIG.PROBE_USDC_AMOUNT * 1_000_000; // USDC has 6 decimals
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
    mint,
    tokenName,
    tokenSymbol,
    compositeScore,
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

  // Bounce confirmation: the last N samples must be strictly rising, so we're
  // buying as it turns back up rather than while it's still falling.
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
    mint,
    tokenName,
    tokenSymbol,
    compositeScore,
    entryReason,
    entryPrice: price,
    entryTime: new Date().toISOString(),
    positionSizeUSDC: SIM_CONFIG.POSITION_SIZE_USDC,
  };
  openPositions.push(position);
  saveState();
  console.log(`Simulated BUY: ${tokenSymbol} (${mint}) — ${entryReason}, score ${compositeScore}`);
}

// =====================================================================
// LAYER 4: watch the position
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
  // otherwise: still open, nothing to do this cycle
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
    wins,
    losses,
    winRatePct,
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

    const [holderConcentration, deployerHistory, liquiditySafety, narrative, smartMoney] =
      await Promise.all([
        getHolderConcentration(mint),
        getDeployerHistory(deployerWallet),
        getLiquiditySafety(mint),
        getNarrativeScore(tokenName, tokenSymbol),
        getSmartMoneyScore(mint),
      ]);

    const components = { holderConcentration, deployerHistory, liquiditySafety, narrative, smartMoney };
    const compositeScore = computeCompositeScore(components);

    console.log(JSON.stringify({ mint, tokenName, tokenSymbol, deployerWallet, compositeScore, components }, null, 2));

    if (compositeScore !== null && compositeScore >= SIM_CONFIG.BUY_THRESHOLD) {
      addToWatchlist(mint, tokenName, tokenSymbol, compositeScore);
    }

    res.status(200).json({ ok: true, compositeScore });
  } catch (err) {
    console.error('Error scoring launch:', err);
    res.status(500).json({ error: 'internal error' });
  }
});

// ---- View the full pipeline: watchlist, open positions, closed trades ----
app.get('/portfolio', (req, res) => {
  res.json({
    summary: computeSummary(),
    watchlist,
    openPositions,
    closedTrades,
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`MemeRadar scorer + simulator listening on port ${PORT}`));

/* ----------------------------------------------------------------------
README

1. Get API keys:
   - Helius: helius.dev — free tier covers RPC + webhooks for this volume
   - Birdeye: birdeye.so — holder data; free tier is rate-limited
   - RugCheck and Jupiter's quote API need no key for this usage

2. Deploy somewhere with a stable public URL and a long-running process
   (Render, Railway, Fly.io, a small VPS). The two setInterval loops need
   the process to stay alive — this won't work on something that sleeps
   between requests.

3. Register the Helius webhook once deployed:

   POST https://api.helius.xyz/v0/webhooks?api-key=YOUR_KEY
   {
     "webhookURL": "https://your-deployed-url.com/new-launch",
     "transactionTypes": ["CREATE"],
     "accountAddresses": ["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"],
     "webhookType": "enhanced"
   }

4. The four layers, end to end:
   - Layer 1 runs the moment a launch is detected and produces a composite
     score. Anything below BUY_THRESHOLD is dropped entirely.
   - Layer 2 puts anything that scores well onto a watchlist and samples
     its price every ENTRY_SAMPLE_INTERVAL_SECONDS. It waits for the price
     to pull back at least PULLBACK_PCT_MIN from its local high since being
     watched, then turn upward for CONFIRMATION_TICKS straight samples —
     that combination is the "bought near a low" signal. If no such pullback
     shows up within MAX_ENTRY_WAIT_MINUTES, it buys anyway rather than risk
     missing the coin entirely.
   - Layer 3 logs the simulated buy at whatever price triggered the entry.
   - Layer 4 checks every open position every CHECK_INTERVAL_SECONDS against
     take profit, stop loss, max hold time, and loss of liquidity.

5. Check GET /portfolio any time to see what's on the watchlist waiting for
   an entry, what's currently open, what's closed and why, and the running
   win rate and total P&L. State persists to paper-trades.json.

6. Fill in getNarrativeScore, getSmartMoneyScore, and countSurvivedLaunches
   once you've picked real data sources — they're stubbed so the rest of
   the pipeline runs now.

7. Caveat: the pullback detection is only as good as how often it samples.
   A coin that dips and bounces entirely between two 15-second samples will
   be invisible to this logic — it's watching for pullbacks measured in
   minutes, not the kind of sub-second wicks a sniper bot might catch.
---------------------------------------------------------------------- */
