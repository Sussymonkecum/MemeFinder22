// memeradar-scorer.js
// Webhook receiver + paper-trading pipeline for new pump.fun launches,
// Cupsey-style filters, with layer 1 running as a repeated evaluation over
// a window rather than a single snapshot. Four layers:
//
//   1. Is the coin good?      -> composite scoring, sampled repeatedly
//                                 over EVALUATION_WINDOW_SECONDS rather
//                                 than once at launch, so the decision is
//                                 based on a trend (holders forming,
//                                 liquidity staying locked, price not
//                                 already dumping) instead of a single
//                                 near-meaningless instant
//   2. When should I buy it?  -> immediate by default once evaluation
//                                 passes; a pullback-wait mode is still
//                                 available via ENTRY_STYLE
//   3. Place the buy          -> simulated entry at the real market price
//                                 via Jupiter's quote API (no funds move)
//   4. Watch the position     -> fast, tight exits
//
// IMPORTANT CAVEAT: the actual edge behind high-frequency sniper traders
// is largely raw latency — being in the same block as the launch. Adding
// a 60-second evaluation window moves further away from that, on purpose:
// the tradeoff here is giving up speed for a better-informed decision.
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
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const USDT_MINT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';
const KNOWN_NON_LAUNCH_MINTS = new Set([SOL_MINT, USDC_MINT, USDT_MINT]);

const WEIGHTS = {
  holderConcentration: 0.20,
  deployerHistory: 0.20,
  liquiditySafety: 0.15,
  devHolding: 0.15,
  dexPaid: 0.10,
  narrative: 0.15,
  smartMoney: 0.05,
};

const SIM_CONFIG = {
  // Layer 1: repeated evaluation instead of a single snapshot
  EVALUATION_WINDOW_SECONDS: 60,
  EVALUATION_SAMPLE_INTERVAL_SECONDS: 12,   // ~5 samples across the window
  MIN_VALID_SAMPLES: 2,                     // need at least this many usable samples to decide at all
  BUY_THRESHOLD: 60,                        // applied to the AVERAGE score across samples
  PRICE_DROP_REJECT_PCT: 30,                // reject if price already fell this much within the window — catches an instant dump a single snapshot would miss

  POSITION_SIZE_USDC: 100,

  ENTRY_STYLE: 'immediate',        // 'immediate' (Cupsey-style) or 'pullback' (wait for a dip)
  PULLBACK_PCT_MIN: 8,
  CONFIRMATION_TICKS: 2,
  MAX_ENTRY_WAIT_MINUTES: 15,
  ENTRY_SAMPLE_INTERVAL_SECONDS: 15,
  PRICE_HISTORY_MAX_SAMPLES: 60,

  MAX_DEV_HOLDING_PCT: 5,
  DEPLOYER_SURVIVAL_MCAP_USD: 10000,

  TAKE_PROFIT_PCT: 20,
  STOP_LOSS_PCT: 12,
  MAX_HOLD_MINUTES: 10,
  CHECK_INTERVAL_SECONDS: 15,

  PROBE_USDC_AMOUNT: 10,
};

const STATE_FILE = './paper-trades.json';
let evaluationQueue = []; // layer 1: coins being sampled repeatedly before a decision
let watchlist = [];       // layer 2 (pullback mode only): passed evaluation, waiting for an entry
let openPositions = [];
let closedTrades = [];

function loadState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    evaluationQueue = parsed.evaluationQueue || [];
    watchlist = parsed.watchlist || [];
    openPositions = parsed.openPositions || [];
    closedTrades = parsed.closedTrades || [];
  } catch {
    evaluationQueue = [];
    watchlist = [];
    openPositions = [];
    closedTrades = [];
  }
}

function saveState() {
  fs.writeFileSync(STATE_FILE, JSON.stringify({ evaluationQueue, watchlist, openPositions, closedTrades }, null, 2));
}

loadState();

// =====================================================================
// Scoring inputs (same as before)
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

  let score = 100 - (100 - lpLockedPct) * 0.5;
  if (report.mintAuthority) score -= 25;
  if (report.freezeAuthority) score -= 25;

  return {
    score: Math.max(0, score),
    raw: { lpLockedPct, mintAuthority: report.mintAuthority, freezeAuthority: report.freezeAuthority },
  };
}

function deriveDeployerHistory(report) {
  const tokens = report?.creatorTokens;
  if (!Array.isArray(tokens) || tokens.length === 0) {
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
  return { score: 50, raw: 'not yet wired to MemeRadar narrative logic' };
}

async function getSmartMoneyScore(mint) {
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

// Birdeye indexes pump.fun's bonding curve directly, so it has a price for
// tokens that haven't migrated to a DEX yet — which is nearly every brand
// new launch. Jupiter only routes trades across DEX pools, so it has no
// route at all for a pre-migration token; it's kept here only as a
// fallback for tokens that have already migrated to Raydium, in case
// Birdeye is slow to pick up a newly created pool.
async function getApproxPrice(mint) {
  try {
    const res = await fetch(
      `https://public-api.birdeye.so/defi/price?address=${mint}`,
      { headers: { 'X-API-KEY': BIRDEYE_API_KEY } }
    );
    const data = await res.json();
    const price = data?.data?.value;
    if (typeof price === 'number' && price > 0) return price;
  } catch {
    // fall through to Jupiter
  }

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

// Picks the actual new-token mint out of a transaction's token transfers,
// skipping SOL/USDC/USDT legs (e.g. the SOL paid for an initial buy) that
// were previously being mistaken for the launch itself.
function extractLaunchMint(event) {
  const transfers = event?.tokenTransfers || [];
  const candidate = transfers.find(t => t?.mint && !KNOWN_NON_LAUNCH_MINTS.has(t.mint));
  return candidate?.mint || transfers[0]?.mint || null;
}

// The webhook payload's inline name/symbol field was never matching the
// real Helius schema (always fell back to "unknown"). Fetching metadata
// directly from Birdeye by mint address is more reliable than guessing
// at the webhook's field names.
async function getTokenMetadata(mint) {
  try {
    const res = await fetch(
      `https://public-api.birdeye.so/defi/v3/token/meta-data/single?address=${mint}`,
      { headers: { 'X-API-KEY': BIRDEYE_API_KEY } }
    );
    const data = await res.json();
    return {
      name: data?.data?.name || 'unknown',
      symbol: data?.data?.symbol || 'unknown',
    };
  } catch {
    return { name: 'unknown', symbol: 'unknown' };
  }
}

// =====================================================================
// LAYER 1: repeated evaluation over a window, not a single snapshot
// =====================================================================

function addToEvaluationQueue(mint, deployerWallet, tokenName, tokenSymbol) {
  evaluationQueue.push({
    mint, deployerWallet, tokenName, tokenSymbol,
    addedTime: new Date().toISOString(),
    snapshots: [],
  });
  saveState();
  console.log(`Evaluating ${tokenSymbol} over the next ${SIM_CONFIG.EVALUATION_WINDOW_SECONDS}s...`);
}

function removeFromEvaluationQueue(mint) {
  evaluationQueue = evaluationQueue.filter(e => e.mint !== mint);
}

async function takeSnapshot(item) {
  const [holderAndDev, rugCheckReport, dexPaid, narrative, smartMoney, price] = await Promise.all([
    getHolderAndDevData(item.mint, item.deployerWallet),
    getRugCheckReport(item.mint),
    getDexPaid(item.mint),
    getNarrativeScore(item.tokenName, item.tokenSymbol),
    getSmartMoneyScore(item.mint),
    getApproxPrice(item.mint),
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

  item.snapshots.push({ time: new Date().toISOString(), compositeScore, price, components });
}

async function finalizeEvaluation(item) {
  const valid = item.snapshots.filter(s => s.compositeScore !== null);

  if (valid.length < SIM_CONFIG.MIN_VALID_SAMPLES) {
    console.log(`Skipped ${item.tokenSymbol} — only ${valid.length} usable samples over the window, not enough data.`);
    return;
  }

  const avgScore = valid.reduce((sum, s) => sum + s.compositeScore, 0) / valid.length;
  const minScore = Math.min(...valid.map(s => s.compositeScore));

  const pricedSamples = valid.filter(s => s.price !== null);
  let priceChangePct = null;
  if (pricedSamples.length >= 2) {
    const first = pricedSamples[0].price;
    const last = pricedSamples[pricedSamples.length - 1].price;
    priceChangePct = ((last - first) / first) * 100;
  }

  const alreadyDumping = priceChangePct !== null && priceChangePct <= -SIM_CONFIG.PRICE_DROP_REJECT_PCT;

  console.log(
    `Evaluation done for ${item.tokenSymbol}: avg ${Math.round(avgScore)}, min ${Math.round(minScore)}, ` +
    `price change ${priceChangePct !== null ? priceChangePct.toFixed(1) + '%' : 'n/a'}, samples ${valid.length}`
  );

  if (avgScore < SIM_CONFIG.BUY_THRESHOLD) {
    console.log(`Rejected ${item.tokenSymbol} — average score ${Math.round(avgScore)} below threshold.`);
    return;
  }
  if (alreadyDumping) {
    console.log(`Rejected ${item.tokenSymbol} — price already down ${priceChangePct.toFixed(1)}% during evaluation, looks like a dump.`);
    return;
  }

  const lastPrice = pricedSamples.length > 0 ? pricedSamples[pricedSamples.length - 1].price : undefined;
  const compositeScoreForRecord = Math.round(avgScore);

  if (SIM_CONFIG.ENTRY_STYLE === 'immediate') {
    await simulateBuy(item.mint, item.tokenName, item.tokenSymbol, compositeScoreForRecord, lastPrice, 'post_evaluation');
  } else {
    addToWatchlist(item.mint, item.tokenName, item.tokenSymbol, compositeScoreForRecord);
  }
}

// =====================================================================
// LAYER 2: when should I buy it? (pullback mode only — immediate mode
// buys right after evaluation finishes, see finalizeEvaluation above)
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
    evaluatingCount: evaluationQueue.length,
    watchlistCount: watchlist.length,
    openPositionCount: openPositions.length,
    totalClosedTrades: totalClosed,
    wins, losses, winRatePct,
    totalRealizedPnlUSDC: Math.round(totalRealizedPnlUSDC * 100) / 100,
  };
}

// ---- Background loops ----

// Layer 1: repeated evaluation
setInterval(async () => {
  for (const item of [...evaluationQueue]) {
    await takeSnapshot(item);
    const elapsedSeconds = (Date.now() - new Date(item.addedTime).getTime()) / 1000;
    if (elapsedSeconds >= SIM_CONFIG.EVALUATION_WINDOW_SECONDS) {
      await finalizeEvaluation(item);
      removeFromEvaluationQueue(item.mint);
    }
  }
  saveState();
}, SIM_CONFIG.EVALUATION_SAMPLE_INTERVAL_SECONDS * 1000);

// Layer 2: pullback-mode entry timing (no-op in 'immediate' mode, watchlist stays empty)
setInterval(async () => {
  for (const item of [...watchlist]) {
    await evaluateEntryTiming(item);
  }
}, SIM_CONFIG.ENTRY_SAMPLE_INTERVAL_SECONDS * 1000);

// Layer 4: position monitoring
setInterval(async () => {
  for (const position of [...openPositions]) {
    await evaluatePosition(position);
  }
}, SIM_CONFIG.CHECK_INTERVAL_SECONDS * 1000);

// ---- Webhook endpoint: new launch detected -> enters layer 1 evaluation ----
app.post('/new-launch', async (req, res) => {
  try {
    const event = Array.isArray(req.body) ? req.body[0] : req.body;
    const mint = extractLaunchMint(event);
    const deployerWallet = event?.feePayer;

    if (!mint || !deployerWallet) {
      return res.status(400).json({ error: 'missing mint or deployer in payload' });
    }

    const { name: tokenName, symbol: tokenSymbol } = await getTokenMetadata(mint);
    addToEvaluationQueue(mint, deployerWallet, tokenName, tokenSymbol);
    res.status(200).json({ ok: true, status: 'queued for evaluation' });
  } catch (err) {
    console.error('Error handling new launch:', err);
    res.status(500).json({ error: 'internal error' });
  }
});

// ---- Simple status page at the root, so the bare URL shows something ----
app.get('/', (req, res) => {
  const s = computeSummary();
  const recentTrades = closedTrades.slice(-10).reverse();

  const tradeRows = recentTrades.length
    ? recentTrades.map(t => `
        <tr>
          <td>${t.tokenSymbol}</td>
          <td>${t.exitReason}</td>
          <td style="color:${t.pnlPct >= 0 ? '#2ecc71' : '#e74c3c'}">${t.pnlPct}%</td>
          <td style="color:${t.pnlUSDC >= 0 ? '#2ecc71' : '#e74c3c'}">$${t.pnlUSDC}</td>
        </tr>`).join('')
    : '<tr><td colspan="4">No closed trades yet</td></tr>';

  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <meta http-equiv="refresh" content="30">
      <title>MemeRadar status</title>
      <style>
        body { font-family: system-ui, sans-serif; background: #0d1117; color: #c9d1d9; padding: 2rem; max-width: 700px; margin: auto; }
        h1 { font-size: 1.3rem; }
        .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.75rem; margin: 1.5rem 0; }
        .stat { background: #161b22; border-radius: 8px; padding: 0.75rem 1rem; }
        .stat .label { font-size: 0.75rem; color: #8b949e; }
        .stat .value { font-size: 1.2rem; font-weight: 600; }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th, td { text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid #21262d; font-size: 0.9rem; }
        a { color: #58a6ff; }
      </style>
    </head>
    <body>
      <h1>MemeRadar scorer + simulator</h1>
      <div class="stats">
        <div class="stat"><div class="label">Evaluating</div><div class="value">${s.evaluatingCount}</div></div>
        <div class="stat"><div class="label">Watchlist</div><div class="value">${s.watchlistCount}</div></div>
        <div class="stat"><div class="label">Open positions</div><div class="value">${s.openPositionCount}</div></div>
        <div class="stat"><div class="label">Closed trades</div><div class="value">${s.totalClosedTrades}</div></div>
        <div class="stat"><div class="label">Win rate</div><div class="value">${s.winRatePct !== null ? s.winRatePct + '%' : '—'}</div></div>
        <div class="stat"><div class="label">Total P&L</div><div class="value" style="color:${s.totalRealizedPnlUSDC >= 0 ? '#2ecc71' : '#e74c3c'}">$${s.totalRealizedPnlUSDC}</div></div>
      </div>
      <h2 style="font-size:1rem;">Recent closed trades</h2>
      <table>
        <tr><th>Coin</th><th>Exit reason</th><th>P&L %</th><th>P&L $</th></tr>
        ${tradeRows}
      </table>
      <p style="margin-top:1.5rem; font-size:0.85rem; color:#8b949e;">
        Auto-refreshes every 30s. Full data: <a href="/portfolio">/portfolio</a>
      </p>
    </body>
    </html>
  `);
});

// ---- View the full pipeline ----
app.get('/portfolio', (req, res) => {
  res.json({ summary: computeSummary(), evaluationQueue, watchlist, openPositions, closedTrades });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`MemeRadar scorer + simulator listening on port ${PORT}`));

/* ----------------------------------------------------------------------
README

1. API keys: only Birdeye (birdeye.so) is called at runtime. RugCheck,
   Jupiter, and DexScreener need no key. Helius is only used to register
   the webhook from your Helius dashboard.

2. Deploy with a stable public URL and a long-running process.

3. Register the Helius webhook as before (see prior setup notes) pointed
   at /new-launch.

4. What changed: a new launch no longer gets scored once and acted on
   immediately. It's added to an evaluation queue and sampled roughly
   every EVALUATION_SAMPLE_INTERVAL_SECONDS (12s) for
   EVALUATION_WINDOW_SECONDS (60s) — about 5 samples. After the window:
     - needs at least MIN_VALID_SAMPLES usable samples to decide at all
     - the AVERAGE composite score across samples must clear BUY_THRESHOLD
     - price is checked start-to-end of the window — if it already fell
       more than PRICE_DROP_REJECT_PCT, it's rejected as a likely dump,
       something a single snapshot could never catch
   Only then does it proceed to layer 2/3 (buy immediately, or go on the
   pullback watchlist if ENTRY_STYLE is 'pullback').

5. This trades speed for information on purpose. A coin is roughly a
   minute old by the time a decision is made, instead of a few seconds.
   That's the real cost of sampling a trend instead of a single instant.

6. GET /portfolio now also shows evaluationQueue — coins currently being
   sampled, with every snapshot so far, alongside the watchlist, open
   positions, and closed trades.

7. Still stubbed: getNarrativeScore, getSmartMoneyScore.
---------------------------------------------------------------------- */
