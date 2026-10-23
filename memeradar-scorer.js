
'use strict';

/**
 * MemeRadar Scorer
 * ----------------
 * Features:
 * 1. Repeated coin evaluation during a launch window.
 * 2. Composite scoring and optional adaptive logistic learning.
 * 3. Independent outcome tracking for EVERY detected coin,
 *    including rejected coins, at 1, 5, 15, and 60 minutes.
 * 4. Paper trading with take-profit, stop-loss, and max hold.
 * 5. Persistent local state and JSON API endpoints.
 *
 * Requirements: Node.js 18+
 * Install: npm install express dotenv
 * Run: node memeradar-scorer.js
 *
 * Paper trading only. No real trades or wallet signing.
 */

require('dotenv').config();

const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json({ limit: '1mb' }));

// ------------------------------------------------------------
// CONFIGURATION
// ------------------------------------------------------------

const CONFIG = {
  PORT: Number(process.env.PORT || 3000),
  STATE_FILE: path.resolve(
    process.env.STATE_FILE || './memeradar-state.json'
  ),

  // Evaluation
  EVALUATION_WINDOW_SECONDS: 60,
  EVALUATION_INTERVAL_SECONDS: 10,
  MIN_VALID_SAMPLES: 3,
  MIN_SCORE: 65,
  MAX_PRICE_DROP_PCT: 15,

  // Paper trading
  PAPER_TRADE_AMOUNT_USD: 10,
  MAX_OPEN_POSITIONS: 5,
  TAKE_PROFIT_PCT: 25,
  STOP_LOSS_PCT: 12,
  MAX_HOLD_MINUTES: 60,
  POSITION_CHECK_SECONDS: 15,

  // Outcome tracking, independent of trading decisions
  OUTCOME_HORIZONS_MINUTES: [1, 5, 15, 60],
  OUTCOME_SAMPLE_INTERVAL_SECONDS: 15,
  MAX_OUTCOME_RECORDS: 50000,

  // Learning
  LEARNING_ENABLED: true,
  LEARNING_MIN_SAMPLES: 50,
  LEARNING_RATE: 0.03,
  LEARNING_L2: 0.001,
  LEARNING_BLEND: 0.35,

  // External data
  BIRDEYE_API_KEY: process.env.BIRDEYE_API_KEY || '',
  RUGCHECK_API_KEY: process.env.RUGCHECK_API_KEY || '',
  WEBHOOK_SECRET: process.env.WEBHOOK_SECRET || '',
  PRICE_API_TIMEOUT_MS: 8000,
};

// Features are scored from 0 to 100.
const FEATURE_NAMES = [
  'holderSafety',
  'deployerHistory',
  'liquiditySafety',
  'devHoldings',
  'dexPaid',
  'narrative',
  'smartMoney',
];

const BASE_WEIGHTS = {
  holderSafety: 0.20,
  deployerHistory: 0.15,
  liquiditySafety: 0.25,
  devHoldings: 0.15,
  dexPaid: 0.05,
  narrative: 0.10,
  smartMoney: 0.10,
};

// ------------------------------------------------------------
// PERSISTENT STATE
// ------------------------------------------------------------

let evaluationQueue = [];
let watchlist = [];
let openPositions = [];
let closedTrades = [];
let coinOutcomes = [];
let seenMints = new Set();
let outcomeTrackerBusy = false;
let evaluationBusy = false;
let positionMonitorBusy = false;

function createLearningModel() {
  return {
    version: 1,
    samples: 0,
    weights: Object.fromEntries(
      FEATURE_NAMES.map(name => [name, 0])
    ),
    bias: 0,
    wins: 0,
    losses: 0,
    lastUpdated: null,
  };
}

let learningModel = createLearningModel();

function finite(value) {
  return Number.isFinite(Number(value));
}

function clamp(value, min = 0, max = 100) {
  return Math.max(min, Math.min(max, Number(value) || 0));
}

function round(value, digits = 2) {
  if (!finite(value)) return null;
  return Number(Number(value).toFixed(digits));
}

function average(values) {
  const valid = values.filter(finite);
  return valid.length
    ? valid.reduce((sum, value) => sum + Number(value), 0) / valid.length
    : null;
}

function saveState() {
  const state = {
    savedAt: new Date().toISOString(),
    evaluationQueue,
    watchlist,
    openPositions,
    closedTrades,
    coinOutcomes,
    seenMints: [...seenMints],
    learningModel,
  };

  const tempFile = `${CONFIG.STATE_FILE}.tmp`;

  try {
    fs.writeFileSync(tempFile, JSON.stringify(state, null, 2));
    fs.renameSync(tempFile, CONFIG.STATE_FILE);
  } catch (error) {
    console.error('Failed to save state:', error.message);
    try {
      if (fs.existsSync(tempFile)) fs.unlinkSync(tempFile);
    } catch (_) {}
  }
}

function loadState() {
  if (!fs.existsSync(CONFIG.STATE_FILE)) return;

  try {
    const state = JSON.parse(
      fs.readFileSync(CONFIG.STATE_FILE, 'utf8')
    );

    evaluationQueue = Array.isArray(state.evaluationQueue)
      ? state.evaluationQueue : [];

    watchlist = Array.isArray(state.watchlist)
      ? state.watchlist : [];

    openPositions = Array.isArray(state.openPositions)
      ? state.openPositions : [];

    closedTrades = Array.isArray(state.closedTrades)
      ? state.closedTrades : [];

    coinOutcomes = Array.isArray(state.coinOutcomes)
      ? state.coinOutcomes : [];

    seenMints = new Set(
      Array.isArray(state.seenMints) ? state.seenMints : []
    );

    if (state.learningModel) {
      learningModel = {
        ...createLearningModel(),
        ...state.learningModel,
      };
    }

    console.log(
      `Loaded state: ${coinOutcomes.length} outcomes, ` +
      `${closedTrades.length} closed trades, ` +
      `${openPositions.length} open positions.`
    );
  } catch (error) {
    console.error('Could not load saved state:', error.message);
  }
}

loadState();

// ------------------------------------------------------------
// HTTP HELPERS
// ------------------------------------------------------------

async function fetchJson(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    CONFIG.PRICE_API_TIMEOUT_MS
  );

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        accept: 'application/json',
        ...(options.headers || {}),
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

function extractLaunchMint(event) {
  const candidates = [
    event?.mint,
    event?.token?.mint,
    event?.tokenMint,
    event?.newToken?.mint,
    event?.data?.mint,
    event?.account,
  ];

  return candidates.find(
    value => typeof value === 'string' &&
      /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(value)
  ) || null;
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, character => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  })[character]);
}

// ------------------------------------------------------------
// PRICE LOOKUP
// ------------------------------------------------------------

async function getApproxPrice(mint) {
  // Primary: Birdeye, if an API key is configured.
  if (CONFIG.BIRDEYE_API_KEY) {
    try {
      const data = await fetchJson(
        `https://public-api.birdeye.so/defi/price?address=${encodeURIComponent(mint)}`,
        {
          headers: {
            'X-API-KEY': CONFIG.BIRDEYE_API_KEY,
            'x-chain': 'solana',
          },
        }
      );

      const price = Number(data?.data?.value);
      if (price > 0 && Number.isFinite(price)) return price;
    } catch (error) {
      console.warn('Birdeye price lookup failed:', error.message);
    }
  }

  // Fallback: DexScreener token pairs.
  try {
    const data = await fetchJson(
      `https://api.dexscreener.com/latest/dex/tokens/${encodeURIComponent(mint)}`
    );

    const pairs = (data?.pairs || [])
      .filter(pair =>
        pair.chainId === 'solana' &&
        Number(pair.priceUsd) > 0
      )
      .sort((a, b) =>
        Number(b.liquidity?.usd || 0) -
        Number(a.liquidity?.usd || 0)
      );

    if (pairs.length) return Number(pairs[0].priceUsd);
  } catch (error) {
    console.warn('DexScreener price lookup failed:', error.message);
  }

  return null;
}

// ------------------------------------------------------------
// TOKEN AND MARKET DATA
// ------------------------------------------------------------

async function getTokenMetadata(mint) {
  try {
    const data = await fetchJson(
      `https://api.dexscreener.com/latest/dex/tokens/${encodeURIComponent(mint)}`
    );

    const pairs = (data?.pairs || [])
      .filter(pair => pair.chainId === 'solana')
      .sort((a, b) =>
        Number(b.liquidity?.usd || 0) -
        Number(a.liquidity?.usd || 0)
      );

    const pair = pairs[0];

    if (!pair) {
      return {
        name: mint.slice(0, 8),
        symbol: mint.slice(0, 6),
        liquidityUsd: 0,
        priceUsd: null,
        marketCap: null,
        pairAddress: null,
        dexPaid: false,
      };
    }

    return {
      name: pair.baseToken?.name || mint.slice(0, 8),
      symbol: pair.baseToken?.symbol || mint.slice(0, 6),
      liquidityUsd: Number(pair.liquidity?.usd || 0),
      priceUsd: Number(pair.priceUsd) || null,
      marketCap: Number(pair.marketCap || pair.fdv) || null,
      pairAddress: pair.pairAddress || null,
      dexPaid: Boolean(pair.boosts?.active || pair.profile),
      volume24h: Number(pair.volume?.h24 || 0),
      priceChange5m: Number(pair.priceChange?.m5 || 0),
      priceChange1h: Number(pair.priceChange?.h1 || 0),
      pairCreatedAt: pair.pairCreatedAt || null,
    };
  } catch (error) {
    console.warn('Token metadata lookup failed:', error.message);
    return {
      name: mint.slice(0, 8),
      symbol: mint.slice(0, 6),
      liquidityUsd: 0,
      priceUsd: null,
      marketCap: null,
      pairAddress: null,
      dexPaid: false,
    };
  }
}

async function getHolderAndDevData(mint, deployer) {
  const result = {
    holderSafety: 50,
    devHoldings: 50,
    holderCount: null,
    topHolderPct: null,
    devHoldingPct: null,
    dataAvailable: false,
  };

  if (!CONFIG.BIRDEYE_API_KEY) return result;

  try {
    const data = await fetchJson(
      `https://public-api.birdeye.so/defi/v3/token/holder?address=${encodeURIComponent(mint)}&offset=0&limit=20`,
      {
        headers: {
          'X-API-KEY': CONFIG.BIRDEYE_API_KEY,
          'x-chain': 'solana',
        },
      }
    );

    const holders = data?.data?.items || data?.data?.holders || [];
    if (!holders.length) return result;

    const total = holders.reduce(
      (sum, holder) => sum + Number(holder.ui_amount || holder.amount || 0),
      0
    );

    const topHolder = total > 0
      ? Number(holders[0].ui_amount || holders[0].amount || 0) / total * 100
      : null;

    const dev = holders.find(holder =>
      holder.owner === deployer ||
      holder.address === deployer
    );

    const devAmount = dev
      ? Number(dev.ui_amount || dev.amount || 0)
      : 0;

    result.holderCount = holders.length;
    result.topHolderPct = finite(topHolder) ? topHolder : null;
    result.devHoldingPct = total > 0 ? devAmount / total * 100 : null;
    result.holderSafety = topHolder === null
      ? 50
      : clamp(100 - topHolder * 2.5);

    result.devHoldings = result.devHoldingPct === null
      ? 50
      : clamp(100 - result.devHoldingPct * 3);

    result.dataAvailable = true;
  } catch (error) {
    console.warn('Holder lookup failed:', error.message);
  }

  return result;
}

async function getRugCheckReport(mint) {
  try {
    const headers = {};
    if (CONFIG.RUGCHECK_API_KEY) {
      headers['Authorization'] = `Bearer ${CONFIG.RUGCHECK_API_KEY}`;
    }

    return await fetchJson(
      `https://api.rugcheck.xyz/v1/tokens/${encodeURIComponent(mint)}/report`,
      { headers }
    );
  } catch (_) {
    return null;
  }
}

function deriveLiquiditySafety(metadata, rugcheck) {
  const liquidity = Number(metadata.liquidityUsd || 0);

  let score = clamp(liquidity / 100 * 10);

  if (liquidity >= 10000) score = Math.max(score, 70);
  if (liquidity >= 25000) score = Math.max(score, 85);
  if (liquidity >= 50000) score = Math.max(score, 95);

  if (rugcheck) {
    const risks = Array.isArray(rugcheck.risks)
      ? rugcheck.risks : [];

    const severe = risks.filter(risk =>
      /high|danger|critical/i.test(String(risk.level || risk.name || ''))
    ).length;

    score = clamp(score - severe * 20);
  }

  return score;
}

function deriveDeployerHistory(deployer) {
  if (!deployer) return 50;

  const past = closedTrades.filter(
    trade => trade.deployerWallet === deployer
  );

  if (!past.length) return 50;

  const wins = past.filter(trade => trade.pnlPct > 0).length;
  return clamp(50 + ((wins / past.length) - 0.5) * 100);
}

function deriveNarrativeScore(metadata) {
  const text = `${metadata.name || ''} ${metadata.symbol || ''}`.toLowerCase();

  const keywords = [
    'ai', 'agent', 'dog', 'cat', 'frog', 'pepe',
    'trump', 'meme', 'community', 'sol',
  ];

  const hits = keywords.filter(word => text.includes(word)).length;

  // This is a simple heuristic, not semantic AI analysis.
  return clamp(40 + hits * 12);
}

function deriveSmartMoneyScore(metadata) {
  // Placeholder until reliable wallet-flow data is connected.
  // Avoid pretending that actual smart-money wallets were identified.
  if (metadata.volume24h > 100000 && metadata.liquidityUsd > 20000) {
    return 65;
  }

  return 50;
}

// ------------------------------------------------------------
// SCORING AND LEARNING
// ------------------------------------------------------------

function getFeatureVector(components) {
  return Object.fromEntries(
    FEATURE_NAMES.map(name => [
      name,
      clamp(components[name] ?? 50) / 100,
    ])
  );
}

function sigmoid(value) {
  const safe = Math.max(-30, Math.min(30, value));
  return 1 / (1 + Math.exp(-safe));
}

function predictLearningProbability(featureVector) {
  let z = learningModel.bias;

  for (const name of FEATURE_NAMES) {
    z += Number(learningModel.weights[name] || 0) *
      Number(featureVector[name] || 0);
  }

  return sigmoid(z);
}

function getLearnedScore(baseScore, featureVector) {
  if (
    !CONFIG.LEARNING_ENABLED ||
    learningModel.samples < CONFIG.LEARNING_MIN_SAMPLES
  ) {
    return baseScore;
  }

  const modelScore = predictLearningProbability(featureVector) * 100;
  const blend = CONFIG.LEARNING_BLEND;

  return clamp(baseScore * (1 - blend) + modelScore * blend);
}

function trainLearningModel(featureVector, won) {
  if (!CONFIG.LEARNING_ENABLED || !featureVector) return;

  const y = won ? 1 : 0;
  const prediction = predictLearningProbability(featureVector);
  const error = y - prediction;

  for (const name of FEATURE_NAMES) {
    const x = Number(featureVector[name] || 0);
    const weight = Number(learningModel.weights[name] || 0);

    learningModel.weights[name] = weight +
      CONFIG.LEARNING_RATE * (
        error * x - CONFIG.LEARNING_L2 * weight
      );
  }

  learningModel.bias += CONFIG.LEARNING_RATE * error;
  learningModel.samples += 1;

  if (won) learningModel.wins += 1;
  else learningModel.losses += 1;

  learningModel.lastUpdated = new Date().toISOString();
}

function computeCompositeScore(components) {
  let weightedSum = 0;
  let totalWeight = 0;

  for (const name of FEATURE_NAMES) {
    const value = components[name];

    if (!finite(value)) continue;

    const weight = BASE_WEIGHTS[name];
    weightedSum += clamp(value) * weight;
    totalWeight += weight;
  }

  const baseScore = totalWeight > 0
    ? weightedSum / totalWeight
    : 0;

  const featureVector = getFeatureVector(components);

  return {
    baseScore: round(baseScore),
    score: round(getLearnedScore(baseScore, featureVector)),
    featureVector,
  };
}

async function collectCoinSnapshot(item) {
  const [metadata, holders, rugcheck, price] = await Promise.all([
    getTokenMetadata(item.mint),
    getHolderAndDevData(item.mint, item.deployerWallet),
    getRugCheckReport(item.mint),
    getApproxPrice(item.mint),
  ]);

  const components = {
    holderSafety: holders.holderSafety,
    deployerHistory: deriveDeployerHistory(item.deployerWallet),
    liquiditySafety: deriveLiquiditySafety(metadata, rugcheck),
    devHoldings: holders.devHoldings,
    dexPaid: metadata.dexPaid ? 80 : 40,
    narrative: deriveNarrativeScore(metadata),
    smartMoney: deriveSmartMoneyScore(metadata),
  };

  const scoring = computeCompositeScore(components);

  return {
    time: new Date().toISOString(),
    price,
    metadata,
    holders,
    components,
    baseScore: scoring.baseScore,
    score: scoring.score,
    featureVector: scoring.featureVector,
  };
}

// ------------------------------------------------------------
// OUTCOME TRACKER
// ------------------------------------------------------------

function createOutcomeRecord(item) {
  const existing = coinOutcomes.find(record => record.mint === item.mint);
  if (existing) return existing;

  const scannedAt = new Date(item.addedTime).getTime();

  const checkpoints = Object.fromEntries(
    CONFIG.OUTCOME_HORIZONS_MINUTES.map(minutes => [
      minutes,
      {
        targetTime: new Date(
          scannedAt + minutes * 60000
        ).toISOString(),
        recorded: false,
        price: null,
        changePct: null,
        status: 'pending',
      },
    ])
  );

  const record = {
    mint: item.mint,
    tokenName: item.tokenName || item.mint.slice(0, 8),
    tokenSymbol: item.tokenSymbol || item.mint.slice(0, 6),
    deployerWallet: item.deployerWallet || null,
    scannedAt: item.addedTime,

    baselinePrice: null,
    baselinePriceTime: null,

    initialScore: null,
    finalScore: null,
    decision: 'evaluating',
    rejectionReason: null,

    paperTraded: false,
    paperTradeEntryPrice: null,

    checkpoints,
    priceHistory: [],
    completed: false,
    lastPriceError: null,
  };

  coinOutcomes.push(record);

  while (coinOutcomes.length > CONFIG.MAX_OUTCOME_RECORDS) {
    const removableIndex = coinOutcomes.findIndex(o => o.completed);
    if (removableIndex === -1) break;
    coinOutcomes.splice(removableIndex, 1);
  }

  saveState();
  return record;
}

function updateOutcomeDecision(mint, {
  decision,
  score = null,
  reason = null,
  paperTraded = false,
  entryPrice = null,
} = {}) {
  const record = coinOutcomes.find(o => o.mint === mint);
  if (!record) return;

  record.decision = decision;

  if (finite(score)) {
    record.finalScore = Number(score);
    if (record.initialScore === null) {
      record.initialScore = Number(score);
    }
  }

  if (reason) record.rejectionReason = reason;

  if (paperTraded) {
    record.paperTraded = true;
    record.paperTradeEntryPrice = entryPrice;
  }

  saveState();
}

async function sampleCoinOutcome(record) {
  if (record.completed) return;

  const price = await getApproxPrice(record.mint);
  const now = Date.now();

  if (!finite(price) || price <= 0) {
    record.lastPriceError = new Date(now).toISOString();
    return;
  }

  if (record.baselinePrice === null) {
    record.baselinePrice = price;
    record.baselinePriceTime = new Date(now).toISOString();
  }

  record.priceHistory.push({
    time: new Date(now).toISOString(),
    price,
  });

  if (record.priceHistory.length > 250) {
    record.priceHistory.shift();
  }

  const scannedAt = new Date(record.scannedAt).getTime();

  for (const minutes of CONFIG.OUTCOME_HORIZONS_MINUTES) {
    const checkpoint = record.checkpoints[minutes];

    if (checkpoint.recorded) continue;

    const targetTime = scannedAt + minutes * 60000;
    if (now < targetTime) continue;

    const changePct =
      ((price - record.baselinePrice) / record.baselinePrice) * 100;

    checkpoint.recorded = true;
    checkpoint.price = price;
    checkpoint.observedAt = new Date(now).toISOString();
    checkpoint.actualElapsedMinutes = round((now - scannedAt) / 60000, 2);
    checkpoint.changePct = round(changePct);
    checkpoint.status = changePct > 0
      ? 'up'
      : changePct < 0
        ? 'down'
        : 'unchanged';
  }

  record.completed = CONFIG.OUTCOME_HORIZONS_MINUTES.every(
    minutes => record.checkpoints[minutes].recorded
  );

  saveState();
}

async function runOutcomeTracker() {
  if (outcomeTrackerBusy) return;
  outcomeTrackerBusy = true;

  try {
    for (const record of [...coinOutcomes]) {
      if (record.completed) continue;

      try {
        await sampleCoinOutcome(record);
      } catch (error) {
        console.error(
          `Outcome tracker error for ${record.tokenSymbol}:`,
          error.message
        );
      }
    }
  } finally {
    outcomeTrackerBusy = false;
  }
}

// ------------------------------------------------------------
// EVALUATION QUEUE
// ------------------------------------------------------------

function addToEvaluationQueue(mint, deployerWallet) {
  if (seenMints.has(mint)) {
    return { accepted: false, reason: 'duplicate_mint' };
  }

  seenMints.add(mint);

  const now = new Date().toISOString();

  const item = {
    mint,
    deployerWallet: deployerWallet || null,
    addedTime: now,
    tokenName: mint.slice(0, 8),
    tokenSymbol: mint.slice(0, 6),
    snapshots: [],
    status: 'evaluating',
    finalScore: null,
    rejectionReason: null,
  };

  evaluationQueue.push(item);
  createOutcomeRecord(item);
  saveState();

  return { accepted: true, item };
}

async function evaluateItem(item) {
  const elapsedSeconds =
    (Date.now() - new Date(item.addedTime).getTime()) / 1000;

  if (elapsedSeconds < 0) return;

  if (
    elapsedSeconds < CONFIG.EVALUATION_WINDOW_SECONDS &&
    item.snapshots.length === 0 ||
    elapsedSeconds < CONFIG.EVALUATION_WINDOW_SECONDS &&
    elapsedSeconds - (item.lastSampleElapsed || 0) >=
      CONFIG.EVALUATION_INTERVAL_SECONDS
  ) {
    const snapshot = await collectCoinSnapshot(item);

    item.snapshots.push(snapshot);
    item.lastSampleElapsed = elapsedSeconds;

    item.tokenName = snapshot.metadata.name || item.tokenName;
    item.tokenSymbol = snapshot.metadata.symbol || item.tokenSymbol;

    const outcome = coinOutcomes.find(o => o.mint === item.mint);
    if (outcome) {
      outcome.tokenName = item.tokenName;
      outcome.tokenSymbol = item.tokenSymbol;
      saveState();
    }

    return;
  }

  if (elapsedSeconds >= CONFIG.EVALUATION_WINDOW_SECONDS) {
    await finalizeEvaluation(item);
  }
}

async function finalizeEvaluation(item) {
  if (item.status !== 'evaluating') return;

  const validSnapshots = item.snapshots.filter(snapshot =>
    finite(snapshot.score)
  );

  if (validSnapshots.length < CONFIG.MIN_VALID_SAMPLES) {
    item.status = 'rejected';
    item.rejectionReason = 'insufficient_valid_samples';

    updateOutcomeDecision(item.mint, {
      decision: 'rejected',
      reason: item.rejectionReason,
    });

    saveState();
    return;
  }

  const averageComponents = {};

  for (const name of FEATURE_NAMES) {
    averageComponents[name] = average(
      validSnapshots.map(snapshot => snapshot.components[name])
    ) ?? 50;
  }

  const scoring = computeCompositeScore(averageComponents);

  const prices = validSnapshots
    .map(snapshot => snapshot.price)
    .filter(price => finite(price) && price > 0);

  const firstPrice = prices[0] ?? null;
  const lastPrice = prices[prices.length - 1] ?? null;

  const priceChangePct =
    firstPrice && lastPrice
      ? ((lastPrice - firstPrice) / firstPrice) * 100
      : null;

  item.finalScore = scoring.score;
  item.finalFeatureVector = scoring.featureVector;
  item.averageComponents = averageComponents;
  item.priceChangePct = round(priceChangePct);

  if (
    finite(priceChangePct) &&
    priceChangePct <= -CONFIG.MAX_PRICE_DROP_PCT
  ) {
    item.status = 'rejected';
    item.rejectionReason = 'price_drop_limit_exceeded';

    updateOutcomeDecision(item.mint, {
      decision: 'rejected',
      score: scoring.score,
      reason: item.rejectionReason,
    });

    saveState();
    return;
  }

  if (scoring.score < CONFIG.MIN_SCORE) {
    item.status = 'rejected';
    item.rejectionReason = 'score_below_threshold';

    updateOutcomeDecision(item.mint, {
      decision: 'rejected',
      score: scoring.score,
      reason: item.rejectionReason,
    });

    saveState();
    return;
  }

  item.status = 'accepted';

  updateOutcomeDecision(item.mint, {
    decision: 'accepted',
    score: scoring.score,
  });

  const entryPrice = await getApproxPrice(item.mint);

  if (finite(entryPrice) && entryPrice > 0) {
    const tradeResult = simulateBuy(item, entryPrice);

    if (tradeResult.opened) {
      item.status = 'paper_traded';

      updateOutcomeDecision(item.mint, {
        decision: 'paper_traded',
        score: scoring.score,
        paperTraded: true,
        entryPrice,
      });
    }
  } else {
    // The coin passed scoring, but no reliable entry price was found.
    item.status = 'accepted_no_entry_price';
  }

  saveState();
}

// ------------------------------------------------------------
// PAPER TRADING
// ------------------------------------------------------------

function simulateBuy(item, price) {
  if (openPositions.length >= CONFIG.MAX_OPEN_POSITIONS) {
    return { opened: false, reason: 'max_open_positions' };
  }

  if (!finite(price) || price <= 0) {
    return { opened: false, reason: 'invalid_entry_price' };
  }

  const existing = openPositions.find(position =>
    position.mint === item.mint
  );

  if (existing) return { opened: false, reason: 'already_open' };

  const position = {
    mint: item.mint,
    tokenName: item.tokenName,
    tokenSymbol: item.tokenSymbol,
    deployerWallet: item.deployerWallet,
    entryPrice: price,
    currentPrice: price,
    amountUsd: CONFIG.PAPER_TRADE_AMOUNT_USD,
    quantity: CONFIG.PAPER_TRADE_AMOUNT_USD / price,
    openedAt: new Date().toISOString(),
    compositeScore: item.finalScore,
    featureVector: item.finalFeatureVector,
    status: 'open',
    lastPrice: price,
    pnlPct: 0,
    unrealizedPnlUsd: 0,
  };

  openPositions.push(position);

  updateOutcomeDecision(item.mint, {
    decision: 'paper_traded',
    score: item.finalScore,
    paperTraded: true,
    entryPrice: price,
  });

  saveState();

  console.log(
    `PAPER BUY ${position.tokenSymbol} at $${price} ` +
    `(score ${position.compositeScore})`
  );

  return { opened: true, position };
}

async function evaluatePosition(position) {
  const price = await getApproxPrice(position.mint);

  if (!finite(price) || price <= 0) return;

  position.currentPrice = price;
  position.lastPrice = price;
  position.pnlPct =
    ((price - position.entryPrice) / position.entryPrice) * 100;

  position.unrealizedPnlUsd =
    position.amountUsd * (position.pnlPct / 100);

  const heldMinutes =
    (Date.now() - new Date(position.openedAt).getTime()) / 60000;

  let closeReason = null;

  if (position.pnlPct >= CONFIG.TAKE_PROFIT_PCT) {
    closeReason = 'take_profit';
  } else if (position.pnlPct <= -CONFIG.STOP_LOSS_PCT) {
    closeReason = 'stop_loss';
  } else if (heldMinutes >= CONFIG.MAX_HOLD_MINUTES) {
    closeReason = 'max_hold';
  }

  if (closeReason) {
    closePosition(position, price, closeReason);
  }
}

function closePosition(position, exitPrice, reason) {
  const index = openPositions.findIndex(
    item => item.mint === position.mint
  );

  if (index === -1) return;

  const pnlPct =
    ((exitPrice - position.entryPrice) / position.entryPrice) * 100;

  const pnlUsd = position.amountUsd * pnlPct / 100;

  const trade = {
    mint: position.mint,
    tokenName: position.tokenName,
    tokenSymbol: position.tokenSymbol,
    deployerWallet: position.deployerWallet,
    entryPrice: position.entryPrice,
    exitPrice,
    amountUsd: position.amountUsd,
    pnlPct: round(pnlPct),
    pnlUsd: round(pnlUsd),
    reason,
    openedAt: position.openedAt,
    closedAt: new Date().toISOString(),
    compositeScore: position.compositeScore,
    featureVector: position.featureVector,
    won: pnlUsd > 0,
  };

  closedTrades.push(trade);
  openPositions.splice(index, 1);

  // Learn from completed paper trades only.
  trainLearningModel(trade.featureVector, trade.won);

  console.log(
    `PAPER SELL ${trade.tokenSymbol}: ${trade.pnlPct}% ` +
    `($${trade.pnlUsd}), reason=${reason}`
  );

  saveState();
}

// ------------------------------------------------------------
// BACKGROUND WORKERS
// ------------------------------------------------------------

async function runEvaluationWorker() {
  if (evaluationBusy) return;
  evaluationBusy = true;

  try {
    for (const item of [...evaluationQueue]) {
      if (item.status !== 'evaluating') continue;

      try {
        await evaluateItem(item);
      } catch (error) {
        console.error(
          `Evaluation error for ${item.tokenSymbol}:`,
          error.message
        );
      }
    }

    // Keep recent evaluations for inspection.
    if (evaluationQueue.length > 5000) {
      evaluationQueue = evaluationQueue.filter(item =>
        item.status === 'evaluating' ||
        Date.now() - new Date(item.addedTime).getTime() < 86400000
      );
    }
  } finally {
    evaluationBusy = false;
  }
}

async function runPositionMonitor() {
  if (positionMonitorBusy) return;
  positionMonitorBusy = true;

  try {
    for (const position of [...openPositions]) {
      try {
        await evaluatePosition(position);
      } catch (error) {
        console.error(
          `Position monitor error for ${position.tokenSymbol}:`,
          error.message
        );
      }
    }
  } finally {
    positionMonitorBusy = false;
  }
}

// ------------------------------------------------------------
// WEBHOOK
// ------------------------------------------------------------

app.post('/new-launch', async (req, res) => {
  try {
    if (CONFIG.WEBHOOK_SECRET) {
      const suppliedSecret =
        req.get('x-webhook-secret') ||
        req.query.secret;

      if (suppliedSecret !== CONFIG.WEBHOOK_SECRET) {
        return res.status(401).json({ error: 'Unauthorized' });
      }
    }

    const event = Array.isArray(req.body)
      ? req.body[0]
      : req.body;

    const mint = extractLaunchMint(event);
    const deployerWallet =
      event?.feePayer ||
      event?.deployer ||
      event?.creator ||
      event?.traderPublicKey ||
      null;

    if (!mint) {
      return res.status(400).json({
        error: 'Could not find a valid token mint in the webhook payload',
      });
    }

    const result = addToEvaluationQueue(mint, deployerWallet);

    if (!result.accepted) {
      return res.json({
        ok: true,
        duplicate: true,
        mint,
      });
    }

    console.log(`New coin detected: ${mint}`);

    return res.json({
      ok: true,
      mint,
      status: 'queued_for_evaluation',
      outcomeTracking: true,
    });
  } catch (error) {
    console.error('Webhook error:', error.message);
    return res.status(500).json({ error: 'Webhook processing failed' });
  }
});

// ------------------------------------------------------------
// API: HEALTH AND STATUS
// ------------------------------------------------------------

app.get('/api/status', (req, res) => {
  res.json({
    ok: true,
    mode: 'paper_trading',
    learningEnabled: CONFIG.LEARNING_ENABLED,
    learningSamples: learningModel.samples,
    evaluationQueue: evaluationQueue.filter(
      item => item.status === 'evaluating'
    ).length,
    acceptedCoins: evaluationQueue.filter(
      item => item.status === 'accepted' ||
              item.status === 'paper_traded' ||
              item.status === 'accepted_no_entry_price'
    ).length,
    rejectedCoins: evaluationQueue.filter(
      item => item.status === 'rejected'
    ).length,
    outcomeRecords: coinOutcomes.length,
    completedOutcomes: coinOutcomes.filter(
      record => record.completed
    ).length,
    openPositions: openPositions.length,
    closedTrades: closedTrades.length,
    timestamp: new Date().toISOString(),
  });
});

app.get('/api/learning', (req, res) => {
  res.json({
    enabled: CONFIG.LEARNING_ENABLED,
    minimumSamplesBeforeInfluence: CONFIG.LEARNING_MIN_SAMPLES,
    samples: learningModel.samples,
    wins: learningModel.wins,
    losses: learningModel.losses,
    weights: learningModel.weights,
    bias: learningModel.bias,
    lastUpdated: learningModel.lastUpdated,
    currentlyInfluencingScores:
      CONFIG.LEARNING_ENABLED &&
      learningModel.samples >= CONFIG.LEARNING_MIN_SAMPLES,
  });
});

app.get('/api/portfolio', (req, res) => {
  const realizedPnlUsd = closedTrades.reduce(
    (sum, trade) => sum + Number(trade.pnlUsd || 0), 0
  );

  const unrealizedPnlUsd = openPositions.reduce(
    (sum, position) => sum + Number(position.unrealizedPnlUsd || 0), 0
  );

  res.json({
    mode: 'paper_trading',
    openPositions,
    closedTrades: closedTrades.slice(-500).reverse(),
    summary: {
      openCount: openPositions.length,
      closedCount: closedTrades.length,
      realizedPnlUsd: round(realizedPnlUsd),
      unrealizedPnlUsd: round(unrealizedPnlUsd),
      totalPnlUsd: round(realizedPnlUsd + unrealizedPnlUsd),
      wins: closedTrades.filter(trade => trade.won).length,
      losses: closedTrades.filter(trade => !trade.won).length,
    },
  });
});

// Complete outcome dataset, including rejected coins.
app.get('/outcomes', (req, res) => {
  const summary = {
    totalScanned: coinOutcomes.length,
    completed: coinOutcomes.filter(record => record.completed).length,
    pending: coinOutcomes.filter(record => !record.completed).length,
    accepted: coinOutcomes.filter(record =>
      record.decision === 'accepted' ||
      record.decision === 'paper_traded'
    ).length,
    rejected: coinOutcomes.filter(
      record => record.decision === 'rejected'
    ).length,
    paperTraded: coinOutcomes.filter(
      record => record.paperTraded
    ).length,
  };

  res.json({
    summary,
    horizonsMinutes: CONFIG.OUTCOME_HORIZONS_MINUTES,
    outcomes: coinOutcomes,
  });
});

// CSV export for analysis and future training.
app.get('/outcomes.csv', (req, res) => {
  const columns = [
    'mint',
    'tokenName',
    'tokenSymbol',
    'scannedAt',
    'decision',
    'rejectionReason',
    'initialScore',
    'baselinePrice',
    'paperTraded',
    'change1m',
    'change5m',
    'change15m',
    'change60m',
  ];

  function csvCell(value) {
    const text = value === null || value === undefined
      ? ''
      : String(value);
    return `"${text.replace(/"/g, '""')}"`;
  }

  const rows = coinOutcomes.map(record => [
    record.mint,
    record.tokenName,
    record.tokenSymbol,
    record.scannedAt,
    record.decision,
    record.rejectionReason,
    record.initialScore,
    record.baselinePrice,
    record.paperTraded,
    record.checkpoints?.[1]?.changePct,
    record.checkpoints?.[5]?.changePct,
    record.checkpoints?.[15]?.changePct,
    record.checkpoints?.[60]?.changePct,
  ]);

  const csv = [
    columns.map(csvCell).join(','),
    ...rows.map(row => row.map(csvCell).join(',')),
  ].join('\n');

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader(
    'Content-Disposition',
    'attachment; filename="memeradar-outcomes.csv"'
  );
  res.send(csv);
});

// ------------------------------------------------------------
// SIMPLE DASHBOARD
// ------------------------------------------------------------

app.get('/', (req, res) => {
  const openPnl = openPositions.reduce(
    (sum, position) => sum + Number(position.unrealizedPnlUsd || 0), 0
  );

  const closedPnl = closedTrades.reduce(
    (sum, trade) => sum + Number(trade.pnlUsd || 0), 0
  );

  const rows = evaluationQueue.slice(-30).reverse().map(item => `
    <tr>
      <td>${escapeHtml(item.tokenSymbol)}</td>
      <td>${escapeHtml(item.mint)}</td>
      <td>${escapeHtml(item.status)}</td>
      <td>${finite(item.finalScore) ? escapeHtml(item.finalScore) : '—'}</td>
      <td>${escapeHtml(item.rejectionReason || '')}</td>
    </tr>
  `).join('');

  res.type('html').send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MemeRadar</title>
<style>
body{font-family:system-ui,sans-serif;max-width:1200px;margin:30px auto;padding:0 18px;background:#111;color:#eee}
h1{margin-bottom:5px}p{color:#aaa}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}
.card{background:#202020;padding:16px;border-radius:10px;overflow-wrap:anywhere}
.value{font-size:24px;font-weight:700}table{border-collapse:collapse;width:100%;margin-top:20px}
th,td{text-align:left;padding:10px;border-bottom:1px solid #333;overflow-wrap:anywhere}
a{color:#8ab4ff;margin-right:14px}
</style>
</head>
<body>
<h1>MemeRadar</h1>
<p>Scanner, adaptive scoring, outcomes and simulated portfolio.</p>
<div class="cards">
<div class="card">Scanned coins<div class="value">${coinOutcomes.length}</div></div>
<div class="card">Pending outcomes<div class="value">${coinOutcomes.filter(o => !o.completed).length}</div></div>
<div class="card">Open positions<div class="value">${openPositions.length}</div></div>
<div class="card">Closed paper trades<div class="value">${closedTrades.length}</div></div>
<div class="card">Realized paper P/L<div class="value">$${round(closedPnl) ?? 0}</div></div>
<div class="card">Unrealized paper P/L<div class="value">$${round(openPnl) ?? 0}</div></div>
<div class="card">Learning samples<div class="value">${learningModel.samples}</div></div>
</div>
<p>
<a href="/api/status">Status JSON</a>
<a href="/api/learning">Learning JSON</a>
<a href="/api/portfolio">Portfolio JSON</a>
<a href="/outcomes">All outcomes JSON</a>
<a href="/outcomes.csv">Download outcomes CSV</a>
</p>
<h2>Recent scanned coins</h2>
<table>
<thead><tr><th>Symbol</th><th>Mint</th><th>Status</th><th>Score</th><th>Reason</th></tr></thead>
<tbody>${rows}</tbody>
</table>
<p>Paper trading only. External data may be incomplete or delayed.</p>
</body>
</html>`);
});

// ------------------------------------------------------------
// START
// ------------------------------------------------------------

setInterval(
  runEvaluationWorker,
  CONFIG.EVALUATION_INTERVAL_SECONDS * 1000
);

setInterval(
  runOutcomeTracker,
  CONFIG.OUTCOME_SAMPLE_INTERVAL_SECONDS * 1000
);

setInterval(
  runPositionMonitor,
  CONFIG.POSITION_CHECK_SECONDS * 1000
);

const server = app.listen(CONFIG.PORT, () => {
  console.log(`MemeRadar running on port ${CONFIG.PORT}`);
  console.log(`Dashboard: http://localhost:${CONFIG.PORT}`);
  console.log(`Webhook:   http://localhost:${CONFIG.PORT}/new-launch`);
  console.log(`Outcomes:  http://localhost:${CONFIG.PORT}/outcomes`);
  console.log(`CSV:       http://localhost:${CONFIG.PORT}/outcomes.csv`);
  console.log('Mode: PAPER TRADING ONLY');
});

function shutdown() {
  console.log('Saving MemeRadar state...');
  saveState();

  server.close(() => {
    process.exit(0);
  });

  setTimeout(() => process.exit(1), 5000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
