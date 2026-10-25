/* SolSwing — authoritative game server (v2.1)
 *
 * Responsibilities (the client is NEVER trusted):
 *  1. Matchmaking: pairs queued players into best-of-3 matches.
 *  2. Rug timing: the rug moment is generated server-side (4–9s) and the
 *     client only learns about it when the server broadcasts { type: 'rug' }.
 *  3. Reaction race: the server timestamps each sell on RECEIPT. The first
 *     valid sell wins the round — resolved immediately, no waiting.
 *  4. Anti-cheat: disconnects / hidden-tab forfeits during a live round
 *     forfeit that round. Late sells (>5s after rug) are ignored.
 *     Per-socket rate limiting drops abusive clients.
 *  5. Settlement: the adjudicator keypair calls the SolSwing Anchor program
 *     (settle_match) so 94% goes to the winner and 6% to the arena treasury.
 *
 * Run:
 *   npm install
 *   node server.js
 *
 * Env (.env):
 *   ADJUDICATOR_KEYPAIR_PATH=~/.config/solana/id.json
 *   SOLANA_RPC=https://api.devnet.solana.com
 *   PROGRAM_ID=<your deployed SolSwing program id>
 *   TREASURY_PUBKEY=GyBLDTX9AVdyd3oJWBedcQ6wSgBUKjGHhYFGSDu9NgMS
 *   PORT=8080
 */

require('dotenv').config();
const fs = require('fs');
const os = require('os');
const path = require('path');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT || '8080', 10);
const SOLANA_RPC = process.env.SOLANA_RPC || 'https://api.devnet.solana.com';
const PROGRAM_ID = process.env.PROGRAM_ID || null;
const TREASURY_PUBKEY = process.env.TREASURY_PUBKEY || null;
const WAGER_SOL = parseFloat(process.env.DEFAULT_WAGER_SOL || '0.1');

const RUG_MIN_MS = 4000;
const RUG_MAX_MS = 9000;
const SELL_WINDOW_MS = 5000;   // sells after this are ignored
const TICK_MS = 150;
const MAX_WAGER_SOL = 100;
const MIN_WAGER_SOL = 0.01;

// Rate limiting: max messages per rolling second per socket.
const RATE_LIMIT_PER_SEC = 20;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function fmt(n) { return Number(n).toFixed(2); }

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function isValidPubkey(s) {
    return typeof s === 'string' && s.length >= 32 && s.length <= 44 &&
        [...s].every(c => B58.includes(c));
}

function shortAddr(addr) {
    return addr.slice(0, 4) + '…' + addr.slice(-4);
}

// ---------------------------------------------------------------------------
// Optional Solana wiring (only loaded when env is configured)
// ---------------------------------------------------------------------------
let solana = null;
const adjudicatorKeypairEnv = process.env.ADJUDICATOR_KEYPAIR;
const adjudicatorKeypairPath = process.env.ADJUDICATOR_KEYPAIR_PATH;
if (PROGRAM_ID && (adjudicatorKeypairEnv || adjudicatorKeypairPath)) {
    try {
        const { Connection, Keypair, PublicKey } = require('@solana/web3.js');
        const anchor = require('@coral-xyz/anchor');
        let secret;
        if (adjudicatorKeypairEnv) {
            // Railway-style: secret key JSON array in an env variable.
            secret = Uint8Array.from(JSON.parse(adjudicatorKeypairEnv));
        } else {
            const kpPath = adjudicatorKeypairPath.replace('~', os.homedir());
            secret = Uint8Array.from(JSON.parse(fs.readFileSync(kpPath, 'utf8')));
        }
        const adjudicator = Keypair.fromSecretKey(secret);
        const connection = new Connection(SOLANA_RPC, 'confirmed');
        const wallet = new anchor.Wallet(adjudicator);
        const provider = new anchor.AnchorProvider(connection, wallet, { commitment: 'confirmed' });
        const idlPath = path.join(__dirname, 'idl', 'solswing.json');
        let program = null;
        if (fs.existsSync(idlPath)) {
            const idl = JSON.parse(fs.readFileSync(idlPath, 'utf8'));
            program = new anchor.Program(idl, new PublicKey(PROGRAM_ID), provider);
        } else {
            console.warn('[solana] no IDL at ./idl/solswing.json — on-chain settlement disabled until `anchor build` copies it there.');
        }
        solana = { connection, adjudicator, program, PublicKey, anchor };
        console.log('[solana] wagering enabled. Adjudicator:', adjudicator.publicKey.toBase58());
    } catch (e) {
        console.warn('[solana] disabled —', e.message);
    }
} else {
    console.warn('[solana] disabled — set PROGRAM_ID and ADJUDICATOR_KEYPAIR (or ADJUDICATOR_KEYPAIR_PATH) to enable on-chain settlement.');
}

// ---------------------------------------------------------------------------
// Match state
// ---------------------------------------------------------------------------
let nextMatchId = 1;
const queue = [];              // waiting players
const matches = new Map();     // matchId -> match
const socketMatch = new Map(); // ws -> matchId

function createMatch(p1, p2) {
    const matchId = nextMatchId++;
    const wager = Math.max(p1.wager, p2.wager);
    const match = {
        id: matchId,
        players: [p1, p2],
        wager,
        pot: wager * 2,
        wins: [0, 0],
        round: null,
        state: 'starting',
    };
    matches.set(matchId, match);
    if (p1.ws) socketMatch.set(p1.ws, matchId);
    if (p2.ws) socketMatch.set(p2.ws, matchId);

    for (let i = 0; i < 2; i++) {
        const me = match.players[i];
        if (!me.ws) continue; // bots have no socket
        const opp = match.players[1 - i];
        me.ws.send(JSON.stringify({
            type: 'match_found',
            opponent: opp.name,
            pot: fmt(match.pot),
            matchId,
        }));
    }
    console.log(`[match ${matchId}] ${p1.name} vs ${p2.name} — pot ${fmt(match.pot)} SOL`);
    setTimeout(() => startRound(match), 1500);
}

function startRound(match) {
    if (match.state === 'ended') return;
    match.state = 'live';
    const rugDelay = RUG_MIN_MS + Math.random() * (RUG_MAX_MS - RUG_MIN_MS);
    const startTime = Date.now();
    const rugAt = startTime + rugDelay;

    match.round = {
        startTime,
        rugAt,
        rugBroadcastAt: 0,
        sells: [null, null],
        tickTimer: null,
        rugTimer: null,
        botTimer: null,
        resolveTimer: null,
    };
    const round = match.round;

    // Solo-vs-bot: the bot's sell is scheduled server-side (250–650ms reflex).
    match.players.forEach((p, i) => {
        if (p.isBot) {
            round.botTimer = setTimeout(() => {
                if (match.state !== 'live' || !match.round || !match.round.rugBroadcastAt) return;
                const ms = Date.now() - match.round.rugBroadcastAt;
                registerSell(match, i, ms);
            }, rugDelay + 250 + Math.random() * 400);
        }
    });

    round.tickTimer = setInterval(() => {
        if (match.state !== 'live' || !match.round) return;
        const now = Date.now();
        const progress = Math.min((now - startTime) / rugDelay, 1);
        const marketCap = Math.floor(5000 + Math.pow(progress, 3) * 85000);
        broadcast(match, { type: 'tick', marketCap, progress });
    }, TICK_MS);

    round.rugTimer = setTimeout(() => {
        if (match.state !== 'live' || !match.round) return;
        clearInterval(round.tickTimer);
        round.rugBroadcastAt = Date.now();
        broadcast(match, { type: 'rug', serverTime: round.rugBroadcastAt });
        console.log(`[match ${match.id}] RUG at +${rugDelay.toFixed(0)}ms`);
        // Safety net: if nobody sells inside the window, coin-flip the round.
        round.resolveTimer = setTimeout(() => resolveRound(match, 'timeout'), SELL_WINDOW_MS + 500);
    }, rugDelay);
}

function registerSell(match, playerIndex, msAfterRug) {
    const round = match.round;
    if (!round || match.state !== 'live' || !round.rugBroadcastAt) return;
    if (!Number.isFinite(msAfterRug) || msAfterRug < 0 || msAfterRug > SELL_WINDOW_MS) return;
    if (round.sells[playerIndex] !== null) return; // first sell counts

    round.sells[playerIndex] = msAfterRug;

    // Sells are timestamped on receipt and processed sequentially, so the
    // first registered sell is provably the fastest — resolve immediately.
    resolveRound(match, 'first_sell');
}

function resolveRound(match, reason) {
    const round = match.round;
    if (!round || match.state !== 'live') return;
    match.state = 'resolving';
    clearTimeout(round.rugTimer);
    clearInterval(round.tickTimer);
    clearTimeout(round.resolveTimer);
    clearTimeout(round.botTimer);

    const times = round.sells.map(s => (s === null ? Infinity : s));
    let winnerIndex;
    if (!isFinite(times[0]) && !isFinite(times[1])) {
        winnerIndex = Math.random() < 0.5 ? 0 : 1; // nobody sold: coin flip
    } else {
        winnerIndex = times[0] <= times[1] ? 0 : 1;
    }

    match.wins[winnerIndex]++;
    console.log(`[match ${match.id}] round -> player ${winnerIndex + 1} (${reason}, ` +
        `${times[0] === Infinity ? '—' : times[0].toFixed(0) + 'ms'} vs ` +
        `${times[1] === Infinity ? '—' : times[1].toFixed(0) + 'ms'})`);

    match.players.forEach((p, i) => {
        if (p.isBot || !p.ws || p.ws.readyState !== WebSocket.OPEN) return;
        p.ws.send(JSON.stringify({
            type: 'round_result',
            winner: i === winnerIndex ? 'player' : 'opponent',
            playerMs: times[i] === Infinity ? -1 : Math.round(times[i]),
            opponentMs: times[1 - i] === Infinity ? -1 : Math.round(times[1 - i]),
        }));
    });

    match.round = null;

    if (match.wins[0] >= 2 || match.wins[1] >= 2) {
        endMatch(match, match.wins[0] >= 2 ? 0 : 1);
    } else {
        match.state = 'starting';
        setTimeout(() => startRound(match), 2000);
    }
}

async function endMatch(match, winnerIndex) {
    match.state = 'ended';
    const winner = match.players[winnerIndex];
    const payout = match.pot * 0.94;

    console.log(`[match ${match.id}] winner: ${winner.name} — payout ${fmt(payout)} SOL`);

    if (solana && solana.program && !winner.isBot && winner.wallet) {
        try {
            await settleOnChain(match, winnerIndex);
        } catch (e) {
            console.error(`[match ${match.id}] on-chain settlement failed:`, e.message);
        }
    }

    match.players.forEach((p, i) => {
        if (p.isBot || !p.ws) return;
        socketMatch.delete(p.ws);
        if (p.ws.readyState === WebSocket.OPEN) {
            p.ws.send(JSON.stringify({
                type: 'match_end',
                outcome: i === winnerIndex ? 'win' : 'loss',
                payout: i === winnerIndex ? payout : 0,
            }));
        }
    });
    matches.delete(match.id);
}

async function settleOnChain(match, winnerIndex) {
    const { program, PublicKey, anchor } = solana;
    const winner = match.players[winnerIndex];
    const matchIdBuf = Buffer.from(new anchor.BN(match.id).toArray('le', 8));
    const programId = new PublicKey(PROGRAM_ID);
    const [matchState] = PublicKey.findProgramAddressSync([Buffer.from('match-state'), matchIdBuf], programId);
    const [vault] = PublicKey.findProgramAddressSync([Buffer.from('vault'), matchIdBuf], programId);

    const tx = await program.methods
        .settleMatch(winnerIndex + 1)
        .accounts({
            matchState,
            vault,
            winner: new PublicKey(winner.wallet),
            platformFeeReceiver: new PublicKey(TREASURY_PUBKEY),
            adjudicator: solana.adjudicator.publicKey,
        })
        .rpc();
    console.log(`[match ${match.id}] settled on-chain: ${tx}`);
}

function broadcast(match, msg) {
    const data = JSON.stringify(msg);
    match.players.forEach(p => {
        if (!p.isBot && p.ws && p.ws.readyState === WebSocket.OPEN) p.ws.send(data);
    });
}

function forfeit(match, playerIndex, reason) {
    if (!match || match.state === 'ended' || playerIndex < 0) return;
    console.log(`[match ${match.id}] forfeit by player ${playerIndex + 1}: ${reason}`);
    if (match.round) {
        clearTimeout(match.round.rugTimer);
        clearInterval(match.round.tickTimer);
        clearTimeout(match.round.resolveTimer);
        clearTimeout(match.round.botTimer);
        match.round = null;
    }
    endMatch(match, 1 - playerIndex);
}

function sendError(ws, message) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message }));
    }
}

// ---------------------------------------------------------------------------
// HTTP + WebSocket server
// ---------------------------------------------------------------------------
const app = express();
// Only the public game client is served — program sources and keypairs
// are never exposed over HTTP.
app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
    ws.msgTimestamps = [];
    ws.lastQueueAt = 0;

    ws.on('message', (raw) => {
        // ---- Rate limiting (rolling 1s window) ----
        const now = Date.now();
        ws.msgTimestamps = ws.msgTimestamps.filter(t => now - t < 1000);
        ws.msgTimestamps.push(now);
        if (ws.msgTimestamps.length > RATE_LIMIT_PER_SEC) {
            sendError(ws, 'Rate limit exceeded. Slow down.');
            return;
        }

        let msg;
        try {
            msg = JSON.parse(raw);
            if (typeof msg !== 'object' || msg === null) throw new Error('bad shape');
        } catch (e) {
            return; // ignore malformed frames
        }

        if (msg.type === 'queue') {
            if (socketMatch.has(ws)) return; // already in a match
            if (now - ws.lastQueueAt < 2000) return; // no queue spam
            ws.lastQueueAt = now;

            const wager = Math.min(
                Math.max(parseFloat(msg.wager) || WAGER_SOL, MIN_WAGER_SOL),
                MAX_WAGER_SOL
            );
            const wallet = isValidPubkey(msg.wallet) ? msg.wallet : null;
            const player = {
                ws,
                wager,
                wallet,
                name: wallet ? shortAddr(wallet) : 'PLAYER',
                isBot: false,
            };

            const waiting = queue.shift();
            if (waiting && waiting.ws.readyState === WebSocket.OPEN) {
                createMatch(waiting, player);
            } else {
                queue.push(player);
                // Human-vs-human only: no bot fallback. The player waits
                // here until another human queues.
                ws.send(JSON.stringify({ type: 'searching' }));
            }
        }

        if (msg.type === 'sell') {
            const match = matches.get(socketMatch.get(ws));
            if (!match || match.state !== 'live' || !match.round || !match.round.rugBroadcastAt) return;
            const idx = match.players.findIndex(p => p.ws === ws);
            if (idx === -1) return;
            // Authoritative: timestamped on receipt; client time is ignored.
            registerSell(match, idx, Date.now() - match.round.rugBroadcastAt);
        }

        if (msg.type === 'forfeit') {
            const match = matches.get(socketMatch.get(ws));
            if (!match) return;
            const idx = match.players.findIndex(p => p.ws === ws);
            forfeit(match, idx, typeof msg.reason === 'string' ? msg.reason.slice(0, 64) : 'client_forfeit');
        }
    });

    ws.on('close', () => {
        const qi = queue.findIndex(p => p.ws === ws);
        if (qi !== -1) {
            queue.splice(qi, 1);
        }
        const match = matches.get(socketMatch.get(ws));
        if (match && match.state !== 'ended') {
            const idx = match.players.findIndex(p => p.ws === ws);
            if (idx !== -1) {
                if (match.state === 'live') {
                    forfeit(match, idx, 'disconnect');
                } else {
                    socketMatch.delete(ws);
                }
            }
        }
    });
});

server.listen(PORT, () => {
    console.log(`🪙 SolSwing server running — http://localhost:${PORT}`);
    console.log(`   Client: open the URL above (served from ./public).`);
    console.log(`   Remote client? Pass ?server=ws://<host>:${PORT} in the page URL.`);
});
