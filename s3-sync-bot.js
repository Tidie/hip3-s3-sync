// s3-sync-bot.js - Sync HIP-3 data from Hyperliquid S3 bucket
// Downloads fills data, filters HIP-3, imports to Supabase
// + Liquidation WebSocket tracker for HIP-3 liquidations

const axios = require('axios');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const lz4 = require('lz4');
const http = require('http');
const WebSocket = require('ws');

// Config
const SUPABASE_URL = 'https://sdcxusytmxaecfnfzweu.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkY3h1c3l0bXhhZWNmbmZ6d2V1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAyMTExNzUsImV4cCI6MjA4NTc4NzE3NX0.oG8UPS9OoXts8CrBVCkCfLBQaLLhSSBx7u1xuCJrTW8';

const S3_BUCKET = 'hl-mainnet-node-data';
const S3_PREFIX = 'node_fills_by_block/hourly/';

// HIP-3 launched October 13, 2025 - skip everything before
const HIP3_START_KEY = 'node_fills_by_block/hourly/20251013/0.lz4';

const SYNC_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

// AWS S3 Client (requester pays - needs AWS credentials)
const s3Client = new S3Client({
    region: process.env.AWS_REGION || 'us-east-1',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
});

let syncCount = 0;
let totalFillsImported = 0;
let lastProcessedKey = null;

function sbHeaders() {
    return {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    };
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Get last processed S3 key from Supabase
async function getLastProcessedKey() {
    try {
        const res = await axios.get(
            `${SUPABASE_URL}/rest/v1/hip3_sync_state?key=eq.last_s3_key&select=value`,
            { headers: sbHeaders() }
        );
        if (res.data && res.data.length > 0) {
            return res.data[0].value;
        }
    } catch (e) {
        console.log('No previous sync state found');
    }
    return null;
}

// Save last processed S3 key
async function saveLastProcessedKey(key) {
    try {
        await axios.post(
            `${SUPABASE_URL}/rest/v1/hip3_sync_state`,
            { key: 'last_s3_key', value: key, updated_at: new Date().toISOString() },
            { headers: { ...sbHeaders(), 'Prefer': 'resolution=merge-duplicates,return=minimal' } }
        );
    } catch (e) {
        console.log('Error saving sync state:', e.message);
    }
}

// List S3 objects - with proper numeric sorting
async function listS3Objects(startAfter = null) {
    let startDate = null;
    let startHour = -1;
    
    if (startAfter) {
        const match = startAfter.match(/(\d{8})\/(\d+)\.lz4/);
        if (match) {
            startDate = match[1];
            startHour = parseInt(match[2]);
        }
    }
    
    const allObjects = [];
    
    const today = new Date();
    const endDateStr = new Date(today.getTime() + 24 * 60 * 60 * 1000)
        .toISOString().slice(0, 10).replace(/-/g, '');
    
    let currentDate = startDate || '20251013';
    
    while (currentDate <= endDateStr) {
        try {
            const prefix = `${S3_PREFIX}${currentDate}/`;
            const command = new ListObjectsV2Command({
                Bucket: S3_BUCKET,
                Prefix: prefix,
                RequestPayer: 'requester'
            });
            const response = await s3Client.send(command);
            const contents = response.Contents || [];
            
            const filtered = contents
                .filter(obj => {
                    const hourMatch = obj.Key.match(/(\d+)\.lz4$/);
                    if (!hourMatch) return false;
                    const hour = parseInt(hourMatch[1]);
                    const dateMatch = obj.Key.match(/(\d{8})/);
                    const date = dateMatch ? dateMatch[1] : '';
                    
                    if (date === startDate && hour <= startHour) return false;
                    if (date < startDate) return false;
                    return true;
                })
                .sort((a, b) => {
                    const aDate = a.Key.match(/(\d{8})/)?.[1] || '';
                    const bDate = b.Key.match(/(\d{8})/)?.[1] || '';
                    if (aDate !== bDate) return aDate.localeCompare(bDate);
                    const aHour = parseInt(a.Key.match(/(\d+)\.lz4$/)?.[1] || '0');
                    const bHour = parseInt(b.Key.match(/(\d+)\.lz4$/)?.[1] || '0');
                    return aHour - bHour;
                });
            
            allObjects.push(...filtered);
            
        } catch (e) {
            // Date folder might not exist, skip
        }
        
        const y = parseInt(currentDate.slice(0, 4));
        const m = parseInt(currentDate.slice(4, 6)) - 1;
        const d = parseInt(currentDate.slice(6, 8));
        const next = new Date(y, m, d + 1);
        currentDate = next.toISOString().slice(0, 10).replace(/-/g, '');
        
        if (allObjects.length >= 100) break;
    }
    
    return allObjects;
}

// Download and parse S3 object (LZ4 compressed)
async function downloadAndParse(key) {
    try {
        const command = new GetObjectCommand({
            Bucket: S3_BUCKET,
            Key: key,
            RequestPayer: 'requester'
        });
        
        const response = await s3Client.send(command);
        
        const contentLength = response.ContentLength || 0;
        console.log(`    File size: ${(contentLength/1024).toFixed(1)}KB`);
        
        if (contentLength > 200 * 1024 * 1024) {
            console.log(`    Skipping - too large (${(contentLength/1024/1024).toFixed(1)}MB)`);
            return [];
        }
        
        const chunks = [];
        for await (const chunk of response.Body) {
            chunks.push(chunk);
        }
        const buffer = Buffer.concat(chunks);
        console.log(`    Downloaded: ${(buffer.length/1024).toFixed(1)}KB`);
        
        chunks.length = 0;
        
        let data = [];
        
        if (key.endsWith('.lz4')) {
            try {
                const decompressed = lz4.decode(buffer);
                const text = decompressed.toString('utf-8');
                console.log(`    Decompressed: ${(text.length/1024).toFixed(1)}KB`);
                
                const lines = text.trim().split('\n');
                console.log(`    Lines: ${lines.length}`);
                
                let totalFills = 0;
                let hip3Fills = 0;
                
                for (const line of lines) {
                    if (line.trim()) {
                        try {
                            const parsed = JSON.parse(line);
                            
                            if (parsed.events && Array.isArray(parsed.events)) {
                                for (const event of parsed.events) {
                                    if (Array.isArray(event) && event.length >= 2) {
                                        const address = event[0];
                                        const fillData = event[1];
                                        
                                        if (fillData && fillData.coin) {
                                            totalFills++;
                                            
                                            // Filter HIP-3 only (coin contains ':')
                                            if (fillData.coin.includes(':')) {
                                                hip3Fills++;
                                                data.push({
                                                    address: address.toLowerCase(),
                                                    coin: fillData.coin,
                                                    px: parseFloat(fillData.px || 0),
                                                    sz: parseFloat(fillData.sz || 0),
                                                    side: fillData.side,
                                                    fee: parseFloat(fillData.fee || 0),
                                                    closed_pnl: parseFloat(fillData.closedPnl || 0),
                                                    tid: fillData.tid,
                                                    trade_time: fillData.time,
                                                    hash: fillData.hash || ''
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (e) {}
                    }
                }
                
                console.log(`    Total fills: ${totalFills}, HIP-3 fills: ${hip3Fills}`);
                
            } catch (e) {
                console.log(`    LZ4 decode error: ${e.message}`);
            }
        }
        
        return data;
        
    } catch (e) {
        console.log(`    Download error: ${e.message}`);
        return [];
    }
}

// Insert fills to Supabase in batches
async function insertFills(fills) {
    if (fills.length === 0) return 0;
    
    const BATCH_SIZE = 500;
    let inserted = 0;
    
    for (let i = 0; i < fills.length; i += BATCH_SIZE) {
        const batch = fills.slice(i, i + BATCH_SIZE);
        
        try {
            await axios.post(
                `${SUPABASE_URL}/rest/v1/hip3_fills?on_conflict=tid,address`,
                batch,
                { headers: { ...sbHeaders(), 'Prefer': 'resolution=ignore-duplicates,return=minimal' } }
            );
            inserted += batch.length;
        } catch (e) {
            console.log(`    Batch insert error: ${e.response?.data?.message || e.message}`);
        }
        
        if (i + BATCH_SIZE < fills.length) {
            await sleep(100);
        }
    }
    
    return inserted;
}

// Main sync function
async function runSync() {
    syncCount++;
    console.log(`\n🔄 Sync #${syncCount} starting...`);
    
    const startTime = Date.now();
    
    const lastKey = await getLastProcessedKey();
    console.log(`  Starting from: ${lastKey || HIP3_START_KEY}`);
    
    const objects = await listS3Objects(lastKey || HIP3_START_KEY);
    console.log(`  Found ${objects.length} new files to process`);
    
    if (objects.length === 0) {
        console.log('  No new files, waiting for next scheduled sync');
        return;
    }
    
    let totalFills = 0;
    const newAddresses = new Set();
    
    for (const obj of objects) {
        console.log(`\n  Processing: ${obj.Key}`);
        
        const fills = await downloadAndParse(obj.Key);
        
        if (fills.length > 0) {
            fills.forEach(f => newAddresses.add(f.address));
            const inserted = await insertFills(fills);
            totalFills += inserted;
            totalFillsImported += inserted;
            console.log(`    Inserted: ${inserted} fills`);
        }
        
        await saveLastProcessedKey(obj.Key);
        lastProcessedKey = obj.Key;
        
        await sleep(500);
    }
    
    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n✅ Sync #${syncCount} complete!`);
    console.log(`   Files processed: ${objects.length}`);
    console.log(`   HIP-3 fills imported: ${totalFills}`);
    console.log(`   Total fills imported: ${totalFillsImported}`);
    console.log(`   Unique addresses: ${newAddresses.size}`);
    console.log(`   Duration: ${duration.toFixed(1)}s`);
    
    if (objects.length > 50) {
        console.log(`\n⚡ More files pending, continuing in 10s...`);
        setTimeout(safeSyncRun, 10000);
    } else {
        console.log(`\n⏰ Next sync in 1 hour (via setInterval)`);
    }
}

// Create sync state table if needed
async function initSyncState() {
    try {
        await axios.post(
            `${SUPABASE_URL}/rest/v1/rpc/exec_sql`,
            { sql: `CREATE TABLE IF NOT EXISTS hip3_sync_state (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ)` },
            { headers: sbHeaders() }
        );
    } catch (e) {
        console.log('Note: sync_state table may need manual creation');
    }
}

// Health check server
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
        status: 'healthy',
        type: 's3-sync + liquidation-tracker',
        syncs: syncCount,
        totalFillsImported: totalFillsImported,
        totalLiqsImported: totalLiqsImported,
        liqWsConnected: liqWs ? liqWs.readyState === WebSocket.OPEN : false,
        liqBufferSize: liqEventsBuffer.length,
        lastProcessedKey: lastProcessedKey,
        uptime: process.uptime(),
        lastSyncAt: lastSyncAt,
        isSyncing: isSyncing
    }));
});

let lastSyncAt = null;
let isSyncing = false;

async function safeSyncRun() {
    if (isSyncing) {
        console.log('⏳ Sync already in progress, skipping...');
        return;
    }
    isSyncing = true;
    try {
        await runSync();
    } catch (e) {
        console.error('❌ Sync error:', e.message);
    } finally {
        isSyncing = false;
        lastSyncAt = new Date().toISOString();
    }
}

// ===== LIQUIDATION WEBSOCKET TRACKER (HIP-3 ONLY) =====
let liqWs = null;
let liqWsRetries = 0;
let liqEventsBuffer = [];
let liqFlushInterval = null;
let totalLiqsImported = 0;
let liqPingInterval = null;

const LIQ_FLUSH_INTERVAL_MS = 10000; // Flush buffer every 10 seconds
const LIQ_BUFFER_MAX = 50; // Flush if buffer reaches 50 events

function connectLiquidationWs() {
    if (liqWs && (liqWs.readyState === WebSocket.OPEN || liqWs.readyState === WebSocket.CONNECTING)) return;

    try {
        liqWs = new WebSocket('wss://api.hyperliquid.xyz/ws');

        liqWs.on('open', () => {
            console.log('🔴 Liquidation WebSocket connected, subscribing...');
            
            // Subscribe to all liquidations
            const subscribeMsg = JSON.stringify({
                method: 'subscribe',
                subscription: { type: 'allLiquidations' }
            });
            liqWs.send(subscribeMsg);
            
            liqWsRetries = 0;
        });

        liqWs.on('message', (raw) => {
            try {
                const msg = JSON.parse(raw.toString());
                
                // Handle subscription confirmation
                if (msg.method === 'subscribed' || msg.channel === 'subscriptionResponse') {
                    console.log('✅ Liquidation subscription confirmed');
                    return;
                }
                
                // Handle pong
                if (msg.method === 'pong' || msg.type === 'pong') {
                    return;
                }

                if (msg.channel === 'allLiquidations' && msg.data) {
                    const liq = msg.data.liquidation || msg.data;
                    const coin = liq.coin || '';

                    // ONLY track HIP-3 coins (contain ':')
                    if (!coin.includes(':')) return;

                    const parts = coin.split(':');
                    const deployer = (parts[0] || 'unknown').toLowerCase();
                    const ticker = parts[1] || coin;
                    const sz = parseFloat(liq.sz || liq.size || 0);
                    const px = parseFloat(liq.px || liq.markPx || liq.price || 0);
                    const ntlValue = Math.abs(parseFloat(liq.liquidatedNtlPos || liq.ntl || 0) || (sz * px));

                    const event = {
                        time: Date.now(),
                        coin,
                        deployer,
                        ticker,
                        is_long: liq.isLong !== undefined ? liq.isLong : (liq.side === 'B' || liq.side === 'buy'),
                        sz,
                        px,
                        ntl_value: ntlValue,
                        user_addr: (liq.liquidatedUser || liq.user || '').toLowerCase()
                    };

                    liqEventsBuffer.push(event);
                    console.log(`🔴 HIP-3 Liquidation: ${coin} | $${ntlValue.toFixed(2)} | buffer: ${liqEventsBuffer.length}`);

                    if (liqEventsBuffer.length >= LIQ_BUFFER_MAX) {
                        flushLiqBuffer();
                    }
                }
            } catch (e) {
                // Silent parse errors
            }
        });

        // CRITICAL: Respond to server pings to keep connection alive
        liqWs.on('ping', (data) => {
            liqWs.pong(data);
        });

        liqWs.on('pong', () => {
            // Our ping was acknowledged, connection is healthy
        });

        liqWs.on('close', (code, reason) => {
            console.log(`⚠️ Liquidation WebSocket closed (code: ${code})`);
            liqWsRetries++;
            const delay = Math.min(5000 * liqWsRetries, 60000);
            console.log(`   Reconnecting in ${delay/1000}s...`);
            setTimeout(connectLiquidationWs, delay);
        });

        liqWs.on('error', (e) => {
            console.error('❌ Liquidation WebSocket error:', e.message);
        });

    } catch (e) {
        console.error('❌ Liquidation WebSocket connection error:', e.message);
        setTimeout(connectLiquidationWs, 5000);
    }
}

async function flushLiqBuffer() {
    if (liqEventsBuffer.length === 0) return;

    const batch = [...liqEventsBuffer];
    liqEventsBuffer = [];

    try {
        await axios.post(
            `${SUPABASE_URL}/rest/v1/hip3_liquidations`,
            batch,
            { headers: { ...sbHeaders(), 'Prefer': 'return=minimal' } }
        );
        totalLiqsImported += batch.length;
        console.log(`🔴 Flushed ${batch.length} liquidations → Supabase (total: ${totalLiqsImported})`);
    } catch (e) {
        console.error('❌ Failed to flush liquidations:', e.response?.data || e.message);
        // Put back in buffer for retry
        liqEventsBuffer = [...batch, ...liqEventsBuffer];
        if (liqEventsBuffer.length > 5000) {
            liqEventsBuffer = liqEventsBuffer.slice(-5000);
        }
    }
}

function startLiquidationTracker() {
    connectLiquidationWs();

    // Periodic flush every 10s
    liqFlushInterval = setInterval(() => {
        flushLiqBuffer();
    }, LIQ_FLUSH_INTERVAL_MS);

    // Send ping every 20s to keep connection alive
    liqPingInterval = setInterval(() => {
        if (liqWs && liqWs.readyState === WebSocket.OPEN) {
            try {
                liqWs.ping();
                // Also send JSON ping (some APIs expect this)
                liqWs.send(JSON.stringify({ method: 'ping' }));
            } catch (e) {}
        }
    }, 20000);

    console.log('🔴 Liquidation tracker started (HIP-3 only, ping every 20s)');
}

// ===== END LIQUIDATION TRACKER =====

// Start everything
async function start() {
    console.log('🪣 HIP-3 S3 Sync Bot starting...');
    console.log('📦 Syncing HIP-3 fills from S3 bucket every hour');
    console.log('🔴 Tracking HIP-3 liquidations via WebSocket\n');
    
    await initSyncState();
    
    // Start liquidation WebSocket tracker
    startLiquidationTracker();
    
    const PORT = process.env.PORT || 8001;
    server.listen(PORT, () => {
        console.log(`🌐 Health check on port ${PORT}`);
        console.log('🚀 Starting initial S3 sync...\n');
        
        // Run S3 sync immediately
        safeSyncRun();
        
        // Schedule hourly S3 sync
        setInterval(() => {
            console.log('\n⏰ Scheduled hourly sync triggered');
            safeSyncRun();
        }, SYNC_INTERVAL_MS);
        
        // Self-ping keep-alive every 5 minutes
        setInterval(() => {
            const url = `http://localhost:${PORT}/`;
            http.get(url, () => {}).on('error', () => {});
        }, 5 * 60 * 1000);
    });
}

start();
