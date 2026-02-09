// s3-sync-bot.js - Sync HIP-3 data from Hyperliquid S3 bucket
// Downloads fills data, filters HIP-3, imports to Supabase

const axios = require('axios');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const lz4 = require('lz4');
const http = require('http');

// Config
const SUPABASE_URL = 'https://sdcxusytmxaecfnfzweu.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkY3h1c3l0bXhhZWNmbmZ6d2V1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAyMTExNzUsImV4cCI6MjA4NTc4NzE3NX0.oG8UPS9OoXts8CrBVCkCfLBQaLLhSSBx7u1xuCJrTW8';

const S3_BUCKET = 'hl-mainnet-node-data';
const S3_PREFIX = 'node_fills_by_block/hourly/';

const HIP3_START_KEY = 'node_fills_by_block/hourly/20251013/0.lz4';

// Sync every 10 minutes
const SYNC_INTERVAL_MS = 10 * 60 * 1000;

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
let lastSyncAt = null;
let isSyncing = false;

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
            console.log(`    Skipping - too large`);
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
                                            
                                            if (fillData.coin.includes(':')) {
                                                hip3Fills++;
                                                data.push({
                                                    address: address.toLowerCase(),
                                                    coin: fillData.coin,
                                                    px: parseFloat(fillData.px || 0),
                                                    sz: parseFloat(fillData.sz || 0),
                                                    side: fillData.side,
                                                    trade_time: fillData.time,
                                                    fee: parseFloat(fillData.fee || 0),
                                                    closed_pnl: parseFloat(fillData.closedPnl || 0),
                                                    tid: fillData.tid || Date.now() + Math.random(),
                                                    hash: fillData.hash
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (e) {}
                    }
                }
                
                console.log(`    Total fills: ${totalFills}, HIP-3: ${hip3Fills}`);
                
            } catch (e) {
                console.log(`    LZ4 error: ${e.message}`);
                return [];
            }
        }
        
        console.log(`  → Found ${data.length} HIP-3 fills`);
        return data;
    } catch (e) {
        console.log(`  Error: ${e.message}`);
        return [];
    }
}

async function saveFills(fills) {
    if (fills.length === 0) return 0;
    
    let inserted = 0;
    
    for (let i = 0; i < fills.length; i += 500) {
        const batch = fills.slice(i, i + 500);
        try {
            await axios.post(
                `${SUPABASE_URL}/rest/v1/hip3_fills?on_conflict=tid,address`,
                batch,
                { headers: { ...sbHeaders(), 'Prefer': 'resolution=ignore-duplicates,return=minimal' } }
            );
            inserted += batch.length;
        } catch (e) {
            console.log(`    Insert error: ${e.response?.data?.message || e.message}`);
        }
    }
    
    return inserted;
}

async function runSync() {
    syncCount++;
    console.log(`\n🔄 Sync #${syncCount} starting...`);
    
    const startTime = Date.now();
    
    const lastKey = await getLastProcessedKey() || HIP3_START_KEY;
    console.log(`  Starting from: ${lastKey}`);
    
    const objects = await listS3Objects(lastKey);
    console.log(`  Found ${objects.length} new files to process`);
    
    if (objects.length === 0) {
        console.log('  No new files, waiting for next scheduled sync');
        return;
    }
    
    let totalFills = 0;
    let processedFiles = 0;
    const newAddresses = new Set();
    
    const filesToProcess = objects.slice(0, 50);
    
    for (const obj of filesToProcess) {
        console.log(`  📥 Processing ${obj.Key}...`);
        
        const hip3Fills = await downloadAndParse(obj.Key);
        
        if (hip3Fills.length > 0) {
            hip3Fills.forEach(f => newAddresses.add(f.address));
            
            const saved = await saveFills(hip3Fills);
            totalFills += saved;
            console.log(`    ✅ Saved ${saved} fills`);
        }
        
        processedFiles++;
        lastProcessedKey = obj.Key;
        
        await saveLastProcessedKey(obj.Key);
        
        if (global.gc) global.gc();
        
        await sleep(200);
    }
    
    totalFillsImported += totalFills;
    
    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n📊 Sync #${syncCount} complete:`);
    console.log(`   Files: ${processedFiles}/${objects.length}`);
    console.log(`   HIP-3 fills: +${totalFills} (total: ${totalFillsImported})`);
    console.log(`   Unique addresses: ${newAddresses.size}`);
    console.log(`   Duration: ${duration.toFixed(1)}s`);
    
    if (objects.length > 50) {
        console.log(`\n⚡ More files pending, continuing in 10s...`);
        setTimeout(safeSyncRun, 10000);
    } else {
        console.log(`\n⏰ Next sync in 10 minutes`);
    }
}

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

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
        status: 'healthy',
        syncs: syncCount,
        totalFillsImported: totalFillsImported,
        lastProcessedKey: lastProcessedKey,
        uptime: process.uptime(),
        lastSyncAt: lastSyncAt,
        isSyncing: isSyncing
    }));
});

async function start() {
    console.log('🪣 HIP-3 S3 Sync Bot starting...');
    console.log('📦 Syncing from Hyperliquid S3 every 10 minutes\n');
    
    await initSyncState();
    
    const PORT = process.env.PORT || 8001;
    server.listen(PORT, () => {
        console.log(`🌐 Port ${PORT}`);
        console.log('🚀 Starting initial sync...\n');
        
        // Run immediately
        safeSyncRun();
        
        // Then every 10 minutes (reliable setInterval)
        setInterval(() => {
            console.log('\n⏰ Scheduled sync triggered');
            safeSyncRun();
        }, SYNC_INTERVAL_MS);
        
        // Self-ping keep-alive every 5 minutes
        setInterval(() => {
            http.get(`http://localhost:${PORT}/`, () => {}).on('error', () => {});
        }, 5 * 60 * 1000);
    });
}

start();
