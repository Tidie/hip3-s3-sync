// s3-sync-bot.js - Sync HIP-3 data from Hyperliquid S3 bucket
// Downloads fills data, filters HIP-3, imports to Supabase

const axios = require('axios');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { createUnzip } = require('zlib');
const { pipeline } = require('stream/promises');
const http = require('http');

// Config
const SUPABASE_URL = 'https://sdcxusytmxaecfnfzweu.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkY3h1c3l0bXhhZWNmbmZ6d2V1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAyMTExNzUsImV4cCI6MjA4NTc4NzE3NX0.oG8UPS9OoXts8CrBVCkCfLBQaLLhSSBx7u1xuCJrTW8';

const S3_BUCKET = 'hl-mainnet-node-data';
const S3_PREFIX = 'node_fills_by_block/';

const SYNC_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

// AWS S3 Client (requester pays - needs AWS credentials)
const s3Client = new S3Client({
    region: process.env.AWS_REGION || 'us-east-1', // Common default region
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

// List S3 objects
async function listS3Objects(startAfter = null) {
    const params = {
        Bucket: S3_BUCKET,
        Prefix: S3_PREFIX,
        MaxKeys: 100,
        RequestPayer: 'requester'
    };
    
    if (startAfter) {
        params.StartAfter = startAfter;
    }
    
    try {
        const command = new ListObjectsV2Command(params);
        const response = await s3Client.send(command);
        return response.Contents || [];
    } catch (e) {
        console.log('S3 list error:', e.message);
        console.log('Full error:', JSON.stringify(e, null, 2));
        
        // If region error, try to get bucket region
        if (e.message.includes('region') || e.Code === 'PermanentRedirect') {
            console.log('Bucket may be in different region. Check AWS_REGION env var.');
        }
        
        return [];
    }
}

// Download and parse S3 object
async function downloadAndParse(key) {
    try {
        const command = new GetObjectCommand({
            Bucket: S3_BUCKET,
            Key: key,
            RequestPayer: 'requester'
        });
        
        const response = await s3Client.send(command);
        
        // Read the stream
        const chunks = [];
        for await (const chunk of response.Body) {
            chunks.push(chunk);
        }
        const buffer = Buffer.concat(chunks);
        
        // Try to parse as JSON (may be compressed or not)
        let data;
        try {
            // Try as plain JSON first
            data = JSON.parse(buffer.toString());
        } catch (e) {
            // Try decompressing (lz4 format)
            console.log(`  File ${key} may be compressed, skipping for now`);
            return [];
        }
        
        return Array.isArray(data) ? data : [data];
    } catch (e) {
        console.log(`  Error downloading ${key}:`, e.message);
        return [];
    }
}

// Filter HIP-3 fills and format for Supabase
function filterHip3Fills(fills) {
    const hip3Fills = [];
    
    for (const fill of fills) {
        // Check if it's a HIP-3 market (coin contains ':')
        const coin = fill.coin || fill.asset || '';
        if (!coin.includes(':')) continue;
        
        // Extract address
        const address = (fill.user || fill.address || '').toLowerCase();
        if (!address || !address.startsWith('0x')) continue;
        
        hip3Fills.push({
            tid: fill.tid || fill.id,
            oid: fill.oid,
            address: address,
            coin: coin,
            side: fill.side,
            px: parseFloat(fill.px || fill.price || 0),
            sz: parseFloat(fill.sz || fill.size || 0),
            fee: parseFloat(fill.fee || 0),
            closed_pnl: parseFloat(fill.closedPnl || fill.closed_pnl || 0),
            trade_time: fill.time || fill.timestamp,
            crossed: fill.crossed || false,
            hash: fill.hash
        });
    }
    
    return hip3Fills;
}

// Save fills to Supabase
async function saveFills(fills) {
    if (fills.length === 0) return 0;
    
    let inserted = 0;
    
    // Insert in batches of 500
    for (let i = 0; i < fills.length; i += 500) {
        const batch = fills.slice(i, i + 500);
        try {
            await axios.post(
                `${SUPABASE_URL}/rest/v1/hip3_fills`,
                batch,
                { headers: { ...sbHeaders(), 'Prefer': 'resolution=ignore-duplicates,return=minimal' } }
            );
            inserted += batch.length;
        } catch (e) {
            // 409 = duplicates, that's fine
            if (e.response?.status !== 409) {
                console.log(`  Batch insert error: ${e.response?.status || e.message}`);
            }
        }
    }
    
    return inserted;
}

// Update hip3_traders stats from hip3_fills
async function updateAllTraderStats() {
    console.log('📊 Updating trader stats...');
    
    try {
        // Get all unique addresses from hip3_fills
        const addressesRes = await axios.get(
            `${SUPABASE_URL}/rest/v1/hip3_fills?select=address&limit=10000`,
            { headers: sbHeaders() }
        );
        
        const uniqueAddresses = [...new Set((addressesRes.data || []).map(f => f.address))];
        console.log(`  Found ${uniqueAddresses.length} unique addresses`);
        
        let updated = 0;
        
        for (const address of uniqueAddresses) {
            try {
                // Get fills for this address
                const fillsRes = await axios.get(
                    `${SUPABASE_URL}/rest/v1/hip3_fills?address=eq.${address}&select=coin,px,sz,fee,closed_pnl`,
                    { headers: sbHeaders() }
                );
                
                const fills = fillsRes.data || [];
                if (fills.length === 0) continue;
                
                // Compute stats
                let totalVolume = 0, totalFees = 0, totalPnl = 0;
                const markets = {};
                
                fills.forEach(f => {
                    const vol = (f.px || 0) * (f.sz || 0);
                    totalVolume += vol;
                    totalFees += f.fee || 0;
                    totalPnl += f.closed_pnl || 0;
                    
                    if (!markets[f.coin]) {
                        markets[f.coin] = { volume: 0, fees: 0, pnl: 0, trades: 0 };
                    }
                    markets[f.coin].volume += vol;
                    markets[f.coin].fees += f.fee || 0;
                    markets[f.coin].pnl += f.closed_pnl || 0;
                    markets[f.coin].trades += 1;
                });
                
                // Upsert into hip3_traders
                await axios.post(
                    `${SUPABASE_URL}/rest/v1/hip3_traders`,
                    {
                        address: address,
                        total_volume: totalVolume,
                        total_fees: totalFees,
                        total_pnl: totalPnl,
                        pairs_traded: Object.keys(markets).length,
                        trades_count: fills.length,
                        markets: markets,
                        last_updated: new Date().toISOString()
                    },
                    { headers: { ...sbHeaders(), 'Prefer': 'resolution=merge-duplicates,return=minimal' } }
                );
                
                updated++;
                
                if (updated % 100 === 0) {
                    console.log(`  Updated ${updated}/${uniqueAddresses.length} traders`);
                }
                
                await sleep(50); // Small delay to not overload Supabase
            } catch (e) {
                // Continue on error
            }
        }
        
        console.log(`  ✅ Updated ${updated} traders`);
    } catch (e) {
        console.log('  Error updating stats:', e.message);
    }
}

// Main sync function
async function runSync() {
    syncCount++;
    console.log(`\n🔄 Sync #${syncCount} starting...`);
    
    const startTime = Date.now();
    
    // Get last processed key
    const lastKey = await getLastProcessedKey();
    console.log(`  Last processed: ${lastKey || 'none (starting fresh)'}`);
    
    // List new S3 objects
    const objects = await listS3Objects(lastKey);
    console.log(`  Found ${objects.length} new files to process`);
    
    if (objects.length === 0) {
        console.log('  No new files, waiting for next sync');
        setTimeout(runSync, SYNC_INTERVAL_MS);
        return;
    }
    
    let totalFills = 0;
    let processedFiles = 0;
    
    for (const obj of objects) {
        console.log(`  📥 Processing ${obj.Key}...`);
        
        const fills = await downloadAndParse(obj.Key);
        const hip3Fills = filterHip3Fills(fills);
        
        if (hip3Fills.length > 0) {
            const saved = await saveFills(hip3Fills);
            totalFills += saved;
            console.log(`    → ${hip3Fills.length} HIP-3 fills, ${saved} saved`);
        }
        
        processedFiles++;
        lastProcessedKey = obj.Key;
        
        // Save progress every 10 files
        if (processedFiles % 10 === 0) {
            await saveLastProcessedKey(obj.Key);
        }
        
        await sleep(100); // Small delay between files
    }
    
    // Save final progress
    if (lastProcessedKey) {
        await saveLastProcessedKey(lastProcessedKey);
    }
    
    totalFillsImported += totalFills;
    
    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n📊 Sync #${syncCount} complete:`);
    console.log(`   Files: ${processedFiles}`);
    console.log(`   HIP-3 fills: +${totalFills} (total: ${totalFillsImported})`);
    console.log(`   Duration: ${duration.toFixed(1)}s`);
    
    // Update trader stats
    if (totalFills > 0) {
        await updateAllTraderStats();
    }
    
    // Schedule next sync
    console.log(`\n⏰ Next sync in 1 hour...`);
    setTimeout(runSync, SYNC_INTERVAL_MS);
}

// Create sync state table if needed
async function initSyncState() {
    try {
        // Try to create the table (will fail silently if exists)
        await axios.post(
            `${SUPABASE_URL}/rest/v1/rpc/exec_sql`,
            { sql: `CREATE TABLE IF NOT EXISTS hip3_sync_state (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ)` },
            { headers: sbHeaders() }
        );
    } catch (e) {
        // Table might already exist or RPC not available, that's fine
        console.log('Note: sync_state table may need manual creation');
    }
}

// Health check server
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
        status: 'healthy',
        type: 's3-sync',
        syncs: syncCount,
        totalFillsImported: totalFillsImported,
        lastProcessedKey: lastProcessedKey,
        uptime: process.uptime()
    }));
});

// Start
async function start() {
    console.log('🪣 HIP-3 S3 Sync Bot starting...');
    console.log('📦 Syncing from Hyperliquid S3 bucket every hour\n');
    
    await initSyncState();
    
    const PORT = process.env.PORT || 8001;
    server.listen(PORT, () => {
        console.log(`🌐 Port ${PORT}`);
        console.log('🚀 Starting initial sync...\n');
        runSync();
    });
}

start();
