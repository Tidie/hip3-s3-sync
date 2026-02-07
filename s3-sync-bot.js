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

// HIP-3 launched October 13, 2025 - skip everything before
const HIP3_START_KEY = 'node_fills_by_block/hourly/20251013/0.lz4';

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

// Download and parse S3 object (LZ4 compressed) - MEMORY OPTIMIZED
async function downloadAndParse(key) {
    try {
        const command = new GetObjectCommand({
            Bucket: S3_BUCKET,
            Key: key,
            RequestPayer: 'requester'
        });
        
        const response = await s3Client.send(command);
        
        // Get file size
        const contentLength = response.ContentLength || 0;
        console.log(`    File size: ${(contentLength/1024).toFixed(1)}KB`);
        
        // Skip files larger than 200MB to avoid OOM
        if (contentLength > 200 * 1024 * 1024) {
            console.log(`    Skipping - too large (${(contentLength/1024/1024).toFixed(1)}MB)`);
            return [];
        }
        
        // Read the stream
        const chunks = [];
        for await (const chunk of response.Body) {
            chunks.push(chunk);
        }
        const buffer = Buffer.concat(chunks);
        console.log(`    Downloaded: ${(buffer.length/1024).toFixed(1)}KB`);
        
        // Free chunks from memory
        chunks.length = 0;
        
        let data = [];
        
        // Check if LZ4 compressed
        if (key.endsWith('.lz4')) {
            try {
                // Decompress LZ4
                const decompressed = lz4.decode(buffer);
                const text = decompressed.toString('utf-8');
                console.log(`    Decompressed: ${(text.length/1024).toFixed(1)}KB`);
                
                // Parse as NDJSON (newline delimited JSON)
                const lines = text.trim().split('\n');
                console.log(`    Lines: ${lines.length}`);
                
                // Show first line structure
                if (lines.length > 0) {
                    try {
                        const sample = JSON.parse(lines[0]);
                        console.log(`    Sample keys: ${Object.keys(sample).join(', ')}`);
                        console.log(`    Sample: ${JSON.stringify(sample).substring(0, 300)}...`);
                    } catch (e) {
                        console.log(`    First line: ${lines[0].substring(0, 300)}...`);
                    }
                }
                
                let totalFills = 0;
                let hip3Fills = 0;
                
                // Process lines
                for (const line of lines) {
                    if (line.trim()) {
                        try {
                            const parsed = JSON.parse(line);
                            
                            // Structure: { events: [[address, fillData], ...] }
                            if (parsed.events && Array.isArray(parsed.events)) {
                                for (const event of parsed.events) {
                                    if (Array.isArray(event) && event.length >= 2) {
                                        const address = event[0];
                                        const fillData = event[1];
                                        
                                        if (fillData && fillData.coin) {
                                            totalFills++;
                                            
                                            // Check if HIP-3 (coin contains ':')
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
                        } catch (e) {
                            // Skip invalid JSON lines
                        }
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
            const res = await axios.post(
                `${SUPABASE_URL}/rest/v1/hip3_fills`,
                batch,
                { headers: { ...sbHeaders(), 'Prefer': 'resolution=ignore-duplicates,return=minimal' } }
            );
            inserted += batch.length;
        } catch (e) {
            console.log(`    Insert error: ${e.response?.status} - ${e.response?.data?.message || e.message}`);
            console.log(`    Sample fill: ${JSON.stringify(batch[0])}`);
            // Try inserting one by one to find the issue
            if (batch.length > 1) {
                for (const fill of batch.slice(0, 3)) {
                    try {
                        await axios.post(
                            `${SUPABASE_URL}/rest/v1/hip3_fills`,
                            fill,
                            { headers: { ...sbHeaders(), 'Prefer': 'resolution=ignore-duplicates,return=minimal' } }
                        );
                        inserted++;
                    } catch (e2) {
                        console.log(`    Single insert error: ${e2.response?.data?.message || e2.message}`);
                    }
                }
            }
        }
    }
    
    return inserted;
}

// Update leaderboard_stats from hip3_fills (batch-safe, no timeout)
async function updateLeaderboardStats(newAddresses) {
    if (!newAddresses || newAddresses.size === 0) {
        console.log('📊 No new addresses to update');
        return;
    }
    
    console.log(`📊 Updating ${newAddresses.size} addresses in leaderboard...`);
    
    let updated = 0;
    
    for (const address of newAddresses) {
        try {
            // Call SQL function to get stats for this address
            const statsRes = await axios.post(
                `${SUPABASE_URL}/rest/v1/rpc/get_address_stats`,
                { addr: address },
                { headers: sbHeaders() }
            );
            
            if (statsRes.data) {
                const stats = statsRes.data;
                
                // Upsert into leaderboard_stats
                await axios.post(
                    `${SUPABASE_URL}/rest/v1/leaderboard_stats`,
                    {
                        address: address,
                        total_volume: stats.volume || 0,
                        total_fees: stats.fees || 0,
                        total_pnl: stats.pnl || 0,
                        trades_count: stats.trades || 0,
                        updated_at: new Date().toISOString()
                    },
                    { headers: { ...sbHeaders(), 'Prefer': 'resolution=merge-duplicates' } }
                );
                updated++;
            }
        } catch (e) {
            // Skip on error, continue with next
        }
        
        // Small delay to not overload
        if (updated % 50 === 0 && updated > 0) {
            console.log(`  Updated ${updated}/${newAddresses.size}...`);
            await sleep(100);
        }
    }
    
    console.log(`  ✅ Updated ${updated} traders in leaderboard_stats`);
}

// Main sync function
async function runSync() {
    syncCount++;
    console.log(`\n🔄 Sync #${syncCount} starting...`);
    
    const startTime = Date.now();
    
    // Get last processed key (or start from HIP-3 launch)
    const lastKey = await getLastProcessedKey() || HIP3_START_KEY;
    console.log(`  Starting from: ${lastKey}`);
    
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
    const newAddresses = new Set(); // Track addresses with new fills
    
    // Process up to 50 files per sync (we have 4GB RAM now)
    const filesToProcess = objects.slice(0, 50);
    
    for (const obj of filesToProcess) {
        console.log(`  📥 Processing ${obj.Key}...`);
        
        const hip3Fills = await downloadAndParse(obj.Key);
        
        if (hip3Fills.length > 0) {
            // Track addresses
            hip3Fills.forEach(f => newAddresses.add(f.address));
            
            const saved = await saveFills(hip3Fills);
            totalFills += saved;
            console.log(`    ✅ Saved ${saved} fills`);
        }
        
        processedFiles++;
        lastProcessedKey = obj.Key;
        
        // Save progress after each file
        await saveLastProcessedKey(obj.Key);
        
        // Force garbage collection hint
        if (global.gc) global.gc();
        
        await sleep(200); // Small delay between files
    }
    
    totalFillsImported += totalFills;
    
    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n📊 Sync #${syncCount} complete:`);
    console.log(`   Files: ${processedFiles}/${objects.length}`);
    console.log(`   HIP-3 fills: +${totalFills} (total: ${totalFillsImported})`);
    console.log(`   Unique addresses: ${newAddresses.size}`);
    console.log(`   Duration: ${duration.toFixed(1)}s`);
    
    // Update leaderboard stats for addresses with new fills
    if (totalFills > 0) {
        await updateLeaderboardStats(newAddresses);
    }
    
    // If more files to process, run again sooner
    if (objects.length > 50) {
        console.log(`\n⏰ More files pending, next sync in 10s...`);
        setTimeout(runSync, 10000);
    } else {
        console.log(`\n⏰ Next sync in 1 hour...`);
        setTimeout(runSync, SYNC_INTERVAL_MS);
    }
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
