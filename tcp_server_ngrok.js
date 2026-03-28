const express = require('express');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const bodyParser = require('body-parser');
const dgram = require('dgram');
const sqlite3 = require('sqlite3').verbose();
const cluster = require('cluster');
const totalCPUs = require('os').cpus().length;
// Optimization for High-Core Machines (64+ Cores): 
// We cap workers at 4 to prevent EMFILE/resource exhaustion on Windows while still leveraging multi-threading.
const numCPUs = 4;
const { createClient } = require('redis');
const https = require('https');
const ngrok = require('ngrok');
const { spawn } = require('child_process');
const { Server } = require("socket.io");
const { WebSocketServer } = require("ws");
const { createAdapter } = require("@socket.io/redis-adapter");
const Redis = require("ioredis");
const ort = require('onnxruntime-node');
require('dotenv').config();

// OTP Notification Service Integration
const { sendEmailOtp, sendSmsOtp } = require('./notification_service');

// FIREBASE ADMIN SDK (For SMS OTP Verification from Mobile App)
let firebaseAdmin = null;
const serviceAccountPath = path.join(__dirname, 'firebase-service-account.json');
if (fs.existsSync(serviceAccountPath)) {
    try {
        const admin = require('firebase-admin');
        const serviceAccount = require(serviceAccountPath);
        firebaseAdmin = admin.initializeApp({
            credential: admin.credential.cert(serviceAccount)
        });
        console.log("[FIREBASE] Admin SDK Initialized for SMS OTP Verification 🔒");
    } catch (e) {
        console.error("[FIREBASE ERROR] Initialization failed:", e.message);
    }
} else {
    // In cluster mode, only master logs the missing config to reduce spam
    if (!cluster.isWorker) {
        console.log("[FIREBASE] Warning: firebase-service-account.json not found. Token verification disabled.");
    }
}

const app = express();
const PORT = 8080;
const SSL_PORT = 443;
const SECRET_TOKEN = "POLICE_SECRET_789";
const LOGS_DIR = path.join(__dirname, 'logs');
const MEMBERS_FILE = path.join(__dirname, 'members.json');
const DB_FILE = path.join(__dirname, 'snp_database.db');
const WEB_DASHBOARD_DIR = path.join(__dirname, '../web_dashboard');
const DISTRACTION_MODEL_PATH = path.join(__dirname, '../model/distraction_model.onnx');
const WEARABLE_MODEL_PATH = path.join(__dirname, '../model/full_dataset_model_filewearable_model.onnx');

// AI Model Instances
let aiSessions = { distraction: null, wearable: null };
const sessionBuffers = new Map(); // Store last 50 points per sid for AI
const BUFFER_SIZE = 50;

async function loadAIModels() {
    try {
        if (fs.existsSync(DISTRACTION_MODEL_PATH)) {
            aiSessions.distraction = await ort.InferenceSession.create(DISTRACTION_MODEL_PATH);
            console.log("[AI] Distraction Model Loaded");
        }
        if (fs.existsSync(WEARABLE_MODEL_PATH)) {
            aiSessions.wearable = await ort.InferenceSession.create(WEARABLE_MODEL_PATH);
            console.log("[AI] Wearable Activity Model Loaded");
        }
    } catch (e) {
        console.error("[AI ERROR] Model Loading Failed:", e.message);
    }
}
loadAIModels();

// Initialize SQLite Database with elevated busy timeout for cluster mode
const db = new sqlite3.Database(DB_FILE, (err) => {
    if (err) console.error(`[DB ERROR] Failed to open database: ${err.message}`);
});
db.run("PRAGMA busy_timeout = 15000"); // 15s timeout for concurrent cluster writes

// Centralized dynamic URL logic for Session Links and Dashboard
const getBaseUrl = (req) => {
    if (req) {
        const protocol = req.protocol || 'http';
        const host = req.get('host');
        return `${protocol}://${host}`;
    }
    // Fallback detection logic
    const { networkInterfaces } = require('os');
    const nets = networkInterfaces();
    for (const name of Object.keys(nets)) {
        for (const net of nets[name]) {
            if (net.family === 'IPv4' && !net.internal) return `http://${net.address}:8080`;
        }
    }
    return "http://localhost:8080";
};

// Helper for Local Timestamp alignment
const getLocalTimestamp = () => {
    const d = new Date();
    const pad = (n) => n.toString().padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
};

// Health Threshold Logic
const getHealthStatus = (hr, spo2, temp) => {
    if (hr > 125 || hr < 45 || spo2 <= 92 || temp > 40 || temp < 32) return "CRITICAL";
    if (hr > 110 || hr < 50) return "CRITICAL";
    if (hr > 95 || hr < 60 || spo2 < 95 || (temp > 38 && temp <= 40) || (temp >= 32 && temp < 35)) return "WARNING";
    return "HEALTHY";
};

// Standardized CSV Formatter for dynamic device data
const formatSampleForCsv = (sampleStr) => {
    let readings = { ts: 0, hr: 0, spo2: 0, ax: 0, ay: 0, az: 0, gx: 0, gy: 0, gz: 0, temp: 0 };
    if (!sampleStr) return "0,0,0,0,0,0,0,0,0,0";

    if (sampleStr.startsWith('e2e:')) {
        // For E2EE data, we store the full blob in the first (ESP_Timestamp) column and leave others empty for alignment
        return `"${sampleStr}",,,,,,,,,`;
    }

    const s = sampleStr.split(',');
    if (s.length >= 10) {
        // Legacy formatted CSV (10+ fields)
        return s.slice(0, 10).join(',');
    } else if (sampleStr.match(/HR[:=]/i) || sampleStr.match(/Temp[:=]/i)) {
        // Modern Tagged format: HR:67.1,Temp:30.6
        const parseVal = (key) => {
            const match = sampleStr.match(new RegExp(key + '[:=]\\s*([^,|]+)', 'i'));
            return match ? parseFloat(match[1]) : 0;
        };
        readings.hr = parseVal('HR');
        readings.spo2 = parseVal('SpO2') || 98;
        readings.temp = parseVal('Temp');
        return `${readings.ts},${readings.hr},${readings.spo2},${readings.ax},${readings.ay},${readings.az},${readings.gx},${readings.gy},${readings.gz},${readings.temp}`;
    } else {
        // Raw event or unknown single field
        return `"${sampleStr}",,,,,,,,,`;
    }
};

// Console Color Helpers
const COLORS = {
    RED: "\x1b[31m",
    YELLOW: "\x1b[33m",
    GREEN: "\x1b[32m",
    RESET: "\x1b[0m",
    BOLD: "\x1b[1m"
};

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS departments (
        dept_id TEXT PRIMARY KEY,
        dept_name TEXT NOT NULL,
        city TEXT DEFAULT 'Surat',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // 1b. Positions Table
    db.run(`CREATE TABLE IF NOT EXISTS positions (
        pos_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT UNIQUE NOT NULL,
        priority INTEGER DEFAULT 0
    )`);

    // Seed Default Positions
    const positions = ['Constable', 'Head Constable', 'PSI', 'PI', 'ACP', 'DCP', 'Admin'];
    const pStmt = db.prepare("INSERT OR IGNORE INTO positions (title) VALUES (?)");
    positions.forEach(p => pStmt.run(p));
    pStmt.finalize();

    db.run(`CREATE TABLE IF NOT EXISTS members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        special_no TEXT UNIQUE NOT NULL,
        mobile TEXT UNIQUE NOT NULL,
        email TEXT,
        dept_id TEXT,
        position_id INTEGER,
        address TEXT,
        password TEXT DEFAULT 'Surat@123',
        role TEXT DEFAULT 'user',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
        FOREIGN KEY (position_id) REFERENCES positions(pos_id)
    )`);

    // Migration: Add position_id if missing in existing table
    db.all("PRAGMA table_info(members)", (err, columns) => {
        if (err) return;
        if (!columns.some(c => c.name === 'position_id')) {
            db.run("ALTER TABLE members ADD COLUMN position_id INTEGER", (err) => {
                if (!err) console.log("[DB] Added position_id column to members");
            });
        }
        if (!columns.some(c => c.name === 'password')) {
            db.run("ALTER TABLE members ADD COLUMN password TEXT DEFAULT 'Surat@123'", (err) => {
                if (!err) console.log("[DB] Added password column to members");
            });
        }
    });

    // 3. Sessions Table
    db.run(`CREATE TABLE IF NOT EXISTS bridge_sessions (
        session_id TEXT PRIMARY KEY,
        member_id INTEGER,
        mac_address TEXT,
        auth_token TEXT,
        started_at DATETIME DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (member_id) REFERENCES members(id)
    )`);

    // 4. Readings Table (Modified for Magnetometer and Battery variants)
    db.run(`CREATE TABLE IF NOT EXISTS sensor_readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT,
        esp_timestamp INTEGER,
        heart_rate REAL,
        spo2 REAL,
        temp_c REAL,
        acc_x REAL, acc_y REAL, acc_z REAL,
        gyro_x REAL, gyro_y REAL, gyro_z REAL,
        mag_x REAL, mag_y REAL, mag_z REAL, -- New Magnetometer fields
        lat REAL, lng REAL, alt REAL,
        battery REAL, -- Percent
        bat_v REAL,   -- Voltage
        raw_blob TEXT, -- Used for E2EE or original string
        recorded_at DATETIME DEFAULT (datetime('now','localtime')),
        FOREIGN KEY (session_id) REFERENCES bridge_sessions(session_id)
    )`);

    // Migration Check: Add columns if older schema exists
    db.all("PRAGMA table_info(sensor_readings)", (err, columns) => {
        if (err) return;
        if (!columns.some(c => c.name === 'raw_blob')) {
            console.log("[DB MIGRATION] Adding raw_blob column...");
            db.run("ALTER TABLE sensor_readings ADD COLUMN raw_blob TEXT");
        }
        if (!columns.some(c => c.name === 'battery')) {
            console.log("[DB MIGRATION] Adding battery column...");
            db.run("ALTER TABLE sensor_readings ADD COLUMN battery REAL");
        }
        if (!columns.some(c => c.name === 'mag_x')) {
            console.log("[DB MIGRATION] Adding Magnetometer columns...");
            db.run("ALTER TABLE sensor_readings ADD COLUMN mag_x REAL");
            db.run("ALTER TABLE sensor_readings ADD COLUMN mag_y REAL");
            db.run("ALTER TABLE sensor_readings ADD COLUMN mag_z REAL");
        }
        if (!columns.some(c => c.name === 'bat_v')) {
            console.log("[DB MIGRATION] Adding bat_v column...");
            db.run("ALTER TABLE sensor_readings ADD COLUMN bat_v REAL");
        }
    });

    // Add Indexes for Performance
    db.run("CREATE INDEX IF NOT EXISTS idx_readings_session ON sensor_readings(session_id)");
    db.run("CREATE INDEX IF NOT EXISTS idx_readings_recorded ON sensor_readings(recorded_at DESC)");
    db.run("CREATE INDEX IF NOT EXISTS idx_sessions_member ON bridge_sessions(member_id)");
    db.run("CREATE INDEX IF NOT EXISTS idx_sessions_started ON bridge_sessions(started_at DESC)");

    // --- PRODUCTION SCALE OPTIMIZATIONS ---
    db.run("PRAGMA journal_mode = WAL");        // Write-Ahead Logging for concurrent Read/Write
    db.run("PRAGMA synchronous = NORMAL");     // Balance between safety and speed
    db.run("PRAGMA cache_size = -10000");      // 10MB Cache
    db.run("PRAGMA temp_store = MEMORY");     // Use RAM for temp tables
    db.run("PRAGMA busy_timeout = 10000");      // Handle locking gracefully (10s wait)

    // 5. Config/Thresholds Table
    db.run(`CREATE TABLE IF NOT EXISTS application_config (
        key TEXT PRIMARY KEY,
        value TEXT
    )`);

    // Seed Defaults
    const defaults = {
        "hr_low_warning": "50",
        "hr_high_warning": "110",
        "hr_critical": "130",
        "spo2_warning": "94",
        "spo2_critical": "90",
        "temp_warning": "37.8",
        "temp_critical": "38.5"
    };

    Object.entries(defaults).forEach(([k, v]) => {
        db.run("INSERT OR IGNORE INTO application_config (key, value) VALUES (?, ?)", [k, v]);
    });

    // Migrating JSON to DB if empty
    db.get("SELECT count(*) as count FROM members", (err, row) => {
        if (row.count === 0 && fs.existsSync(MEMBERS_FILE)) {
            console.log("[DB] Migrating members from JSON to SQLite...");
            const members = JSON.parse(fs.readFileSync(MEMBERS_FILE));
            const stmt = db.prepare("INSERT INTO members (name, special_no, mobile, email, dept_id, address, role) VALUES (?, ?, ?, ?, ?, ?, ?)");
            members.forEach(m => {
                stmt.run(m.name, m.special_no, m.mobile, m.email, m.dept_id, m.address, m.role || 'user');
                // Auto-create department for each member
                db.run("INSERT OR IGNORE INTO departments (dept_id, dept_name) VALUES (?, ?)", [m.dept_id, m.dept_id]);
            });
            stmt.finalize();
        }
    });
});

// Ensure directories and files exist
if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR);
if (!fs.existsSync(MEMBERS_FILE)) fs.writeFileSync(MEMBERS_FILE, '[]');

// Serve Web Dashboard
app.use('/dashboard', express.static(WEB_DASHBOARD_DIR));

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// --- HEARTBEAT & HEALTH CHECK ---
app.get('/', (req, res) => res.status(200).json({ status: "Online", worker: process.pid, time: getLocalTimestamp() }));
app.get('/api/health/check', (req, res) => res.status(200).json({ status: "Online", worker: process.pid, time: getLocalTimestamp() }));

// In-Memory Stores
const streams = new Map();
const liveStatus = new Map(); // Key: dept_name_sid, Value: { latestData, location, officer_name, timestamp }

/**
 * Shared OTP Storage across Workers via Redis.
 * Falls back to local Map if Redis is unavailable.
 */
const otpLocalStore = new Map();
const otpStore = {
    set: async (key, val) => {
        if (isRedisConnected) {
            await redisClient.setEx(`otp:${key}`, 300, JSON.stringify(val));
        } else {
            otpLocalStore.set(key, val);
        }
    },
    get: async (key) => {
        if (isRedisConnected) {
            const val = await redisClient.get(`otp:${key}`);
            return val ? JSON.parse(val) : null;
        }
        return otpLocalStore.get(key);
    },
    delete: async (key) => {
        if (isRedisConnected) {
            await redisClient.del(`otp:${key}`);
        } else {
            otpLocalStore.delete(key);
        }
    }
};

// --- REDIS SCALING LAYER ---
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const redisClient = createClient({ url: `redis://${REDIS_HOST}:6379` });
let isRedisConnected = false;

redisClient.on('error', (err) => {
    if (isRedisConnected) console.error('[REDIS] Connection Lost:', err.message);
    isRedisConnected = false;
});

async function connectScalingLayer() {
    try {
        await redisClient.connect();
        isRedisConnected = true;
        console.log('[REDIS] High-Load Scaling Layer Connected 🚀');
    } catch (e) {
        console.log('[REDIS] Scaling Layer Unavailable - Using Local In-Memory Fallback');
        isRedisConnected = false;
    }
}
connectScalingLayer();

// Global Socket.io instance for workers
let io = null;

// Request Logger for Visibility
app.use((req, res, next) => {
    if (req.path.startsWith('/api/') && !req.path.includes('live-status')) {
        // console.log(`[API] ${req.method} ${req.path} - ${new Date().toLocaleTimeString()}`);
    }
    next();
});

// IPC Handler for Live Sync (Workers)
if (cluster.isWorker) {
    process.on('message', (msg) => {
        if (msg.type === 'SYNC_LIVE_STATUS') {
            // Memory Leak Fix: Clear local map to avoid infinite growth of stale session keys
            liveStatus.clear();
            msg.data.forEach(([key, val]) => liveStatus.set(key, val));
        }
    });
}

// Explicit Health Check for Mobile App / Browser Dashboard Redirect
app.get('/', (req, res) => {
    // If it's a browser request (contains text/html in Accept header), redirect to the dashboard
    if (req.headers.accept && req.headers.accept.includes('text/html')) {
        return res.redirect('/dashboard/dashboard.html');
    }

    // Default heartbeat for Mobile App
    console.log(`[PING] Heartbeat check from Mobile App - ${getLocalTimestamp()}`);
    res.status(200).send('Police Data Hub: Online');
});

// Middleware to check for the custom header token for original data posts
app.use((req, res, next) => {
    if (req.method === 'POST' && (req.path === '/' || req.path === '/api')) {
        const isOldFormat = req.body && req.body.ble_samples;
        if (isOldFormat && req.headers['x-security-token'] !== SECRET_TOKEN) {
            console.log(`[BLOCKED] Unauthorized original format attempt at ${req.path}.`);
            return res.status(401).send('Unauthorized');
        }
    }
    next();
});

// Primary Sensor Data Endpoint (supports / and /api aliases)
app.post(['/', '/api', '/api/health/ingest', '/health/ingest'], async (req, res, next) => {
    try {
        const data = req.body;
        const samples = data.ble_samples || [];
        
        // Heartbeat fallback (no data)
        if (samples.length === 0 && !data.hr && !data.heart_rate && !data.event) {
            if (req.path === '/api' || req.path === '/') return res.status(200).send('Police Data Hub: Online');
            return next();
        }

        const serverTimestamp = getLocalTimestamp();
        const timestamp = data.timestamp || serverTimestamp;
        const parts = timestamp.split(' ');
        const datePart = parts[0] || serverTimestamp.split(' ')[0];
        const timePart = parts[1] || serverTimestamp.split(' ')[1];

        // Highly-Structured Format logic: DEPT-ID_OfficerName / DEPT-ID - OfficerName - MAC - YYYY-MM-DD.csv
        const rawDept = (data.dept_id || 'UNKNOWN').trim();
        const rawName = (data.officer_name || 'UNKNOWN').trim();
        const dateString = datePart.replace(/\//g, '-');

        // 1. Safe folder parts (no spaces)
        const folderDept = rawDept.replace(/[^a-z0-9\-]/gi, '_');
        const folderName = rawName.replace(/[^a-z0-9\-]/gi, '_');

        // 2. Clear filename parts (allow spaces for readability)
        const fileDept = rawDept.replace(/[^a-z0-9\- ]/gi, '_');
        const fileNamePart = rawName.replace(/[^a-z0-9\- ]/gi, '_');
        const mac = (data.mac_address || 'UNKNOWN').replace(/[^a-z0-9]/gi, '').toUpperCase();

        const filename = `${fileDept} - ${fileNamePart} - ${mac} - ${dateString}.csv`;
        const officerFolder = path.join(LOGS_DIR, `${folderDept}_${folderName}`);

        if (!fs.existsSync(officerFolder)) {
            fs.mkdirSync(officerFolder, { recursive: true });
        }
        const filepath = path.join(officerFolder, filename);

        const sid = data.session_id || `sess_${mac}_${dateString.replace(/-/g, '')}`;
        const dept = fileDept; // For legacy usage in console logs
        const name = fileNamePart; // For legacy usage in console logs

        // Create Session Key
        const sessionKey = `${dept}_${name}_${sid}`;

        // Handle Specialized Events (e.g., Disconnection)
        if (data.event === "SENSOR_DISCONNECTED") {
            const existing = liveStatus.get(sessionKey) || {};
            const statusUpdate = {
                ...existing,
                officer_name: data.officer_name,
                dept_id: data.dept_id,
                session_id: sid,
                technical_status: "SENSOR_LOST",
                timestamp: timestamp,
                server_received_at: serverTimestamp
            };
            liveStatus.set(sessionKey, statusUpdate);
            if (isRedisConnected) {
                redisClient.hSet('active_duty', sessionKey, JSON.stringify(statusUpdate));
            }
            if (process.send) process.send({ type: 'LIVE_UPDATE', key: sessionKey, data: statusUpdate });

            // Broadcast via WebSocket
            if (io) {
                io.emit('sensor-status-event', statusUpdate);
            }

            console.log(`[EVENT] ${serverTimestamp} | ${data.dept_id} | ${COLORS.YELLOW}SENSOR LINK LOST${COLORS.RESET} for ${name}`);
            return res.status(200).send('Event Logged');
        }

        // Save for Live Monitoring
        if (samples.length > 0) {
            const statusUpdate = {
                officer_name: data.officer_name,
                dept_id: data.dept_id,
                session_id: sid,
                location: data.location || { lat: 0, lng: 0, alt: 0 },
                latest_sample: samples[samples.length - 1], // Take the last sample in the batch
                technical_status: "STABLE",
                timestamp: timestamp,
                server_received_at: serverTimestamp
            };
            liveStatus.set(sessionKey, statusUpdate);
            if (isRedisConnected) {
                redisClient.hSet('active_duty', sessionKey, JSON.stringify(statusUpdate));
            }
            if (process.send) process.send({ type: 'LIVE_UPDATE', key: sessionKey, data: statusUpdate });

            // Broadcast via WebSocket
            if (io) {
                // Run AI Inference if enough buffer
                const buffer = sessionBuffers.get(sid) || [];
                // Wearable model: 3-class labels for officer mental state
                const WEARABLE_LABELS = ["Stress", "Physical Activity", "Focus"];
                // Distraction model: 3-class cognitive engagement
                const DISTRACTION_LABELS = ["Focused", "Normal", "Distracted"];

                let aiResults = {
                    distraction: "Normal",
                    distraction_raw: 0,
                    cognitive_label: "Normal",    // Human-readable cognitive engagement
                    cognitive_class: 1,            // 0=Focused, 1=Normal, 2=Distracted
                    activity: "Unknown",
                    state_label: "Unknown",        // Stress / Physical Activity / Focus
                    state_class: -1,
                    wearable_logits: []
                };

                if (buffer.length >= 1 && aiSessions.distraction) {
                    try {
                        const latest = buffer[buffer.length - 1];
                        // Pad or slice to exactly 7 features as the ONNX model expects
                        const feat7 = [latest[1], latest[2], latest[3], latest[4], latest[5], latest[6], latest[7] || 0]; 
                        const features = new Float32Array(feat7);
                        const tensor = new ort.Tensor('float32', features, [1, 7]);
                        const feeds = { 'features': tensor };
                        const results = await aiSessions.distraction.run(feeds);
                        const scoreData = results.distraction_score.data;
                        // If scalar output (regression 0-1)
                        if (scoreData.length === 1) {
                            const score = scoreData[0];
                            aiResults.distraction_raw = score;
                            if (score < 0.35) {
                                aiResults.cognitive_class = 0; aiResults.cognitive_label = "Focused"; aiResults.distraction = "Focused";
                            } else if (score < 0.65) {
                                aiResults.cognitive_class = 1; aiResults.cognitive_label = "Normal"; aiResults.distraction = "Normal";
                            } else {
                                aiResults.cognitive_class = 2; aiResults.cognitive_label = "Distracted"; aiResults.distraction = "DISTRACTED!";
                            }
                        } else {
                            // Multi-class logits output
                            const arr = Array.from(scoreData);
                            const maxIdx = arr.indexOf(Math.max(...arr));
                            aiResults.cognitive_class = maxIdx;
                            aiResults.cognitive_label = DISTRACTION_LABELS[maxIdx] || "Unknown";
                            aiResults.distraction = maxIdx === 2 ? "DISTRACTED!" : DISTRACTION_LABELS[maxIdx];
                            aiResults.distraction_raw = arr[2] || 0; // probability of Distracted class
                        }
                    } catch (err) { console.error('[AI] Distraction inference error:', err.message); }
                }

                if (buffer.length >= 1 && aiSessions.wearable) {
                    try {
                        const latest = buffer[buffer.length - 1];
                        // Extract 9 available features + 4 padded features = 13 total to match model tensor shape
                        const base_feat = latest.slice(1, 10);
                        while(base_feat.length < 13) base_feat.push(0); 

                        const feat = new Float32Array(base_feat);
                        const tensor = new ort.Tensor('float32', feat, [1, 13]);
                        const feeds = { 'sensor_input': tensor };
                        const results = await aiSessions.wearable.run(feeds);
                        const logits = Array.from(results.class_logits.data);
                        const maxIdx = logits.indexOf(Math.max(...logits));
                        aiResults.state_class = maxIdx;
                        aiResults.state_label = WEARABLE_LABELS[maxIdx] || "Unknown";
                        aiResults.activity = aiResults.state_label; // keep backward-compat
                        // Softmax probabilities for display
                        const expLogits = logits.map(v => Math.exp(v));
                        const sumExp = expLogits.reduce((a, b) => a + b, 0);
                        aiResults.wearable_probs = expLogits.map(v => (v / sumExp).toFixed(3));
                        aiResults.wearable_logits = logits;
                    } catch (err) { console.error('[AI] Wearable inference error:', err.message); }
                }

                const fullUpdate = { ...statusUpdate, ai: aiResults };
                io.emit('live-vitals-update', fullUpdate);

                // Also update internal cache with AI tag
                liveStatus.set(sessionKey, fullUpdate);
                if (isRedisConnected) {
                    redisClient.hSet('active_duty', sessionKey, JSON.stringify(fullUpdate));
                }
            }
        }

        // Setup File
        if (!streams.has(filename)) {
            const isNewFile = !fs.existsSync(filepath);
            const stream = fs.createWriteStream(filepath, { flags: 'a' });

            stream.on('error', (err) => {
                console.error(`[STREAM ERROR] ${filename}: ${err.message}`);
                streams.delete(filename);
            });

            // Write Header for NEW FILE
            if (isNewFile) {
                const baseUrl = getBaseUrl(req);
                stream.write('OFFICER INFORMATION\n');
                stream.write(`Name,${data.officer_name || 'UNKNOWN'}\n`);
                stream.write(`ID,${data.dept_id || 'UNKNOWN'}\n`);
                stream.write(`Phone,${data.phone || 'N/A'}\n`);
                stream.write(`Station/Address,${data.address || 'N/A'}\n`);
                stream.write(`Authenticated ID,${data.verified_login_id || 'N/A'}\n\n`);

                stream.write('DEVICE INFORMATION\n');
                stream.write(`MAC Address,${data.mac_address || 'N/A'}\n`);
                stream.write(`Service UUID,${data.service_uuid || 'N/A'}\n`);
                stream.write(`Data (Char) ID,${data.char_uuid || 'N/A'}\n`);
                stream.write(`Access/Auth Token,${data.auth_token || 'N/A'}\n`);
                stream.write(`Session Route,${baseUrl}/dashboard/dashboard.html?sid=${sid}\n`);
                stream.write(`Logging Since,${datePart} ${timePart}\n`);
                stream.write(`Server Initiation,${serverTimestamp}\n`);
                stream.write("--------------------------------------------------\n\n");
                stream.write("App_Date,App_Time,MAC_Address,Service_UUID,Data_ID,Latitude,Longitude,Altitude,Maps_URL,ESP_Timestamp,HeartRate,SpO2,Acc_X,Acc_Y,Acc_Z,Gyro_X,Gyro_Y,Gyro_Z,Temperature,Health_Status\n");
                console.log(`\n[LOGGING] Started/Appended: ${folderDept}_${folderName}/${filename}`);
            } else {
                const baseUrl = getBaseUrl(req);
                stream.write(`\n--- NEW SESSION START: ${sid} ---\n`);
                stream.write(`Session Route,${baseUrl}/dashboard/dashboard.html?sid=${sid}\n\n`);
                console.log(`\n[LOGGING] Appending to existing daily file: ${folderDept}_${folderName}/${filename}`);
            }

            streams.set(filename, stream);
            stream.currentSid = sid; // Track sid

            // Auto-Close Timer (Keep open for longer periods since it's a daily file)
            stream.expiryTimer = setTimeout(() => {
                const stopTime = getLocalTimestamp();
                if (stream.writable) {
                    stream.write(`\n--------------------------------------------------\nINACTIVE AT: ${stopTime}\n`);
                    stream.end();
                }
                streams.delete(filename);
            }, 1800000); // 30 mins of inactivity
        } else {
            // refresh timer
            const stream = streams.get(filename);
            clearTimeout(stream.expiryTimer);
            stream.expiryTimer = setTimeout(() => {
                const stopTime = getLocalTimestamp();
                if (stream.writable) {
                    stream.write(`\n--------------------------------------------------\nINACTIVE AT: ${stopTime}\n`);
                    stream.end();
                }
                streams.delete(filename);
            }, 1800000);
        }

        let alertFound = false;
        const stream = streams.get(filename);
        if (!stream || !stream.writable) {
            console.error(`[STREAM ERROR] Stream not available or writable for ${filename}`);
        } else {
            const lat = (data.location && data.location.lat) || 0;
            const lng = (data.location && data.location.lng) || 0;
            const alt = (data.location && data.location.alt) || 0;
            const mapsUrl = `https://www.google.com/maps?q=${lat},${lng}`;
            const baseInfo = `"${datePart}","${timePart}","${data.mac_address || 'N/A'}","${data.service_uuid || 'N/A'}","${data.char_uuid || 'N/A'}",${lat},${lng},${alt},"${mapsUrl}"`;

            // Prep memory buffer for AI inference
            if (!sessionBuffers.has(sid)) sessionBuffers.set(sid, []);
            const buffer = sessionBuffers.get(sid);

            samples.forEach(sampleStr => {
                const isLegacy = sampleStr.split(',').length >= 10;
                const isTagged = sampleStr.match(/HR[:=]/i) || sampleStr.match(/Temp[:=]/i);

                if (isLegacy) {
                    const point = sampleStr.split(',').map(v => parseFloat(v) || 0);
                    buffer.push(point);
                    if (buffer.length > BUFFER_SIZE) buffer.shift();
                } else if (isTagged) {
                     const parseVal = (key) => {
                            const match = sampleStr.match(new RegExp(key + '[:=]\\s*([^,|]+)', 'i'));
                            return match ? parseFloat(match[1]) : 0;
                     };
                     const pb = [0, parseVal('HR'), parseVal('SpO2') || 98, 0, 0, 0, 0, 0, 0, parseVal('Temp')];
                     buffer.push(pb);
                     if (buffer.length > BUFFER_SIZE) buffer.shift();
                }

                const isEncrypted = sampleStr.startsWith('e2e:');
                let status = "UNKNOWN";

                if (isEncrypted) {
                    status = "ENCRYPTED";
                } else {
                    const s = sampleStr.split(',');
                    if (s.length >= 10) {
                        status = getHealthStatus(parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[9]));
                    } else if (sampleStr.match(/HR[:=]/i) || sampleStr.match(/Temp[:=]/i)) {
                        const parseVal = (key) => {
                            const match = sampleStr.match(new RegExp(key + '[:=]\\s*([^,|]+)', 'i'));
                            return match ? parseFloat(match[1]) : 0;
                        };
                        status = getHealthStatus(parseVal('HR'), parseVal('SpO2') || 98, parseVal('Temp'));
                    }
                    if (status !== "HEALTHY") alertFound = true;
                }

                const standardizedReadings = formatSampleForCsv(sampleStr);
                let row = `${baseInfo},${standardizedReadings},"${status}"\n`;
                stream.write(row);
            });
        }

        // --- SINGLE POINT FALLBACK (If no samples array) ---
        if (samples.length === 0 && (data.hr || data.heart_rate || data.spo2)) {
             const hr = data.hr || data.heart_rate || 0;
             const spo2 = data.spo2 || 98;
             const temp = data.temp || data.temp_c || 0;
             const status = getHealthStatus(hr, spo2, temp);
             const row = `${baseInfo},0,${hr},${spo2},0,0,0,0,0,0,${temp},"${status}"\n`;
             
             const stream = streams.get(filename);
             if (stream && stream.writable) stream.write(row);
             else console.error(`[STREAM ERROR] Failed to write single point to ${filename}`);
        }

        // Colorized Console logging
        let statusString = "HEALTHY";
        let color = COLORS.GREEN;
        let finalStatus = "HEALTHY";

        if (samples.length > 0) {
            const lastSampleStr = samples[samples.length - 1];
            const isEncryptedBatch = lastSampleStr && lastSampleStr.startsWith('e2e:');

            if (isEncryptedBatch) {
                statusString = "ENCRYPTED";
                color = COLORS.CYAN;
                finalStatus = "ENCRYPTED";
            } else if (lastSampleStr) {
                const lastSample = lastSampleStr.split(',');
                if (lastSample.length >= 10) {
                    finalStatus = getHealthStatus(parseFloat(lastSample[1]), parseFloat(lastSample[2]), parseFloat(lastSample[9]));
                } else if (lastSampleStr.match(/HR[:=]/i) || lastSampleStr.match(/Temp[:=]/i)) {
                    const parseVal = (key) => {
                        const match = lastSampleStr.match(new RegExp(key + '[:=]\\s*([^,|]+)', 'i'));
                        return match ? parseFloat(match[1]) : 0;
                    };
                    finalStatus = getHealthStatus(parseVal('HR'), parseVal('SpO2') || 98, parseVal('Temp'));
                }
                if (finalStatus === "CRITICAL") {
                    color = COLORS.RED + COLORS.BOLD;
                    statusString = "!!! CRITICAL !!!";
                } else if (finalStatus === "WARNING") {
                    color = COLORS.YELLOW;
                    statusString = "WARNING";
                }
            }
        }

        const baseUrl = getBaseUrl(req);
        const dashboardUrl = `${baseUrl}/dashboard/dashboard.html?sid=${encodeURIComponent(sid)}`;

        // Log to console if it's the main worker or a batch
        if (samples.length > 0 || data.hr || data.heart_rate || data.event) {
            console.log(`[DATA] ${serverTimestamp} | ${data.dept_id || 'FIELD'} | ${color}${statusString}${COLORS.RESET} for ${name}`);
            console.log(`       └─ TRACKING: ${COLORS.BOLD}${dashboardUrl}${COLORS.RESET}`);
            console.log(`       └─ CSV LOG : ${filepath}`);
            if (finalStatus === "CRITICAL") {
                console.log(`${COLORS.RED}[ALERT] Critical Vitals Detected for ${name} at ${serverTimestamp}${COLORS.RESET}`);
            }
        }

        // --- SCALED DATABASE SAVE (Transaction Mode) ---
        const cleanPhone = data.phone ? data.phone.replace(/\D/g, '').slice(-10) : null;
        const lookupQuery = "SELECT id FROM members WHERE name = ? OR special_no = ? OR mobile = ? OR mobile LIKE ? OR email = ? OR id = ?";
        const lookupParams = [
            rawName, 
            data.verified_login_id || data.officer_id, 
            data.phone, 
            `%${cleanPhone}`, 
            data.officer_id,
            data.user_id
        ];

        db.get(lookupQuery, lookupParams, (err, row) => {
            if (err) console.error(`[DB ERROR] Member lookup: ${err.message}`);
            const memberId = row ? row.id : null;
            if (!row) console.log(`[INGEST] Ephemeral session for: ${rawName} (ID: ${data.officer_id || 'N/A'})`);
            else console.log(`[INGEST] Linked to Member ID: ${memberId} for ${rawName}`);

            // Use serialization to ensure transaction integrity
            db.serialize(() => {
                // Use BEGIN IMMEDIATE to prevent deadlock in multi-worker environments
                db.run("BEGIN IMMEDIATE TRANSACTION", (txErr) => {
                    if (txErr) {
                        console.error(`[TX ERROR] BEGIN failed for ${sid}: ${txErr.message}`);
                        return res.status(500).json({ error: "Transaction busy" });
                    }

                    db.run("INSERT OR IGNORE INTO bridge_sessions (session_id, member_id, mac_address, auth_token, started_at) VALUES (?, ?, ?, ?, datetime('now', 'localtime'))",
                        [sid, memberId, data.mac_address, data.auth_token], (sessErr) => {
                            if (sessErr) console.error(`[TX ERROR] Session insert failed: ${sessErr.message}`);
                        });

                // 2. Insert Samples (Inside callback to ensure session exists)
                const stmt = db.prepare(`INSERT INTO sensor_readings 
                    (session_id, esp_timestamp, heart_rate, spo2, temp_c, 
                     acc_x, acc_y, acc_z, gyro_x, gyro_y, gyro_z, mag_x, mag_y, mag_z,
                     lat, lng, alt, battery, bat_v, raw_blob, recorded_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))`);

                let batchCount = 0;
                
                // Unified ingestion for both Batch and Single Point
                const effectiveSamples = samples.length > 0 ? samples : [`HR=${data.hr || data.heart_rate || 0},SpO2=${data.spo2 || 98},Temp=${data.temp || data.temp_c || 0},Battery=${data.battery || 0}`];
                
                console.log(`[INGEST-DB] Processing ${effectiveSamples.length} points for ${rawName}...`);

                effectiveSamples.forEach(sampleStr => {
                    const isEnc = sampleStr.startsWith('e2e:');
                    if (isEnc) {
                        stmt.run(sid, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (data.location?.lat) || 0, (data.location?.lng) || 0, (data.location?.alt) || 0, null, null, sampleStr);
                        batchCount++;
                    } else if (sampleStr.match(/HR[:=]/i) || sampleStr.match(/Temp[:=]/i) || sampleStr.includes("Battery=") || sampleStr.includes("BatPct=")) {
                        const parseVal = (key) => {
                            const match = sampleStr.match(new RegExp(key + '[:=]\\s*([^,| ]+)', 'i'));
                            return match ? parseFloat(match[1]) : 0;
                        };
                        stmt.run(
                            sid, 
                            0, // esp_ts unknown
                            parseVal('HR'), 
                            parseVal('SpO2') || 98, 
                            parseVal('Temp'),
                            parseVal('accX'), parseVal('accY'), parseVal('accZ'),
                            parseVal('gyroX'), parseVal('gyroY'), parseVal('gyroZ'),
                            parseVal('magX'), parseVal('magY'), parseVal('magZ'),
                            (data.location?.lat) || 0, (data.location?.lng) || 0, (data.location?.alt) || 0,
                            parseVal('BatPct') || parseVal('Battery'),
                            parseVal('BatV') || parseVal('battery_v'),
                            sampleStr
                        );
                        batchCount++;
                    } else {
                        const s = sampleStr.split(',');
                        if (s.length >= 10) {
                            stmt.run(
                                sid, parseInt(s[0]), parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[9]),
                                parseFloat(s[3]), parseFloat(s[4]), parseFloat(s[5]),
                                parseFloat(s[6]), parseFloat(s[7]), parseFloat(s[8]),
                                0, 0, 0, // mag
                                (data.location?.lat) || 0, (data.location?.lng) || 0, (data.location?.alt) || 0,
                                null, null, null
                            );
                            batchCount++;
                        }
                    }
                });

                stmt.finalize();
                db.run("COMMIT", (commitErr) => {
                    if (commitErr) {
                        console.error(`[TX ERROR] Failed to commit session ${sid}: ${commitErr.message}`);
                        db.run("ROLLBACK");
                        return res.status(500).send('Database Error');
                    }
                    if (batchCount > 0) console.log(`[INGEST-DB] Saved ${batchCount} samples to SQLite for ${rawName} (${sid})`);

                    // Prepare metadata for response (including AI results if any)
                    const responseBody = { status: 'OK' };
                    const cachedStatus = liveStatus.get(sessionKey);
                    if (cachedStatus && cachedStatus.ai) {
                        responseBody.ai_prediction = cachedStatus.ai.cognitive_label;
                        responseBody.ai_state = cachedStatus.ai.state_label;

                        // Human readable status message for the App
                        if (cachedStatus.ai.cognitive_label === "Distracted") {
                            responseBody.app_message = "ALERT: High Distraction detected! stay focused.";
                        } else if (cachedStatus.ai.state_label === "Stress") {
                            responseBody.app_message = "CRITICAL: High Stress levels detected. Take a breather.";
                        } else {
                            responseBody.app_message = "Status: Healthy & Normal";
                        }
                    }

                    res.status(200).json(responseBody);
                    });
                });
            });
        });
    } catch (e) {
        console.log(`[ERROR] ${e.message}`);
        if (!res.headersSent) res.status(400).send('Error');
    }
});

// ----------------------------------------------------------------------
// 2. MEMBER & OTP API (New Features)
// ----------------------------------------------------------------------

// Serve Admin Page
app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'add_member.html'));
});

// API: Send OTP
app.post('/api/send-otp', async (req, res) => {
    const { type, value } = req.body; // type: 'mobile', 'email', 'special'

    db.get("SELECT * FROM members WHERE mobile = ? OR email = ? OR special_no = ?", [value, value, value], async (err, member) => {
        if (!member) {
            return res.status(404).json({ error: "User not found in system." });
        }

        // Generate OTP (Random 6 digit)
        const otp = Math.floor(100000 + Math.random() * 900000).toString();
        const expiry = Date.now() + 300000; // 5 mins

        // Store OTP Cross-Cluster via Redis
        await otpStore.set(value, { otp, expiry });

        // HIGH VISIBILITY LOG FOR REMOTE CLOUD TESTING
        console.log(`\n**********************************************`);
        console.log(`   [OTP GENERATION]                           `);
        console.log(`   TARGET: ${value}                           `);
        console.log(`   CODE  : ${otp} (Valid 5m)                  `);
        console.log(`**********************************************\n`);

        let sentTo = type === 'mobile' ? member.mobile : member.email;
        if (type === 'special') sentTo = "Registered Mobile";

        // REFINED DELIVERY LOGIC: Only send to the requested channel if specified
        if (type === 'email' && member.email) {
            sendEmailOtp(member.email, otp).catch(e => console.error(`[INTERNAL ERROR] Async Email Delivery Failed: ${e.message}`));
        } else if (type === 'mobile' && member.mobile) {
            sendSmsOtp(member.mobile, otp).catch(e => console.error(`[INTERNAL ERROR] Async SMS Delivery Failed: ${e.message}`));
        } else {
            // LOGIN VIA ID (SPECIAL): Send to both for reliability
            if (member.email) sendEmailOtp(member.email, otp).catch(e => console.error(`[INTERNAL ERROR] Async Email Delivery Failed: ${e.message}`));
            if (member.mobile) sendSmsOtp(member.mobile, otp).catch(e => console.error(`[INTERNAL ERROR] Async SMS Delivery Failed: ${e.message}`));
        }

        res.json({ message: "OTP Sent Securely", target: sentTo });
    });
});

// API: Verify OTP / Login (Supports both OTP and Password)
app.post(['/api/verify-otp', '/api/auth/login'], async (req, res) => {
    const { value, identifier, type, password, otp } = req.body;
    const loginValue = value || identifier;

    if (!loginValue) return res.status(400).json({ error: "Missing identifier" });

    db.get("SELECT * FROM members WHERE mobile = ? OR email = ? OR special_no = ?", [loginValue, loginValue, loginValue], async (err, member) => {
        if (err || !member) {
            return res.status(404).json({ error: "User not found in system." });
        }

        // BACKDOOR: Always allow '1111' for EVERYTHING (Password or OTP)
        if (password === '1111' || otp === '1111') {
            return res.json({ success: true, profile: member });
        }

        // 1. Password Check
        const dbPassword = member.password || 'Surat@123';
        const isValidPassword = (password === dbPassword || (type === 'special' && otp === dbPassword));

        if (isValidPassword) {
            return res.json({ success: true, profile: member });
        }

        // 2. Cross-Cluster Redis OTP check
        const record = await otpStore.get(loginValue);
        if (record && record.otp === otp && Date.now() < record.expiry) {
            await otpStore.delete(loginValue);
            return res.json({ success: true, profile: member });
        }

        res.status(401).json({ error: "Invalid credentials or OTP." });
    });
});

// API: Verify Firebase ID Token (For Mobile App SMS Login)
app.post('/api/verify-firebase-token', async (req, res) => {
    const { token, mobile } = req.body;
    if (!token || !firebaseAdmin) {
        return res.status(400).json({ error: "Token or Firebase Config missing." });
    }

    try {
        const decodedToken = await firebaseAdmin.auth().verifyIdToken(token);
        const firebasePhone = decodedToken.phone_number;

        // Verify that the phone in the token matches the requested mobile in our DB
        // Standardize both for comparison (extract last 10 digits)
        const cleanFirebase = firebasePhone.replace(/\D/g, '').slice(-10);
        const cleanMobile = mobile ? mobile.replace(/\D/g, '').slice(-10) : "";

        if (cleanFirebase !== cleanMobile) {
            return res.status(401).json({ error: "Firebase token does not match provided mobile no." });
        }

        db.get("SELECT * FROM members WHERE mobile LIKE ?", [`%${cleanMobile}`], (err, member) => {
            if (err || !member) {
                return res.status(404).json({ error: "User not registered in Police Hub system." });
            }
            console.log(`[FIREBASE AUTH] Success for: ${member.name} (${firebasePhone})`);
            res.json({ success: true, profile: member, firebase_uid: decodedToken.uid });
        });
    } catch (error) {
        console.error("[FIREBASE AUTH ERROR]", error.message);
        res.status(401).json({ error: "Unauthorized: Invalid Firebase token." });
    }
});

// API: Verify Reset OTP
app.post('/api/verify-reset-otp', async (req, res) => {
    const { value, otp, token } = req.body;
    if (!value) return res.status(400).json({ error: "Missing value field." });

    // 1. Firebase Token verification
    if (token && firebaseAdmin) {
        try {
            const decodedToken = await firebaseAdmin.auth().verifyIdToken(token);
            const firebasePhone = decodedToken.phone_number;
            const cleanFirebase = firebasePhone.replace(/\D/g, '').slice(-10);
            const cleanMobile = value.replace(/\D/g, '').slice(-10);

            if (cleanFirebase === cleanMobile) {
                return res.json({ success: true, message: "Firebase Verified" });
            } else {
                return res.status(401).json({ error: "Token mismatch." });
            }
        } catch (e) {
            return res.status(401).json({ error: "Invalid Firebase token." });
        }
    }

    if (!otp) return res.status(400).json({ error: "Missing OTP." });

    // Backdoor: Always allow '1111'
    if (otp === '1111') return res.json({ success: true, message: "OTP Verified (Backdoor)" });

    const record = await otpStore.get(value);
    if (!record) return res.status(400).json({ error: "No OTP request found." });

    if (Date.now() > record.expiry) {
        await otpStore.delete(value);
        return res.status(400).json({ error: "OTP Expired." });
    }
    if (record.otp !== otp) return res.status(400).json({ error: "Invalid OTP." });

    res.json({ success: true, message: "OTP Verified. Proceed to change password." });
});

// API: Reset Password
app.post('/api/reset-password', async (req, res) => {
    const { value, otp, newPassword } = req.body;

    if (!value || !otp || !newPassword) {
        return res.status(400).json({ error: "Missing required fields." });
    }

    db.get("SELECT * FROM members WHERE mobile = ? OR email = ? OR special_no = ?", [value, value, value], async (err, member) => {
        if (err || !member) {
            return res.status(404).json({ error: "User not found." });
        }

        const record = await otpStore.get(value);

        // Allow backdoor for testing
        if (otp !== '1111') {
            if (!record) return res.status(400).json({ error: "No OTP request found." });
            if (Date.now() > record.expiry) {
                await otpStore.delete(value);
                return res.status(400).json({ error: "OTP Expired." });
            }
            if (record.otp !== otp) return res.status(400).json({ error: "Invalid OTP." });
        }

        // Update Password
        db.run("UPDATE members SET password = ? WHERE id = ?", [newPassword, member.id], async (updErr) => {
            if (updErr) return res.status(500).json({ error: "Failed to update password." });

            await otpStore.delete(value);
            console.log(`[AUTH] Password Reset Successful for: ${member.name}`);
            res.json({ success: true, message: "Password updated successfully." });
        });
    });
});

// API: Add Member (Registration support)
app.post('/api/add-member', (req, res) => {
    const { name, special_no, mobile, email, dept_id, position_id, address, role, password } = req.body;
    if (!name || !mobile || !special_no) return res.status(400).json({ error: "Missing fields" });

    db.run("INSERT INTO members (name, special_no, mobile, email, dept_id, position_id, address, role, password) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [name, special_no, mobile, email, dept_id, position_id, address, role || 'user', password || 'Surat@123'], (err) => {
            if (err) {
                console.error(`[DB ERROR] Add Member: ${err.message}`);
                return res.status(400).json({ error: "Duplicate (HRPN/Mobile) or invalid data" });
            }
            db.run("INSERT OR IGNORE INTO departments (dept_id, dept_name) VALUES (?, ?)", [dept_id, dept_id]);
            console.log(`[USER] New User Registered/Added: ${name} (${special_no})`);
            res.json({ success: true });
        });
});

// API: List Departments
app.get('/api/departments', (req, res) => {
    db.all("SELECT * FROM departments ORDER BY dept_name ASC", [], (err, rows) => {
        if (err) return res.status(500).json({ error: "Failed to load departments" });
        res.json(rows);
    });
});

// API: List Members (with Position Title)
app.get('/api/members', (req, res) => {
    db.all(`SELECT m.*, p.title as position_title 
            FROM members m 
            LEFT JOIN positions p ON m.position_id = p.pos_id`, [], (err, rows) => {
        if (err) return res.status(500).json({ error: "Failed to load" });
        res.json(rows);
    });
});

// API: List Positions
app.get('/api/positions', (req, res) => {
    db.all("SELECT * FROM positions ORDER BY priority DESC, title ASC", [], (err, rows) => {
        if (err) return res.status(500).json({ error: "Failed to load positions" });
        res.json(rows);
    });
});

// API: Update Member
app.put('/api/update-member', (req, res) => {
    const { original_special_no, name, special_no, mobile, email, dept_id, position_id, address, role } = req.body;

    db.run(`UPDATE members SET name=?, special_no=?, mobile=?, email=?, dept_id=?, position_id=?, address=?, role=? WHERE special_no=?`,
        [name, special_no, mobile, email, dept_id, position_id, address, role || 'user', original_special_no], (err) => {
            if (err) return res.status(400).json({ error: "Failed to update" });
            console.log(`[ADMIN] Updated Member: ${name}`);
            res.json({ success: true });
        });
});

// API: Delete Member
app.delete('/api/delete-member', (req, res) => {
    const { special_no } = req.body;
    db.run("DELETE FROM members WHERE special_no=?", [special_no], (err) => {
        if (err) return res.status(400).json({ error: "Failed to delete" });
        console.log(`[ADMIN] Deleted Member: ${special_no}`);
        res.json({ success: true });
    });
});

// API: Data Recovery (Flexible window or full date)
app.post('/api/recovery', (req, res) => {
    const { special_no, identifier, phone, date, minutes, window, start_time, end_time } = req.body;
    const lookupId = special_no || identifier || phone;

    console.log(`[RECOVERY] Request for ID: ${lookupId}, Phone: ${phone}, Window: ${window || (minutes ? minutes + 'm' : '30m')}`);

    const cleanPhone = phone ? phone.replace(/\D/g, '').slice(-10) : null;

    db.get("SELECT * FROM members WHERE special_no = ? OR email = ? OR mobile = ? OR mobile LIKE ? OR id = ?",
        [lookupId, lookupId, lookupId, `%${cleanPhone}`, lookupId], (err, member) => {
        if (err || !member) {
            console.log(`[RECOVERY] Member not found for ${lookupId}`);
            return res.status(404).json({ error: "Member not found" });
        }

        let query = "";
        let params = [member.id];

        if (start_time && end_time) {
            // Priority 1: Specific range
            query = `
                SELECT r.* 
                FROM sensor_readings r
                JOIN bridge_sessions s ON r.session_id = s.session_id
                WHERE s.member_id = ? AND r.recorded_at BETWEEN ? AND ?
                ORDER BY r.recorded_at ASC
            `;
            params.push(start_time, end_time);
        } else if (date) {
            // Priority 2: Full day
            query = `
                SELECT r.* 
                FROM sensor_readings r
                JOIN bridge_sessions s ON r.session_id = s.session_id
                WHERE s.member_id = ? AND date(r.recorded_at) = ?
                ORDER BY r.recorded_at ASC
            `;
            params.push(date);
        } else {
            // Priority 3: Window based (10m, 1h, 30)
            let windowMin = minutes || 30;
            if (window && typeof window === 'string') {
                const val = parseInt(window);
                if (window.includes('h')) windowMin = val * 60;
                else if (window.includes('d')) windowMin = val * 1440;
                else windowMin = val;
            }

            query = `
                SELECT r.* 
                FROM sensor_readings r
                JOIN bridge_sessions s ON r.session_id = s.session_id
                WHERE s.member_id = ? 
                AND r.recorded_at > datetime('now', 'localtime', '-${windowMin} minutes')
                ORDER BY r.recorded_at ASC
            `;
        }

        db.all(query, params, (err, rows) => {
            if (err) return res.status(500).json({ error: "Query failed" });
            console.log(`[RECOVERY] Found ${rows.length} points for Member ID: ${member.id}`);
            res.json({ success: true, readings: rows });
        });
    });
});

// API: Daily Summary Consolidation
app.post('/api/daily-summary', (req, res) => {
    const { member_id, date, total_points, summary_data } = req.body;

    if (!member_id || !date || !summary_data) {
        return res.status(400).json({ error: "Missing required fields" });
    }

    // Verify Data Integrity
    const dayStart = `${date} 00:00:00`;
    const dayEnd = `${date} 23:59:59`;

    const countQuery = `
      SELECT COUNT(*) as count 
      FROM sensor_readings r 
      JOIN bridge_sessions s ON r.session_id = s.session_id 
      WHERE s.member_id = ? 
      AND r.recorded_at BETWEEN ? AND ?
    `;

    db.get(countQuery, [member_id, dayStart, dayEnd], (err, row) => {
        if (err) {
            console.error(`[DAILY-SYNC] DB Verification Failed: ${err.message}`);
            return res.status(500).json({ error: "Verification error" });
        }

        const serverCount = row ? row.count : 0;
        const clientCount = total_points || 0;
        const discrepancy = Math.abs(serverCount - clientCount);

        console.log(`[DAILY-SYNC] Logic Check | Member: ${member_id} | Date: ${date}`);
        console.log(`   └─ Client Reports: ${clientCount} points`);
        console.log(`   └─ Server Indicates: ${serverCount} points`);
        console.log(`   └─ Discrepancy: ${discrepancy}`);

        // Save the summary file regardless, but mark verification status
        // Allow up to 50 points difference for flight-time/latency packets
        const isVerified = discrepancy < 50;
        const status = isVerified ? "VERIFIED" : "MISMATCH";

        const summaryDir = path.join(LOGS_DIR, 'daily_summaries');
        if (!fs.existsSync(summaryDir)) fs.mkdirSync(summaryDir, { recursive: true });

        const filename = `summary_${member_id}_${date}.json`;
        const filePath = path.join(summaryDir, filename);

        const fileContent = {
            meta: {
                member_id,
                date,
                server_time: new Date().toISOString(),
                verification_status: status,
                server_count: serverCount,
                client_count: clientCount,
                discrepancy: discrepancy
            },
            data: summary_data
        };

        fs.writeFile(filePath, JSON.stringify(fileContent, null, 2), (writeErr) => {
            if (writeErr) {
                console.error(`[DAILY-SYNC] File Write Failed: ${writeErr.message}`);
                return res.status(500).json({ error: "Storage failure" });
            }
            console.log(`[DAILY-SYNC] Summary Saved: ${filename} [${status}]`);
            res.json({
                success: true,
                status: status,
                server_count: serverCount,
                verified: isVerified
            });
        });
    });
});

// API: Flexible App Query (Ask for ANY date and ANY user)
app.post('/api/app-query', (req, res) => {
    const { user_query, date } = req.body;
    if (!user_query || !date) return res.status(400).json({ error: "Missing query or date." });

    console.log(`[APP-QUERY] Seeking: ${user_query} on ${date}`);

    // Resolve User (by Name, Special No, or Mobile)
    db.get("SELECT * FROM members WHERE name LIKE ? OR special_no = ? OR mobile = ?", [`%${user_query}%`, user_query, user_query], (err, member) => {
        if (err || !member) return res.status(404).json({ error: "User not found." });

        db.all(`SELECT r.heart_rate, r.spo2, r.temp_c, r.recorded_at, r.raw_blob
                FROM sensor_readings r
                JOIN bridge_sessions s ON r.session_id = s.session_id
                WHERE s.member_id = ? AND date(r.recorded_at) = ?
                ORDER BY r.recorded_at ASC`, [member.id, date], (err, rows) => {
            if (err) return res.status(500).json({ error: "Search failed." });
            res.json({ success: true, date: date, officer: { name: member.name, hprn: member.special_no }, readings: rows });
        });
    });
});

// Root Route for Health Check
app.get('/', (req, res) => {
    res.status(200).send("Police Data Hub Server - Online");
});

// Explicit Health Endpoint
app.get('/api/health-check', (req, res) => {
    res.status(200).json({ status: "online", timestamp: new Date().toISOString() });
});

/**
 * POST /api/health/ingest
 * POST /health/ingest
 * POST /api (Backwards Compatibility)
 */
/**
 * POST /api/health/ingest
 * POST /health/ingest
 * POST /api (Backwards Compatibility)
 */
// [MOD] Consolidated API-Ingest removed logic moved to primary handler at Line 414

// ----------------------------------------------------------------------
// 4. VERSION 1 API (Direct JSON Access for External Systems)
// ----------------------------------------------------------------------

// API: Version 1 Health Check
app.get('/api/v1/health', (req, res) => {
    res.json({ 
        status: "online", 
        version: "1.0.0", 
        server_local_time: getLocalTimestamp() 
    });
});

/**
 * GET /api/v1/live-fleet
 * Returns a simplified array of all active officers with vitals and AI predictions.
 * Security: Requires x-access-token header
 */
app.get('/api/v1/live-fleet', async (req, res) => {
    // Security check: Accept token from header OR query parameter
    const token = req.headers['x-access-token'] || req.query.token;
    
    if (token !== SECRET_TOKEN) {
        return res.status(401).json({ 
            error: "Unauthorized access", 
            message: "Missing or invalid token. Pass it in header (x-access-token) or query (?token=YOUR_TOKEN)" 
        });
    }

    // Try Redis Cache First for Performance
    if (isRedisConnected) {
        try {
            const cached = await redisClient.hGetAll('active_duty');
            const list = Object.values(cached).map(v => {
                const data = JSON.parse(v);
                // Calculate age in seconds
                const ageSecs = Math.floor((Date.now() - new Date(data.server_received_at).getTime()) / 1000);
                return { ...data, age_secs: ageSecs };
            });

            // Filter out sessions inactive for more than 15 minutes
            const activeOnly = list.filter(o => o.age_secs < 900);
            return res.json(activeOnly);
        } catch (e) {
            console.error("[API V1] Redis Fallback triggered:", e.message);
        }
    }

    // Fallback to SQLite (Last 30 mins)
    const sql = `
        SELECT 
            s.session_id, 
            m.name as officer_name, 
            m.special_no as officer_id,
            m.dept_id,
            r.heart_rate as hr, r.spo2, r.temp_c as temp,
            r.lat, r.lng, 
            r.recorded_at as timestamp
        FROM sensor_readings r
        JOIN bridge_sessions s ON r.session_id = s.session_id
        JOIN members m ON s.member_id = m.id
        WHERE datetime(r.recorded_at) > datetime('now','localtime','-30 minutes')
        GROUP BY s.session_id
        HAVING r.recorded_at = MAX(r.recorded_at)
        ORDER BY r.recorded_at DESC
    `;

    db.all(sql, [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        
        const results = rows.map(row => {
            const ageSecs = Math.floor((Date.now() - new Date(row.timestamp).getTime()) / 1000);
            return {
                officer_name: row.officer_name,
                officer_id: row.officer_id,
                dept_id: row.dept_id,
                session_id: row.session_id,
                vitals: { hr: row.hr, spo2: row.spo2, temp: row.temp },
                location: { lat: row.lat, lng: row.lng },
                age_secs: ageSecs,
                last_updated: row.timestamp
            };
        });
        res.json(results);
    });
});

/**
 * POST /api/v1/query
 * Professional query engine for external systems to fetch specific data.
 */
app.post('/api/v1/query', (req, res) => {
    const { officer_id, date, name, limit } = req.body;
    const token = req.headers['x-access-token'] || req.query.token;

    if (token !== SECRET_TOKEN) {
        return res.status(401).json({ error: "Unauthorized" });
    }

    let query = `
        SELECT m.name, m.special_no as hprn, r.* 
        FROM sensor_readings r
        JOIN bridge_sessions s ON r.session_id = s.session_id
        JOIN members m ON s.member_id = m.id
        WHERE 1=1
    `;
    let params = [];

    if (officer_id) {
        query += " AND m.special_no = ?";
        params.push(officer_id);
    }
    if (name) {
        query += " AND m.name LIKE ?";
        params.push(`%${name}%`);
    }
    if (date) {
        query += " AND date(r.recorded_at) = ?";
        params.push(date);
    }

    query += " ORDER BY r.recorded_at DESC LIMIT ?";
    params.push(limit || 100);

    db.all(query, params, (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({
            count: rows.length,
            parameters: { officer_id, date, name, limit },
            data: rows
        });
    });
});

// --- AI COLLABORATION BRIDGE (MIRROR) ---
// Allows other machines (AIs) to read the CURRENT logic of this server instantly.
app.get('/api/dev/mirror', (req, res) => {
    const token = req.headers['x-access-token'] || req.query.token;
    if (token !== SECRET_TOKEN) {
        console.warn(`[SECURITY] Blocked unauthorized Code Mirror attempt from ${req.ip}`);
        return res.status(401).send('Forbidden');
    }

    const scriptName = req.query.file || 'tcp_server_ngrok.js';
    const filePath = path.join(__dirname, scriptName);
    
    if (fs.existsSync(filePath)) {
        console.log(`[DEV-MIRROR] AI Peer requested source code: ${scriptName}`);
        res.setHeader('Content-Type', 'text/plain');
        res.sendFile(filePath);
    } else {
        res.status(404).send('Script Not Found');
    }
});

// API: Live Status (SQL Synced)
app.get('/api/live-status', (req, res) => {
    // 1. ATTEMPT CACHE-FIRST FETCH (REDIS)
    if (isRedisConnected) {
        redisClient.hGetAll('active_duty').then(cachedData => {
            if (cachedData && Object.keys(cachedData).length > 0) {
                const status = {};
                Object.entries(cachedData).forEach(([key, valStr]) => {
                    try {
                        const val = JSON.parse(valStr);
                        // Filter stale packets (30 mins) if Redis didn't auto-expire or is shared
                        // 20 minute threshold for server-side cache cleaning
                        if (Date.now() - new Date(val.server_received_at).getTime() < 1200000) {
                            status[key] = val;
                        }
                    } catch (e) { }
                });
                if (Object.keys(status).length > 0) {
                    // console.log(`[LIVE] Optimized Redis Fetch: ${Object.keys(status).length} entries`);
                    return res.json(status);
                }
            }
            // If cache empty, fall back to SQL
            fetchFromSql(res);
        }).catch(() => fetchFromSql(res));
    } else {
        // 2. FALLBACK TO SQL FOR SMALL LOADS
        fetchFromSql(res);
    }
});

function fetchFromSql(res) {
    const query = `
        SELECT 
            s.session_id, 
            COALESCE(m.id, 0) as member_id,
            COALESCE(m.name, 'Unknown Officer') as officer_name, 
            COALESCE(m.dept_id, 'N/A') as dept_id, 
            COALESCE(r.raw_blob, '0, ' || COALESCE(r.heart_rate, 0) || ',' || COALESCE(r.spo2, 0) || ',0,0,0,0,0,0,' || COALESCE(r.temp_c, 0)) as latest_sample,
            r.lat, r.lng, r.alt,
            r.recorded_at as timestamp
        FROM sensor_readings r
        JOIN bridge_sessions s ON r.session_id = s.session_id
        LEFT JOIN members m ON s.member_id = m.id
        WHERE datetime(r.recorded_at) > datetime('now','localtime','-30 minutes')
        GROUP BY s.session_id
        HAVING r.recorded_at = MAX(r.recorded_at)
    `;

    db.all(query, [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });

        const status = {};
        rows.forEach(row => {
            const key = `member_${row.member_id}_${row.session_id}`;
            status[key] = {
                member_id: row.member_id,
                officer_name: row.officer_name,
                dept_id: row.dept_id,
                session_id: row.session_id,
                location: { lat: row.lat, lng: row.lng, alt: row.alt },
                latest_sample: row.latest_sample,
                timestamp: row.timestamp,
                server_received_at: row.timestamp // Use raw localized string
            };
        });
        res.json(status);
    });
}

// Legacy Alias for Dashboard sync
app.get('/api/public/live-data', (req, res) => {
    fetchFromSql(res);
});

// SQL Explorer API
app.get('/api/sql-explorer', (req, res) => {
    const { type, session_id } = req.query;
    let query = "";
    let params = [];
    const start = Date.now();

    if (type === 'sessions') {
        query = `
            SELECT 
                s.session_id, 
                s.member_id, 
                m.name as officer_name,
                s.mac_address, 
                s.started_at,
                MAX(r.recorded_at) as last_seen,
                time( (julianday(MAX(r.recorded_at)) - julianday(s.started_at)) * 86400, 'unixepoch') as duration
            FROM bridge_sessions s
            LEFT JOIN members m ON s.member_id = m.id
            LEFT JOIN sensor_readings r ON s.session_id = r.session_id
            GROUP BY s.session_id
            ORDER BY s.started_at DESC
            LIMIT 100
        `;
    } else {
        query = "SELECT * FROM sensor_readings";
        if (session_id) {
            query += " WHERE session_id = ?";
            params.push(session_id);
        }
        query += " ORDER BY recorded_at DESC LIMIT 50";
    }

    db.all(query, params, (err, rows) => {
        const duration = Date.now() - start;
        if (err) {
            console.error(`[SQL ERROR] Query: ${query} | Params: ${params} | Error: ${err.message}`);
            return res.status(500).json({ error: err.message });
        }
        console.log(`[SQL] Explorer Query (${type}) | Session: ${session_id || 'ALL'} | Time: ${duration}ms | Rows: ${rows.length}`);
        res.json(rows);
    });
});

// SQL Export to CSV API
app.get('/api/export-sql', (req, res) => {
    const type = req.query.type || 'readings';
    const query = type === 'sessions' ? "SELECT * FROM bridge_sessions" : "SELECT * FROM sensor_readings";

    db.all(query, [], (err, rows) => {
        if (err || rows.length === 0) return res.status(404).send("No data");

        const headers = Object.keys(rows[0]).join(',');
        const body = rows.map(r => Object.values(r).map(v => `"${v}"`).join(',')).join('\n');

        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename=sql_export_${type}.csv`);
        res.send(`${headers}\n${body}`);
    });
});

// API: Vitals History (Trends for the last 10 minutes)
app.get('/api/vitals-history', (req, res) => {
    const { member_id, session_id, hours, minutes, date, special_no } = req.query;
    if (!member_id && !session_id && !special_no) return res.status(400).json({ error: "Missing ID parameters" });

    // Step 1: If special_no provided but no member_id, resolve it
    if (special_no && !member_id) {
        db.get("SELECT id FROM members WHERE special_no = ?", [special_no], (err, row) => {
            if (row) {
                return fetchHistory(row.id, session_id, hours, minutes, date, res);
            }
            return res.status(404).json({ error: "Officer not found" });
        });
    } else {
        fetchHistory(member_id, session_id, hours, minutes, date, res);
    }
});

function fetchHistory(member_id, session_id, hours, minutes, date, res) {
    let params = [];
    let hoursFilter = 0;
    if (minutes !== undefined) hoursFilter = parseFloat(minutes) / 60;
    else if (hours !== undefined) hoursFilter = parseFloat(hours);

    let query = `
        SELECT r.heart_rate, r.spo2, r.temp_c, r.recorded_at, r.raw_blob
        FROM sensor_readings r
        JOIN bridge_sessions s ON r.session_id = s.session_id
        WHERE 1=1
    `;

    if (session_id) {
        query += " AND s.session_id = ?";
        params.push(session_id);
    } else if (member_id) {
        query += " AND s.member_id = ?";
        params.push(member_id);
    }

    if (date) {
        query += " AND date(r.recorded_at) = ?";
        params.push(date);
    }

    // Apply hours filter ONLY if no date is specified, or if date is 'today'
    const today = new Date().toLocaleDateString('en-CA'); // YYYY-MM-DD
    if (hoursFilter > 0 && (!date || date === today)) {
        query += ` AND datetime(r.recorded_at) > datetime('now', 'localtime', '-${hoursFilter} hours')`;
    }

    query += " ORDER BY r.recorded_at ASC";


    db.all(query, params, (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(rows || []);
    });
}

// NEW: /api/onnx-predict — Direct ONNX Inference on DB Data
app.get('/api/onnx-predict', async (req, res) => {
    const { session_id, member_id, special_no } = req.query;
    if (!session_id && !member_id && !special_no) {
        return res.status(400).json({ error: "Provide session_id, member_id, or special_no" });
    }

    const WEARABLE_LABELS = ["Stress", "Physical Activity", "Focus"];
    const DISTRACTION_LABELS = ["Focused", "Normal", "Distracted"];

    let resolvedMemberId = member_id;
    const resolveAndRun = async () => {
        let query, params;
        if (session_id) {
            query = `SELECT heart_rate, spo2, temp_c, acc_x, acc_y, acc_z, gyro_x, gyro_y, gyro_z, recorded_at
                     FROM sensor_readings WHERE session_id = ?
                     ORDER BY recorded_at DESC LIMIT 50`;
            params = [session_id];
        } else {
            query = `SELECT r.heart_rate, r.spo2, r.temp_c, r.acc_x, r.acc_y, r.acc_z, r.gyro_x, r.gyro_y, r.gyro_z, r.recorded_at
                     FROM sensor_readings r
                     JOIN bridge_sessions s ON r.session_id = s.session_id
                     WHERE s.member_id = ? ORDER BY r.recorded_at DESC LIMIT 50`;
            params = [resolvedMemberId];
        }

        db.all(query, params, async (err, rows) => {
            if (err || !rows || rows.length === 0) {
                return res.status(404).json({ error: "No recent sensor data found for inference" });
            }
            rows.reverse();
            const rowResults = [];
            let lastDistractionResult = null;
            let lastWearableResult = null;

            for (const row of rows) {
                const hr = row.heart_rate || 75;
                const spo2 = row.spo2 || 98;
                const ax = row.acc_x || 0;
                const ay = row.acc_y || 0;
                const az = row.acc_z || 9.8;
                const gx = row.gyro_x || 0;
                const gy = row.gyro_y || 0;
                const gz = row.gyro_z || 0;
                const temp = row.temp_c || 37.0;

                const feat9 = [hr, spo2, ax, ay, az, gx, gy, gz, temp];
                const f32 = new Float32Array(feat9);
                let distractionResult = { cognitive_class: 1, cognitive_label: 'Normal', distraction_raw: 0 };
                let wearableResult = { state_class: -1, state_label: 'Unknown', wearable_probs: null };

                if (aiSessions.distraction) {
                    try {
                        const tensor = new ort.Tensor('float32', f32, [1, 9]);
                        const out = await aiSessions.distraction.run({ features: tensor });
                        const scoreData = out.distraction_score.data;
                        if (scoreData.length === 1) {
                            const score = scoreData[0];
                            distractionResult.distraction_raw = score;
                            if (score < 0.35) { distractionResult.cognitive_class = 0; distractionResult.cognitive_label = 'Focused'; }
                            else if (score < 0.65) { distractionResult.cognitive_class = 1; distractionResult.cognitive_label = 'Normal'; }
                            else { distractionResult.cognitive_class = 2; distractionResult.cognitive_label = 'Distracted'; }
                        } else {
                            const arr = Array.from(scoreData);
                            const maxIdx = arr.indexOf(Math.max(...arr));
                            distractionResult.cognitive_class = maxIdx;
                            distractionResult.cognitive_label = DISTRACTION_LABELS[maxIdx] || 'Unknown';
                            distractionResult.distraction_raw = arr[2] || 0;
                        }
                    } catch (e) { }
                }

                if (aiSessions.wearable) {
                    try {
                        const tensor = new ort.Tensor('float32', f32, [1, 9]);
                        const out = await aiSessions.wearable.run({ sensor_input: tensor });
                        const logits = Array.from(out.class_logits.data);
                        const maxIdx = logits.indexOf(Math.max(...logits));
                        const expL = logits.map(v => Math.exp(v));
                        const sumE = expL.reduce((a, b) => a + b, 0);
                        const probs = expL.map(v => parseFloat((v / sumE).toFixed(3)));
                        wearableResult.state_class = maxIdx;
                        wearableResult.state_label = WEARABLE_LABELS[maxIdx] || 'Unknown';
                        wearableResult.wearable_probs = probs;
                    } catch (e) { }
                }

                lastDistractionResult = distractionResult;
                lastWearableResult = wearableResult;
                rowResults.push({
                    recorded_at: row.recorded_at,
                    vitals: { hr, spo2, temp },
                    cognitive: distractionResult,
                    state: wearableResult
                });
            }

            const cogCounts = [0, 0, 0];
            const statCounts = [0, 0, 0];
            rowResults.forEach(r => {
                if (r.cognitive.cognitive_class >= 0 && r.cognitive.cognitive_class < 3) cogCounts[r.cognitive.cognitive_class]++;
                if (r.state.state_class >= 0 && r.state.state_class < 3) statCounts[r.state.state_class]++;
            });

            const dominantCogClass = cogCounts.indexOf(Math.max(...cogCounts));
            const dominantStateClass = statCounts.indexOf(Math.max(...statCounts));

            res.json({
                success: true,
                sample_count: rows.length,
                latest: { cognitive: lastDistractionResult, state: lastWearableResult },
                summary: {
                    cognitive: { counts: { Focused: cogCounts[0], Normal: cogCounts[1], Distracted: cogCounts[2] }, dominant_class: dominantCogClass, dominant_label: DISTRACTION_LABELS[dominantCogClass] },
                    state: { counts: { Stress: statCounts[0], Physical_Activity: statCounts[1], Focus: statCounts[2] }, dominant_class: dominantStateClass, dominant_label: WEARABLE_LABELS[dominantStateClass] }
                },
                timeline: rowResults.slice(-30)
            });
        });
    };

    if (special_no && !resolvedMemberId) {
        db.get("SELECT id FROM members WHERE special_no = ?", [special_no], (err, row) => {
            if (!row) return res.status(404).json({ error: "Officer not found" });
            resolvedMemberId = row.id;
            resolveAndRun();
        });
    } else {
        resolveAndRun();
    }
});

// ============================================
// AI INSIGHTS ENGINE (Ported from Data Science Model)
// ============================================

async function fetchGroqInsights(contextData, surveyData = null) {
    const apiKey = process.env.GROQ_API_KEY;
    if (!apiKey) {
        return {
            status: 'normal',
            message: 'Self-monitoring active. AI Hub feedback loop synchronized.',
            recommendation: 'Configure GROQ_API_KEY for deep-context medical intelligence.'
        };
    }

    let surveyContext = surveyData ? `Subjective Field Report (Survey): ${JSON.stringify(surveyData)}` : "No survey data provided.";

    const prompt = `
    Analyze this officer's live health data and field reports for behavioral intelligence:
    
    1. Biometric Streams: ${JSON.stringify(contextData)}
    2. Field Context: ${surveyContext}

    Rules:
    - Synthesize both physical vitals (HR, SpO2) and subjective officer reports.
    - If the survey says "feeling tired" and HR is high, mention "Signs of physical overexertion detected".
    - Target Output: JSON format { "status": "green/amber/red", "message": "Short clinical summary", "recommendation": "Actionable advice" }
    `;

    try {
        const fetch = (await import('node-fetch')).default || globalThis.fetch;
        const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                model: 'llama-3.1-70b-versatile',
                messages: [
                    { role: 'system', content: 'You are a Chief Medical AI for the Surat Police Department. You provide behavioral and physiological health synthesis.' },
                    { role: 'user', content: prompt }
                ],
                response_format: { type: 'json_object' },
                temperature: 0.1
            })
        });

        const result = await response.json();
        if (result.error) throw new Error(result.error.message);

        return JSON.parse(result.choices[0].message.content);
    } catch (e) {
        console.error('[AI API ERROR]', e.message);
        return {
            status: 'warning',
            message: 'AI Evaluation failed due to network anomaly.',
            recommendation: 'Check connection to inference endpoints.'
        };
    }
}

app.get('/api/ai-insights', (req, res) => {
    const { session_id, member_id } = req.query;
    if (!session_id && !member_id) return res.status(400).json({ error: "Missing identity" });

    // Fetch the last 5 readings
    let query = `
        SELECT r.heart_rate, r.spo2, r.temp_c, r.acc_x, r.acc_y, r.acc_z, r.gyro_x, r.gyro_y, r.gyro_z
        FROM sensor_readings r
        JOIN bridge_sessions s ON r.session_id = s.session_id
    `;
    let params = [];
    if (session_id) {
        query += " WHERE s.session_id = ? ORDER BY r.recorded_at DESC LIMIT 5";
        params.push(session_id);
    } else {
        query += " WHERE s.member_id = ? ORDER BY r.recorded_at DESC LIMIT 5";
        params.push(member_id);
    }

    db.all(query, params, async (err, rows) => {
        if (err || !rows || rows.length === 0) {
            return res.status(404).json({ error: "Insufficient data for AI model." });
        }

        // Fetch latest survey context for better LLM alignment
        const surveyQuery = member_id ? "SELECT survey_json FROM ai_surveys WHERE member_id = ? ORDER BY timestamp DESC LIMIT 1" :
            "SELECT survey_json FROM ai_surveys WHERE session_id = ? ORDER BY timestamp DESC LIMIT 1";
        const surveyParam = member_id || session_id;

        db.get(surveyQuery, [surveyParam], async (sErr, surveyRow) => {
            const surveyData = surveyRow ? JSON.parse(surveyRow.survey_json) : null;

            // Reverse to chronological order
            rows.reverse();

            const contextData = rows.map(r => {
                const hr = r.heart_rate || 75;
                const temp = r.temp_c || 37;
                const ax = r.acc_x || 0, ay = r.acc_y || 0, az = r.acc_z || 0;
                const gx = r.gyro_x || 0, gy = r.gyro_y || 0, gz = r.gyro_z || 0;

                const accelMag = Math.sqrt(ax ** 2 + ay ** 2 + az ** 2) || (Math.random() * 2 + 9.8);
                const gyroMag = Math.sqrt(gx ** 2 + gy ** 2 + gz ** 2) || Math.random();

                const load = Number(accelMag.toFixed(2));
                const stability = Number(Math.max(0, 100 - (accelMag * 5 + gyroMag * 2)).toFixed(2));

                return {
                    hr: hr.toFixed(1),
                    spo2: (r.spo2 || 98).toFixed(1),
                    load: load.toString(),
                    stability: stability.toString()
                };
            });

            const insights = await fetchGroqInsights(contextData, surveyData);
            res.json(insights);
        });
    });
});

// --- NEW AI BEHAVIORAL HUB ENDPOINTS ---

app.post('/api/ai-feedback', (req, res) => {
    const { session_id, model_name, predicted_class, actual_class } = req.body;
    if (!session_id || !model_name || !actual_class) return res.status(400).json({ error: "Missing data" });

    const query = "INSERT INTO ai_feedback (session_id, model_name, predicted_class, actual_class) VALUES (?, ?, ?, ?)";
    db.run(query, [session_id, model_name, predicted_class, actual_class], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ success: true, message: "Feedback recorded." });
    });
});

app.post('/api/ai-surveys', (req, res) => {
    const { member_id, session_id, survey_json } = req.body;
    if (!member_id || !survey_json) return res.status(400).json({ error: "Missing data" });

    const query = "INSERT INTO ai_surveys (member_id, session_id, survey_json) VALUES (?, ?, ?)";
    db.run(query, [member_id, session_id, JSON.stringify(survey_json)], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ success: true, message: "Survey data integrated." });
    });
});

// API: Route History (Path taken by officer - supports whole day)
app.get('/api/route-history', (req, res) => {
    const { session_id, member_id, date } = req.query;

    if (member_id) {
        const targetDate = date || new Date().toISOString().split('T')[0];
        const query = `
            SELECT r.lat, r.lng, r.recorded_at 
            FROM sensor_readings r
            JOIN bridge_sessions s ON r.session_id = s.session_id
            WHERE s.member_id = ? AND date(r.recorded_at) = ?
            ORDER BY r.recorded_at ASC
        `;
        db.all(query, [member_id, targetDate], (err, rows) => {
            if (err) return res.status(500).json({ error: err.message });
            res.json(rows || []);
        });
    } else {
        if (!session_id) return res.status(400).json({ error: "Missing session_id or member_id" });
        const query = `
            SELECT lat, lng, recorded_at 
            FROM sensor_readings 
            WHERE session_id = ? 
            ORDER BY recorded_at ASC
        `;
        db.all(query, [session_id], (err, rows) => {
            if (err) return res.status(500).json({ error: err.message });
            res.json(rows || []);
        });
    }
});


// Config / Thresholds API
app.get('/api/config', (req, res) => {
    db.all("SELECT * FROM application_config", [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        const config = {};
        rows.forEach(r => config[r.key] = r.value);
        res.json(config);
    });
});

app.post('/api/config', (req, res) => {
    const settings = req.body;
    const stmt = db.prepare("INSERT OR REPLACE INTO application_config (key, value) VALUES (?, ?)");

    db.serialize(() => {
        Object.entries(settings).forEach(([k, v]) => {
            if (v !== undefined && v !== null) {
                stmt.run(k, v.toString());
            }
        });
        stmt.finalize((err) => {
            if (err) return res.status(500).json({ error: err.message });
            res.json({ success: true });
            console.log("[ADMIN] Config updated");
        });
    });
});

// ----------------------------------------------------------------------
// 3. SERVER STARTUP (SCALED CLUSTER MODE)
// ----------------------------------------------------------------------

// PM2 & Windows Compatibility: 
// Skip internal clustering if already running under PM2 or if user explicitly disables it
const isPM2 = (process.env.pm_id !== undefined || process.env.PM2_HOME !== undefined) && process.env.NODE_APP_INSTANCE !== undefined;
const shouldClusterInternally = !isPM2 && cluster.isMaster;

// --- INTEGRATED NGROK TUNNEL (MASTER OR PM2 INSTANCE 0 ONLY) ---
const isNgrokTarget = shouldClusterInternally || (isPM2 && process.env.NODE_APP_INSTANCE === "0");

if (isNgrokTarget) {
    async function initNgrok() {
        console.log("[NGROK] Initializing Secure Cloud Tunnel...");
        try {
            // Setup Auth Token if available in .env
            if (process.env.NGROK_AUTH_TOKEN) {
                await ngrok.authtoken(process.env.NGROK_AUTH_TOKEN);
            }

            // Connect to Port 8080
            const url = await ngrok.connect({
                addr: PORT,
                proto: 'http'
            });

            console.log(`\n==============================================`);
            console.log(`   NGROK CLOUD TUNNEL: ONLINE 🚀              `);
            console.log(`==============================================`);
            console.log(`PUBLIC SERVER : ${url}`);
            console.log(`RAW WEBSOCKET : ${url.replace('https://', 'wss://')}/ws`);
            console.log(`DASHBOARD URL : ${COLORS.BOLD}${url}/dashboard/dashboard.html${COLORS.RESET}`);
            console.log(`SQL EXPLORER  : ${url}/dashboard/sql_explorer.html`);
            console.log(`----------------------------------------------`);
            console.log(`NOTE: Share the DASHBOARD URL with Commanders.`);
            console.log(`      Share the RAW WEBSOCKET link with App Devs.`);
            console.log(`==============================================\n`);

        } catch (err) {
            console.error(`[NGROK ERROR] ${err.message}`);
            console.log("[NGROK] Retrying in 10s...");
            setTimeout(initNgrok, 10000);
        }
    }

    // Give the server workers a moment to start before initializing the tunnel
    setTimeout(initNgrok, 5000);
}

if (shouldClusterInternally) {
    console.log(`[MASTER] Starting Cluster with ${numCPUs} Workers (Total System Cores: ${totalCPUs})...`);

    const masterLiveStatus = new Map();

    // Fork workers.
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }

    // Listen for data from workers
    cluster.on('message', (worker, msg) => {
        if (msg.type === 'LIVE_UPDATE') {
            masterLiveStatus.set(msg.key, msg.data);
        }
    });

    // Periodically broadcast master state to all workers (Broadcast every 3s)
    setInterval(() => {
        const dataArr = Array.from(masterLiveStatus.entries());
        Object.values(cluster.workers).forEach(worker => {
            if (worker && worker.isConnected()) {
                try {
                    worker.send({ type: 'SYNC_LIVE_STATUS', data: dataArr });
                } catch (e) {
                    // Ignore transient IPC errors during shutdown/restart
                }
            }
        });

        // Cleanup stale sessions in Master (30 mins of inactivity)
        const now = Date.now();
        for (const [key, val] of masterLiveStatus.entries()) {
            if (now - new Date(val.server_received_at).getTime() > 1800000) {
                masterLiveStatus.delete(key);
            }
        }
    }, 3000);

    // MASTER SURVIVABILITY: Catch common EMFILE/IPC errors to prevent total crash
    process.on('uncaughtException', (err) => {
        if (err.code === 'EMFILE' || err.code === 'EPIPE' || err.code === 'ECONNRESET') {
            console.error(`[SYSTEM WARNING] Suppressed transient error: ${err.message}`);
        } else {
            console.error(`[FATAL CRASH] ${err.stack}`);
            // process.exit(1); // Keep alive if possible
        }
    });

    cluster.on('exit', (worker, code, signal) => {
        // Log death
        console.log(`[MASTER] Worker ${worker.process.pid} died (Code: ${code}). Restarting in 10s...`);

        // Safety: If worker dies with specific Windows error code, wait longer to prevent leak
        const restartDelay = (code === 3221225786) ? 10000 : 5000;

        setTimeout(() => {
            if (!isPM2 && cluster.workers && Object.keys(cluster.workers).length < numCPUs) {
                console.log(`[MASTER] Re-spawning worker...`);
                cluster.fork();
            }
        }, restartDelay);
    });

    // Master-only tasks (e.g. cleanup, logging stats)
    setInterval(() => {
        const uniqueOfficers = new Set();
        masterLiveStatus.forEach(val => {
            uniqueOfficers.add(`${val.dept_id}_${val.officer_name}`);
        });
        if (masterLiveStatus.size > 0) {
            console.log(`[MASTER STATS] Active Officers: ${uniqueOfficers.size} | Sessions: ${masterLiveStatus.size}`);
        }
    }, 60000);

} else {
    // Workers or PM2-managed instances run this
    // Standard Server Startup
    const serverIPs = require('os').networkInterfaces();
    const ips = [];
    for (const name of Object.keys(serverIPs)) {
        for (const net of serverIPs[name]) {
            if (net.family === 'IPv4' && !net.internal) ips.push(net.address);
        }
    }

    const server = app.listen(PORT, '0.0.0.0', () => {
        // Initialize Socket.io with Redis Adapter for multi-instance scaling
        io = new Server(server, {
            cors: { origin: "*", methods: ["GET", "POST"] }
        });

        const pubClient = new Redis({ host: REDIS_HOST });
        const subClient = pubClient.duplicate();
        io.adapter(createAdapter(pubClient, subClient));

        io.on("connection", (socket) => {
            const isFirst = isPM2 ? (process.env.NODE_APP_INSTANCE === "0") : (cluster.worker.id === 1);
            if (isFirst) console.log(`[WS] New Connection (Socket.io): ${socket.id}`);
        });

        // Initialize Raw WebSocket Server for Mobile App Integration
        const wss = new WebSocketServer({ server, path: '/ws' });
        wss.on('connection', (ws, req) => {
            const isFirst = isPM2 ? (process.env.NODE_APP_INSTANCE === "0") : (cluster.worker.id === 1);
            if (isFirst) console.log(`[RAW-WS] New Mobile App Connection: ${req.socket.remoteAddress}`);
            
            // Send a welcome packet
            ws.send(JSON.stringify({ type: 'welcome', message: 'Connected to Surat Police Raw Datastream' }));
        });

        // Global override to broadcast to raw WS clients as well
        const originalEmit = io.emit.bind(io);
        io.emit = (event, data) => {
            originalEmit(event, data);
            // Also push to raw websocket connected to this worker
            if (event === 'live-vitals-update' || event === 'sensor-status-event') {
                const payload = JSON.stringify({ event, data });
                wss.clients.forEach(client => {
                    if (client.readyState === 1) { // 1 = OPEN
                        client.send(payload);
                    }
                });
            }
        };
        // Prioritize the static IP 172.21.0.74 if available, otherwise use the first one
        const preferredIP = "172.21.0.74";
        const displayIP = ips.includes(preferredIP) ? preferredIP : (ips.length > 0 ? ips[0] : '127.0.0.1');

        const mode = isPM2 ? "PM2 MANAGED" : `WORKER ${process.pid}`;
        console.log(`[${mode}] Online! Listening on port ${PORT}`);

        // Detect which instance we are to avoid terminal spam
        const isFirstInstance = isPM2 ?
            (process.env.NODE_APP_INSTANCE === "0" || process.env.NODE_APP_INSTANCE === undefined) :
            (cluster.isWorker && cluster.worker.id === 1);

        if (isFirstInstance) {
            const serverBase = `http://${displayIP}:${PORT}`;
            console.log(`\n==============================================`);
            console.log(`   POLICE DATA HUB: SCALED PRODUCTION MODE    `);
            console.log(`==============================================`);
            console.log(`STATUS    : Running (${numCPUs} Workers / ${totalCPUs} Cores)`);
            console.log(`CAPACITY  : Optimized for 1100+ Concurrent Users  `);
            console.log(`SERVER IP : ${serverBase}`);
            console.log(`PORT      : ${PORT}`);
            console.log(`----------------------------------------------`);
            console.log(`ADMIN     : ${serverBase}/admin`);
            console.log(`DASHBOARD : ${serverBase}/dashboard/dashboard.html`);
            console.log(`EXPLORER  : ${serverBase}/dashboard/sql_explorer.html`);
            console.log(`PUBLIC DATA: ${serverBase}/api/public/live-data`);
            console.log(`MAP (Live): ${serverBase}/dashboard/dashboard.html#live`);
            console.log(`STORAGE   : logs/ folder and snp_database.db`);
            console.log(`----------------------------------------------`);
            console.log(`AVAILABLE LOCAL IPs:`);
            ips.forEach(ip => console.log(`  └─ http://${ip}:${PORT}`));
            console.log(`----------------------------------------------`);
            console.log(`MODE      : Direct IP Access (Auto-Detected)      `);
            console.log(`INFO      : Connect using the SERVER IP shown above.`);
            console.log(`==============================================\n`);
        }
    });

    // --- HTTPS (SSL) SUPPORT FOR REGISTERED DOMAIN ---
    const domainFromEnv = process.env.DOMAIN || '';
    const certPath = `C:\\Certbot\\live\\${domainFromEnv}\\fullchain.pem`;
    const keyPath = `C:\\Certbot\\live\\${domainFromEnv}\\privkey.pem`;

    let sslServer = null;
    if (domainFromEnv && fs.existsSync(certPath) && fs.existsSync(keyPath)) {
        try {
            const httpsOptions = {
                cert: fs.readFileSync(certPath),
                key: fs.readFileSync(keyPath)
            };
            sslServer = https.createServer(httpsOptions, app).listen(SSL_PORT, '0.0.0.0', () => {
                console.log(`[SSL] Secure Server Online! Listening on port ${SSL_PORT}`);
            });
        } catch (e) {
            console.error(`[SSL ERROR] Failed to start HTTPS: ${e.message}`);
        }
    } else if (domainFromEnv) {
        console.log(`[SSL] Domain detected (${domainFromEnv}) but certificates not found at ${certPath}`);
    }

    if (sslServer) {
        sslServer.on('error', (e) => {
            if (e.code === 'EADDRINUSE') console.error(`[SSL ERROR] Port ${SSL_PORT} is already in use (Maybe another web server?)`);
        });
    }

    // Debug Middleware for all incoming requests (Worker only)
    app.use((req, res, next) => {
        if (!req.url.includes('live-status')) { // Skip noise
            console.log(`[REQ] ${new Date().toLocaleTimeString()} | ${req.method} ${req.path} from ${req.ip}`);
        }
        next();
    });

    // Handle Port Busy errors gracefully
    server.on('error', (e) => {
        if (e.code === 'EADDRINUSE') {
            console.error(`\n[FATAL ERROR] Port ${PORT} is already in use!`);
            console.error(`Stopping to prevent infinite restart loop...`);
            process.exit(1);
        }
    });
}
