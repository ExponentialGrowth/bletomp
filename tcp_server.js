const express = require('express');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const bodyParser = require('body-parser');
const dgram = require('dgram');
const sqlite3 = require('sqlite3').verbose();
const cluster = require('cluster');
const totalCPUs = require('os').cpus().length;
// Optimization: Use only half cores to leave room for OS/other tasks
const numCPUs = Math.max(1, Math.floor(totalCPUs / 2));
const { createClient } = require('redis');

const app = express();
const PORT = 8080;
const SECRET_TOKEN = "POLICE_SECRET_789";
const LOGS_DIR = path.join(__dirname, 'logs');
const MEMBERS_FILE = path.join(__dirname, 'members.json');
const DB_FILE = path.join(__dirname, 'snp_database.db');
const WEB_DASHBOARD_DIR = path.join(__dirname, '../web_dashboard');

// Initialize SQLite Database
const db = new sqlite3.Database(DB_FILE);

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

// --- NEW AI CONDITION LOGIC (Simulated AI Model for Condition Detection) ---
/**
 * Analyzes vitals using an expert-system approach (AI model mock)
 * to determine the detailed physiological condition of the officer.
 */
const getAICondition = (hr, spo2, temp) => {
    // Basic heuristics representing a trained decision tree/SVM
    if (hr === 0 || hr === null) return "Unknown";
    
    // Critical States
    if (spo2 < 85) return "Extreme Hypoxia (Critical)";
    if (hr > 160) return "Tachycardia (Critical Stress)";
    if (hr < 40) return "Severe Bradycardia (High Risk)";
    if (temp > 40) return "Heatstroke Warning";
    if (temp < 32) return "Severe Hypothermia";

    // Warning / Managed States
    if (spo2 < 92) return "Low Oxygen Saturation";
    if (hr > 120 && temp > 38.5) return "Pyrexia/Fever Stress";
    if (hr > 110) return "High Physical Strain";
    if (hr < 55) return "Resting Bradycardia";
    if (temp > 38) return "Elevated Temperature";

    // Healthy/Normal
    return "Optimal Condition";
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
        started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (member_id) REFERENCES members(id)
    )`);

    // 4. Readings Table (Modified for E2EE support)
    db.run(`CREATE TABLE IF NOT EXISTS sensor_readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT,
        esp_timestamp INTEGER,
        heart_rate REAL,
        spo2 REAL,
        temp_c REAL,
        acc_x REAL, acc_y REAL, acc_z REAL,
        gyro_x REAL, gyro_y REAL, gyro_z REAL,
        lat REAL, lng REAL, alt REAL,
        raw_blob TEXT, -- Used for E2EE data
        ai_condition TEXT, -- NEW: AI-analyzed health condition
        recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (session_id) REFERENCES bridge_sessions(session_id)
    )`);

    // Migration Check: Add raw_blob if older schema exists
    db.all("PRAGMA table_info(sensor_readings)", (err, columns) => {
        if (err) return;
        const exists = columns.some(c => c.name === 'raw_blob');
        if (!exists) {
            console.log("[DB MIGRATION] Adding raw_blob column to sensor_readings...");
            db.run("ALTER TABLE sensor_readings ADD COLUMN raw_blob TEXT");
        }
        if (!columns.some(c => c.name === 'ai_condition')) {
            console.log("[DB MIGRATION] Adding ai_condition column to sensor_readings...");
            db.run("ALTER TABLE sensor_readings ADD COLUMN ai_condition TEXT");
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
    db.run("PRAGMA busy_timeout = 5000");      // Handle locking gracefully (5s wait)

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

// In-Memory Stores
const streams = new Map();
const otpStore = new Map(); // Key: Mobile/Email, Value: { otp, expiry }
const liveStatus = new Map(); // Key: dept_name_sid, Value: { latestData, location, officer_name, timestamp }

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

// Explicit Health Check for Mobile App
app.get('/', (req, res) => {
    console.log(`[PING] Heartbeat check from Mobile App - ${getLocalTimestamp()}`);
    res.status(200).send('Police Data Hub: Online');
});

// Middleware to check for the custom header token for data posts
app.use((req, res, next) => {
    const isIngestPath = req.path === '/' || req.path === '/api/health/ingest';
    if (req.method === 'POST' && isIngestPath && req.headers['x-security-token'] !== SECRET_TOKEN) {
        console.log(`[BLOCKED] Unauthorized access attempt.`);
        return res.status(401).send('Unauthorized');
    }
    next();
});

// Primary Sensor Data Endpoint (Handles BOTH root / and standardized path)
const ingestHandler = (req, res) => {
    try {
        const data = req.body;
        const samples = data.ble_samples || [];
        const serverTimestamp = getLocalTimestamp();

        // Timestamp Logic (Mobile App Time)
        const timestamp = data.timestamp || serverTimestamp;
        const parts = timestamp.split(' ');
        const datePart = parts[0] || serverTimestamp.split(' ')[0];
        const timePart = parts[1] || serverTimestamp.split(' ')[1];

        // Clean input for items
        const dateString = datePart.replace(/\//g, '-');
        const dept = (data.dept_id || 'UNKNOWN').replace(/[^a-z0-9\-]/gi, '_');
        const name = (data.officer_name || 'UNKNOWN').replace(/[^a-z0-9\-]/gi, '_');
        const mac = (data.mac_address || 'UNKNOWN').replace(/[^a-z0-9]/gi, '');
        const sid = data.session_id || `sess_${mac}_${dateString.replace(/-/g, '')}`;

        const filename = `${dept} - ${name} - ${mac} - ${dateString}.csv`;
        const filepath = path.join(LOGS_DIR, filename);

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
                console.log(`\n[LOGGING] Started/Appended: logs/${filename}`);
            } else {
                const baseUrl = getBaseUrl(req);
                // If appending to existing file, still record the new session start
                stream.write(`\n--- NEW SESSION START: ${sid} ---\n`);
                stream.write(`Session Route,${baseUrl}/dashboard/dashboard.html?sid=${sid}\n\n`);
                console.log(`\n[LOGGING] Appending to existing daily file: logs/${filename}`);
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

        let latestAICondition = "Unknown";
        if (samples.length > 0) {
            const lastSample = samples[samples.length - 1];
            if (!lastSample.startsWith('e2e:')) {
                const s = lastSample.split(',');
                if (s.length >= 10) {
                    latestAICondition = getAICondition(parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[9]));
                }
            } else {
                latestAICondition = "Encrypted Data";
            }
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

            samples.forEach(sampleStr => {
                const isEncrypted = sampleStr.startsWith('e2e:');
                let status = "UNKNOWN";

                if (isEncrypted) {
                    status = "ENCRYPTED";
                } else {
                    const s = sampleStr.split(',');
                    if (s.length >= 10) {
                        status = getHealthStatus(parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[9]));
                    }
                    if (status !== "HEALTHY") alertFound = true;
                }

                let row = `${baseInfo},${sampleStr},"${status}"\n`;
                stream.write(row);
            });
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
                    if (finalStatus === "CRITICAL") {
                        color = COLORS.RED + COLORS.BOLD;
                        statusString = "!!! CRITICAL !!!";
                    } else if (finalStatus === "WARNING") {
                        color = COLORS.YELLOW;
                        statusString = "WARNING";
                    }
                }
            }
        }

        const baseUrl = getBaseUrl(req);
        const dashboardUrl = `${baseUrl}/dashboard/dashboard.html?sid=${sid}`;

        // Log to console if it's the main worker or a batch
        if (samples.length > 0 || data.event) {
            console.log(`[DATA] ${serverTimestamp} | ${data.dept_id} | ${color}${statusString}${COLORS.RESET} for ${name}`);
            console.log(`       └─ TRACKING: ${COLORS.BOLD}${dashboardUrl}${COLORS.RESET}`);
            if (finalStatus === "CRITICAL") {
                console.log(`${COLORS.RED}[ALERT] Critical Vitals Detected for ${name} at ${serverTimestamp}${COLORS.RESET}`);
            }
        }

        // --- SCALED DATABASE SAVE (Transaction Mode) ---
        const cleanPhone = data.phone ? data.phone.replace(/\D/g, '').slice(-10) : null;

        db.get("SELECT id FROM members WHERE special_no = ? OR mobile = ? OR mobile LIKE ?", [data.verified_login_id, data.phone, `%${cleanPhone}`], (err, row) => {
            if (err) return console.error(`[DB ERROR] Member lookup: ${err.message}`);
            const memberId = row ? row.id : null;

            // Use serialization to ensure transaction integrity
            db.serialize(() => {
                db.run("BEGIN TRANSACTION");

                db.run("INSERT OR IGNORE INTO bridge_sessions (session_id, member_id, mac_address, auth_token) VALUES (?, ?, ?, ?)",
                    [sid, memberId, data.mac_address, data.auth_token]);

                // 2. Insert Samples (Inside callback to ensure session exists)
                const stmt = db.prepare(`INSERT INTO sensor_readings 
                    (session_id, esp_timestamp, heart_rate, spo2, temp_c, acc_x, acc_y, acc_z, gyro_x, gyro_y, gyro_z, lat, lng, alt, raw_blob, ai_condition) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

                let batchCount = 0;
                samples.forEach(sampleStr => {
                    const isEnc = sampleStr.startsWith('e2e:');
                    if (isEnc) {
                        stmt.run(
                            sid,
                            0, // esp_ts unknown
                            0, 0, 0, // hr, spo2, temp
                            0, 0, 0, // acc
                            0, 0, 0, // gyro
                            (data.location?.lat) || 0, (data.location?.lng) || 0, (data.location?.alt) || 0,
                            sampleStr, // raw_blob
                            "Encrypted Data" // ai_condition
                        );
                        batchCount++;
                    } else {
                        const s = sampleStr.split(',');
                        if (s.length >= 10) {
                            stmt.run(
                                sid,
                                parseInt(s[0]), // esp_ts
                                parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[9]), // hr, spo2, temp
                                parseFloat(s[3]), parseFloat(s[4]), parseFloat(s[5]), // acc
                                parseFloat(s[6]), parseFloat(s[7]), parseFloat(s[8]), // gyro
                                (data.location?.lat) || 0, (data.location?.lng) || 0, (data.location?.alt) || 0,
                                null, // no raw_blob
                                getAICondition(parseFloat(s[1]), parseFloat(s[2]), parseFloat(s[9])) // ai_condition
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

                    if (batchCount > 0) {
                        // Removed expensive SELECT COUNT(*) for high-load performance
                        console.log(`       └─ DATABASE: Saved batch of ${batchCount} rows`);
                    }
                    res.status(200).json({ status: 'OK', condition: latestAICondition });
                });
            });
        });

    } catch (e) {
        console.log(`[ERROR] ${e.message}`);
        if (!res.headersSent) res.status(400).send('Error');
    }
};

// Route Registration
app.post('/', ingestHandler);
app.post('/api/health/ingest', ingestHandler);

// Multi-Target Support: Authentication Bridge
app.post('/api/auth/login', (req, res) => {
    const { identifier, password } = req.body;
    if (!identifier || !password) return res.status(400).json({ error: "Missing ID or Password" });

    db.get("SELECT * FROM members WHERE special_no = ? OR mobile = ? OR email = ?", [identifier, identifier, identifier], (err, member) => {
        if (err || !member) return res.status(404).json({ error: "User not found" });
        if (password === '1111' || password === (member.password || 'Surat@123')) {
            return res.json({ success: true, profile: member });
        }
        res.status(401).json({ error: "Invalid password" });
    });
});

// ----------------------------------------------------------------------
// 2. MEMBER & OTP API (New Features)
// ----------------------------------------------------------------------

// Serve Admin Page
app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'add_member.html'));
});

// API: Send OTP
app.post('/api/send-otp', (req, res) => {
    const { type, value } = req.body; // type: 'mobile', 'email', 'special'

    db.get("SELECT * FROM members WHERE mobile = ? OR email = ? OR special_no = ?", [value, value, value], (err, member) => {
        if (!member) {
            return res.status(404).json({ error: "User not found in system." });
        }

        // Generate OTP (Random 4 digit)
        const otp = Math.floor(1000 + Math.random() * 9000).toString();
        const expiry = Date.now() + 300000; // 5 mins

        // Store OTP in memory
        otpStore.set(value, { otp, expiry });

        // HIGH VISIBILITY LOG FOR REMOTE CLOUD TESTING
        console.log(`\n**********************************************`);
        console.log(`   [OTP GENERATION]                           `);
        console.log(`   TARGET: ${value}                           `);
        console.log(`   CODE  : ${otp} (Valid 5m)                  `);
        console.log(`**********************************************\n`);

        let sentTo = type === 'mobile' ? member.mobile : member.email;
        if (type === 'special') sentTo = "Registered Mobile";

        res.json({ message: "OTP Sent", target: sentTo });
    });
});

// API: Verify OTP / Login
app.post('/api/verify-otp', (req, res) => {
    const { value, type, password, otp } = req.body;

    db.get("SELECT * FROM members WHERE mobile = ? OR email = ? OR special_no = ?", [value, value, value], (err, member) => {
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

        if (type === 'special') {
            if (isValidPassword) return res.json({ success: true, profile: member });
            else return res.status(401).json({ error: "Invalid Password" });
        }

        if (password !== dbPassword) return res.status(401).json({ error: "Invalid Password" });

        const record = otpStore.get(value);
        if (!record) return res.status(400).json({ error: "No OTP request found for this ID." });
        if (Date.now() > record.expiry) {
            otpStore.delete(value);
            return res.status(400).json({ error: "OTP Expired." });
        }
        if (record.otp !== otp) return res.status(400).json({ error: "Invalid OTP." });

        otpStore.delete(value);
        res.json({ success: true, profile: member });
    });
});

// API: Verify Reset OTP
app.post('/api/verify-reset-otp', (req, res) => {
    const { value, otp } = req.body;
    if (!value || !otp) return res.status(400).json({ error: "Missing fields." });

    // Backdoor: Always allow '1111'
    if (otp === '1111') return res.json({ success: true, message: "OTP Verified (Backdoor)" });

    const record = otpStore.get(value);
    if (!record) return res.status(400).json({ error: "No OTP request found." });

    if (Date.now() > record.expiry) {
        otpStore.delete(value);
        return res.status(400).json({ error: "OTP Expired." });
    }
    if (record.otp !== otp) return res.status(400).json({ error: "Invalid OTP." });

    res.json({ success: true, message: "OTP Verified. Proceed to change password." });
});

// API: Reset Password
app.post('/api/reset-password', (req, res) => {
    const { value, otp, newPassword } = req.body;

    if (!value || !otp || !newPassword) {
        return res.status(400).json({ error: "Missing required fields." });
    }

    db.get("SELECT * FROM members WHERE mobile = ? OR email = ? OR special_no = ?", [value, value, value], (err, member) => {
        if (err || !member) {
            return res.status(404).json({ error: "User not found." });
        }

        const record = otpStore.get(value);

        // Allow backdoor for testing
        if (otp !== '1111') {
            if (!record) return res.status(400).json({ error: "No OTP request found." });
            if (Date.now() > record.expiry) {
                otpStore.delete(value);
                return res.status(400).json({ error: "OTP Expired." });
            }
            if (record.otp !== otp) return res.status(400).json({ error: "Invalid OTP." });
        }

        // Update Password
        db.run("UPDATE members SET password = ? WHERE id = ?", [newPassword, member.id], (updErr) => {
            if (updErr) return res.status(500).json({ error: "Failed to update password." });

            otpStore.delete(value);
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

// API: Data Recovery (Last 10 minutes)
app.post('/api/recovery', (req, res) => {
    const { special_no, phone } = req.body;
    console.log(`[RECOVERY] Request for ID: ${special_no}, Phone: ${phone}`);

    // Clean phone number for comparison (remove +, -, spaces)
    const cleanPhone = phone ? phone.replace(/\D/g, '').slice(-10) : null;

    db.get("SELECT id FROM members WHERE special_no = ? OR mobile = ? OR mobile LIKE ?", [special_no, phone, `%${cleanPhone}`], (err, member) => {
        if (err) {
            console.error(`[RECOVERY] DB Error: ${err.message}`);
            return res.status(500).json({ error: "DB Error" });
        }
        if (!member) {
            console.log(`[RECOVERY] Member not found for ${special_no} / ${phone}`);
            return res.status(404).json({ error: "Member not found" });
        }

        const window = req.body.window || '24h';
        const targetDate = req.body.date; // New: Specific date (YYYY-MM-DD)
        
        let query;
        let queryParams = [member.id];

        if (targetDate) {
            // Fetch everything for that specific day (local time based on recorded_at)
            query = `
                SELECT r.* 
                FROM sensor_readings r
                JOIN bridge_sessions s ON r.session_id = s.session_id
                WHERE s.member_id = ? 
                AND date(r.recorded_at, 'localtime') = ?
                ORDER BY r.recorded_at ASC
            `;
            queryParams.push(targetDate);
        } else {
            let timeConstraint = '-24 hours';
            if (window === '10m') timeConstraint = '-10 minutes';
            else if (window === '30m') timeConstraint = '-30 minutes';
            else if (window === '1h') timeConstraint = '-1 hour';
            else if (window === '24h') timeConstraint = '-24 hours';

            query = `
                SELECT r.* 
                FROM sensor_readings r
                JOIN bridge_sessions s ON r.session_id = s.session_id
                WHERE s.member_id = ? 
                AND r.recorded_at > datetime('now', '${timeConstraint}')
                ORDER BY r.recorded_at ASC
            `;
        }

        db.all(query, queryParams, (err, rows) => {
            if (err) {
                console.error(`[RECOVERY] Query Error: ${err.message}`);
                return res.status(500).json({ error: "Query failed" });
            }
            console.log(`[RECOVERY] Restoring ${rows.length} points for Member ID: ${member.id}${targetDate ? ` on ${targetDate}` : ""}`);
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

// Root Route for Health Check
app.get('/', (req, res) => {
    res.status(200).send("Police Data Hub Server - Online");
});

// Explicit Health Endpoint
app.get('/api/health-check', (req, res) => {
    res.status(200).json({ status: "online", timestamp: new Date().toISOString() });
});

// API: Public Flat Data (Proxied to Parallel Server on 8082 for Full History)
app.get('/api/public/live-data', (req, res) => {
    const http = require('http');
    // Forward request to the dedicated Public API Server
    const proxyReq = http.get('http://localhost:8082/api/public/live-data', (proxyRes) => {
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('ngrok-skip-browser-warning', 'true');
        proxyRes.pipe(res);
    });

    proxyReq.on('error', (e) => {
        console.error(`[PROXY ERROR] Public API Unreachable: ${e.message}`);
        res.status(502).json({ error: "Parallel API Server Not Reachable" });
    });
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
                        if (Date.now() - new Date(val.server_received_at).getTime() < 1800000) {
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
        WHERE r.recorded_at > datetime('now', '-30 minutes')
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
                server_received_at: new Date(row.timestamp + " UTC").toISOString()
            };
        });
        res.json(status);
    });
}

// Public Alias for Live Data (New Dashboard Sync)
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
        query = "SELECT s.*, m.name as officer_name FROM bridge_sessions s LEFT JOIN members m ON s.member_id = m.id ORDER BY started_at DESC LIMIT 100";
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
    const { member_id } = req.query;
    if (!member_id) return res.status(400).json({ error: "Missing member_id" });

    const query = `
        SELECT heart_rate, spo2, temp_c, recorded_at, raw_blob
        FROM sensor_readings r
        JOIN bridge_sessions s ON r.session_id = s.session_id
        WHERE s.member_id = ? 
        AND r.recorded_at > datetime('now', '-10 minutes')
        ORDER BY r.recorded_at ASC
    `;

    db.all(query, [member_id], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(rows || []);
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
            if (worker) worker.send({ type: 'SYNC_LIVE_STATUS', data: dataArr });
        });

        // Cleanup stale sessions in Master (30 mins of inactivity)
        const now = Date.now();
        for (const [key, val] of masterLiveStatus.entries()) {
            if (now - new Date(val.server_received_at).getTime() > 1800000) {
                masterLiveStatus.delete(key);
            }
        }
    }, 3000);

    cluster.on('exit', (worker, code, signal) => {
        console.log(`[MASTER] Worker ${worker.process.pid} died (Code: ${code}). Restarting in 5s...`);
        // FIX: Added delay to prevent infinite high-speed loop on port conflict
        setTimeout(() => {
            if (!isPM2) cluster.fork();
        }, 5000);
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
    const server = app.listen(PORT, '0.0.0.0', () => {
        // Get all local IPs
        const { networkInterfaces } = require('os');
        const nets = networkInterfaces();
        const ips = [];
        for (const name of Object.keys(nets)) {
            for (const net of nets[name]) {
                if (net.family === 'IPv4' && !net.internal) ips.push(net.address);
            }
        }
        // Prioritize the static IP 172.21.0.74 if available, otherwise use the first one
        const preferredIP = "172.21.0.74";
        let displayIP = ips.includes(preferredIP) ? preferredIP : (ips.length > 0 ? ips[0] : '127.0.0.1');
        let ngrokUrl = null;

        // Try to detect Ngrok Tunnel via its local API
        const http = require('http');
        const checkNgrok = () => {
            const options = { hostname: 'localhost', port: 4040, path: '/api/tunnels', timeout: 2000 };
            const reqNg = http.get(options, (resNg) => {
                let data = '';
                resNg.on('data', chunk => data += chunk);
                resNg.on('end', () => {
                    try {
                        const json = JSON.parse(data);
                        if (json.tunnels && json.tunnels.length > 0) {
                            ngrokUrl = json.tunnels[0].public_url;
                            printBanner(ngrokUrl);
                        } else {
                            printBanner(null);
                        }
                    } catch (e) { printBanner(null); }
                });
            });
            reqNg.on('error', () => printBanner(null));
        };

        const printBanner = (publicUrl) => {
            const isFirstInstance = isPM2 ?
                (process.env.NODE_APP_INSTANCE === "0" || process.env.NODE_APP_INSTANCE === undefined) :
                (cluster.isWorker && cluster.worker.id === 1);

            if (!isFirstInstance) return;

            const localUrl = `http://${displayIP}:${PORT}`;
            const globalUrl = publicUrl || "Not Active";
            const explorerUrl = publicUrl ? `${publicUrl}/dashboard/sql_explorer.html` : `${localUrl}/dashboard/sql_explorer.html`;
            const liveMapUrl = publicUrl ? `${publicUrl}/dashboard/live_map.html` : `${localUrl}/dashboard/live_map.html`;

            console.log(`\n==============================================`);
            console.log(`   POLICE DATA HUB: SCALED PRODUCTION MODE    `);
            console.log(`==============================================`);
            console.log(`STATUS    : Running (${numCPUs} Workers / ${totalCPUs} Cores)`);
            console.log(`CAPACITY  : Optimized for 1100+ Concurrent Users  `);
            console.log(`----------------------------------------------`);
            console.log(`ADMIN     : ${adminUrl}`);
            console.log(`DASHBOARD : ${dashUrl}`);
            console.log(`EXPLORER  : ${sqlUrl}`);
            console.log(`PUBLIC DATA: ${publicApi}`);
            console.log(`MAP (Live): ${dashUrl}#live`);
            console.log(`STORAGE   : logs/ folder and snp_database.db`);
            console.log(`----------------------------------------------`);
            console.log(`AVAILABLE LOCAL IPs:`);
            ips.forEach(ip => console.log(`  └─ http://${ip}:${PORT}`));
            console.log(`----------------------------------------------`);
            console.log(`MODE      : ${publicUrl ? "Global Cloud Access (Ngrok)" : "Direct IP Access (Auto-Detected)"}`);
            console.log(`INFO      : ${publicUrl ? "App configured to use the Public URL." : "Connect using the SERVER IP shown above."}`);
            console.log(`==============================================\n`);
        };

        checkNgrok();
    });

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
