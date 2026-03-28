const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = 8082; // Different port to avoid conflict
const DB_FILE = path.join(__dirname, 'snp_database.db');

// Initialize DB connection (Read-Only mode is safer for parallel access)
const db = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READONLY, (err) => {
    if (err) console.error("Database connection error:", err.message);
});

app.use(cors());
app.use(express.json());

// Public API for Live Data (Reads from DB)
app.get('/api/public/live-data', (req, res) => {
    // Fetches all records from the last 30 minutes, including non-registered members (simulations)
    const query = `
        SELECT 
            r.id,
            COALESCE(m.name, 'Officer_' || s.session_id) as officer_name, 
            COALESCE(m.dept_id, 'N/A') as dept_id, 
            r.heart_rate, 
            r.spo2, 
            r.lat, 
            r.lng, 
            r.alt,
            r.recorded_at as timestamp
        FROM sensor_readings r
        LEFT JOIN bridge_sessions s ON r.session_id = s.session_id
        LEFT JOIN members m ON s.member_id = m.id
        WHERE r.recorded_at > datetime('now', '-30 minutes')
        ORDER BY r.id DESC
        LIMIT 1000
    `;

    db.all(query, [], (err, rows) => {
        if (err) {
            console.error("Query Error:", err.message);
            return res.status(500).json({ error: "Internal Server Error" });
        }

        console.log(`[DEBUG] Query returned ${rows.length} raw rows from DB`);
        if (rows.length > 0) console.log(`[DEBUG] Sample Row Keys: ${Object.keys(rows[0]).join(', ')}`);

        // Map to the format the friend needs
        const officers = rows.map(row => ({
            id: row.id,
            officer_name: row.officer_name,
            dept_id: row.dept_id,
            metrics: {
                heart_rate: row.heart_rate,
                spo2: row.spo2
            },
            location: {
                lat: row.lat,
                lng: row.lng,
                alt: row.alt
            },
            timestamp: row.timestamp
        }));

        console.log(`[PUBLIC API] Serving ${officers.length} rows to client at ${new Date().toLocaleTimeString()}`);
        res.json(officers);
    });
});

app.get('/', (req, res) => {
    res.send("Public Live Data API Server is Running on port " + PORT);
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n==============================================`);
    console.log(`   PUBLIC LIVE-DATA API SERVER (PARALLEL)     `);
    console.log(`==============================================`);
    console.log(`PORT        : ${PORT}`);
    console.log(`ENDPOINT    : http://localhost:${PORT}/api/public/live-data`);
    console.log(`DB SOURCE   : ${DB_FILE}`);
    console.log(`STATUS      : Online & Serving...`);
    console.log(`==============================================\n`);
});
