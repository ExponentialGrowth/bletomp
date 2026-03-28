const fs = require('fs');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const DB_FILE = path.join(__dirname, 'snp_database.db');
const LOG_FILE = path.join(__dirname, 'logs', 'DEPT_126 - mayankbhai - 1051DB1C71E6 - 2026-02-10.csv');

if (!fs.existsSync(LOG_FILE)) {
    console.error("Log file not found:", LOG_FILE);
    process.exit(1);
}

const db = new sqlite3.Database(DB_FILE);

async function run() {
    console.log("Starting data recovery from CSV...");
    const content = fs.readFileSync(LOG_FILE, 'utf8');
    const lines = content.split('\n');
    console.log(`Analyzing ${lines.length} lines...`);

    let currentSessionId = null;
    let memberId = 2; // mayankbhai
    let insertCount = 0;

    db.serialize(() => {
        const stmt = db.prepare(`INSERT OR IGNORE INTO sensor_readings 
            (session_id, esp_timestamp, heart_rate, spo2, temp_c, acc_x, acc_y, acc_z, gyro_x, gyro_y, gyro_z, lat, lng, alt, recorded_at) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

        for (let line of lines) {
            line = line.trim();
            if (!line) continue;

            if (line.includes('sid=')) {
                const match = line.match(/sid=([0-9\-\s:]+)/);
                if (match) {
                    currentSessionId = match[1].trim();
                }
            }

            if (line.startsWith('"2026-02-10"') && currentSessionId) {
                const parts = line.split(',');
                if (parts.length < 13) continue;

                try {
                    const datePart = parts[0].replace(/"/g, '');
                    const timePart = parts[1].replace(/"/g, '');
                    const recordedAt = `${datePart} ${timePart}`;
                    const lat = parseFloat(parts[5]);
                    const lng = parseFloat(parts[6]);
                    const alt = parseFloat(parts[7]);
                    const espTs = parseInt(parts[9]);
                    const hr = parseFloat(parts[12]) || 0;
                    const spo2 = parseFloat(parts[13]) || 0;

                    stmt.run(currentSessionId, espTs, hr, spo2, 36.5, 0, 0, 0, 0, 0, 0, lat, lng, alt, recordedAt);
                    insertCount++;
                } catch (e) { }
            }
        }

        stmt.finalize(() => {
            console.log(`Success! Inserted ${insertCount} readings back into database.`);
            db.close();
        });
    });
}

run();
