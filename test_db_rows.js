const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const DB_FILE = path.join(__dirname, 'snp_database.db');

const db = new sqlite3.Database(DB_FILE, sqlite3.OPEN_READONLY, (err) => {
    if (err) console.error("Database connection error:", err.message);
});

const query = `
    SELECT 
        id,
        heart_rate, 
        spo2, 
        recorded_at as timestamp
    FROM sensor_readings
    ORDER BY id DESC
    LIMIT 20
`;

db.all(query, [], (err, rows) => {
    if (err) {
        console.error("Query Error:", err.message);
    } else {
        console.log("Rows Found in Test Script:", rows.length);
        if (rows.length > 0) {
            console.log("First Row ID:", rows[0].id);
            console.log("Last Row ID:", rows[rows.length - 1].id);
        }
    }
    db.close();
});
