const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const DB_FILE = path.join(__dirname, 'snp_database.db');
const db = new sqlite3.Database(DB_FILE);

db.all("SELECT count(*) as count FROM sensor_readings", (err, rows) => {
    if (err) {
        console.error(err);
    } else {
        console.log(`Readings count: ${rows[0].count}`);
    }
    db.all("SELECT * FROM bridge_sessions LIMIT 5", (err, sessions) => {
        console.log("Recent Sessions:", sessions);
        db.close();
    });
});
