const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const DB_FILE = path.join(__dirname, 'snp_database.db');
const db = new sqlite3.Database(DB_FILE);

console.log("--- DATABASE DEBUG DUMP ---");

const tables = ['members', 'bridge_sessions', 'sensor_readings', 'departments', 'positions', 'application_config'];

function queryTable(index) {
    if (index >= tables.length) {
        db.close();
        return;
    }
    const table = tables[index];
    db.get(`SELECT count(*) as count FROM ${table}`, (err, row) => {
        if (err) {
            console.error(`Error counting ${table}:`, err.message);
        } else {
            console.log(`Table '${table}': ${row.count} records`);
            if (row.count > 0) {
                db.all(`SELECT * FROM ${table} LIMIT 2`, (err, rows) => {
                    if (!err) console.log(`  Sample:`, rows);
                    queryTable(index + 1);
                });
                return;
            }
        }
        queryTable(index + 1);
    });
}

queryTable(0);
