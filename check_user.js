const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('e:/Project/snp/ble_to_mp/server/snp_database.db');
db.get("SELECT name, special_no, mobile, password, role FROM members WHERE mobile = '9876543210' OR special_no = '9876543210';", (err, row) => {
    if (err) console.error(err);
    console.log(JSON.stringify(row, null, 2));
    db.close();
});
