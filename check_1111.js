const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('e:/Project/snp/ble_to_mp/server/snp_database.db');
db.all("SELECT * FROM members WHERE special_no = '1111' OR mobile = '1111' OR email = '1111';", (err, rows) => {
    if (err) console.error(err);
    console.log(JSON.stringify(rows, null, 2));
    db.close();
});
