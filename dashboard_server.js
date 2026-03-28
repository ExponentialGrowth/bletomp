const express = require('express');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const bodyParser = require('body-parser');
const sqlite3 = require('sqlite3').verbose();

const app = express();
const PORT = 8080; // DASHBOARD USES 8080
const DB_FILE = path.join(__dirname, 'snp_database.db');
const WEB_DASHBOARD_DIR = path.join(__dirname, '../web_dashboard');

// Initialize SQLite Database
const db = new sqlite3.Database(DB_FILE);

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Serve Static Files
app.use('/dashboard', express.static(WEB_DASHBOARD_DIR));

// 1. DASHBOARD HOME
app.get('/', (req, res) => {
    res.redirect('/dashboard/dashboard.html');
});

app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'add_member.html'));
});

// 2. LIVE STATUS FROM DATABASE
app.get('/api/live-status', (req, res) => {
    const query = `
        SELECT 
            s.session_id, 
            m.id as member_id,
            m.name as officer_name, 
            m.dept_id, 
            r.heart_rate || ',' || r.spo2 || ',' || r.temp_c as latest_sample,
            r.lat, r.lng, r.alt,
            r.recorded_at as timestamp,
            'HEALTHY' as status
        FROM sensor_readings r
        JOIN bridge_sessions s ON r.session_id = s.session_id
        JOIN members m ON s.member_id = m.id
        WHERE r.recorded_at > datetime('now', '-2 minutes')
        GROUP BY m.id -- UNIQUE PER OFFICER
        HAVING r.recorded_at = MAX(r.recorded_at)
    `;

    db.all(query, [], (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });

        const status = {};
        rows.forEach(row => {
            const key = `member_${row.member_id}`;
            status[key] = {
                member_id: row.member_id,
                officer_name: row.officer_name,
                dept_id: row.dept_id,
                session_id: row.session_id,
                location: { lat: row.lat, lng: row.lng, alt: row.alt },
                latest_sample: row.latest_sample,
                timestamp: row.timestamp
            };
        });
        res.json(status);
    });
});

// 3. MEMBER MANAGEMENT APIs
app.get('/api/members', (req, res) => {
    db.all("SELECT m.*, p.title as position_title FROM members m LEFT JOIN positions p ON m.position_id = p.pos_id", [], (err, rows) => {
        if (err) return res.status(500).json({ error: "Failed to load" });
        res.json(rows);
    });
});

app.post('/api/add-member', (req, res) => {
    const { name, special_no, mobile, email, dept_id, position_id, address, role } = req.body;
    db.run("INSERT INTO members (name, special_no, mobile, email, dept_id, position_id, address, role) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [name, special_no, mobile, email, dept_id, position_id, address, role || 'user'], (err) => {
            if (err) return res.status(400).json({ error: "Failed to add" });
            console.log(`[UI-ADMIN] New Member Added: ${name}`);
            res.json({ success: true });
        });
});

app.put('/api/update-member', (req, res) => {
    const { original_special_no, name, special_no, mobile, email, dept_id, position_id, address, role } = req.body;
    db.run(`UPDATE members SET name=?, special_no=?, mobile=?, email=?, dept_id=?, position_id=?, address=?, role=? WHERE special_no=?`,
        [name, special_no, mobile, email, dept_id, position_id, address, role || 'user', original_special_no], (err) => {
            if (err) return res.status(400).json({ error: "Failed to update" });
            console.log(`[UI-ADMIN] Updated Member: ${name}`);
            res.json({ success: true });
        });
});

app.delete('/api/delete-member', (req, res) => {
    const { special_no } = req.body;
    db.run("DELETE FROM members WHERE special_no=?", [special_no], (err) => {
        console.log(`[UI-ADMIN] Deleted Member: ${special_no}`);
        res.json({ success: true });
    });
});

// 4. SQL EXPLORER
app.get('/api/sql-explorer', (req, res) => {
    const { type, session_id } = req.query;
    let query = type === 'sessions' ? "SELECT s.*, m.name as officer_name FROM bridge_sessions s LEFT JOIN members m ON s.member_id = m.id ORDER BY started_at DESC LIMIT 50" : "SELECT * FROM sensor_readings ORDER BY recorded_at DESC LIMIT 50";
    if (type !== 'sessions' && session_id) query = `SELECT * FROM sensor_readings WHERE session_id = ? ORDER BY recorded_at DESC LIMIT 50`;

    db.all(query, session_id ? [session_id] : [], (err, rows) => {
        console.log(`[SQL] Explorer Query (${type}) | Rows: ${rows ? rows.length : 0}`);
        res.json(rows || []);
    });
});

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
        db.all("SELECT lat, lng, recorded_at FROM sensor_readings WHERE session_id = ? ORDER BY recorded_at ASC", [session_id], (err, rows) => {
            res.json(rows || []);
        });
    }
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n==============================================`);
    console.log(`   POLICE DATA HUB: DASHBOARD SERVER        `);
    console.log(`==============================================`);
    console.log(`PORT    : ${PORT}`);
    console.log(`UI      : http://localhost:${PORT}/dashboard`);
    console.log(`STATUS  : Serving Web Content & APIs...`);
    console.log(`==============================================\n`);
});
