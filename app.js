const express = require('express');
const app = express();
app.use(express.json());

const API_KEY = "my_secret_123";

app.post('/upload', (req, res) => {
    // Check API Key
    if (req.header('x-api-key') !== API_KEY) {
        return res.status(403).send("Unauthorized");
    }

    console.log("--- New Data Bundle ---");
    console.log(`Heartbeat: ${req.body.heartbeat} bpm`);
    console.log(`Temp: ${req.body.temperature}°C`);
    console.log(`Location: ${req.body.gps.lat}, ${req.body.gps.lng}`);
    
    res.status(200).send("Data Received");
});

app.listen(3000, '0.0.0.0', () => {
    console.log("Server running on http://172.21.177.7:3000");
});