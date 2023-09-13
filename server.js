const express = require('express');
const cors = require('cors');
const graphRoutes = require('./routes/graph');
const { spawn } = require('child_process');

const app = express();
const PORT = 5000;

app.use(cors()); // Allow cross-origin requests (adjust as needed for security)
app.use(express.json()); // Middleware to parse JSON requests

app.use('/api/graph', graphRoutes); // Route for all graph-related API endpoints

function runPythonScript(scriptPath, callback) {
  const pythonProcess = spawn('python', [scriptPath]);
  let results = '';

  pythonProcess.stdout.on('data', (data) => {
    results += data.toString();
  });

  pythonProcess.on('close', (code) => {
    if (code !== 0) {
        callback({ error: 'Failed to run script' }, null);
    } else {
        callback(null, JSON.parse(results));
    }
  });
}

app.post('/runImportCpgData', (req, res) => {
  runPythonScript('./scripts/import-cgp-data.py', (err, data) => {
    if (err) {
      return res.status(500).json(err);
    }
    res.json(data);
  });
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
