const express = require('express');
const cors = require('cors');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const csv = require('csv-parser');
const stream = require('stream');

const storage = multer.memoryStorage(); // This will store the file in memory. Can also configure it to save to disk.
const upload = multer({ storage: storage });

const app = express();
const PORT = 5001;

app.use(cors({
  origin: 'http://localhost:3000' // Allow only this origin
})); // Allow cross-origin requests (adjust as needed for security)
app.use(express.json()); // Middleware to parse JSON requests

function parseCSV(fileBuffer) {
  return new Promise((resolve, reject) => {
    const results = []; // array containing cpg names from csv file
    const bufferStream = new stream.PassThrough();
    bufferStream.end(fileBuffer);
    bufferStream.pipe(csv())
      .on('data', (data) => {
        const value = Object.values(data)[0];
        console.log("Parsed Value:", value); // Log each parsed value
        results.push(value);  // extracting first column
      })
      .on('end', () => {
        console.log("Final Parsed Data:", results); // Log the final results
        resolve(results);
      })
      .on('error', (err) => reject(err));
  });
}

function runPythonScript(scriptPath, args, callback) {
  const pythonProcess = spawn('python3', [scriptPath, ...args]);
  let results = '';
  let errors = '';

  pythonProcess.stderr.on('data', (data) => {
    errors += data.toString();
  });

  pythonProcess.stdout.on('data', (data) => {
    results += data.toString();
  });

  pythonProcess.on('close', (code) => {
    if (code !== 0) {
        callback({ error: 'Failed to run script', details: errors  }, null);
    } else {
      console.log('>>> results before parsing: ', results)  
      try {
        const parsedResults = JSON.parse(results);
        callback(null, parsedResults);
      } catch (err) {
          callback({ error: 'Failed to parse script output as JSON', details: results }, null);
      }
    }
  });
  
  pythonProcess.on('error', (error) => {
    callback({ error: 'Error spawning python process', details: error.message }, null);
  });
  
}

app.post('/run-create-cpg-group', upload.single('cpgFile'), async (req, res) => {
  try {
    // Access the uploaded file
    const fileBuffer = req.file.buffer;

    // Parse CSV
    const parsedData = await parseCSV(fileBuffer);
    console.log("Parsed Data:", parsedData);  // This should log the array of strings

    // Access the group name
    const groupName = req.body.groupName;

    // Since an array cannot be sent directly as a command-line argument, 
    // we need to stringify the array before passing it to Python.
    const args = [JSON.stringify(parsedData), groupName];

    runPythonScript('./scripts/create_cpg_group.py', args, (err, data) => {
        if (err) {
            return res.status(500).json(err);
        }
        res.json(data);
    });
  } catch (err) {
      console.error(err);
      res.status(500).send("Internal Server Error");
  }
});


app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
