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
    const results = [];
    const bufferStream = new stream.PassThrough();
    bufferStream.end(fileBuffer);
    bufferStream.pipe(csv())
      .on('data', (data) => results.push(Object.values(data)[0]))  // extracting first column
      .on('end', () => {
          resolve(results);
      })
      .on('error', (err) => reject(err));
  });
}

function runPythonScript(scriptPath, args, callback) {
  const pythonProcess = spawn('python', [scriptPath, ...args]);
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

app.post('/run-import-cpg-data', upload.single('cpgFile'), (req, res) => {
  try {
    // Access the uploaded file
    const file = req.file;
    console.log(file.originalname); // name of the uploaded file
    console.log(file.buffer);      // file contents as a buffer

    // Access the group name
    const groupName = req.body.groupName;
    console.log(groupName);

    // Save the file temporarily
    const tempFilePath = path.join(__dirname, 'temp', file.originalname);
    fs.writeFileSync(tempFilePath, file.buffer);

    // Call the Python script
    // runPythonScript('./scripts/import-cgp-data.py', [tempFilePath, groupName], (err, data) => {
    //   // After processing, delete the temporary file
    //   fs.unlinkSync(tempFilePath);

    //   if (err) {
    //     return res.status(500).json(err);
    //   }
    //   res.json(data);
    // });

    res.status(200).send("Data processed successfully");
    console.log("Sent response to frontend");
    
} catch (err) {
    console.error(err);
    res.status(500).send("Internal Server Error");
}
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
