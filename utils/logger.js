const fs = require('fs');
const path = require('path');

const logDirectory = path.join(__dirname, '../logs');
const logFile = path.join(logDirectory, 'RSBSAetl.log');

if (!fs.existsSync(logDirectory)) {
  fs.mkdirSync(logDirectory, { recursive: true });
}

const logger = {
  log: (message) => {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}\n`;
    console.log(logMessage);
    fs.appendFileSync(logFile, logMessage, { flag: 'a' });
  },
  error: (message) => {
    const timestamp = new Date().toISOString();
    const errorMessage = `[${timestamp}] ERROR: ${message}\n`;
    console.error(errorMessage);
    fs.appendFileSync(logFile, errorMessage, { flag: 'a' });
  },
  debug: (message) => {
    const timestamp = new Date().toISOString();
    const debugMessage = `[${timestamp}] DEBUG: ${message}\n`;
    fs.appendFileSync(logFile, debugMessage, { flag: 'a' });
  }
};

module.exports = logger;