const fs = require('node:fs/promises');
const path = require('node:path');
const { parentPort, workerData } = require('node:worker_threads');
const { formatTimestamp } = require('./access-log');

const directory = workerData.directory;
const batches = [];
let handle;
let openDay;
let directoryReady;
let draining = false;
let closing = false;
let maintenanceDue = true;
let cleanupDay;
let retryTimer;

function ensureDirectory() {
  if (!directoryReady) {
    directoryReady = fs.mkdir(directory, { recursive: true }).catch((err) => {
      directoryReady = undefined;
      throw err;
    });
  }
  return directoryReady;
}

function warning(err) {
  parentPort.postMessage({ type: 'warning', code: err.code || 'FILE_ERROR' });
}

async function closeFile() {
  const previous = handle;
  handle = undefined;
  openDay = undefined;
  if (previous) await previous.close().catch(warning);
}

async function cleanup() {
  await ensureDirectory();
  const today = formatTimestamp().slice(0, 10);
  // Today's file plus the preceding 89 calendar days, in Asia/Shanghai.
  const cutoff = new Date(Date.parse(`${today}T00:00:00Z`) - 89 * 86400000).toISOString().slice(0, 10);
  const entries = await fs.readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const match = /^access-(\d{4}-\d{2}-\d{2})\.log$/.exec(entry.name);
    if (!entry.isFile() || !match || match[1] >= cutoff) continue;
    if (openDay === match[1]) await closeFile();
    try { await fs.unlink(path.join(directory, entry.name)); }
    catch (err) { if (err.code !== 'ENOENT') warning(err); }
  }
  cleanupDay = today;
}

async function append(day, lines) {
  await ensureDirectory();
  if (openDay !== day) {
    await closeFile();
    handle = await fs.open(path.join(directory, `access-${day}.log`), 'a');
    openDay = day;
  }
  await handle.appendFile(lines, 'utf8');
}

async function writeBatch(records) {
  let day;
  let lines = '';
  for (const record of records) {
    const { at, fields, ...details } = record;
    const time = formatTimestamp(at);
    const recordDay = time.slice(0, 10);
    if (day && day !== recordDay) {
      await append(day, lines);
      lines = '';
    }
    day = recordDay;
    lines += `${JSON.stringify({ time, ...fields, ...details })}\n`;
  }
  if (lines) await append(day, lines);
}

async function drain() {
  if (draining || retryTimer) return;
  draining = true;
  try {
    while (maintenanceDue || batches.length) {
      if (maintenanceDue || cleanupDay !== formatTimestamp().slice(0, 10)) {
        maintenanceDue = false;
        try { await cleanup(); }
        catch (err) {
          cleanupDay = formatTimestamp().slice(0, 10);
          warning(err);
        }
      }
      if (!batches.length) break;
      const records = batches.shift();
      let dropped = 0;
      try { await writeBatch(records); }
      catch (err) {
        dropped = records.length;
        warning(err);
        await closeFile();
        directoryReady = undefined;
        // Pause disk retries while the bounded parent queue keeps forwarding free.
        retryTimer = setTimeout(() => {
          retryTimer = undefined;
          drain().catch(warning);
        }, 1000);
      }
      parentPort.postMessage({ type: 'ack', count: records.length, dropped });
      if (retryTimer) break;
    }
  } finally {
    draining = false;
  }
  if (closing && !batches.length && !retryTimer) {
    clearInterval(maintenanceTimer);
    await closeFile();
    parentPort.postMessage({ type: 'closed' });
    parentPort.close();
  }
}

parentPort.on('message', (message) => {
  if (message.type === 'batch') batches.push(message.records);
  else if (message.type === 'close') closing = true;
  drain().catch((err) => { warning(err); });
});
const maintenanceTimer = setInterval(() => {
  maintenanceDue = true;
  drain().catch(warning);
}, 60 * 60 * 1000);
maintenanceTimer.unref();
drain().catch(warning);
