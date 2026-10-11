const path = require('node:path');
const { Worker } = require('node:worker_threads');

function formatTimestamp(timestamp = Date.now()) {
  return new Date(timestamp + 8 * 60 * 60 * 1000).toISOString().replace('Z', '+08:00');
}

function createAccessLogger({
  directory = path.join(__dirname, 'logs'),
  maxPending = 8192,
  flushInterval = 100,
  closeTimeout = 1000,
} = {}) {
  let worker;
  let available = false;
  let queue = [];
  let offset = 0;
  let pending = 0;
  let flushing = false;
  let closing = false;
  let closed = false;
  let closeSent = false;
  let closePromise;
  let resolveClose;
  let closeTimer;
  let restartTimer;
  let dropped = 0;
  let lastWarning = -Infinity;

  function warn(message) {
    const now = Date.now();
    if (now - lastWarning < 30000) return;
    lastWarning = now;
    console.error(`[${formatTimestamp(now)}] Access log warning: ${message}`);
  }

  function finishClose() {
    if (closed) return;
    closed = true;
    clearTimeout(closeTimer);
    clearTimeout(restartTimer);
    queue = [];
    pending = 0;
    worker?.unref();
    worker?.terminate().catch(() => {});
    resolveClose?.();
  }

  function failWorker(instance, err) {
    if (instance !== worker || !available) return;
    available = false;
    dropped += pending;
    pending = 0;
    queue = [];
    offset = 0;
    warn(`worker unavailable (${err.code || err.message}); forwarding continues`);
    instance.unref();
    instance.terminate().catch(() => {});
    if (closing) finishClose();
    else {
      restartTimer = setTimeout(startWorker, 5000);
      restartTimer.unref();
    }
  }

  function startWorker() {
    if (closing) return;
    try {
      worker = new Worker(path.join(__dirname, 'access-log-worker.js'), {
        workerData: { directory: path.resolve(directory) },
      });
    } catch (err) {
      warn(`cannot start worker (${err.code || err.message}); forwarding continues`);
      restartTimer = setTimeout(startWorker, 5000);
      restartTimer.unref();
      return;
    }
    const instance = worker;
    available = true;
    instance.on('message', (message) => {
      if (instance !== worker || closed) return;
      if (message.type === 'ack') {
        pending = Math.max(0, pending - message.count);
        dropped += message.dropped || 0;
      } else if (message.type === 'warning') {
        warn(`file operation failed (${message.code}); forwarding continues`);
      } else if (message.type === 'closed') finishClose();
    });
    instance.on('error', (err) => failWorker(instance, err));
    instance.on('exit', (code) => {
      if (instance !== worker) return;
      if (closing) finishClose();
      else failWorker(instance, new Error(`exit ${code}`));
    });
    // An idle logger must not keep a stopped proxy alive.
    instance.unref();
  }

  function sendClose() {
    if (!closing || closeSent || flushing || offset < queue.length) return;
    closeSent = true;
    if (!available) finishClose();
    else {
      try { worker.postMessage({ type: 'close' }); }
      catch (err) { failWorker(worker, err); }
    }
  }

  function flush() {
    if (flushing || !available || offset >= queue.length) {
      sendClose();
      return;
    }
    flushing = true;
    setImmediate(function sendBatch() {
      if (closed || !available) {
        flushing = false;
        sendClose();
        return;
      }
      const batch = queue.slice(offset, offset + 256);
      offset += batch.length;
      try { worker.postMessage({ type: 'batch', records: batch }); }
      catch (err) { failWorker(worker, err); }
      if (available && offset < queue.length) setImmediate(sendBatch);
      else {
        queue = [];
        offset = 0;
        flushing = false;
        sendClose();
      }
    });
  }

  startWorker();
  const timer = setInterval(() => {
    if (dropped && Date.now() - lastWarning >= 30000) {
      warn(`${dropped} records dropped while storage or queue was unavailable`);
      dropped = 0;
    }
    flush();
  }, flushInterval);
  timer.unref();

  return {
    write(record) {
      if (closing) return false;
      if (!available || pending >= maxPending) {
        dropped++;
        return false;
      }
      queue.push(record);
      pending++;
      return true;
    },
    close() {
      if (closePromise) return closePromise;
      closing = true;
      if (dropped) warn(`${dropped} records dropped while storage or queue was unavailable`);
      clearInterval(timer);
      clearTimeout(restartTimer);
      closePromise = new Promise((resolve) => { resolveClose = resolve; });
      worker?.ref();
      closeTimer = setTimeout(() => {
        warn('flush deadline reached; remaining records discarded');
        finishClose();
      }, closeTimeout);
      flush();
      return closePromise;
    },
  };
}

module.exports = { createAccessLogger, formatTimestamp };
