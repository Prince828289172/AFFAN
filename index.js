import express from 'express';
import fs from 'fs';
import chalk from 'chalk';
import { 
  makeWASocket,
  useMultiFileAuthState,
  Browsers,
  fetchLatestBaileysVersion,
  DisconnectReason
} from '@whiskeysockets/baileys';
import pino from 'pino';
import sqlite3 from 'sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';
import cors from 'cors';
import QRCode from 'qrcode';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

/* ================================
   DATABASE
================================ */
const db = new sqlite3.Database(path.join(__dirname, 'automation.db'));

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, phone TEXT, isConnected INTEGER DEFAULT 0, sentCount INTEGER DEFAULT 0)`);
  db.run(`CREATE TABLE IF NOT EXISTS messageQueue (id INTEGER PRIMARY KEY AUTOINCREMENT, sessionId TEXT, target TEXT, prefix TEXT, message TEXT, speed INTEGER, isActive INTEGER DEFAULT 1, sentCount INTEGER DEFAULT 0)`);
  db.run(`CREATE TABLE IF NOT EXISTS sentLogs (id INTEGER PRIMARY KEY AUTOINCREMENT, sessionId TEXT, target TEXT, message TEXT, sentAt DATETIME DEFAULT CURRENT_TIMESTAMP)`);
});

/* ================================
   MEMORY
================================ */
const sockets = {};
const qrCodes = {};
const queueTimers = {};
const queueProcessing = {};
const connectingSessions = {};

function cleanPhone(phone) {
  return String(phone || '').replace(/\D/g, '');
}

/* ================================
   WHATSAPP CONNECTION (Stable)
================================ */
async function connectWA(phone, sessionId) {
  // Agar pehle se connect ho raha hai toh wait karo
  if (connectingSessions[sessionId]) return sockets[sessionId];
  connectingSessions[sessionId] = true;

  try {
    const sessionPath = path.join(__dirname, 'sessions', sessionId);
    if (!fs.existsSync(sessionPath)) fs.mkdirSync(sessionPath, { recursive: true });

    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
    const { version } = await fetchLatestBaileysVersion();

    // 🔥 STABILITY FIX: Purane socket ko clean karo taaki conflict na ho
    if (sockets[sessionId]) {
      try {
        sockets[sessionId].ev.removeAllListeners();
        sockets[sessionId].end(undefined);
        delete sockets[sessionId];
      } catch (e) {}
    }

    const socket = makeWASocket({
      version,
      logger: pino({ level: 'silent' }),
      browser: Browsers.windows('Chrome'),
      auth: state,
      printQRInTerminal: false,
      markOnlineOnConnect: true,
      generateHighQualityLinkPreview: false,
      syncFullHistory: false,
      keepAliveIntervalMs: 30000, // Connection alive rakhne ke liye
      connectTimeoutMs: 60000,
      defaultQueryTimeoutMs: 60000,
      // 🔥 FIX: Multi-device conflict se bachne ke liye
      getMessage: async () => { return { conversation: 'Wait' } }
    });

    sockets[sessionId] = socket;

    socket.ev.on('creds.update', saveCreds);

    socket.ev.on('connection.update', async (update) => {
      const { connection, qr, lastDisconnect } = update;

      if (connection === 'connecting') {
        console.log(chalk.yellow(`[${sessionId}] WhatsApp connecting...`));
      }

      if (qr) {
        try {
          qrCodes[sessionId] = await QRCode.toDataURL(qr, { errorCorrectionLevel: 'M', margin: 2, width: 300 });
          console.log(chalk.cyan(`[${sessionId}] QR Code Generated`));
        } catch (err) {
          console.log(chalk.red(`[${sessionId}] QR Error:`), err.message);
        }
      }

      if (connection === 'open') {
        console.log(chalk.green(`[${sessionId}] ✅ WhatsApp CONNECTED Successfully!`));
        db.run(`UPDATE sessions SET isConnected = 1 WHERE id = ?`, [sessionId]);
        delete qrCodes[sessionId];
        delete connectingSessions[sessionId];
        
        // Queue start karo jab connection open ho
        processQueue(sessionId);
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const errorMessage = lastDisconnect?.error?.message || 'Unknown';
        console.log(chalk.red(`[${sessionId}] ⚠️ Connection Closed. Reason: ${statusCode} (${errorMessage})`));
        
        db.run(`UPDATE sessions SET isConnected = 0 WHERE id = ?`, [sessionId]);
        
        if (queueTimers[sessionId]) {
          clearTimeout(queueTimers[sessionId]);
          delete queueTimers[sessionId];
        }
        queueProcessing[sessionId] = false;

        // 🔥 STABILITY FIX: 515 (Restart Required) ya 428 (Connection Closed) par turant reconnect karo
        if (statusCode === DisconnectReason.restartRequired || statusCode === 515 || statusCode === 428 || statusCode === 408) {
          console.log(chalk.yellow(`[${sessionId}] 🔄 Fast Reconnecting (Code: ${statusCode})...`));
          delete connectingSessions[sessionId];
          setTimeout(() => connectWA(phone, sessionId), 1000); // 1 second baad turant wapas
        } 
        else if (statusCode !== DisconnectReason.loggedOut) {
          console.log(chalk.yellow(`[${sessionId}] 🔄 Reconnecting in 5 seconds...`));
          delete connectingSessions[sessionId];
          setTimeout(() => connectWA(phone, sessionId), 5000);
        } 
        else {
          console.log(chalk.red(`[${sessionId}] 🚨 Logged Out. Please scan QR again.`));
          delete sockets[sessionId];
          delete connectingSessions[sessionId];
        }
      }
    });

    return socket;
  } catch (error) {
    delete connectingSessions[sessionId];
    console.log(chalk.red(`[${sessionId}] Connection error:`), error.message);
    throw error;
  }
}

/* ================================
   ROUTES
================================ */
app.post('/api/login', async (req, res) => {
  try {
    const phone = cleanPhone(req.body.phone);
    if (!phone) return res.json({ success: false, message: 'Phone number required' });

    const sessionId = `session_${Date.now()}`;
    db.run(`INSERT INTO sessions (id, phone, isConnected, sentCount) VALUES (?, ?, 0, 0)`, [sessionId, phone]);

    await connectWA(phone, sessionId);
    res.json({ success: true, sessionId, message: 'QR Code is being generated' });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

app.get('/api/qr/:sessionId', (req, res) => {
  const qr = qrCodes[req.params.sessionId];
  if (!qr) return res.json({ success: false, status: 'waiting' });
  res.json({ success: true, status: 'ready', qrCode: qr });
});

app.post('/api/start-automation', (req, res) => {
  const { sessionId, target, prefix = '', messages = [], speed = 5 } = req.body;
  if (!sessionId || !target || !Array.isArray(messages) || messages.length === 0) {
    return res.json({ success: false, message: 'Missing data' });
  }

  const pending = messages.filter(m => String(m).trim());
  if (pending.length === 0) return res.json({ success: false, message: 'No valid messages' });

  const delaySeconds = Math.max(1, Number(speed) || 5);

  pending.forEach(message => {
    db.run(`INSERT INTO messageQueue (sessionId, target, prefix, message, speed, isActive, sentCount) VALUES (?, ?, ?, ?, ?, 1, 0)`, [sessionId, String(target).trim(), prefix, String(message), delaySeconds]);
  });

  processQueue(sessionId);
  res.json({ success: true, message: `${pending.length} message(s) queued`, delay: delaySeconds });
});

app.post('/api/stop-automation', (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.json({ success: false, message: 'Session required' });

  db.run(`UPDATE messageQueue SET isActive = 0 WHERE sessionId = ?`, [sessionId]);

  if (queueTimers[sessionId]) {
    clearTimeout(queueTimers[sessionId]);
    delete queueTimers[sessionId];
  }
  queueProcessing[sessionId] = false;
  res.json({ success: true, message: 'Automation stopped' });
});

/* ================================
   QUEUE PROCESSOR
================================ */
async function processQueue(sessionId) {
  if (queueProcessing[sessionId]) return;
  queueProcessing[sessionId] = true;

  try {
    const socket = sockets[sessionId];

    // Agar socket connected nahi hai, toh wait karo
    if (!socket) {
      console.log(chalk.yellow(`[${sessionId}] Socket not ready. Waiting for connection...`));
      queueProcessing[sessionId] = false;
      return;
    }

    db.get(`SELECT * FROM messageQueue WHERE sessionId = ? AND isActive = 1 ORDER BY id ASC LIMIT 1`, [sessionId], async (error, item) => {
      if (error || !item) {
        queueProcessing[sessionId] = false;
        return;
      }

      try {
        const target = String(item.target || '').trim();
        if (!target) {
          db.run(`UPDATE messageQueue SET isActive = 0 WHERE id = ?`, [item.id], () => {
            queueProcessing[sessionId] = false;
            processQueue(sessionId);
          });
          return;
        }

        const chatId = target.endsWith('@g.us') ? target : `${cleanPhone(target)}@s.whatsapp.net`;
        const fullMsg = `${item.prefix || ''} ${item.message}`.trim();

        console.log(chalk.cyan(`[${sessionId}] Sending message #${item.id} to ${target}...`));
        await socket.sendMessage(chatId, { text: fullMsg });
        console.log(chalk.green(`[${sessionId}] ✅ Message sent successfully!`));

        db.run(`INSERT INTO sentLogs (sessionId, target, message) VALUES (?, ?, ?)`, [sessionId, target, fullMsg]);
        db.run(`UPDATE sessions SET sentCount = sentCount + 1 WHERE id = ?`, [sessionId]);

        db.run(`UPDATE messageQueue SET sentCount = sentCount + 1, isActive = 0 WHERE id = ?`, [item.id], () => {
          queueProcessing[sessionId] = false;
          const delayMs = Math.max(1, Number(item.speed) || 5) * 1000;
          
          queueTimers[sessionId] = setTimeout(() => {
            delete queueTimers[sessionId];
            processQueue(sessionId);
          }, delayMs);
        });

      } catch (error) {
        console.log(chalk.red(`[${sessionId}] ❌ Message send error:`), error.message);
        db.run(`UPDATE messageQueue SET isActive = 0 WHERE id = ?`, [item.id], () => {
          queueProcessing[sessionId] = false;
          queueTimers[sessionId] = setTimeout(() => {
            delete queueTimers[sessionId];
            processQueue(sessionId);
          }, 1000);
        });
      }
    });
  } catch (error) {
    queueProcessing[sessionId] = false;
  }
}

/* ================================
   OTHER ROUTES
================================ */
app.get('/api/sessions', (req, res) => {
  db.all(`SELECT * FROM sessions ORDER BY rowid DESC`, [], (error, rows) => {
    if (error) return res.json({ success: false, message: error.message });
    res.json({ success: true, sessions: rows });
  });
});

app.get('/api/groups/:sessionId', async (req, res) => {
  try {
    const socket = sockets[req.params.sessionId];
    if (!socket) return res.json({ success: false, message: 'Session not connected' });

    const groups = await socket.groupFetchAllParticipating();
    const groupList = Object.values(groups || {}).map(g => ({ id: g.id, subject: g.subject || 'Unnamed Group' })).sort((a, b) => a.subject.localeCompare(b.subject));
    res.json({ success: true, groups: groupList });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

app.get('/api/logs/:sessionId', (req, res) => {
  db.all(`SELECT * FROM sentLogs WHERE sessionId = ? ORDER BY id DESC LIMIT 100`, [req.params.sessionId], (error, rows) => {
    if (error) return res.json({ success: false, message: error.message });
    res.json({ success: true, logs: rows });
  });
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(chalk.green(`✅ Server running on port ${PORT}`));
});
