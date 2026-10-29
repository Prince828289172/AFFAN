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

const db = new sqlite3.Database(path.join(__dirname, 'automation.db'));

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, phone TEXT, isConnected INTEGER DEFAULT 0, sentCount INTEGER DEFAULT 0)`);
  db.run(`CREATE TABLE IF NOT EXISTS messageQueue (id INTEGER PRIMARY KEY AUTOINCREMENT, sessionId TEXT, target TEXT, prefix TEXT, message TEXT, speed INTEGER, isActive INTEGER DEFAULT 1, sentCount INTEGER DEFAULT 0)`);
  db.run(`CREATE TABLE IF NOT EXISTS sentLogs (id INTEGER PRIMARY KEY AUTOINCREMENT, sessionId TEXT, target TEXT, message TEXT, sentAt DATETIME DEFAULT CURRENT_TIMESTAMP)`);
});

const sockets = {};
const qrCodes = {};
const queueTimers = {};
const queueProcessing = {};
const connectingSessions = {};

function cleanPhone(phone) {
  return String(phone || '').replace(/\D/g, '');
}

async function connectWA(phone, sessionId) {
  if (connectingSessions[sessionId]) return sockets[sessionId];
  connectingSessions[sessionId] = true;

  try {
    const sessionPath = path.join(__dirname, 'sessions', sessionId);
    if (!fs.existsSync(sessionPath)) fs.mkdirSync(sessionPath, { recursive: true });

    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
    const { version } = await fetchLatestBaileysVersion();

    if (sockets[sessionId]) {
      try { sockets[sessionId].ev.removeAllListeners(); sockets[sessionId].end(undefined); delete sockets[sessionId]; } catch (e) {}
    }

    const socket = makeWASocket({
      version,
      logger: pino({ level: 'silent' }),
      browser: Browsers.windows('Chrome'),
      auth: state,
      printQRInTerminal: false,
      markOnlineOnConnect: true,
      keepAliveIntervalMs: 30000,
      connectTimeoutMs: 60000
    });

    sockets[sessionId] = socket;
    socket.ev.on('creds.update', saveCreds);

    socket.ev.on('connection.update', async (update) => {
      const { connection, qr, lastDisconnect } = update;

      if (qr) {
        try {
          qrCodes[sessionId] = await QRCode.toDataURL(qr, { errorCorrectionLevel: 'M', margin: 2, width: 300 });
          console.log(chalk.cyan(`[${sessionId}] QR Code Generated`));
        } catch (err) {}
      }

      if (connection === 'open') {
        console.log(chalk.green(`[${sessionId}] ✅ WhatsApp CONNECTED Successfully!`));
        db.run(`UPDATE sessions SET isConnected = 1 WHERE id = ?`, [sessionId]);
        delete qrCodes[sessionId];
        delete connectingSessions[sessionId];
        processQueue(sessionId);
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        db.run(`UPDATE sessions SET isConnected = 0 WHERE id = ?`, [sessionId]);
        
        if (queueTimers[sessionId]) { clearTimeout(queueTimers[sessionId]); delete queueTimers[sessionId]; }
        queueProcessing[sessionId] = false;

        if (statusCode !== DisconnectReason.loggedOut) {
          delete connectingSessions[sessionId];
          setTimeout(() => connectWA(phone, sessionId), 3000);
        }
      }
    });
    return socket;
  } catch (error) {
    delete connectingSessions[sessionId];
    throw error;
  }
}

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
  
  if (!sessionId) {
    return res.json({ success: false, message: 'Session ID missing! Please reload and try again.' });
  }

  if (!target || !Array.isArray(messages) || messages.length === 0) {
    return res.json({ success: false, message: 'Missing data' });
  }

  const pending = messages.filter(m => String(m).trim());
  const delaySeconds = Math.max(1, Number(speed) || 5);
  let insertedCount = 0;

  console.log(chalk.blue(`[API] Starting automation for ${sessionId} with ${pending.length} msgs.`));

  pending.forEach(message => {
    db.run(`INSERT INTO messageQueue (sessionId, target, prefix, message, speed, isActive, sentCount) VALUES (?, ?, ?, ?, ?, 1, 0)`, 
    [sessionId, String(target).trim(), prefix, String(message), delaySeconds], 
    (err) => {
      insertedCount++;
      if (insertedCount === pending.length) {
        processQueue(sessionId);
      }
    });
  });

  res.json({ success: true, message: `${pending.length} message(s) queued`, delay: delaySeconds });
});

app.post('/api/stop-automation', (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.json({ success: false, message: 'Session required' });
  db.run(`UPDATE messageQueue SET isActive = 0 WHERE sessionId = ?`, [sessionId]);
  if (queueTimers[sessionId]) { clearTimeout(queueTimers[sessionId]); delete queueTimers[sessionId]; }
  queueProcessing[sessionId] = false;
  res.json({ success: true, message: 'Automation stopped' });
});

async function processQueue(sessionId) {
  if (queueProcessing[sessionId]) return;
  queueProcessing[sessionId] = true;

  const socket = sockets[sessionId];
  if (!socket) {
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
      // 🔥 FIX: Group ID clean mat karo, number ko clean karo
      const chatId = target.endsWith('@g.us') ? target : `${cleanPhone(target)}@s.whatsapp.net`;
      const fullMsg = `${item.prefix || ''} ${item.message}`.trim();

      console.log(chalk.cyan(`[QUEUE] 📤 Sending to ${chatId}...`));
      await socket.sendMessage(chatId, { text: fullMsg });
      console.log(chalk.green(`[QUEUE] ✅ Sent successfully!`));

      db.run(`INSERT INTO sentLogs (sessionId, target, message) VALUES (?, ?, ?)`, [sessionId, target, fullMsg]);
      db.run(`UPDATE sessions SET sentCount = sentCount + 1 WHERE id = ?`, [sessionId]);

      db.run(`UPDATE messageQueue SET sentCount = sentCount + 1, isActive = 0 WHERE id = ?`, [item.id], () => {
        queueProcessing[sessionId] = false;
        const delayMs = Math.max(1, Number(item.speed) || 5) * 1000;
        queueTimers[sessionId] = setTimeout(() => { delete queueTimers[sessionId]; processQueue(sessionId); }, delayMs);
      });

    } catch (error) {
      console.log(chalk.red(`[QUEUE] ❌ Send error:`), error.message);
      db.run(`UPDATE messageQueue SET isActive = 0 WHERE id = ?`, [item.id], () => {
        queueProcessing[sessionId] = false;
        queueTimers[sessionId] = setTimeout(() => { delete queueTimers[sessionId]; processQueue(sessionId); }, 1000);
      });
    }
  });
}

app.get('/api/sessions', (req, res) => {
  db.all(`SELECT * FROM sessions ORDER BY rowid DESC`, [], (error, rows) => {
    res.json({ success: true, sessions: rows || [] });
  });
});

app.get('/api/groups/:sessionId', async (req, res) => {
  try {
    const socket = sockets[req.params.sessionId];
    if (!socket) return res.json({ success: false, message: 'Session not connected' });
    const groups = await socket.groupFetchAllParticipating();
    res.json({ success: true, groups: Object.values(groups || {}).map(g => ({ id: g.id, subject: g.subject })) });
  } catch (error) { res.json({ success: false, message: error.message }); }
});

app.get('/api/logs/:sessionId', (req, res) => {
  db.all(`SELECT * FROM sentLogs WHERE sessionId = ? ORDER BY id DESC LIMIT 50`, [req.params.sessionId], (error, rows) => {
    res.json({ success: true, logs: rows || [] });
  });
});

app.get('/', (req, res) => { res.sendFile(path.join(__dirname, 'public', 'index.html')); });
app.listen(PORT, '0.0.0.0', () => { console.log(chalk.green(`✅ Server on port ${PORT}`)); });
