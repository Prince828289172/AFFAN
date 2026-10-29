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
   WHATSAPP CONNECTION
================================ */
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

      if (connection === 'connecting') console.log(chalk.yellow(`[${sessionId}] WhatsApp connecting...`));

      if (qr) {
        try {
          qrCodes[sessionId] = await QRCode.toDataURL(qr, { errorCorrectionLevel: 'M', margin: 2, width: 300 });
          console.log(chalk.cyan(`[${sessionId}] QR Code Generated`));
        } catch (err) { console.log(chalk.red(`[${sessionId}] QR Error:`), err.message); }
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
        console.log(chalk.red(`[${sessionId}] ⚠️ Connection Closed. Reason: ${statusCode}`));
        db.run(`UPDATE sessions SET isConnected = 0 WHERE id = ?`, [sessionId]);
        
        if (queueTimers[sessionId]) { clearTimeout(queueTimers[sessionId]); delete queueTimers[sessionId]; }
        queueProcessing[sessionId] = false;

        if (statusCode === DisconnectReason.restartRequired || statusCode === 515 || statusCode === 428) {
          console.log(chalk.yellow(`[${sessionId}] 🔄 Fast Reconnecting...`));
          delete connectingSessions[sessionId];
          setTimeout(() => connectWA(phone, sessionId), 1000);
        } else if (statusCode !== DisconnectReason.loggedOut) {
          console.log(chalk.yellow(`[${sessionId}] 🔄 Reconnecting in 5 seconds...`));
          delete connectingSessions[sessionId];
          setTimeout(() => connectWA(phone, sessionId), 5000);
        } else {
          console.log(chalk.red(`[${sessionId}] 🚨 Logged Out.`));
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

// 🔥 FIX: Start Automation (Database write completion wait karega)
app.post('/api/start-automation', (req, res) => {
  const { sessionId, target, prefix = '', messages = [], speed = 5 } = req.body;
  if (!sessionId || !target || !Array.isArray(messages) || messages.length === 0) {
    return res.json({ success: false, message: 'Missing data' });
  }

  const pending = messages.filter(m => String(m).trim());
  if (pending.length === 0) return res.json({ success: false, message: 'No valid messages' });

  const delaySeconds = Math.max(1, Number(speed) || 5);
  let insertedCount = 0;

  pending.forEach(message => {
    db.run(`INSERT INTO messageQueue (sessionId, target, prefix, message, speed, isActive, sentCount) VALUES (?, ?, ?, ?, ?, 1, 0)`, 
    [sessionId, String(target).trim(), prefix, String(message), delaySeconds], 
    (err) => {
      if (err) console.log("Queue Insert Error:", err);
      insertedCount++;
      // 🔥 Jab saare messages DB mein save ho jayein, tabhi Queue start karo
      if (insertedCount === pending.length) {
        console.log(chalk.blue(`[${sessionId}] ${pending.length} messages added to DB. Starting Queue...`));
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

/* ================================
   QUEUE PROCESSOR
================================ */
async function processQueue(sessionId) {
  if (queueProcessing[sessionId]) return;
  queueProcessing[sessionId] = true;

  try {
    const socket = sockets[sessionId];

    if (!socket) {
      console.log(chalk.red(`[${sessionId}] ⏸️ Queue paused: Socket not connected yet.`));
      queueProcessing[sessionId] = false;
      return;
    }

    db.get(`SELECT * FROM messageQueue WHERE sessionId = ? AND isActive = 1 ORDER BY id ASC LIMIT 1`, [sessionId], async (error, item) => {
      if (error) {
        console.log("DB Fetch Error:", error);
        queueProcessing[sessionId] = false;
        return;
      }

      if (!item) {
        console.log(chalk.gray(`[${sessionId}] Queue empty or stopped. Waiting...`));
        queueProcessing[sessionId] = false;
        return;
      }

      try {
        const target = String(item.target || '').trim();
        const chatId = target.endsWith('@g.us') ? target : `${cleanPhone(target)}@s.whatsapp.net`;
        const fullMsg = `${item.prefix || ''} ${item.message}`.trim();

        // 🔥 DEBUG LOG: Pata chale message send ho raha hai
        console.log(chalk.cyan(`[${sessionId}] 📤 Sending message #${item.id} to ${chatId}...`));
        
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
