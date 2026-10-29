import express from 'express';
import fs from 'fs';
import chalk from 'chalk';
import makeWASocket, {
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
import QRCode from 'qrcode'; // 🔥 QR Code ke liye import kiya

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '2mb' }));

app.use(
  express.static(
    path.join(__dirname, 'public')
  )
);


/* ================================
   DATABASE
================================ */

const db = new sqlite3.Database(
  path.join(__dirname, 'automation.db')
);

db.serialize(() => {

  db.run(`
    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      phone TEXT,
      isConnected INTEGER DEFAULT 0,
      sentCount INTEGER DEFAULT 0
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS messageQueue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sessionId TEXT,
      target TEXT,
      prefix TEXT,
      message TEXT,
      speed INTEGER,
      isActive INTEGER DEFAULT 1,
      sentCount INTEGER DEFAULT 0
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS sentLogs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sessionId TEXT,
      target TEXT,
      message TEXT,
      sentAt DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

});


/* ================================
   MEMORY
================================ */

const sockets = {};
const qrCodes = {}; // 🔥 OTP ki jagah QR Codes store honge
const queueTimers = {};
const queueProcessing = {};
const connectingSessions = {};


/* ================================
   PHONE CLEANER
================================ */

function cleanPhone(phone) {

  return String(phone || '')
    .replace(/\D/g, '');

}


/* ================================
   WHATSAPP CONNECTION
================================ */

async function connectWA(phone, sessionId) {

  if (connectingSessions[sessionId]) {
    return sockets[sessionId];
  }

  connectingSessions[sessionId] = true;

  try {

    const sessionPath = path.join(
      __dirname,
      'sessions',
      sessionId
    );

    if (!fs.existsSync(sessionPath)) {

      fs.mkdirSync(
        sessionPath,
        {
          recursive: true
        }
      );

    }


    const {
      state,
      saveCreds
    } = await useMultiFileAuthState(
      sessionPath
    );


    const {
      version
    } = await fetchLatestBaileysVersion();


    const socket = makeWASocket({

      version,

      logger: pino({
        level: 'silent'
      }),

      browser:
        Browsers.windows('Chrome'),

      auth: state,

      printQRInTerminal: false,

      markOnlineOnConnect: true,

      keepAliveIntervalMs: 30000,

      connectTimeoutMs: 60000

    });


    sockets[sessionId] = socket;


    /* ================================
       SAVE CREDENTIALS
    ================================= */

    socket.ev.on(
      'creds.update',
      saveCreds
    );


    /* ================================
       CONNECTION UPDATE
    ================================= */

    socket.ev.on(
      'connection.update',
      async (update) => {

        const {
          connection,
          qr, // 🔥 QR String nikal rahe hain
          lastDisconnect
        } = update;


        if (
          connection === 'connecting'
        ) {

          console.log(
            chalk.yellow(
              `[${sessionId}] WhatsApp connecting...`
            )
          );

        }

        /* 🔥 QR CODE GENERATION LOGIC YAHAN HAI */
        if (qr) {
          try {
            qrCodes[sessionId] = await QRCode.toDataURL(qr, {
              errorCorrectionLevel: 'M',
              margin: 2,
              width: 300
            });
            console.log(chalk.cyan(`[${sessionId}] QR Code Generated`));
          } catch (err) {
            console.log(chalk.red(`[${sessionId}] QR Error:`), err.message);
          }
        }


        if (
          connection === 'open'
        ) {

          console.log(
            chalk.green(
              `[${sessionId}] WhatsApp CONNECTED`
            )
          );


          db.run(
            `
            UPDATE sessions
            SET isConnected = 1
            WHERE id = ?
            `,
            [sessionId]
          );


          delete qrCodes[
            sessionId
          ];


          delete connectingSessions[
            sessionId
          ];


          /*
           * Start queue.
           * Processor itself controls
           * the exact message delay.
           */

          processQueue(
            sessionId
          );

        }


        if (
          connection === 'close'
        ) {

          console.log(
            chalk.red(
              `[${sessionId}] WhatsApp connection closed`
            )
          );


          db.run(
            `
            UPDATE sessions
            SET isConnected = 0
            WHERE id = ?
            `,
            [sessionId]
          );


          delete sockets[
            sessionId
          ];


          delete connectingSessions[
            sessionId
          ];


          /*
           * Clear pending timer.
           */

          if (
            queueTimers[sessionId]
          ) {

            clearTimeout(
              queueTimers[sessionId]
            );

            delete queueTimers[
              sessionId
            ];

          }


          queueProcessing[
            sessionId
          ] = false;


          /*
           * Reconnect after 5 seconds.
           */

          const statusCode = (lastDisconnect?.error)?.output?.statusCode;
          
          if (statusCode !== DisconnectReason.loggedOut) {
            setTimeout(
              () => {
                connectWA(
                  phone,
                  sessionId
                ).catch(
                  error => {
                    console.log(
                      chalk.red(
                        `[${sessionId}] Reconnect error:`
                      ),
                      error.message
                    );
                  }
                );
              },
              5000
            );
          }

        }

      }
    );


    return socket;

  } catch (error) {

    delete connectingSessions[
      sessionId
    ];

    console.log(
      chalk.red(
        `[${sessionId}] Connection error:`
      ),
      error.message
    );

    throw error;

  }

}


/* ================================
   LOGIN
================================ */

app.post(
  '/api/login',
  async (req, res) => {

    try {

      const phone =
        cleanPhone(
          req.body.phone
        );


      if (!phone) {

        return res.json({

          success: false,

          message:
            'Phone number required'

        });

      }


      const sessionId =
        `session_${Date.now()}`;


      db.run(
        `
        INSERT INTO sessions
        (id, phone, isConnected, sentCount)
        VALUES (?, ?, 0, 0)
        `,
        [
          sessionId,
          phone
        ]
      );


      await connectWA(
        phone,
        sessionId
      );


      res.json({

        success: true,

        sessionId,

        message:
          'QR Code is being generated'

      });


    } catch (error) {

      console.error(error);


      res.json({

        success: false,

        message:
          error.message

      });

    }

  }
);


/* ================================
   QR CODE ENDPOINT (Naya)
================================ */

app.get(
  '/api/qr/:sessionId',
  (req, res) => {

    const sessionId = req.params.sessionId;
    const qr = qrCodes[sessionId];

    if (!qr) {

      return res.json({
        success: false,
        status: 'waiting'
      });

    }

    res.json({
      success: true,
      status: 'ready',
      qrCode: qr
    });

  }
);


/* ================================
   START AUTOMATION
================================ */

app.post(
  '/api/start-automation',
  (req, res) => {

    const {
      sessionId,
      target,
      prefix = '',
      messages = [],
      speed = 5
    } = req.body;


    if (
      !sessionId ||
      !target ||
      !Array.isArray(messages) ||
      messages.length === 0
    ) {

      return res.json({

        success: false,

        message:
          'Missing automation data'

      });

    }


    const pending =
      messages.filter(
        message =>
          String(message).trim()
      );


    if (
      pending.length === 0
    ) {

      return res.json({

        success: false,

        message:
          'No valid messages'

      });

    }


    const delaySeconds =
      Math.max(
        1,
        Number(speed) || 5
      );


    pending.forEach(
      message => {

        db.run(
          `
          INSERT INTO messageQueue
          (
            sessionId,
            target,
            prefix,
            message,
            speed,
            isActive,
            sentCount
          )
          VALUES (?, ?, ?, ?, ?, 1, 0)
          `,
          [
            sessionId,
            String(target).trim(),
            prefix,
            String(message),
            delaySeconds
          ]
        );

      }
    );


    /*
     * Start processor immediately.
     * It will respect each item's
     * configured delay.
     */

    processQueue(
      sessionId
    );


    res.json({

      success: true,

      message:
        `${pending.length} message(s) added to queue`,

      delay:
        delaySeconds

    });

  }
);


/* ================================
   STOP AUTOMATION
================================ */

app.post(
  '/api/stop-automation',
  (req, res) => {

    const {
      sessionId
    } = req.body;


    if (!sessionId) {

      return res.json({

        success: false,

        message:
          'Session ID required'

      });

    }


    db.run(
      `
      UPDATE messageQueue
      SET isActive = 0
      WHERE sessionId = ?
      `,
      [sessionId]
    );


    if (
      queueTimers[sessionId]
    ) {

      clearTimeout(
        queueTimers[sessionId]
      );

      delete queueTimers[
        sessionId
      ];

    }


    queueProcessing[
      sessionId
    ] = false;


    res.json({

      success: true,

      message:
        'Automation stopped'

    });

  }
);


/* ================================
   QUEUE PROCESSOR
================================ */

async function processQueue(
  sessionId
) {

  if (
    queueProcessing[sessionId]
  ) {

    return;

  }


  queueProcessing[
    sessionId
  ] = true;


  try {

    const socket =
      sockets[sessionId];


    if (!socket) {

      queueProcessing[
        sessionId
      ] = false;

      return;

    }


    db.get(
      `
      SELECT *
      FROM messageQueue
      WHERE sessionId = ?
      AND isActive = 1
      ORDER BY id ASC
      LIMIT 1
      `,
      [sessionId],
      async (
        error,
        item
      ) => {

        if (error) {

          console.log(
            chalk.red(
              `[${sessionId}] Queue database error:`
            ),
            error.message
          );

          queueProcessing[
            sessionId
          ] = false;

          return;

        }


        if (!item) {

          queueProcessing[
            sessionId
          ] = false;

          return;

        }


        try {

          const target =
            String(item.target || '').trim();


          if (!target) {

            db.run(
              `
              UPDATE messageQueue
              SET isActive = 0
              WHERE id = ?
              `,
              [item.id],
              () => {

                queueProcessing[
                  sessionId
                ] = false;

                processQueue(
                  sessionId
                );

              }
            );

            return;

          }


          /*
           * NUMBER = @s.whatsapp.net
           * GROUP  = @g.us
           */

          const chatId =
            target.endsWith('@g.us')
              ? target
              : `${cleanPhone(target)}@s.whatsapp.net`;


          const fullMsg =
            `${item.prefix || ''} ${item.message}`
              .trim();


          console.log(
            chalk.cyan(
              `[${sessionId}] Sending message #${item.id} to ${target}...`
            )
          );


          /*
           * ACTUAL SEND
           */

          await socket.sendMessage(
            chatId,
            {
              text: fullMsg
            }
          );


          console.log(
            chalk.green(
              `[${sessionId}] Message sent successfully`
            )
          );


          /*
           * SAVE LOG
           */

          db.run(
            `
            INSERT INTO sentLogs
            (
              sessionId,
              target,
              message
            )
            VALUES (?, ?, ?)
            `,
            [
              sessionId,
              target,
              fullMsg
            ]
          );


          /*
           * UPDATE SESSION COUNT
           */

          db.run(
            `
            UPDATE sessions
            SET sentCount =
              sentCount + 1
            WHERE id = ?
            `,
            [sessionId]
          );


          /*
           * MARK CURRENT MESSAGE COMPLETE
           */

          db.run(
            `
            UPDATE messageQueue
            SET
              sentCount =
                sentCount + 1,
              isActive = 0
            WHERE id = ?
            `,
            [item.id],
            () => {

              queueProcessing[
                sessionId
              ] = false;


              /*
               * IMPORTANT:
               *
               * Wait exactly the number
               * of seconds selected by user.
               */

              const delaySeconds =
                Math.max(
                  1,
                  Number(item.speed) || 5
                );


              const delayMs =
                delaySeconds * 1000;


              console.log(
                chalk.yellow(
                  `[${sessionId}] Next message in ${delaySeconds} second(s)`
                )
              );


              queueTimers[
                sessionId
              ] = setTimeout(
                () => {

                  delete queueTimers[
                    sessionId
                  ];

                  processQueue(
                    sessionId
                  );

                },
                delayMs
              );

            }
          );


        } catch (error) {

          console.log(
            chalk.red(
              `[${sessionId}] Message send error:`
            ),
            error.message
          );


          /*
           * Failed message is stopped
           * so it doesn't retry endlessly.
           */

          db.run(
            `
            UPDATE messageQueue
            SET isActive = 0
            WHERE id = ?
            `,
            [item.id],
            () => {

              queueProcessing[
                sessionId
              ] = false;


              /*
               * Continue with next queued
               * message after a small delay.
               */

              queueTimers[
                sessionId
              ] = setTimeout(
                () => {

                  delete queueTimers[
                    sessionId
                  ];

                  processQueue(
                    sessionId
                  );

                },
                1000
              );

            }
          );

        }

      }
    );

  } catch (error) {

    console.log(
      chalk.red(
        `[${sessionId}] Queue processor error:`
      ),
      error.message
    );


    queueProcessing[
      sessionId
    ] = false;

  }

}


/* ================================
   SESSIONS
================================ */

app.get(
  '/api/sessions',
  (req, res) => {

    db.all(
      `
      SELECT *
      FROM sessions
      ORDER BY rowid DESC
      `,
      [],
      (error, rows) => {

        if (error) {

          return res.json({

            success: false,

            message:
              error.message

          });

        }


        res.json({

          success: true,

          sessions:
            rows

        });

      }
    );

  }
);


/* ================================
   GROUPS
================================ */

app.get(
  '/api/groups/:sessionId',
  async (req, res) => {

    try {

      const sessionId =
        req.params.sessionId;


      const socket =
        sockets[sessionId];


      if (!socket) {

        return res.json({

          success: false,

          message:
            'WhatsApp session is not connected'

        });

      }


      const groups =
        await socket.groupFetchAllParticipating();


      const groupList =
        Object.values(groups || {})
          .map(group => ({

            id:
              group.id,

            subject:
              group.subject || 'Unnamed Group'

          }))
          .sort(
            (a, b) =>
              a.subject.localeCompare(
                b.subject
              )
          );


      res.json({

        success: true,

        groups:
          groupList

      });

    } catch (error) {

      console.log(
        chalk.red(
          'Group fetch error:'
        ),
        error.message
      );


      res.json({

        success: false,

        message:
          error.message

      });

    }

  }
);


/* ================================
   LOGS
================================ */

app.get(
  '/api/logs/:sessionId',
  (req, res) => {

    db.all(
      `
      SELECT *
      FROM sentLogs
      WHERE sessionId = ?
      ORDER BY id DESC
      LIMIT 100
      `,
      [
        req.params.sessionId
      ],
      (error, rows) => {

        if (error) {

          return res.json({

            success: false,

            message:
              error.message

          });

        }


        res.json({

          success: true,

          logs:
            rows

        });

      }
    );

  }
);


/* ================================
   HOME
================================ */

app.get(
  '/',
  (req, res) => {

    res.sendFile(
      path.join(
        __dirname,
        'public',
        'index.html'
      )
    );

  }
);


/* ================================
   SERVER
================================ */

app.listen(
  PORT,
  '0.0.0.0',
  () => {

    console.log('');

    console.log(
      chalk.green(
        '======================================'
      )
    );

    console.log(
      chalk.green(
        '       THE MOST WANTED AFFAN PAPA TOOL'
      )
    );

    console.log(
      chalk.green(
        '       WHATSAPP SERVER ONLINE'
      )
    );

    console.log(
      chalk.green(
        `       PORT: ${PORT}`
      )
    );

    console.log(
      chalk.green(
        '======================================'
      )
    );

  }
);
