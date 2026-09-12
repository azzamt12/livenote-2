require("dotenv").config();

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");

const app = express();

const PORT = Number(process.env.PORT || 3000);
const NOTE_ID = 1;
const DB_HOST = process.env.DB_HOST || "localhost";
const DB_PORT = Number(process.env.DB_PORT || 3306);
const DB_USER = process.env.DB_USER || "livenote";
const DB_PASSWORD = process.env.DB_PASSWORD || "livenote";
const DB_NAME = process.env.DB_NAME || "livenote";
const DB_ROOT_USER = process.env.DB_ROOT_USER || "root";
const DB_ROOT_PASSWORD = process.env.DB_ROOT_PASSWORD || "root";

app.use(cors());
app.use(express.json({ limit: "2mb" }));

function log(message, details = {}) {
  console.log(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      message,
      ...details
    })
  );
}

function logError(message, error, details = {}) {
  console.error(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      message,
      error: error.message,
      stack: error.stack,
      ...details
    })
  );
}

app.use((req, res, next) => {
  const startedAt = Date.now();

  res.on("finish", () => {
    log("request completed", {
      method: req.method,
      path: req.originalUrl,
      statusCode: res.statusCode,
      durationMs: Date.now() - startedAt
    });
  });

  next();
});

function createAppPool() {
  return mysql.createPool({
    host: DB_HOST,
    port: DB_PORT,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  });
}

let pool = createAppPool();

const clients = new Set();

function sendEvent(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function broadcastNote(note) {
  log("broadcasting note update", {
    revision: note.revision,
    connectedClients: clients.size
  });

  for (const client of clients) {
    sendEvent(client, "note", note);
  }
}

function uniqueLines(lines) {
  return lines.filter((line, index) => lines.indexOf(line) === index);
}

function assertSafeIdentifier(value, label) {
  if (!/^[A-Za-z0-9_]+$/.test(value)) {
    throw new Error(`${label} may only contain letters, numbers, and underscores`);
  }
}

function mergeConcurrentText(baseContent, currentContent, incomingContent) {
  if (incomingContent === currentContent) {
    return currentContent;
  }

  if (baseContent === currentContent) {
    return incomingContent;
  }

  if (baseContent === incomingContent) {
    return currentContent;
  }

  if (currentContent.includes(incomingContent)) {
    return currentContent;
  }

  if (incomingContent.includes(currentContent)) {
    return incomingContent;
  }

  const baseLines = baseContent.split("\n");
  const currentLines = currentContent.split("\n");
  const incomingLines = incomingContent.split("\n");
  const addedLines = uniqueLines(incomingLines.filter((line) => !baseLines.includes(line)));

  if (addedLines.length === 0) {
    return currentContent;
  }

  const missingLines = addedLines.filter((line) => !currentLines.includes(line));

  if (missingLines.length === 0) {
    return currentContent;
  }

  const separator = currentContent.endsWith("\n") || currentContent.length === 0 ? "" : "\n";
  return `${currentContent}${separator}${missingLines.join("\n")}`;
}

async function bootstrapDatabase() {
  assertSafeIdentifier(DB_NAME, "DB_NAME");

  log("bootstrapping missing database", {
    host: DB_HOST,
    port: DB_PORT,
    database: DB_NAME,
    rootUser: DB_ROOT_USER,
    appUser: DB_USER
  });

  const connection = await mysql.createConnection({
    host: DB_HOST,
    port: DB_PORT,
    user: DB_ROOT_USER,
    password: DB_ROOT_PASSWORD,
    database: "mysql"
  });

  try {
    await connection.query(`CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\``);
    await connection.query(
      `GRANT ALL PRIVILEGES ON \`${DB_NAME}\`.* TO ?@'%'`,
      [DB_USER]
    );
    await connection.query("FLUSH PRIVILEGES");

    log("missing database bootstrapped", {
      database: DB_NAME,
      appUser: DB_USER
    });
  } finally {
    await connection.end();
  }
}

async function createNotesTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS notes (
      id INT PRIMARY KEY,
      content MEDIUMTEXT NOT NULL,
      revision INT NOT NULL DEFAULT 1,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(
    "INSERT IGNORE INTO notes (id, content, revision) VALUES (?, ?, ?)",
    [NOTE_ID, "", 1]
  );
}

async function initializeDatabase() {
  log("initializing database", {
    host: DB_HOST,
    port: DB_PORT,
    database: DB_NAME,
    user: DB_USER
  });

  try {
    await createNotesTable();
  } catch (error) {
    if (error.code !== "ER_BAD_DB_ERROR") {
      throw error;
    }

    logError("configured database is missing", error, { database: DB_NAME });
    await pool.end();
    await bootstrapDatabase();
    pool = createAppPool();
    await createNotesTable();
  }

  log("database initialized");
}

async function getNote(connection = pool) {
  log("fetching note", { noteId: NOTE_ID });

  const [rows] = await connection.query(
    "SELECT id, content, revision, updated_at AS updatedAt FROM notes WHERE id = ?",
    [NOTE_ID]
  );

  log("note fetched", {
    noteId: NOTE_ID,
    revision: rows[0]?.revision,
    contentLength: rows[0]?.content?.length ?? 0
  });

  return rows[0];
}

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

app.get("/api/note", async (req, res, next) => {
  try {
    res.json(await getNote());
  } catch (error) {
    next(error);
  }
});

app.get("/api/note/events", async (req, res, next) => {
  try {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders?.();

    clients.add(res);
    log("event client connected", { connectedClients: clients.size });
    sendEvent(res, "note", await getNote());

    req.on("close", () => {
      clients.delete(res);
      log("event client disconnected", { connectedClients: clients.size });
      res.end();
    });
  } catch (error) {
    next(error);
  }
});

app.put("/api/note", async (req, res, next) => {
  const { content, baseContent = "", baseRevision } = req.body;

  log("save requested", {
    incomingContentLength: typeof content === "string" ? content.length : null,
    baseContentLength: typeof baseContent === "string" ? baseContent.length : null,
    baseRevision: baseRevision ?? null
  });

  if (typeof content !== "string") {
    log("save rejected", { reason: "content must be a string" });
    res.status(400).json({ error: "content must be a string" });
    return;
  }

  if (baseRevision !== undefined && !Number.isInteger(baseRevision)) {
    log("save rejected", { reason: "baseRevision must be an integer", baseRevision });
    res.status(400).json({ error: "baseRevision must be an integer" });
    return;
  }

  log("acquiring database connection for save");
  const connection = await pool.getConnection();

  try {
    log("save transaction starting");
    await connection.beginTransaction();

    const [rows] = await connection.query(
      "SELECT id, content, revision, updated_at AS updatedAt FROM notes WHERE id = ? FOR UPDATE",
      [NOTE_ID]
    );

    const current = rows[0];
    const shouldMerge = Number.isInteger(baseRevision) && baseRevision < current.revision;
    log("current note loaded for save", {
      currentRevision: current.revision,
      currentContentLength: current.content.length,
      shouldMerge
    });

    const nextContent = shouldMerge
      ? mergeConcurrentText(baseContent, current.content, content)
      : content;
    const nextRevision = current.revision + 1;

    log("saving note update", {
      fromRevision: current.revision,
      toRevision: nextRevision,
      nextContentLength: nextContent.length,
      merged: shouldMerge
    });

    await connection.query(
      "UPDATE notes SET content = ?, revision = ? WHERE id = ?",
      [nextContent, nextRevision, NOTE_ID]
    );

    await connection.commit();
    log("save transaction committed", { revision: nextRevision });

    const note = await getNote();
    broadcastNote(note);

    res.json({
      ...note,
      merged: shouldMerge,
      requestedBaseRevision: baseRevision ?? null
    });
  } catch (error) {
    logError("save failed, rolling back", error);
    await connection.rollback();
    next(error);
  } finally {
    log("database connection released after save");
    connection.release();
  }
});

app.use((error, req, res, next) => {
  logError("request failed", error, {
    method: req.method,
    path: req.originalUrl
  });
  res.status(500).json({ error: "Internal server error" });
});

initializeDatabase()
  .then(() => {
    app.listen(PORT, () => {
      log("Livenote backend listening", { port: PORT });
    });
  })
  .catch((error) => {
    logError("Failed to initialize database", error);
    process.exit(1);
  });
