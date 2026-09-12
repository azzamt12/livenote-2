require("dotenv").config();

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");

const app = express();

const PORT = Number(process.env.PORT || 3000);
const NOTE_ID = 1;

app.use(cors());
app.use(express.json({ limit: "2mb" }));

const pool = mysql.createPool({
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "livenote",
  password: process.env.DB_PASSWORD || "livenote",
  database: process.env.DB_NAME || "livenote",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

const clients = new Set();

function sendEvent(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function broadcastNote(note) {
  for (const client of clients) {
    sendEvent(client, "note", note);
  }
}

function uniqueLines(lines) {
  return lines.filter((line, index) => lines.indexOf(line) === index);
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

async function initializeDatabase() {
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

async function getNote(connection = pool) {
  const [rows] = await connection.query(
    "SELECT id, content, revision, updated_at AS updatedAt FROM notes WHERE id = ?",
    [NOTE_ID]
  );

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
    sendEvent(res, "note", await getNote());

    req.on("close", () => {
      clients.delete(res);
      res.end();
    });
  } catch (error) {
    next(error);
  }
});

app.put("/api/note", async (req, res, next) => {
  const { content, baseContent = "", baseRevision } = req.body;

  if (typeof content !== "string") {
    res.status(400).json({ error: "content must be a string" });
    return;
  }

  if (baseRevision !== undefined && !Number.isInteger(baseRevision)) {
    res.status(400).json({ error: "baseRevision must be an integer" });
    return;
  }

  const connection = await pool.getConnection();

  try {
    await connection.beginTransaction();

    const [rows] = await connection.query(
      "SELECT id, content, revision, updated_at AS updatedAt FROM notes WHERE id = ? FOR UPDATE",
      [NOTE_ID]
    );

    const current = rows[0];
    const shouldMerge = Number.isInteger(baseRevision) && baseRevision < current.revision;
    const nextContent = shouldMerge
      ? mergeConcurrentText(baseContent, current.content, content)
      : content;
    const nextRevision = current.revision + 1;

    await connection.query(
      "UPDATE notes SET content = ?, revision = ? WHERE id = ?",
      [nextContent, nextRevision, NOTE_ID]
    );

    await connection.commit();

    const note = await getNote();
    broadcastNote(note);

    res.json({
      ...note,
      merged: shouldMerge,
      requestedBaseRevision: baseRevision ?? null
    });
  } catch (error) {
    await connection.rollback();
    next(error);
  } finally {
    connection.release();
  }
});

app.use((error, req, res, next) => {
  console.error(error);
  res.status(500).json({ error: "Internal server error" });
});

initializeDatabase()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Livenote backend listening on port ${PORT}`);
    });
  })
  .catch((error) => {
    console.error("Failed to initialize database", error);
    process.exit(1);
  });
