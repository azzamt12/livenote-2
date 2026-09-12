const API_BASE_URL =
  window.LIVENOTE_API_BASE_URL ||
  `${window.location.protocol === "file:" ? "http:" : window.location.protocol}//${
    window.location.hostname || "localhost"
  }:3000`;

const editor = document.getElementById("editor");
const saveButton = document.getElementById("saveButton");
const refreshButton = document.getElementById("refreshButton");
const statusLabel = document.getElementById("status");
const revisionLabel = document.getElementById("revisionLabel");

let baseContent = "";
let revision = null;
let isSaving = false;
let hasLocalChanges = false;
let lastRemoteContent = "";

function setStatus(message) {
  statusLabel.textContent = message;
}

function setNote(note, options = {}) {
  revision = note.revision;
  baseContent = note.content;
  lastRemoteContent = note.content;
  revisionLabel.textContent = `Revision ${note.revision}`;

  if (options.force || !hasLocalChanges) {
    editor.value = note.content;
    hasLocalChanges = false;
  }
}

async function fetchNote(options = {}) {
  setStatus("Loading");

  const response = await fetch(`${API_BASE_URL}/api/note`);

  if (!response.ok) {
    throw new Error("Failed to load note");
  }

  setNote(await response.json(), options);
  setStatus("Synced");
}

async function saveNote() {
  isSaving = true;
  saveButton.disabled = true;
  setStatus("Saving");

  try {
    const response = await fetch(`${API_BASE_URL}/api/note`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        content: editor.value,
        baseContent,
        baseRevision: revision
      })
    });

    if (!response.ok) {
      throw new Error("Failed to save note");
    }

    const note = await response.json();
    setNote(note, { force: true });
    setStatus(note.merged ? "Merged" : "Saved");
  } catch (error) {
    console.error(error);
    setStatus("Save failed");
  } finally {
    isSaving = false;
    saveButton.disabled = false;
  }
}

function connectEvents() {
  const events = new EventSource(`${API_BASE_URL}/api/note/events`);

  events.addEventListener("open", () => {
    setStatus(hasLocalChanges ? "Editing" : "Connected");
  });

  events.addEventListener("note", (event) => {
    const note = JSON.parse(event.data);

    if (note.revision === revision || note.content === lastRemoteContent) {
      return;
    }

    setNote(note);
    setStatus(hasLocalChanges ? "Remote update" : "Synced");
  });

  events.addEventListener("error", () => {
    setStatus("Reconnecting");
  });
}

editor.addEventListener("input", () => {
  hasLocalChanges = editor.value !== baseContent;
  setStatus(hasLocalChanges ? "Unsaved" : "Synced");
});

saveButton.addEventListener("click", () => {
  if (!isSaving) {
    saveNote();
  }
});

refreshButton.addEventListener("click", () => {
  fetchNote({ force: true }).catch((error) => {
    console.error(error);
    setStatus("Load failed");
  });
});

fetchNote({ force: true })
  .then(connectEvents)
  .catch((error) => {
    console.error(error);
    setStatus("Offline");
    revisionLabel.textContent = "Backend unavailable";
  });
