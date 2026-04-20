import { useCallback, useEffect, useState } from "react";
import {
  createNote,
  deleteNote,
  fetchHealth,
  fetchNotes,
  updateNote,
  type HealthResponse,
} from "./api";
import { ChatPanel } from "./components/ChatPanel";
import { Editor } from "./components/Editor";
import { ExtractionPanel } from "./components/ExtractionPanel";
import { GraphView } from "./components/GraphView";
import { Sidebar } from "./components/Sidebar";
import type { Note } from "./types";

type View = "editor" | "graph" | "chat";

export function App() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notes, setNotes] = useState<Note[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [saveCount, setSaveCount] = useState(0);
  const [view, setView] = useState<View>("editor");

  useEffect(() => {
    fetchHealth()
      .then(setHealth)
      .catch((e) => setError(e.message));
    loadNotes();
  }, []);

  const loadNotes = async () => {
    try {
      const list = await fetchNotes();
      list.sort((a, b) => b.updated_at - a.updated_at);
      setNotes(list);
    } catch {
      /* backend not ready yet */
    }
  };

  const handleCreate = async () => {
    const note = await createNote("Untitled");
    setNotes((prev) => [note, ...prev]);
    setActiveId(note.id);
    setView("editor");
  };

  const handleDelete = async (id: string) => {
    await deleteNote(id);
    setNotes((prev) => prev.filter((n) => n.id !== id));
    if (activeId === id) setActiveId(null);
  };

  const handleSave = useCallback(
    async (id: string, fields: { title?: string; content?: string }) => {
      const updated = await updateNote(id, fields);
      setNotes((prev) =>
        prev
          .map((n) => (n.id === id ? updated : n))
          .sort((a, b) => b.updated_at - a.updated_at)
      );
      setSaveCount((c) => c + 1);
    },
    []
  );

  const handleSelectFromGraph = (noteId: string) => {
    setActiveId(noteId);
    setView("editor");
  };

  const activeNote = notes.find((n) => n.id === activeId) ?? null;

  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1 style={styles.title}>Reasoning Notebook</h1>
        <div style={styles.tabs}>
          <button
            style={{
              ...styles.tab,
              ...(view === "editor" ? styles.tabActive : {}),
            }}
            onClick={() => setView("editor")}
          >
            Editor
          </button>
          <button
            style={{
              ...styles.tab,
              ...(view === "graph" ? styles.tabActive : {}),
            }}
            onClick={() => setView("graph")}
          >
            Graph
          </button>
          <button
            style={{
              ...styles.tab,
              ...(view === "chat" ? styles.tabActive : {}),
            }}
            onClick={() => setView("chat")}
          >
            Chat
          </button>
        </div>
        <StatusBadge health={health} error={error} />
      </header>
      <div style={styles.body}>
        {view === "editor" && (
          <>
            <Sidebar
              notes={notes}
              activeId={activeId}
              onSelect={setActiveId}
              onCreate={handleCreate}
              onDelete={handleDelete}
            />
            <main style={styles.main}>
              {activeNote ? (
                <div style={styles.editorContainer}>
                  <Editor note={activeNote} onSave={handleSave} />
                  <ExtractionPanel
                    noteId={activeNote.id}
                    refreshKey={saveCount}
                  />
                </div>
              ) : (
                <p style={styles.placeholder}>
                  {notes.length === 0
                    ? "Create a note to get started"
                    : "Select a note from the sidebar"}
                </p>
              )}
            </main>
          </>
        )}
        {view === "graph" && (
          <GraphView
            refreshKey={saveCount}
            onSelectNote={handleSelectFromGraph}
          />
        )}
        {view === "chat" && <ChatPanel />}
      </div>
    </div>
  );
}

function StatusBadge({
  health,
  error,
}: {
  health: HealthResponse | null;
  error: string | null;
}) {
  if (error) {
    return (
      <span style={{ ...styles.badge, background: "#f38ba8" }}>
        Disconnected
      </span>
    );
  }
  if (!health) {
    return (
      <span style={{ ...styles.badge, background: "#6c7086" }}>
        Connecting...
      </span>
    );
  }
  const ok = health.engine === "connected";
  return (
    <span
      style={{
        ...styles.badge,
        background: ok ? "#a6e3a1" : "#f38ba8",
        color: "#1e1e2e",
      }}
    >
      {ok ? "Connected" : "Engine error"}
    </span>
  );
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    minHeight: "100vh",
    display: "flex",
    flexDirection: "column",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "10px 24px",
    borderBottom: "1px solid rgba(255,255,255,0.06)",
    background: "#11111b",
    flexShrink: 0,
    gap: 16,
  },
  title: {
    fontSize: 16,
    fontWeight: 600,
    letterSpacing: -0.3,
  },
  tabs: {
    display: "flex",
    gap: 2,
    background: "rgba(255,255,255,0.04)",
    borderRadius: 8,
    padding: 2,
  },
  tab: {
    background: "none",
    border: "none",
    color: "#6c7086",
    fontSize: 12,
    fontWeight: 500,
    padding: "6px 16px",
    borderRadius: 6,
    cursor: "pointer",
    transition: "all 0.15s",
  },
  tabActive: {
    background: "rgba(137,180,250,0.12)",
    color: "#89b4fa",
  },
  badge: {
    fontSize: 11,
    fontWeight: 600,
    padding: "4px 12px",
    borderRadius: 6,
    letterSpacing: 0.3,
    flexShrink: 0,
  },
  body: {
    flex: 1,
    display: "flex",
    overflow: "hidden",
    height: "calc(100vh - 49px)",
  },
  main: {
    flex: 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    background: "#1e1e2e",
  },
  editorContainer: {
    display: "flex",
    flexDirection: "column" as const,
    flex: 1,
    height: "100%",
  },
  placeholder: {
    color: "#6c7086",
    fontSize: 14,
  },
};
