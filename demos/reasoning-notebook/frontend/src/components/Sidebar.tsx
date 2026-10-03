import type { Note } from "../types";

interface SidebarProps {
  notes: Note[];
  activeId: string | null;
  onSelect: (id: string) => void;
  onCreate: () => void;
  onDelete: (id: string) => void;
}

export function Sidebar({
  notes,
  activeId,
  onSelect,
  onCreate,
  onDelete,
}: SidebarProps) {
  return (
    <aside style={styles.sidebar}>
      <div style={styles.header}>
        <span style={styles.title}>Notes</span>
        <button style={styles.newBtn} onClick={onCreate} title="New note">
          +
        </button>
      </div>
      <div style={styles.list}>
        {notes.length === 0 && (
          <p style={styles.empty}>No notes yet. Create one!</p>
        )}
        {notes.map((note) => (
          <div
            key={note.id}
            style={{
              ...styles.item,
              ...(note.id === activeId ? styles.itemActive : {}),
            }}
            onClick={() => onSelect(note.id)}
          >
            <div style={styles.itemTitle}>{note.title || "Untitled"}</div>
            <div style={styles.itemMeta}>
              {new Date(note.updated_at * 1000).toLocaleDateString()}
            </div>
            <button
              style={styles.deleteBtn}
              onClick={(e) => {
                e.stopPropagation();
                onDelete(note.id);
              }}
              title="Delete note"
            >
              &times;
            </button>
          </div>
        ))}
      </div>
    </aside>
  );
}

const styles: Record<string, React.CSSProperties> = {
  sidebar: {
    width: 260,
    flexShrink: 0,
    borderRight: "1px solid rgba(255,255,255,0.06)",
    background: "#181825",
    display: "flex",
    flexDirection: "column",
    height: "100%",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "14px 16px",
    borderBottom: "1px solid rgba(255,255,255,0.06)",
  },
  title: {
    fontSize: 13,
    fontWeight: 600,
    letterSpacing: 0.5,
    textTransform: "uppercase" as const,
    color: "#a6adc8",
  },
  newBtn: {
    background: "rgba(166,227,161,0.12)",
    color: "#a6e3a1",
    border: "none",
    borderRadius: 6,
    width: 28,
    height: 28,
    fontSize: 18,
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  },
  list: {
    flex: 1,
    overflowY: "auto" as const,
    padding: "8px 0",
  },
  empty: {
    color: "#6c7086",
    fontSize: 12,
    padding: "16px",
    textAlign: "center" as const,
  },
  item: {
    padding: "10px 16px",
    cursor: "pointer",
    position: "relative" as const,
    borderLeftWidth: 3,
    borderLeftStyle: "solid" as const,
    borderLeftColor: "transparent",
    transition: "background 0.1s",
  },
  itemActive: {
    background: "rgba(137,180,250,0.08)",
    borderLeftColor: "#89b4fa",
  },
  itemTitle: {
    fontSize: 13,
    fontWeight: 500,
    color: "#cdd6f4",
    whiteSpace: "nowrap" as const,
    overflow: "hidden" as const,
    textOverflow: "ellipsis" as const,
    paddingRight: 24,
  },
  itemMeta: {
    fontSize: 11,
    color: "#6c7086",
    marginTop: 2,
  },
  deleteBtn: {
    position: "absolute" as const,
    right: 12,
    top: 10,
    background: "none",
    border: "none",
    color: "#6c7086",
    fontSize: 16,
    cursor: "pointer",
    opacity: 0.6,
    padding: "0 4px",
  },
};
