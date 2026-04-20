import { useCallback, useEffect, useRef, useState } from "react";
import type { Note } from "../types";

interface EditorProps {
  note: Note;
  onSave: (id: string, fields: { title?: string; content?: string }) => void;
}

export function Editor({ note, onSave }: EditorProps) {
  const [title, setTitle] = useState(note.title);
  const [content, setContent] = useState(note.content);
  const [dirty, setDirty] = useState(false);
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    setTitle(note.title);
    setContent(note.content);
    setDirty(false);
  }, [note.id, note.title, note.content]);

  const scheduleSave = useCallback(
    (newTitle: string, newContent: string) => {
      setDirty(true);
      if (saveTimer.current) clearTimeout(saveTimer.current);
      saveTimer.current = setTimeout(() => {
        onSave(note.id, { title: newTitle, content: newContent });
        setDirty(false);
      }, 800);
    },
    [note.id, onSave]
  );

  const handleTitleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const v = e.target.value;
    setTitle(v);
    scheduleSave(v, content);
  };

  const handleContentChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const v = e.target.value;
    setContent(v);
    scheduleSave(title, v);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "s") {
      e.preventDefault();
      if (saveTimer.current) clearTimeout(saveTimer.current);
      onSave(note.id, { title, content });
      setDirty(false);
    }
  };

  return (
    <div style={styles.editor} onKeyDown={handleKeyDown}>
      <div style={styles.toolbar}>
        <span style={styles.meta}>
          {new Date(note.updated_at * 1000).toLocaleString()}
        </span>
        {dirty && <span style={styles.dirty}>Unsaved</span>}
      </div>
      <input
        style={styles.title}
        value={title}
        onChange={handleTitleChange}
        placeholder="Untitled"
        spellCheck={false}
      />
      <textarea
        style={styles.content}
        value={content}
        onChange={handleContentChange}
        placeholder="Start writing..."
        spellCheck={false}
      />
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  editor: {
    flex: 1,
    display: "flex",
    flexDirection: "column",
    height: "100%",
    overflow: "hidden",
  },
  toolbar: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "8px 24px",
    borderBottom: "1px solid rgba(255,255,255,0.04)",
    minHeight: 36,
  },
  meta: {
    fontSize: 11,
    color: "#6c7086",
  },
  dirty: {
    fontSize: 10,
    fontWeight: 600,
    color: "#fab387",
    background: "rgba(250,179,135,0.1)",
    padding: "2px 8px",
    borderRadius: 4,
  },
  title: {
    background: "none",
    border: "none",
    outline: "none",
    color: "#cdd6f4",
    fontSize: 28,
    fontWeight: 700,
    letterSpacing: -0.5,
    padding: "20px 24px 8px",
    fontFamily: "inherit",
  },
  content: {
    flex: 1,
    background: "none",
    border: "none",
    outline: "none",
    color: "#bac2de",
    fontSize: 15,
    lineHeight: 1.7,
    padding: "8px 24px 24px",
    resize: "none" as const,
    fontFamily: "inherit",
  },
};
