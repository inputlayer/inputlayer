import { useEffect, useState } from "react";
import {
  extractNote,
  fetchNoteEntities,
  type NoteEntities,
} from "../api";

interface ExtractionPanelProps {
  noteId: string;
  refreshKey: number;
}

export function ExtractionPanel({ noteId, refreshKey }: ExtractionPanelProps) {
  const [data, setData] = useState<NoteEntities | null>(null);
  const [extracting, setExtracting] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    fetchNoteEntities(noteId).then(setData).catch(() => setData(null));
  }, [noteId, refreshKey]);

  const handleExtract = async () => {
    setExtracting(true);
    try {
      const result = await extractNote(noteId);
      if (result.error) {
        setToast(`Extraction error: ${result.error}`);
      } else {
        setToast(`Extracted ${result.entities} entities, ${result.relationships} relationships`);
      }
      const updated = await fetchNoteEntities(noteId);
      setData(updated);
      setTimeout(() => setToast(null), 6000);
    } catch (e) {
      setToast(`Extraction failed: ${e instanceof Error ? e.message : "unknown"}`);
      setTimeout(() => setToast(null), 6000);
    } finally {
      setExtracting(false);
    }
  };

  const entityCount = data?.entities.length ?? 0;
  const relCount = data?.relationships.length ?? 0;

  return (
    <div style={styles.panel}>
      <div style={styles.header} onClick={() => setExpanded(!expanded)}>
        <button style={styles.toggle}>{expanded ? "▾" : "▸"}</button>
        <span style={styles.title}>Knowledge Graph</span>
        <div style={styles.stats}>
          <span style={styles.stat}>
            <span style={{ ...styles.dot, background: "#89b4fa" }} />
            {entityCount} entities
          </span>
          <span style={styles.stat}>
            <span style={{ ...styles.dot, background: "#a6e3a1" }} />
            {relCount} relationships
          </span>
        </div>
        <button
          style={{
            ...styles.extractBtn,
            opacity: extracting ? 0.5 : 1,
          }}
          onClick={(e) => {
            e.stopPropagation();
            handleExtract();
          }}
          disabled={extracting}
        >
          {extracting ? "Extracting..." : "Extract"}
        </button>
      </div>
      {toast && <div style={styles.toast}>{toast}</div>}
      {expanded && data && entityCount > 0 && (
        <div style={styles.body}>
          <div style={styles.section}>
            {data.entities.map((e) => (
              <span key={e.id} style={styles.tag}>
                <span style={styles.kindBadge}>{e.kind}</span>
                {e.name}
              </span>
            ))}
          </div>
          {relCount > 0 && (
            <div style={styles.rels}>
              {data.relationships.map((r) => (
                <div key={r.id} style={styles.rel}>
                  <span style={styles.relEntity}>{r.subject}</span>
                  <span style={styles.relPred}>{r.predicate}</span>
                  <span style={styles.relEntity}>{r.object}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  panel: {
    borderTop: "1px solid rgba(255,255,255,0.06)",
    background: "#181825",
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 10,
    padding: "10px 24px",
    cursor: "pointer",
    userSelect: "none" as const,
  },
  toggle: {
    background: "none",
    border: "none",
    color: "#6c7086",
    fontSize: 11,
    padding: 0,
    cursor: "pointer",
    width: 14,
    flexShrink: 0,
  },
  title: {
    fontSize: 11,
    fontWeight: 600,
    textTransform: "uppercase" as const,
    letterSpacing: 1,
    color: "#a6adc8",
  },
  stats: {
    display: "flex",
    gap: 12,
    flex: 1,
  },
  stat: {
    fontSize: 11,
    color: "#6c7086",
    display: "flex",
    alignItems: "center",
    gap: 5,
  },
  dot: {
    width: 6,
    height: 6,
    borderRadius: "50%",
    display: "inline-block",
  },
  extractBtn: {
    background: "rgba(137,180,250,0.12)",
    color: "#89b4fa",
    border: "none",
    borderRadius: 6,
    padding: "5px 14px",
    fontSize: 11,
    fontWeight: 600,
    cursor: "pointer",
  },
  toast: {
    padding: "6px 24px",
    fontSize: 12,
    color: "#a6e3a1",
    background: "rgba(166,227,161,0.06)",
    borderTop: "1px solid rgba(166,227,161,0.1)",
  },
  body: {
    padding: "8px 24px 14px",
  },
  section: {
    display: "flex",
    flexWrap: "wrap" as const,
    gap: 6,
  },
  tag: {
    display: "inline-flex",
    alignItems: "center",
    gap: 5,
    fontSize: 11,
    color: "#cdd6f4",
    background: "rgba(255,255,255,0.04)",
    borderRadius: 5,
    padding: "3px 8px",
  },
  kindBadge: {
    fontSize: 9,
    fontWeight: 600,
    textTransform: "uppercase" as const,
    color: "#89b4fa",
    background: "rgba(137,180,250,0.12)",
    padding: "1px 5px",
    borderRadius: 3,
  },
  rels: {
    marginTop: 8,
    display: "flex",
    flexDirection: "column" as const,
    gap: 3,
  },
  rel: {
    display: "flex",
    alignItems: "center",
    gap: 6,
    fontSize: 11,
    color: "#6c7086",
  },
  relEntity: {
    color: "#cdd6f4",
  },
  relPred: {
    color: "#a6e3a1",
    fontStyle: "italic" as const,
  },
};
