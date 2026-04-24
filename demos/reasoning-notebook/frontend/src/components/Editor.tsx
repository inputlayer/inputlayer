import { useCallback, useEffect, useRef, useState } from "react";
import Markdown from "react-markdown";
import { uploadImage, type ImageUploadResult } from "../api";
import type { Note } from "../types";

interface EditorProps {
  note: Note;
  onSave: (id: string, fields: { title?: string; content?: string }) => void;
  onImageUploaded?: () => void;
}

interface ImageAttachment {
  url: string;
  description: string;
  uploading?: boolean;
  entities?: number;
  relationships?: number;
}

export function Editor({ note, onSave, onImageUploaded }: EditorProps) {
  const [title, setTitle] = useState(note.title);
  const [content, setContent] = useState(note.content);
  const [dirty, setDirty] = useState(false);
  const [images, setImages] = useState<ImageAttachment[]>([]);
  const [dragOver, setDragOver] = useState(false);
  const [showPreview, setShowPreview] = useState(false);
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const noteIdRef = useRef(note.id);
  const onSaveRef = useRef(onSave);
  const onImageUploadedRef = useRef(onImageUploaded);

  onSaveRef.current = onSave;
  onImageUploadedRef.current = onImageUploaded;

  useEffect(() => {
    setTitle(note.title);
    setContent(note.content);
    noteIdRef.current = note.id;
    setDirty(false);
    setImages([]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [note.id]);

  const scheduleSave = useCallback((newTitle: string, newContent: string) => {
    setDirty(true);
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      onSaveRef.current(noteIdRef.current, {
        title: newTitle,
        content: newContent,
      });
      setDirty(false);
    }, 1000);
  }, []);

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
      onSaveRef.current(noteIdRef.current, { title, content });
      setDirty(false);
    }
  };

  const handleImageFile = async (file: File) => {
    if (!file.type.startsWith("image/")) return;

    const previewUrl = URL.createObjectURL(file);
    setImages((prev) => [
      ...prev,
      { url: previewUrl, description: "", uploading: true },
    ]);

    try {
      const result: ImageUploadResult = await uploadImage(noteIdRef.current, file);

      setImages((prev) =>
        prev.map((img) =>
          img.url === previewUrl
            ? {
                url: `/api${result.url}`,
                description: result.description,
                uploading: false,
                entities: result.entities,
                relationships: result.relationships,
              }
            : img
        )
      );

      if (result.description) {
        const imageText = `\n\n![${file.name}](/api${result.url})\n\n*${result.description}*`;
        setContent((prev) => {
          const updated = prev + imageText;
          onSaveRef.current(noteIdRef.current, { title, content: updated });
          return updated;
        });
      }
      onImageUploadedRef.current?.();
    } catch {
      setImages((prev) => prev.filter((img) => img.url !== previewUrl));
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    Array.from(e.dataTransfer.files).forEach(handleImageFile);
  };

  const handlePaste = (e: React.ClipboardEvent) => {
    const items = Array.from(e.clipboardData.items);
    for (const item of items) {
      if (item.type.startsWith("image/")) {
        e.preventDefault();
        const file = item.getAsFile();
        if (file) handleImageFile(file);
        return;
      }
    }
  };

  return (
    <div
      style={{ ...styles.editor, ...(dragOver ? styles.dragOver : {}) }}
      onKeyDown={handleKeyDown}
      onDragOver={(e) => {
        e.preventDefault();
        setDragOver(true);
      }}
      onDragLeave={() => setDragOver(false)}
      onDrop={handleDrop}
    >
      <div style={styles.toolbar}>
        <span style={styles.meta}>
          {new Date(note.updated_at * 1000).toLocaleString()}
        </span>
        {dirty && <span style={styles.dirty}>Unsaved</span>}
        <button
          style={{
            ...styles.previewToggle,
            ...(showPreview ? styles.previewToggleActive : {}),
          }}
          onClick={() => setShowPreview(!showPreview)}
        >
          {showPreview ? "Edit" : "Preview"}
        </button>
        <span style={styles.dropHint}>Drop images here</span>
      </div>
      <input
        style={styles.title}
        value={title}
        onChange={handleTitleChange}
        placeholder="Untitled"
        spellCheck={false}
      />
      <div style={styles.contentArea}>
        {showPreview ? (
          <div style={styles.preview}>
            <Markdown
              components={{
                img: ({ src, alt }) => (
                  <img src={src} alt={alt || ""} style={styles.previewImg} />
                ),
              }}
            >
              {content}
            </Markdown>
          </div>
        ) : (
          <textarea
            style={styles.textarea}
            value={content}
            onChange={handleContentChange}
            onPaste={handlePaste}
            placeholder="Start writing markdown... (drag or paste images)"
            spellCheck={false}
          />
        )}
      </div>

      {images.length > 0 && (
        <div style={styles.imageStrip}>
          {images.map((img, i) => (
            <div key={i} style={styles.imageCard}>
              <img src={img.url} alt="" style={styles.imageThumb} />
              <div style={styles.imageInfo}>
                {img.uploading ? (
                  <span style={styles.imageUploading}>Analyzing...</span>
                ) : (
                  <>
                    <span style={styles.imageDesc}>
                      {img.description?.slice(0, 80) || "No description"}
                    </span>
                    {(img.entities ?? 0) > 0 && (
                      <span style={styles.imageBadge}>
                        {img.entities} entities, {img.relationships} rels
                      </span>
                    )}
                  </>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
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
    transition: "box-shadow 0.15s",
  },
  dragOver: {
    boxShadow: "inset 0 0 0 3px rgba(137,180,250,0.4)",
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
  previewToggle: {
    background: "rgba(255,255,255,0.04)",
    borderWidth: 1,
    borderStyle: "solid" as const,
    borderColor: "rgba(255,255,255,0.08)",
    borderRadius: 6,
    padding: "3px 12px",
    fontSize: 11,
    color: "#6c7086",
    cursor: "pointer",
  },
  previewToggleActive: {
    background: "rgba(137,180,250,0.12)",
    borderColor: "rgba(137,180,250,0.2)",
    color: "#89b4fa",
  },
  dropHint: {
    fontSize: 10,
    color: "#45475a",
    marginLeft: "auto",
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
  contentArea: {
    flex: 1,
    overflow: "auto",
  },
  textarea: {
    width: "100%",
    height: "100%",
    background: "none",
    border: "none",
    outline: "none",
    color: "#bac2de",
    fontSize: 15,
    lineHeight: 1.7,
    padding: "8px 24px 24px",
    resize: "none" as const,
    fontFamily: "'JetBrains Mono', 'SF Mono', Consolas, monospace",
    tabSize: 2,
  },
  preview: {
    padding: "8px 24px 24px",
    color: "#cdd6f4",
    fontSize: 15,
    lineHeight: 1.7,
  },
  previewImg: {
    maxWidth: "100%",
    borderRadius: 8,
    margin: "8px 0",
  },
  imageStrip: {
    borderTop: "1px solid rgba(255,255,255,0.06)",
    padding: "12px 24px",
    display: "flex",
    gap: 12,
    overflowX: "auto" as const,
    background: "#181825",
  },
  imageCard: {
    flexShrink: 0,
    width: 220,
    borderRadius: 8,
    overflow: "hidden",
    border: "1px solid rgba(255,255,255,0.06)",
    background: "rgba(255,255,255,0.02)",
  },
  imageThumb: {
    width: "100%",
    height: 120,
    objectFit: "cover" as const,
    display: "block",
  },
  imageInfo: {
    padding: "8px 10px",
    display: "flex",
    flexDirection: "column" as const,
    gap: 4,
  },
  imageUploading: {
    fontSize: 11,
    color: "#89b4fa",
    fontWeight: 500,
  },
  imageDesc: {
    fontSize: 11,
    color: "#a6adc8",
    lineHeight: 1.4,
  },
  imageBadge: {
    fontSize: 10,
    color: "#a6e3a1",
    fontWeight: 500,
  },
};
