import { useCallback, useEffect, useRef, useState } from "react";
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
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    setTitle(note.title);
    setContent(note.content);
    setDirty(false);
    setImages([]);
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

  const handleImageFile = async (file: File) => {
    if (!file.type.startsWith("image/")) return;

    const previewUrl = URL.createObjectURL(file);
    const placeholder: ImageAttachment = {
      url: previewUrl,
      description: "",
      uploading: true,
    };
    setImages((prev) => [...prev, placeholder]);

    try {
      const result: ImageUploadResult = await uploadImage(note.id, file);

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

      // Append image description to note content
      if (result.description) {
        const imageText = `\n\n[Image: ${file.name}]\n${result.description}`;
        const newContent = content + imageText;
        setContent(newContent);
        onSave(note.id, { title, content: newContent });
      }
      onImageUploaded?.();
    } catch {
      setImages((prev) => prev.filter((img) => img.url !== previewUrl));
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const files = Array.from(e.dataTransfer.files);
    files.forEach(handleImageFile);
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
        <span style={styles.dropHint}>Drop images here</span>
      </div>
      <input
        style={styles.title}
        value={title}
        onChange={handleTitleChange}
        placeholder="Untitled"
        spellCheck={false}
      />
      <textarea
        ref={textareaRef}
        style={styles.content}
        value={content}
        onChange={handleContentChange}
        onPaste={handlePaste}
        placeholder="Start writing... (drag or paste images)"
        spellCheck={false}
      />

      {images.length > 0 && (
        <div style={styles.imageStrip}>
          {images.map((img, i) => (
            <div key={i} style={styles.imageCard}>
              <img
                src={img.url}
                alt=""
                style={styles.imageThumb}
              />
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
                        {img.entities} entities, {img.relationships} relationships
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
    overflow: "hidden" as const,
    textOverflow: "ellipsis" as const,
    display: "-webkit-box",
    WebkitLineClamp: 2,
    WebkitBoxOrient: "vertical" as const,
  },
  imageBadge: {
    fontSize: 10,
    color: "#a6e3a1",
    fontWeight: 500,
  },
};
