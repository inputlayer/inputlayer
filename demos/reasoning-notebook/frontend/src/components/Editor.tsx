import { useCallback, useEffect, useRef, useState } from "react";
import { Editor as MilkdownEditor } from "@milkdown/core";
import { commonmark } from "@milkdown/preset-commonmark";
import { gfm } from "@milkdown/preset-gfm";
import { nord } from "@milkdown/theme-nord";
import { listener, listenerCtx } from "@milkdown/plugin-listener";
import { Milkdown, MilkdownProvider, useEditor } from "@milkdown/react";
import { replaceAll } from "@milkdown/utils";
import { uploadImage, type ImageUploadResult } from "../api";
import type { Note } from "../types";

import "@milkdown/theme-nord/style.css";

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

function MilkdownEditorInner({
  note,
  onSave,
  onImageUploaded,
}: EditorProps) {
  const [title, setTitle] = useState(note.title);
  const [dirty, setDirty] = useState(false);
  const [images, setImages] = useState<ImageAttachment[]>([]);
  const [dragOver, setDragOver] = useState(false);
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const contentRef = useRef(note.content);
  const titleRef = useRef(note.title);
  const noteIdRef = useRef(note.id);
  const suppressSaveRef = useRef(false);

  // Only reset editor state when switching to a different note
  useEffect(() => {
    setTitle(note.title);
    titleRef.current = note.title;
    contentRef.current = note.content;
    noteIdRef.current = note.id;
    suppressSaveRef.current = true;
    setDirty(false);
    setImages([]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [note.id]);

  const scheduleSave = useCallback(
    (newContent: string) => {
      if (suppressSaveRef.current) return;
      setDirty(true);
      if (saveTimer.current) clearTimeout(saveTimer.current);
      saveTimer.current = setTimeout(() => {
        onSave(noteIdRef.current, {
          title: titleRef.current,
          content: newContent,
        });
        setDirty(false);
      }, 800);
    },
    [onSave]
  );

  const handleTitleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const v = e.target.value;
    setTitle(v);
    titleRef.current = v;
    setDirty(true);
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      onSave(noteIdRef.current, {
        title: titleRef.current,
        content: contentRef.current,
      });
      setDirty(false);
    }, 800);
  };

  const { get } = useEditor(
    (root) =>
      MilkdownEditor.make()
        .config(nord)
        .config((ctx) => {
          ctx.set(rootCtx, root);
          ctx
            .get(listenerCtx)
            .markdownUpdated((_ctx, markdown, _prev) => {
              contentRef.current = markdown;
              scheduleSave(markdown);
            });
        })
        .use(commonmark)
        .use(gfm)
        .use(listener),
    [note.id]
  );

  // Sync editor content only when switching notes
  useEffect(() => {
    const editor = get();
    if (editor) {
      suppressSaveRef.current = true;
      editor.action(replaceAll(note.content));
      requestAnimationFrame(() => {
        suppressSaveRef.current = false;
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [note.id, get]);

  const handleImageFile = async (file: File) => {
    if (!file.type.startsWith("image/")) return;

    const previewUrl = URL.createObjectURL(file);
    setImages((prev) => [
      ...prev,
      { url: previewUrl, description: "", uploading: true },
    ]);

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

      // Insert image markdown into editor
      if (result.description) {
        const editor = get();
        const imageMarkdown = `\n\n![${file.name}](/api${result.url})\n\n*${result.description}*\n`;
        const newContent = contentRef.current + imageMarkdown;
        contentRef.current = newContent;
        if (editor) {
          suppressSaveRef.current = true;
          editor.action(replaceAll(newContent));
          requestAnimationFrame(() => {
            suppressSaveRef.current = false;
          });
        }
        onSave(noteIdRef.current, {
          title: titleRef.current,
          content: newContent,
        });
      }
      onImageUploaded?.();
    } catch {
      setImages((prev) => prev.filter((img) => img.url !== previewUrl));
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    Array.from(e.dataTransfer.files).forEach(handleImageFile);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "s") {
      e.preventDefault();
      if (saveTimer.current) clearTimeout(saveTimer.current);
      onSave(noteIdRef.current, {
        title: titleRef.current,
        content: contentRef.current,
      });
      setDirty(false);
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
      <div style={styles.milkdownContainer}>
        <Milkdown />
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

// Need to import rootCtx for the config
import { rootCtx } from "@milkdown/core";

export function Editor(props: EditorProps) {
  return (
    <MilkdownProvider>
      <MilkdownEditorInner {...props} />
    </MilkdownProvider>
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
  milkdownContainer: {
    flex: 1,
    overflow: "auto",
    padding: "0 24px 24px",
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
