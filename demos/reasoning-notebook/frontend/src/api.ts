import type { Note } from "./types";

const BASE = "/api";

export interface HealthResponse {
  status: string;
  engine: string;
  kg: string;
}

export async function fetchHealth(): Promise<HealthResponse> {
  const res = await fetch(`${BASE}/health`);
  if (!res.ok) throw new Error(`Health check failed: ${res.status}`);
  return res.json();
}

export async function fetchNotes(): Promise<Note[]> {
  const res = await fetch(`${BASE}/notes`);
  if (!res.ok) throw new Error(`Failed to fetch notes: ${res.status}`);
  return res.json();
}

export async function fetchNote(id: string): Promise<Note> {
  const res = await fetch(`${BASE}/notes/${id}`);
  if (!res.ok) throw new Error(`Failed to fetch note: ${res.status}`);
  return res.json();
}

export async function createNote(title: string, content = ""): Promise<Note> {
  const res = await fetch(`${BASE}/notes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title, content }),
  });
  if (!res.ok) throw new Error(`Failed to create note: ${res.status}`);
  return res.json();
}

export async function updateNote(
  id: string,
  fields: { title?: string; content?: string }
): Promise<Note> {
  const res = await fetch(`${BASE}/notes/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(fields),
  });
  if (!res.ok) throw new Error(`Failed to update note: ${res.status}`);
  return res.json();
}

export async function deleteNote(id: string): Promise<void> {
  const res = await fetch(`${BASE}/notes/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`Failed to delete note: ${res.status}`);
}

export interface ExtractionResult {
  entities: number;
  relationships: number;
}

export async function extractNote(id: string): Promise<ExtractionResult> {
  const res = await fetch(`${BASE}/notes/${id}/extract`, { method: "POST" });
  if (!res.ok) throw new Error(`Extraction failed: ${res.status}`);
  return res.json();
}

export interface NoteEntities {
  entities: Array<{
    id: string;
    name: string;
    kind: string;
    description: string;
    source_note_id: string;
  }>;
  relationships: Array<{
    id: string;
    subject: string;
    predicate: string;
    object: string;
    source_note_id: string;
  }>;
}

export async function fetchNoteEntities(id: string): Promise<NoteEntities> {
  const res = await fetch(`${BASE}/notes/${id}/entities`);
  if (!res.ok) throw new Error(`Failed to fetch entities: ${res.status}`);
  return res.json();
}
