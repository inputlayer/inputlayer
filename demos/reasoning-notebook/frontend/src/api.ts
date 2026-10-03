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
  error?: string;
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

export interface GraphData {
  nodes: Array<{
    id: string;
    name: string;
    kind: string;
    description: string;
    source_note_id: string;
  }>;
  edges: Array<{
    id: string;
    subject: string;
    predicate: string;
    object: string;
    source_note_id: string;
    derived?: boolean;
  }>;
}

export interface ImageSceneData {
  image_id: string;
  note_id: string;
  scene: string;
  objects: string;
  people: string;
  emotion: string;
  event_type: string;
  aesthetic: string;
  caption_seed: string;
  cultural_context: string;
  visible_text: string;
}

export async function fetchImageScenes(noteId: string): Promise<ImageSceneData[]> {
  const res = await fetch(`${BASE}/notes/${noteId}/scenes`);
  if (!res.ok) return [];
  return res.json();
}

export async function fetchGraph(): Promise<GraphData> {
  const res = await fetch(`${BASE}/graph`);
  if (!res.ok) throw new Error(`Failed to fetch graph: ${res.status}`);
  return res.json();
}

export interface ConsolidationResult {
  status: string;
  predicate_merges?: Array<{ variants: string[]; canonical: string }>;
  entity_merges?: Array<{ variants: string[]; canonical: string }>;
  predicates_renamed?: number;
  entities_renamed?: number;
}

export interface ResolutionResult {
  status: string;
  merges?: Array<{ canonical: string; variant: string; similarity: number }>;
  entities_renamed?: number;
}

export async function resolveEntities(): Promise<ResolutionResult> {
  const res = await fetch(`${BASE}/ontology/resolve`, { method: "POST" });
  if (!res.ok) throw new Error(`Resolution failed: ${res.status}`);
  return res.json();
}

export async function consolidateOntology(): Promise<ConsolidationResult> {
  const res = await fetch(`${BASE}/ontology/consolidate`, { method: "POST" });
  if (!res.ok) throw new Error(`Consolidation failed: ${res.status}`);
  return res.json();
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

// ── Images ──

export interface ImageUploadResult {
  image_id: string;
  filename: string;
  url: string;
  description: string;
  entities: number;
  relationships: number;
}

export async function uploadImage(
  noteId: string,
  file: File
): Promise<ImageUploadResult> {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(`${BASE}/notes/${noteId}/images`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) throw new Error(`Image upload failed: ${res.status}`);
  return res.json();
}

// ── Provenance ──

export interface ProofNode {
  kind: string;
  conclusion: { pred: string; args: string[] };
  children: string[];
  source: string | null;
  rule_id: string | null;
  bindings: Record<string, string> | null;
}

export interface ProofTreeData {
  roots: string[];
  nodes: Record<string, ProofNode>;
  query: string | null;
}

export interface WhyResponse {
  columns: string[];
  rows: string[][];
  proof_trees: ProofTreeData[];
}

export async function fetchWhy(query: string): Promise<WhyResponse> {
  const res = await fetch(`${BASE}/why`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`Why query failed: ${res.status}`);
  return res.json();
}

// ── Chat ──

export async function sendChat(
  message: string,
  history: ChatMessage[]
): Promise<string> {
  const res = await fetch(`${BASE}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message, history }),
  });
  if (!res.ok) throw new Error(`Chat failed: ${res.status}`);
  const data = await res.json();
  return data.reply;
}
