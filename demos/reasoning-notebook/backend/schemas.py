from __future__ import annotations

from pydantic import BaseModel


class NoteCreate(BaseModel):
    title: str
    content: str = ""


class NoteUpdate(BaseModel):
    title: str | None = None
    content: str | None = None


class NoteResponse(BaseModel):
    id: str
    title: str
    content: str
    created_at: int
    updated_at: int


class ChatRequest(BaseModel):
    message: str
    history: list[dict[str, str]] = []


class ChatResponse(BaseModel):
    reply: str
