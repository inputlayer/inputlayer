from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from inputlayer import InputLayer

from config import (
    FRONTEND_ORIGIN,
    INPUTLAYER_PASSWORD,
    INPUTLAYER_URL,
    INPUTLAYER_USER,
    KG_NAME,
)

logger = logging.getLogger("reasoning_notebook")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    il = InputLayer(INPUTLAYER_URL, username=INPUTLAYER_USER, password=INPUTLAYER_PASSWORD)
    await il.connect()
    logger.info("Connected to InputLayer at %s", INPUTLAYER_URL)

    kg = il.knowledge_graph(KG_NAME)
    app.state.il = il
    app.state.kg = kg

    yield

    await il.close()
    logger.info("Disconnected from InputLayer")


app = FastAPI(title="Reasoning Notebook", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_kg(request: Request) -> Any:
    return request.app.state.kg


@app.get("/health")
async def health(request: Request):
    kg = get_kg(request)
    try:
        result = await kg.execute("?__health(1)")
    except Exception:
        result = None
    return {
        "status": "ok",
        "engine": "connected" if result is not None else "error",
        "kg": KG_NAME,
    }
