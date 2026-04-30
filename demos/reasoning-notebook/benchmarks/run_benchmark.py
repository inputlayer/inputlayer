#!/usr/bin/env python3
"""Run extraction benchmarks across multiple LLMs.

Usage:
    uv run python run_benchmark.py                    # all enabled models, all inputs
    uv run python run_benchmark.py --models local     # only local models
    uv run python run_benchmark.py --models cloud     # only cloud models
    uv run python run_benchmark.py --input text_corporate.txt  # single input
    uv run python run_benchmark.py --image-only       # only image inputs
    uv run python run_benchmark.py --text-only        # only text inputs
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from config import get_enabled_models, MODELS


# ── Extraction schemas (same as the app) ───────────────────────────


class ExtractedEntity(BaseModel):
    name: str = Field(description="Entity name, lowercase")
    kind: str = Field(description="Type: person, organization, technology, concept, place, event, object, building, artwork, animal, role")
    description: str = Field(description="One-sentence description")


class ExtractedRelationship(BaseModel):
    subject: str = Field(description="Source entity name")
    predicate: str = Field(description="Relationship type")
    object: str = Field(description="Target entity name")


class TextExtraction(BaseModel):
    entities: list[ExtractedEntity] = Field(default_factory=list)
    relationships: list[ExtractedRelationship] = Field(default_factory=list)


class ImageAnalysis(BaseModel):
    scene: str = Field(default="", description="Brief scene description")
    objects: list[str] = Field(default_factory=list, description="Visible objects")
    people: str = Field(default="none", description="People count and description")
    emotion: str = Field(default="neutral", description="Emotional quality")
    event_type: str = Field(default="", description="Type of event")
    aesthetic: str = Field(default="", description="Visual style")
    caption_seed: str = Field(default="", description="Short caption phrase")
    cultural_context: str = Field(default="", description="Cultural context")
    visible_text: str = Field(default="", description="Visible text in image")
    entities: list[ExtractedEntity] = Field(default_factory=list)
    relationships: list[ExtractedRelationship] = Field(default_factory=list)


# ── Prompts ────────────────────────────────────────────────────────


TEXT_PROMPT = (
    "Extract all notable entities and their relationships from the following text.\n\n"
    "Entity types: people (named or described), places, objects, organizations, "
    "technologies, concepts, events, roles.\n\n"
    "Rules:\n"
    "- Normalize all entity names to lowercase\n"
    "- Use short predicate names (works_at, manages, uses, located_at, part_of, reports_to)\n"
    "- Every relationship's subject and object must match an entity name\n\n"
    "Text:\n{content}"
)

IMAGE_PROMPT = (
    "Analyze this image and extract structured information.\n\n"
    "Provide: scene, objects (list), people (count/description or 'none'), "
    "emotion, event_type, aesthetic, caption_seed, cultural_context, visible_text, "
    "entities (name, kind, description), relationships (subject, predicate, object)."
)


# ── Benchmark runner ───────────────────────────────────────────────


async def benchmark_text(model_cfg: dict, text: str, input_name: str) -> dict[str, Any]:
    """Run text extraction benchmark for one model."""
    result: dict[str, Any] = {
        "model": model_cfg["name"],
        "input": input_name,
        "type": "text",
        "input_chars": len(text),
    }

    try:
        if model_cfg["provider"] == "anthropic":
            from langchain_anthropic import ChatAnthropic
            llm = ChatAnthropic(
                api_key=model_cfg["api_key"],
                model=model_cfg["model"],
                temperature=0,
                max_tokens=1024,
            )
        else:
            llm = ChatOpenAI(
                base_url=model_cfg.get("base_url"),
                api_key=model_cfg.get("api_key", ""),
                model=model_cfg["model"],
                temperature=0,
                max_tokens=1024,
            )

        extractor = llm.with_structured_output(TextExtraction)
        prompt = TEXT_PROMPT.format(content=text)

        start = time.time()
        extraction = await extractor.ainvoke(prompt)
        elapsed = time.time() - start

        result["success"] = True
        result["time_seconds"] = round(elapsed, 2)
        result["entities_count"] = len(extraction.entities)
        result["relationships_count"] = len(extraction.relationships)
        result["entities"] = [
            {"name": e.name, "kind": e.kind, "description": e.description}
            for e in extraction.entities
        ]
        result["relationships"] = [
            {"subject": r.subject, "predicate": r.predicate, "object": r.object}
            for r in extraction.relationships
        ]

    except Exception as e:
        result["success"] = False
        result["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        result["time_seconds"] = 0

    return result


async def benchmark_image(model_cfg: dict, image_path: Path, input_name: str) -> dict[str, Any]:
    """Run image extraction benchmark for one model."""
    result: dict[str, Any] = {
        "model": model_cfg["name"],
        "input": input_name,
        "type": "image",
        "input_bytes": image_path.stat().st_size,
    }

    if not model_cfg.get("multimodal"):
        result["success"] = False
        result["error"] = "Model does not support multimodal input"
        result["time_seconds"] = 0
        return result

    img_b64 = base64.b64encode(image_path.read_bytes()).decode()
    ext = image_path.suffix.lower()
    mime = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png"}.get(ext, "image/jpeg")

    try:
        if model_cfg["provider"] == "anthropic":
            from langchain_anthropic import ChatAnthropic
            llm = ChatAnthropic(
                api_key=model_cfg["api_key"],
                model=model_cfg["model"],
                temperature=0,
                max_tokens=1024,
            )
        else:
            llm = ChatOpenAI(
                base_url=model_cfg.get("base_url"),
                api_key=model_cfg.get("api_key", ""),
                model=model_cfg["model"],
                temperature=0,
                max_tokens=1024,
            )

        from langchain_core.messages import HumanMessage
        extractor = llm.with_structured_output(ImageAnalysis)

        start = time.time()
        analysis = await extractor.ainvoke([
            HumanMessage(content=[
                {"type": "text", "text": IMAGE_PROMPT},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_b64}"}},
            ])
        ])
        elapsed = time.time() - start

        result["success"] = True
        result["time_seconds"] = round(elapsed, 2)
        result["scene"] = analysis.scene
        result["objects"] = analysis.objects
        result["people"] = analysis.people
        result["emotion"] = analysis.emotion
        result["event_type"] = analysis.event_type
        result["aesthetic"] = analysis.aesthetic
        result["caption_seed"] = analysis.caption_seed
        result["cultural_context"] = analysis.cultural_context
        result["visible_text"] = analysis.visible_text
        result["entities_count"] = len(analysis.entities)
        result["relationships_count"] = len(analysis.relationships)
        result["entities"] = [
            {"name": e.name, "kind": e.kind, "description": e.description}
            for e in analysis.entities
        ]
        result["relationships"] = [
            {"subject": r.subject, "predicate": r.predicate, "object": r.object}
            for r in analysis.relationships
        ]

    except Exception as e:
        result["success"] = False
        result["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        result["time_seconds"] = 0

    return result


# ── Main ───────────────────────────────────────────────────────────


BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


async def main():
    parser = argparse.ArgumentParser(description="Benchmark LLM extraction")
    parser.add_argument("--models", choices=["all", "local", "cloud"], default="all")
    parser.add_argument("--input", type=str, help="Specific input file name")
    parser.add_argument("--text-only", action="store_true")
    parser.add_argument("--image-only", action="store_true")
    args = parser.parse_args()

    models = get_enabled_models()
    if args.models == "local":
        models = [m for m in models if "localhost" in m.get("base_url", "")]
    elif args.models == "cloud":
        models = [m for m in models if "localhost" not in m.get("base_url", "")]

    if not models:
        print(f"{RED}No models available. Set OPENAI_API_KEY / ANTHROPIC_API_KEY for cloud models,")
        print(f"or start LM Studio for local models.{RESET}")
        print(f"\nConfigured models:")
        for m in MODELS:
            key_env = m.get("api_key_env", "")
            status = "available" if not key_env else ("set" if key_env and __import__("os").environ.get(key_env) else f"missing {key_env}")
            print(f"  {m['name']}: {status}")
        return

    inputs_dir = Path(__file__).parent / "inputs"
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)

    # Collect inputs
    text_inputs = []
    image_inputs = []
    for f in sorted(inputs_dir.iterdir()):
        if args.input and f.name != args.input:
            continue
        if f.suffix == ".txt" and not args.image_only:
            text_inputs.append(f)
        elif f.suffix in (".jpg", ".jpeg", ".png") and not args.text_only:
            image_inputs.append(f)

    print(f"\n{BOLD}{'=' * 60}{RESET}")
    print(f"{BOLD}  LLM Extraction Benchmark{RESET}")
    print(f"{BOLD}{'=' * 60}{RESET}")
    print(f"\n  Models: {len(models)}")
    print(f"  Text inputs: {len(text_inputs)}")
    print(f"  Image inputs: {len(image_inputs)}")
    print()

    all_results = []

    for model in models:
        print(f"{CYAN}{BOLD}  Model: {model['name']}{RESET}")

        for text_file in text_inputs:
            text = text_file.read_text().strip()
            print(f"    {DIM}Text: {text_file.name} ({len(text)} chars)...{RESET}", end=" ", flush=True)
            result = await benchmark_text(model, text, text_file.name)
            all_results.append(result)

            if result["success"]:
                print(f"{GREEN}{result['entities_count']} entities, "
                      f"{result['relationships_count']} rels "
                      f"({result['time_seconds']}s){RESET}")
            else:
                print(f"{RED}FAILED: {result.get('error', '?')[:60]}{RESET}")

        for img_file in image_inputs:
            print(f"    {DIM}Image: {img_file.name}...{RESET}", end=" ", flush=True)
            result = await benchmark_image(model, img_file, img_file.name)
            all_results.append(result)

            if result["success"]:
                print(f"{GREEN}{result['entities_count']} entities, "
                      f"{result['relationships_count']} rels "
                      f"({result['time_seconds']}s){RESET}")
                print(f"      {DIM}Scene: {result.get('scene', '?')[:60]}{RESET}")
                print(f"      {DIM}Caption: {result.get('caption_seed', '?')[:60]}{RESET}")
            else:
                print(f"{YELLOW}{result.get('error', '?')[:60]}{RESET}")

        print()

    # Save results
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = results_dir / f"benchmark_{timestamp}.json"
    output_file.write_text(json.dumps(all_results, indent=2))
    print(f"  Results saved to: {output_file}")

    # Summary table
    print(f"\n{BOLD}{'=' * 60}{RESET}")
    print(f"{BOLD}  Summary{RESET}")
    print(f"{BOLD}{'=' * 60}{RESET}\n")

    # Group by input
    inputs_seen = sorted({r["input"] for r in all_results})
    for input_name in inputs_seen:
        print(f"  {BOLD}{input_name}{RESET}")
        print(f"  {'Model':<25} {'Status':<8} {'Entities':<10} {'Rels':<8} {'Time':<8}")
        print(f"  {'-'*25} {'-'*8} {'-'*10} {'-'*8} {'-'*8}")
        for r in all_results:
            if r["input"] != input_name:
                continue
            status = f"{GREEN}OK{RESET}" if r["success"] else f"{RED}FAIL{RESET}"
            ents = str(r.get("entities_count", "-"))
            rels = str(r.get("relationships_count", "-"))
            t = f"{r['time_seconds']}s" if r["success"] else "-"
            print(f"  {r['model']:<25} {status:<17} {ents:<10} {rels:<8} {t:<8}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
