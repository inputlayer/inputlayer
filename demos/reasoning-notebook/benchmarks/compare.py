#!/usr/bin/env python3
"""Compare benchmark results and generate a report.

Usage:
    uv run python compare.py                           # terminal output, latest results
    uv run python compare.py --html                    # open HTML report in browser
    uv run python compare.py results/benchmark_*.json  # specific file
"""

from __future__ import annotations

import json
import sys
import time
import webbrowser
from pathlib import Path


def load_results(path: Path) -> list[dict]:
    return json.loads(path.read_text())


def print_comparison(results: list[dict]) -> None:
    inputs = sorted({r["input"] for r in results})
    models = sorted({r["model"] for r in results})

    print(f"\n{'=' * 80}")
    print(f"  Benchmark Comparison — {len(results)} runs across {len(models)} models")
    print(f"{'=' * 80}\n")

    for input_name in inputs:
        input_results = [r for r in results if r["input"] == input_name]
        input_type = input_results[0].get("type", "?")
        print(f"  Input: {input_name} ({input_type})")
        print(f"  {'Model':<28} {'Ent':>5} {'Rel':>5} {'Time':>7} {'Status':<12}")
        print(f"  {'-'*28} {'-'*5} {'-'*5} {'-'*7} {'-'*12}")

        for r in sorted(input_results, key=lambda x: x.get("time_seconds", 999)):
            if not r["success"]:
                print(f"  {r['model']:<28} {'—':>5} {'—':>5} {'—':>7} FAILED")
                continue
            ents = r.get("entities_count", 0)
            rels = r.get("relationships_count", 0)
            t = f"{r['time_seconds']}s"
            print(f"  {r['model']:<28} {ents:>5} {rels:>5} {t:>7} OK")

        print(f"\n  Entities extracted:")
        for r in sorted(input_results, key=lambda x: x["model"]):
            if not r["success"]:
                continue
            ent_names = sorted({e["name"] for e in r.get("entities", [])})
            print(f"    {r['model']:<25} {', '.join(ent_names[:10])}")

        if input_type == "image":
            print(f"\n  Scene analysis:")
            for r in sorted(input_results, key=lambda x: x["model"]):
                if not r["success"]:
                    continue
                print(f"    {r['model']:<25}")
                for field in ["scene", "emotion", "event_type", "aesthetic", "caption_seed"]:
                    val = r.get(field, "")
                    if val:
                        print(f"      {field:<16} {val[:60]}")

        print()

    print(f"{'=' * 80}")
    print(f"  Overall Summary")
    print(f"{'=' * 80}\n")
    print(f"  {'Model':<28} {'Avg Ent':>8} {'Avg Rel':>8} {'Avg Time':>9} {'Success':>8}")
    print(f"  {'-'*28} {'-'*8} {'-'*8} {'-'*9} {'-'*8}")

    for model in models:
        model_results = [r for r in results if r["model"] == model]
        successes = [r for r in model_results if r["success"]]
        if not successes:
            print(f"  {model:<28} {'—':>8} {'—':>8} {'—':>9} {f'0/{len(model_results)}':>8}")
            continue
        avg_ents = sum(r.get("entities_count", 0) for r in successes) / len(successes)
        avg_rels = sum(r.get("relationships_count", 0) for r in successes) / len(successes)
        avg_time = sum(r["time_seconds"] for r in successes) / len(successes)
        print(
            f"  {model:<28} {avg_ents:>8.1f} {avg_rels:>8.1f} {avg_time:>8.1f}s "
            f"{f'{len(successes)}/{len(model_results)}':>8}"
        )
    print()


# ── HTML Report ────────────────────────────────────────────────────


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def generate_html(results: list[dict], source_file: str) -> str:
    inputs = sorted({r["input"] for r in results})
    models = sorted({r["model"] for r in results})
    ts = time.strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LLM Extraction Benchmark</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    background: #1e1e2e; color: #cdd6f4; padding: 40px;
  }}
  h1 {{ font-size: 24px; margin-bottom: 6px; color: #cdd6f4; }}
  .meta {{ font-size: 12px; color: #6c7086; margin-bottom: 32px; }}
  h2 {{ font-size: 16px; color: #a6adc8; margin: 32px 0 12px; }}
  h3 {{ font-size: 13px; color: #89b4fa; margin: 20px 0 8px; text-transform: uppercase; letter-spacing: 1px; }}
  table {{
    width: 100%; border-collapse: collapse; font-size: 13px;
    background: #181825; border-radius: 8px; overflow: hidden;
    margin-bottom: 16px;
  }}
  th {{
    text-align: left; padding: 10px 14px; font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.5px;
    color: #6c7086; background: #11111b; font-weight: 600;
  }}
  td {{ padding: 8px 14px; border-top: 1px solid rgba(255,255,255,0.04); }}
  tr:hover td {{ background: rgba(255,255,255,0.02); }}
  .ok {{ color: #a6e3a1; }}
  .fail {{ color: #f38ba8; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .entities {{ font-size: 11px; color: #a6adc8; line-height: 1.6; }}
  .entity-tag {{
    display: inline-block; background: rgba(137,180,250,0.08);
    border: 1px solid rgba(137,180,250,0.15); border-radius: 4px;
    padding: 1px 6px; margin: 1px 2px; font-size: 10px; color: #89b4fa;
  }}
  .scene-grid {{ display: grid; grid-template-columns: 100px 1fr; gap: 4px 12px; font-size: 11px; }}
  .scene-label {{ color: #6c7086; font-weight: 600; }}
  .scene-value {{ color: #a6adc8; }}
  .summary {{ background: #181825; border-radius: 8px; padding: 20px; margin-top: 32px; }}
  .bar-container {{ width: 100%; height: 6px; background: rgba(255,255,255,0.04); border-radius: 3px; overflow: hidden; }}
  .bar {{ height: 100%; border-radius: 3px; }}
  .bar-ent {{ background: #89b4fa; }}
  .bar-time {{ background: #a6e3a1; }}
</style>
</head>
<body>
<h1>LLM Extraction Benchmark</h1>
<div class="meta">{ts} &mdash; {len(results)} runs across {len(models)} models &mdash; {source_file}</div>
"""

    # Per-input tables
    for input_name in inputs:
        input_results = [r for r in results if r["input"] == input_name]
        input_type = input_results[0].get("type", "?")

        html += f'<h2>{_esc(input_name)} <span style="color:#6c7086;font-size:12px;">({input_type})</span></h2>\n'
        html += '<table><thead><tr><th>Model</th><th class="num">Entities</th><th class="num">Relationships</th><th class="num">Time</th><th>Status</th></tr></thead><tbody>\n'

        for r in sorted(input_results, key=lambda x: x.get("time_seconds", 999)):
            if not r["success"]:
                err = _esc(r.get("error", "unknown")[:80])
                html += f'<tr><td>{_esc(r["model"])}</td><td class="num">&mdash;</td><td class="num">&mdash;</td><td class="num">&mdash;</td><td class="fail" title="{err}">FAILED</td></tr>\n'
                continue
            ents = r.get("entities_count", 0)
            rels = r.get("relationships_count", 0)
            t = r["time_seconds"]
            html += f'<tr><td>{_esc(r["model"])}</td><td class="num">{ents}</td><td class="num">{rels}</td><td class="num">{t}s</td><td class="ok">OK</td></tr>\n'

        html += '</tbody></table>\n'

        # Entities extracted
        html += '<h3>Entities Extracted</h3>\n'
        for r in sorted(input_results, key=lambda x: x["model"]):
            if not r["success"]:
                continue
            ent_tags = "".join(
                f'<span class="entity-tag">{_esc(e["name"])}<span style="color:#6c7086;margin-left:4px;">{_esc(e.get("kind",""))}</span></span>'
                for e in sorted(r.get("entities", []), key=lambda e: e["name"])
            )
            html += f'<div class="entities"><strong style="color:#cdd6f4;">{_esc(r["model"])}</strong><br>{ent_tags or "<em>none</em>"}</div>\n'

        # Image scene analysis
        if input_type == "image":
            html += '<h3>Scene Analysis</h3>\n'
            for r in sorted(input_results, key=lambda x: x["model"]):
                if not r["success"]:
                    continue
                html += f'<div style="margin-bottom:12px;"><strong style="color:#cdd6f4;">{_esc(r["model"])}</strong>\n<div class="scene-grid">\n'
                for field in ["scene", "objects", "people", "emotion", "event_type", "aesthetic", "caption_seed", "cultural_context", "visible_text"]:
                    val = r.get(field, "")
                    if isinstance(val, list):
                        val = ", ".join(val)
                    if val:
                        html += f'<span class="scene-label">{field}</span><span class="scene-value">{_esc(str(val)[:120])}</span>\n'
                html += '</div></div>\n'

    # Overall summary
    html += '<div class="summary"><h2 style="margin-top:0;">Overall Summary</h2>\n'
    html += '<table><thead><tr><th>Model</th><th class="num">Avg Entities</th><th class="num">Avg Relationships</th><th class="num">Avg Time</th><th class="num">Success</th><th>Entity Bar</th><th>Time Bar</th></tr></thead><tbody>\n'

    max_ents = 1
    max_time = 1
    model_stats = {}
    for model in models:
        model_results = [r for r in results if r["model"] == model]
        successes = [r for r in model_results if r["success"]]
        if successes:
            avg_ents = sum(r.get("entities_count", 0) for r in successes) / len(successes)
            avg_time = sum(r["time_seconds"] for r in successes) / len(successes)
            max_ents = max(max_ents, avg_ents)
            max_time = max(max_time, avg_time)
            model_stats[model] = (avg_ents, sum(r.get("relationships_count", 0) for r in successes) / len(successes), avg_time, len(successes), len(model_results))

    for model in models:
        if model not in model_stats:
            model_results = [r for r in results if r["model"] == model]
            html += f'<tr><td>{_esc(model)}</td><td class="num">&mdash;</td><td class="num">&mdash;</td><td class="num">&mdash;</td><td class="num">0/{len(model_results)}</td><td></td><td></td></tr>\n'
            continue
        avg_ents, avg_rels, avg_time, succ, total = model_stats[model]
        ent_pct = (avg_ents / max_ents) * 100
        time_pct = (avg_time / max_time) * 100
        html += (
            f'<tr><td>{_esc(model)}</td>'
            f'<td class="num">{avg_ents:.1f}</td>'
            f'<td class="num">{avg_rels:.1f}</td>'
            f'<td class="num">{avg_time:.1f}s</td>'
            f'<td class="num">{succ}/{total}</td>'
            f'<td><div class="bar-container"><div class="bar bar-ent" style="width:{ent_pct:.0f}%"></div></div></td>'
            f'<td><div class="bar-container"><div class="bar bar-time" style="width:{time_pct:.0f}%"></div></div></td>'
            f'</tr>\n'
        )

    html += '</tbody></table></div>\n</body></html>'
    return html


if __name__ == "__main__":
    results_dir = Path(__file__).parent / "results"
    do_html = "--html" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--html"]

    if args:
        path = Path(args[0])
    else:
        files = sorted(results_dir.glob("benchmark_*.json"))
        if not files:
            print("No benchmark results found. Run run_benchmark.py first.")
            sys.exit(1)
        path = files[-1]

    print(f"Loading: {path}")
    results = load_results(path)
    print_comparison(results)

    if do_html:
        html = generate_html(results, path.name)
        out = results_dir / f"{path.stem}.html"
        out.write_text(html)
        print(f"\nHTML report: {out}")
        webbrowser.open(f"file://{out.resolve()}")
