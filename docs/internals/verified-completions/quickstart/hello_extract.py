#!/usr/bin/env python3
"""hello_extract.py — the other half of the loop, minimal.

Claude turns two sentences into the +claim lines that hello-consistency.iql
hardcodes. Paste this script's output into the InputLayer CLI after loading
the three rules, then run  ?conflict(C1, C2)  and  .why.

Usage:  ANTHROPIC_API_KEY=sk-ant-...  python3 hello_extract.py
(Production uses structured outputs + prompt caching; this is the naive core.)
"""
import json, anthropic

MESSAGES = [
    (1, "We fly out of Geneva on August 14th."),
    (3, "Since we leave on the 12th, can Bob get an aisle seat?"),
]

PROMPT = """Turn each message into atomic factual claims for a logic engine.
Return ONLY a JSON array. Each item:
  {"id","entity","attribute","value","modality","msg","surface"}
Rules: one fact per claim; snake_case entity/attribute; dates ISO-8601,
resolving partial dates like "the 12th" from earlier context; modality is
one of asserted|hedged|question — presuppositions stated as fact ("since we
leave on the 12th") are asserted, requests are question; surface is a
verbatim substring of the source message; when unsure, extract nothing."""

client = anthropic.Anthropic()
text = "\n".join(f"[msg {i}] {t}" for i, t in MESSAGES)
r = client.messages.create(
    model="claude-haiku-4-5", max_tokens=1024, temperature=0,
    system=PROMPT, messages=[{"role": "user", "content": text}],
)
raw = r.content[0].text
claims = json.loads(raw[raw.find("[") : raw.rfind("]") + 1])

for c in claims:
    s = c["surface"].replace('"', "'")
    print(f'+claim[("{c["id"]}", "{c["entity"]}", "{c["attribute"]}", "{c["value"]}")]')
    print(f'+claim_modality[("{c["id"]}", "{c["modality"]}")]')
    print(f'+claim_source[("{c["id"]}", {c["msg"]}, "{s}")]')
