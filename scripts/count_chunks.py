"""
Run this once on your machine to get the true research chunk total.
It uses the already-cached CSV from run.py so no re-download needed.

Usage:
    python count_chunks.py
"""
import csv, math, sys
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(10_000_000)  # research CSV has large content fields

PAGES_PER_CHUNK   = 2
RESEARCH_CACHE    = Path("data_cache/research_data.csv")
NEWS_CACHE        = Path("data_cache/news_data.csv")

if not RESEARCH_CACHE.exists():
    sys.exit(f"Not found: {RESEARCH_CACHE}\nRun run.py first so the CSV gets cached.")

# ── Research ──────────────────────────────────────────────────────────────────
doc_pages = defaultdict(int)
with open(RESEARCH_CACHE, encoding="utf-8", errors="replace") as f:
    for row in csv.DictReader(f):
        fname   = (row.get("filename") or "").strip()
        content = (row.get("content")  or "").strip()
        if fname and content:
            doc_pages[fname] += 1

research_chunks = sum(math.ceil(p / PAGES_PER_CHUNK) for p in doc_pages.values())
print(f"Research: {len(doc_pages):,} documents → {research_chunks:,} chunks")

# ── News ──────────────────────────────────────────────────────────────────────
if NEWS_CACHE.exists():
    with open(NEWS_CACHE, encoding="utf-8", errors="replace") as f:
        news_rows = sum(1 for _ in f) - 1  # subtract header
    print(f"News:     {news_rows:,} articles → {news_rows:,} chunks")
else:
    print("News CSV not cached — skipping.")
    news_rows = None

print()
print("── Paste these two lines into app.py ────────────────────────")
print(f"GRAND_NEWS_CHUNKS     = {news_rows or '???':,}")
print(f"GRAND_RESEARCH_CHUNKS = {research_chunks:,}")
