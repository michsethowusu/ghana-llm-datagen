"""
Ghana LLM Dataset Generator — Volunteer Entry Point
=====================================================
One command. Runs your assigned news slice, then research slice, back to back.

Usage:
    python run.py --code YOUR_VOLUNTEER_CODE
"""

import sys
import subprocess
from pathlib import Path


# ── Auto-install requirements ─────────────────────────────────────────────────

def install_requirements():
    req_file = Path(__file__).parent / "requirements.txt"
    if not req_file.exists():
        print("Warning: requirements.txt not found - skipping auto-install.")
        return
    print("Checking requirements...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req_file), "-q"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Failed to install requirements:\n{result.stderr}")
        sys.exit(1)
    print("Requirements ready.\n")

install_requirements()


# ── Now safe to import third-party packages ───────────────────────────────────

import json
import argparse
import base64
import time
import hashlib
import urllib.request
import urllib.error
import pandas as pd
from tqdm import tqdm
import openai

# ── Config — owner updates these before pushing ───────────────────────────────

GITHUB_REPO        = "GhanaNLP/ghana-llm-datagen"
RELEASE_TAG        = "v1.0-data"
NEWS_FILENAME      = "news_data.csv"
RESEARCH_FILENAME  = "research_data.csv"

# ── Model config ──────────────────────────────────────────────────────────────

NVIDIA_BASE_URL   = "https://integrate.api.nvidia.com/v1"
NVIDIA_MODEL      = "meta/llama-3.1-70b-instruct"
RETRY_ATTEMPTS    = 4
RETRY_DELAY       = 8
MAX_CONTENT_CHARS = 3500
PAGES_PER_CHUNK   = 2


# ── Decode volunteer code ─────────────────────────────────────────────────────

def decode_code(code: str) -> dict:
    try:
        padded  = code + "=" * (4 - len(code) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        return {
            "news_start": payload["ns"],
            "news_end":   payload["ne"],
            "res_start":  payload["rs"],
            "res_end":    payload["re"],
            "api_key":    payload["k"],
        }
    except Exception:
        sys.exit("❌  Invalid volunteer code. Please double-check and try again.")


# ── Download CSV (cached) ─────────────────────────────────────────────────────

def get_csv(data_type: str) -> Path:
    filename   = NEWS_FILENAME if data_type == "news" else RESEARCH_FILENAME
    cache_path = Path("data_cache") / filename

    if cache_path.exists():
        size_mb = cache_path.stat().st_size / 1_048_576
        print(f"📂  Using cached file: {cache_path}  ({size_mb:.1f} MB)")
        return cache_path

    url = f"https://github.com/{GITHUB_REPO}/releases/download/{RELEASE_TAG}/{filename}"
    print(f"⬇️   Downloading {data_type} dataset...")
    print(f"    {url}")
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    last_pct = [-1]
    def progress(block_num, block_size, total_size):
        if total_size > 0:
            pct = min(int(block_num * block_size / total_size * 100), 100)
            if pct != last_pct[0]:
                print(f"\r    {pct}% of {total_size/1_048_576:.1f} MB", end="", flush=True)
                last_pct[0] = pct

    try:
        urllib.request.urlretrieve(url, cache_path, progress)
        print()
    except urllib.error.HTTPError as e:
        sys.exit(f"\n❌  Download failed (HTTP {e.code}).\n    {url}")
    except Exception as e:
        sys.exit(f"\n❌  Download failed: {e}")

    print(f"    ✅  Saved to {cache_path}  ({cache_path.stat().st_size/1_048_576:.1f} MB)\n")
    return cache_path


# ── API ───────────────────────────────────────────────────────────────────────

def make_client(api_key: str):
    return openai.OpenAI(api_key=api_key, base_url=NVIDIA_BASE_URL)


def call_api(client, prompt: str):
    for attempt in range(RETRY_ATTEMPTS):
        try:
            resp = client.chat.completions.create(
                model=NVIDIA_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.75,
                max_tokens=2048,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            wait = RETRY_DELAY * (attempt + 1)
            tqdm.write(f"  ⚠️  Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(wait)
    return None


# ── Chunk builders ────────────────────────────────────────────────────────────

def build_news_chunks(df, row_start: int) -> list:
    required = {"url", "title", "content", "date", "category"}
    if not required.issubset(df.columns):
        sys.exit(f"❌  News CSV missing columns. Expected: {required}\n    Found: {set(df.columns)}")
    df = df.dropna(subset=["content", "title"]).reset_index(drop=True)
    chunks = []
    for i, (_, row) in enumerate(df.iterrows()):
        title    = str(row["title"])
        content  = str(row["content"])[:MAX_CONTENT_CHARS]
        date     = str(row.get("date", ""))
        category = str(row.get("category", ""))
        url      = str(row.get("url", ""))
        combined = f"Title: {title}\nDate: {date}\nCategory: {category}\n\n{content}"
        chunk_id = hashlib.md5((url + title).encode()).hexdigest()
        chunks.append({
            "chunk_id": chunk_id, "title": title, "category": category,
            "url": url, "date": date, "combined_text": combined,
        })
    return chunks


def build_research_chunks(df, row_start: int) -> list:
    required = {"filename", "page_range", "content"}
    if not required.issubset(df.columns):
        sys.exit(f"❌  Research CSV missing columns. Expected: {required}\n    Found: {set(df.columns)}")
    df = df.dropna(subset=["filename", "content"]).reset_index(drop=True)
    df["content"] = df["content"].astype(str).str.strip()
    df = df[df["content"] != ""].reset_index(drop=True)
    chunks = []
    for filename, group in df.groupby("filename", sort=False):
        rows = group.reset_index(drop=True)
        for i in range(0, len(rows), PAGES_PER_CHUNK):
            chunk_rows  = rows.iloc[i:i + PAGES_PER_CHUNK]
            page_ranges = " + ".join(chunk_rows["page_range"].astype(str).tolist())
            combined    = "\n\n".join(chunk_rows["content"].astype(str).tolist())
            chunk_id    = hashlib.md5(f"{filename}::{row_start}::{combined[:200]}".encode()).hexdigest()
            chunks.append({
                "chunk_id": chunk_id, "filename": filename,
                "page_ranges": page_ranges, "content": combined,
            })
    return chunks


# ── Prompts ───────────────────────────────────────────────────────────────────

def news_prompt(chunk: dict) -> str:
    return f"""You are a dataset creator. Generate a high-quality multi-turn conversation in the style of UltraChat, based strictly on this Ghanaian news article.

## News Article
{chunk['combined_text']}

## Instructions:
- Generate a realistic multi-turn conversation between a curious USER and a knowledgeable ASSISTANT.
- The conversation must have 4-6 turns (USER and ASSISTANT alternating).
- Ground all facts strictly in the article. Do not invent facts.
- USER asks progressively deeper questions (causes, implications, stakeholders, comparisons).
- ASSISTANT gives accurate, well-explained answers from the article.
- Output ONLY valid JSON — no markdown, no preamble, no extra text.

Required format:
{{
  "id": "ghana_news_conv",
  "source_title": "{chunk['title'].replace('"', '')}",
  "category": "{chunk['category']}",
  "conversations": [
    {{"role": "user", "content": "..."}},
    {{"role": "assistant", "content": "..."}},
    {{"role": "user", "content": "..."}},
    {{"role": "assistant", "content": "..."}}
  ]
}}"""


def research_prompt(chunk: dict) -> str:
    return f"""You are a dataset creator. Generate a high-quality multi-turn educational conversation in the style of UltraChat, grounded in this excerpt from a Ghanaian research article.

## Research Excerpt:
{chunk['content']}

## Instructions:
- Generate a realistic multi-turn conversation between a curious USER and a knowledgeable ASSISTANT.
- The conversation should have 4-6 turns (USER and ASSISTANT alternating).
- Base all factual content strictly on the excerpt. Do not invent facts.
- USER asks progressively deeper questions.
- ASSISTANT gives accurate, well-explained answers.
- Output ONLY valid JSON — no markdown, no preamble, no extra text.

Required format:
{{
  "id": "ghana_research_conv",
  "conversations": [
    {{"role": "user", "content": "..."}},
    {{"role": "assistant", "content": "..."}},
    {{"role": "user", "content": "..."}},
    {{"role": "assistant", "content": "..."}}
  ]
}}"""


# ── JSON parsing ──────────────────────────────────────────────────────────────

def parse_json(raw: str, chunk: dict, data_type: str):
    try:
        cleaned = raw.strip()
        if "```" in cleaned:
            for part in cleaned.split("```"):
                part = part.strip().lstrip("json").strip()
                if part.startswith("{"):
                    cleaned = part
                    break
        start, end = cleaned.find("{"), cleaned.rfind("}")
        if start != -1 and end != -1:
            cleaned = cleaned[start:end + 1]
        data = json.loads(cleaned)
        data["chunk_id"] = chunk["chunk_id"]
        if data_type == "news":
            data["source_url"]  = chunk["url"]
            data["source_date"] = chunk["date"]
        else:
            data["source_file"]  = chunk["filename"]
            data["source_pages"] = chunk["page_ranges"]
        return data
    except json.JSONDecodeError as e:
        tqdm.write(f"  ⚠️  JSON parse error: {e} | preview: {raw[:120]}")
        return None


# ── Resume support ────────────────────────────────────────────────────────────

def load_completed(path: Path) -> set:
    done = set()
    if not path.exists():
        return done
    with open(path) as f:
        for line in f:
            try:
                cid = json.loads(line.strip()).get("chunk_id")
                if cid:
                    done.add(cid)
            except Exception:
                pass
    return done


# ── Run one data type ─────────────────────────────────────────────────────────

def run_type(data_type: str, row_start: int, row_end: int,
             client, output_path: Path):

    print(f"\n{'─'*55}")
    print(f"  Starting {data_type.upper()}  (rows {row_start:,} – {row_end:,})")
    print(f"{'─'*55}")

    csv_path = get_csv(data_type)
    df = pd.read_csv(csv_path, skiprows=range(1, row_start + 1), nrows=row_end - row_start)
    df = df.reset_index(drop=True)
    print(f"📊  Loaded {len(df):,} rows\n")

    chunks    = build_news_chunks(df, row_start) if data_type == "news" else build_research_chunks(df, row_start)
    completed = load_completed(output_path)
    pending   = [c for c in chunks if c["chunk_id"] not in completed]

    print(f"📦  Chunks: {len(chunks):,} total  |  {len(completed):,} done  |  {len(pending):,} remaining")

    if not pending:
        print(f"  ✅  {data_type.upper()} already complete!\n")
        return 0, 0

    with open(output_path, "a", encoding="utf-8") as out_f:
        for chunk in tqdm(pending, desc=data_type.upper()):
            label = chunk.get("title", chunk.get("filename", ""))[:65]
            tqdm.write(f"\n  → {label}")

            prompt     = news_prompt(chunk) if data_type == "news" else research_prompt(chunk)
            raw_output = call_api(client, prompt)

            if raw_output is None:
                tqdm.write("  ⏭️  Skipped (all retries failed)")
                continue

            record = parse_json(raw_output, chunk, data_type)
            if record:
                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_f.flush()
                tqdm.write(f"  ✅  {len(record.get('conversations', []))} turns saved")
            else:
                fallback = {"chunk_id": chunk["chunk_id"], "raw_output": raw_output, "parse_error": True}
                if data_type == "news":
                    fallback.update({"source_url": chunk["url"], "category": chunk.get("category")})
                else:
                    fallback.update({"source_file": chunk["filename"], "source_pages": chunk["page_ranges"]})
                out_f.write(json.dumps(fallback, ensure_ascii=False) + "\n")
                out_f.flush()
                tqdm.write("  ⚠️   Raw output saved (parse failed)")

    lines = [json.loads(l) for l in open(output_path) if l.strip()]
    good  = sum(1 for l in lines if not l.get("parse_error"))
    return len(lines), good


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ghana LLM Dataset Generator")
    parser.add_argument("--code",   required=True, help="Your volunteer code")
    parser.add_argument("--output", default=None,  help="Custom output path (optional)")
    args = parser.parse_args()

    info = decode_code(args.code)

    news_label = f"news_{info['news_start']}_{info['news_end']}"
    res_label  = f"research_{info['res_start']}_{info['res_end']}"
    output_path = Path(args.output or f"results/volunteer_{news_label}.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    news_count = info['news_end'] - info['news_start']
    res_count  = info['res_end']  - info['res_start']
    est_hours  = (news_count + res_count) * 8 / 3600

    print(f"""
╔══════════════════════════════════════════════════════╗
║       Ghana LLM Dataset Generator — Volunteer        ║
╠══════════════════════════════════════════════════════╣
║  News     : rows {info['news_start']:,} – {info['news_end']:,} ({news_count:,} rows){' '*(22-len(f"{info['news_start']:,} – {info['news_end']:,} ({news_count:,} rows)"))}║
║  Research : rows {info['res_start']:,} – {info['res_end']:,} ({res_count:,} rows){' '*(22-len(f"{info['res_start']:,} – {info['res_end']:,} ({res_count:,} rows)"))}║
║  Model    : {NVIDIA_MODEL:<41} ║
║  Output   : {str(output_path):<41} ║
║  Est. time: ~{est_hours:.1f}h (auto-resumes if interrupted){' '*(18-len(f"~{est_hours:.1f}h (auto-resumes if interrupted)"))}║
╚══════════════════════════════════════════════════════╝
""")

    client = make_client(info["api_key"])

    # ── Run news, then research ────────────────────────────────────────────
    news_out  = output_path.parent / f"{news_label}.jsonl"
    res_out   = output_path.parent / f"{res_label}.jsonl"

    run_type("news",     info["news_start"], info["news_end"], client, news_out)
    run_type("research", info["res_start"],  info["res_end"],  client, res_out)

    # ── Final summary ──────────────────────────────────────────────────────
    total, good = 0, 0
    for path in [news_out, res_out]:
        if path.exists():
            lines = [json.loads(l) for l in open(path) if l.strip()]
            total += len(lines)
            good  += sum(1 for l in lines if not l.get("parse_error"))

    print(f"""
╔══════════════════════════════════════════════════════╗
║              🎉  ALL DONE!                           ║
╠══════════════════════════════════════════════════════╣
║  Total records : {total:<35,} ║
║  Parsed OK     : {good:<35,} ║
╚══════════════════════════════════════════════════════╝

📤  Submit your two result files:
    {news_out.resolve()}
    {res_out.resolve()}

Open a GitHub issue at:
  https://github.com/{GITHUB_REPO}/issues/new?template=result_submission.md

Thank you for contributing to the Ghana LLM project! 🇬🇭
""")


if __name__ == "__main__":
    main()
