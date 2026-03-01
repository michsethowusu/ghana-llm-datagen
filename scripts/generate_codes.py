"""
Generate Volunteer Codes
========================
Generates ONE code per volunteer that covers both their news slice
and research slice. They run the script once and it handles both.

Setup:
    1. Make sure .env has NVIDIA_KEY_1 … NVIDIA_KEY_5
    2. Set NEWS_CSV_PATH and RESEARCH_CSV_PATH below
    3. pip install pandas
    4. python generate_codes.py
"""

import base64
import json
import math
import os
import sys

# ════════════════════════════════════════════
#  ✏️  CONFIGURE HERE
# ════════════════════════════════════════════

NEWS_CSV_PATH     = "/media/owusus/Godstestimo/NLP-Projects/Ghana-1B/data/news_data.csv"
RESEARCH_CSV_PATH = "/media/owusus/Godstestimo/NLP-Projects/Ghana-1B/data/research_data.csv"

NUM_VOLUNTEERS    = 5   # must be <= number of keys in .env

# ════════════════════════════════════════════


def load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        sys.exit("❌  .env file not found. It should be in the same folder as this script.")
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())


def get_keys() -> list:
    keys = [os.environ.get(f"NVIDIA_KEY_{i}", "").strip() for i in range(1, 6)]
    keys = [k for k in keys if k]
    if not keys:
        sys.exit("❌  No NVIDIA keys found. Set NVIDIA_KEY_1 … NVIDIA_KEY_5 in .env")
    return keys


def check_csv_paths():
    errors = []
    for label, path in [("NEWS_CSV_PATH", NEWS_CSV_PATH), ("RESEARCH_CSV_PATH", RESEARCH_CSV_PATH)]:
        if path.startswith("/path/to/"):
            errors.append(f"  {label} still has a placeholder — update it at the top of this script")
        elif not os.path.exists(path):
            errors.append(f"  File not found for {label}: {path}")
    if errors:
        sys.exit("❌  Fix these before running:\n" + "\n".join(errors))


def encode(news_start: int, news_end: int,
           res_start: int,  res_end: int,
           api_key: str) -> str:
    payload = json.dumps({
        "ns": news_start,
        "ne": news_end,
        "rs": res_start,
        "re": res_end,
        "k":  api_key,
    }, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def generate(keys: list, news_total: int, res_total: int) -> list:
    n          = len(keys)
    news_slice = math.ceil(news_total / n)
    res_slice  = math.ceil(res_total  / n)
    results    = []
    for i, key in enumerate(keys):
        ns = i * news_slice;  ne = min(ns + news_slice, news_total)
        rs = i * res_slice;   re = min(rs + res_slice,  res_total)
        results.append({
            "volunteer":   i + 1,
            "news_rows":   f"{ns:,} – {ne:,}",
            "news_count":  ne - ns,
            "res_rows":    f"{rs:,} – {re:,}",
            "res_count":   re - rs,
            "code":        encode(ns, ne, rs, re, key),
        })
    return results


if __name__ == "__main__":
    load_env()
    check_csv_paths()

    try:
        import pandas as pd
    except ImportError:
        sys.exit("❌  Run: pip install pandas")

    keys = get_keys()
    if NUM_VOLUNTEERS > len(keys):
        sys.exit(f"❌  NUM_VOLUNTEERS is {NUM_VOLUNTEERS} but only {len(keys)} keys found in .env")
    keys = keys[:NUM_VOLUNTEERS]

    print("📂  Reading CSV files...")
    news_rows = len(pd.read_csv(NEWS_CSV_PATH))
    res_rows  = len(pd.read_csv(RESEARCH_CSV_PATH))
    print(f"    News     : {news_rows:,} rows  ({NEWS_CSV_PATH})")
    print(f"    Research : {res_rows:,} rows  ({RESEARCH_CSV_PATH})")
    print(f"    Keys     : {len(keys)} loaded from .env (NUM_VOLUNTEERS={NUM_VOLUNTEERS})\n")

    volunteers = generate(keys, news_rows, res_rows)

    print(f"{'='*100}")
    print(f"  VOLUNTEER CODES  |  News: {news_rows:,} rows  |  Research: {res_rows:,} rows  |  {len(keys)} volunteers")
    print(f"{'='*100}")
    print(f"  {'#':<4} {'NEWS ROWS':<22} {'N.COUNT':<9} {'RESEARCH ROWS':<22} {'R.COUNT':<9} CODE")
    print(f"  {'-'*4} {'-'*22} {'-'*9} {'-'*22} {'-'*9} {'-'*55}")

    for v in volunteers:
        print(f"  {v['volunteer']:<4} {v['news_rows']:<22} {v['news_count']:<9,} "
              f"{v['res_rows']:<22} {v['res_count']:<9,} {v['code']}")

    print(f"{'='*100}\n")

    with open("volunteer_codes.json", "w") as f:
        json.dump(volunteers, f, indent=2)
    print("  ✅  Codes saved to volunteer_codes.json")
    print("  ⚠️   Keep that file private — share only the individual codes.\n")
