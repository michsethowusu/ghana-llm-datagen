"""
Merge Results — Admin Tool
===========================
Automatically fetches all .xz attachments from GitHub issues tagged
'results', downloads them, and merges into a single clean dataset.

Requirements:
    pip install requests

Usage:
    python scripts/merge_results.py

You will be prompted for your GitHub personal access token on first run.
It is saved to .github_token (gitignored) so you only enter it once.

To create a token:
    1. Go to https://github.com/settings/tokens/new
    2. Name it anything e.g. "ghana-llm-merge"
    3. Tick: repo (read access is enough)
    4. Generate and copy the token
"""

import json
import lzma
import sys
import os
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("❌  Run: pip install requests")

# ── Config — must match run.py ────────────────────────────────────────────────

GITHUB_REPO   = "GhanaNLP/ghana-llm-datagen"   # e.g. "kwameai/ghana-llm-datagen"
RESULTS_LABEL = "results"
DOWNLOAD_DIR  = Path("results")
OUTPUT_FILE   = "final_dataset.jsonl"
TOKEN_FILE    = Path(".github_token")

# ── Token handling ────────────────────────────────────────────────────────────

def get_token() -> str:
    """Return saved token if available, otherwise None (works for public repos)."""
    if TOKEN_FILE.exists():
        token = TOKEN_FILE.read_text().strip()
        if token:
            return token
    return None


def prompt_for_token() -> str:
    """Ask the user for a token and save it for future runs."""
    print("\n🔒  This repo appears to be private — a GitHub token is required.")
    print("    Create one at: https://github.com/settings/tokens/new")
    print("    - Name: ghana-llm-merge")
    print("    - Tick: repo\n")
    token = input("Paste your token here: ").strip()
    if not token:
        sys.exit("❌  No token provided.")
    TOKEN_FILE.write_text(token)
    print(f"✅  Token saved to {TOKEN_FILE} (won't ask again)\n")
    return token


# ── GitHub API ────────────────────────────────────────────────────────────────

def get_issues(token: str | None) -> list:
    """Fetch all closed and open issues with the results label.
    Works without a token for public repos."""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    issues, page = [], 1
    while True:
        url  = (f"https://api.github.com/repos/{GITHUB_REPO}/issues"
                f"?labels={RESULTS_LABEL}&state=all&per_page=100&page={page}")
        resp = requests.get(url, headers=headers)
        if resp.status_code in (401, 403):
            return None   # signal that auth is needed
        if resp.status_code == 404:
            sys.exit(f"❌  Repo not found: {GITHUB_REPO}\n    Update GITHUB_REPO in merge_results.py")
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        issues += batch
        page   += 1
    return issues


def extract_xz_urls(issue: dict) -> list:
    """Parse the issue body for GitHub attachment URLs ending in .xz"""
    import re
    body = issue.get("body") or ""
    # GitHub renders attachments as markdown links: [filename.xz](https://...)
    return re.findall(r'\(https://github\.com/[^\)]+\.xz\)', body)


def download_file(url: str, dest: Path, token: str | None) -> bool:
    headers = {"Authorization": f"token {token}"} if token else {}
    resp    = requests.get(url, headers=headers, stream=True)
    if resp.status_code != 200:
        print(f"    ⚠️  Failed to download {url} (HTTP {resp.status_code})")
        return False
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    return True


# ── Merge ─────────────────────────────────────────────────────────────────────

def iter_lines(path: Path):
    if path.suffix == ".xz":
        with lzma.open(path, "rt", encoding="utf-8") as f:
            yield from f
    else:
        with open(path, encoding="utf-8") as f:
            yield from f


def merge_files(files: list) -> tuple:
    total, good, errors, duplicates = 0, 0, 0, 0
    seen_ids = set()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        for fpath in files:
            file_total, file_good, file_errors, file_dupes = 0, 0, 0, 0
            try:
                for line in iter_lines(fpath):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        cid    = record.get("chunk_id", "")
                        if cid in seen_ids:
                            file_dupes += 1
                            continue
                        seen_ids.add(cid)
                        file_total += 1
                        if record.get("parse_error"):
                            file_errors += 1
                        else:
                            file_good += 1
                            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    except json.JSONDecodeError:
                        file_errors += 1
            except Exception as e:
                print(f"  ⚠️  Could not read {fpath.name}: {e}")
                continue

            print(f"  {fpath.name:<45} {file_total:>5} records  "
                  f"({file_good} good, {file_errors} errors, {file_dupes} dupes skipped)")
            total      += file_total
            good       += file_good
            errors     += file_errors
            duplicates += file_dupes

    return total, good, errors, duplicates


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if GITHUB_REPO.startswith("YOUR_USERNAME"):
        sys.exit("❌  Update GITHUB_REPO at the top of merge_results.py")

    token = get_token()   # None if no token saved yet
    DOWNLOAD_DIR.mkdir(exist_ok=True)

    # ── Fetch issues (try unauthenticated first for public repos) ──────────
    print(f"🔍  Fetching issues labelled '{RESULTS_LABEL}' from {GITHUB_REPO}...")
    issues = get_issues(token)

    if issues is None:
        # Got a 401/403 — repo is private, need a token
        token  = prompt_for_token()
        issues = get_issues(token)
        if issues is None:
            sys.exit("❌  Invalid token. Delete .github_token and try again.")

    print(f"    Found {len(issues)} submission(s)\n")

    if not issues:
        sys.exit("No issues found. Check that volunteers have submitted their results.")

    # ── Download attachments ───────────────────────────────────────────────
    downloaded = []
    for issue in issues:
        title    = issue.get("title", "untitled")
        urls     = extract_xz_urls(issue)
        if not urls:
            print(f"  ⚠️  No .xz attachments found in: '{title}' (issue #{issue['number']})")
            continue

        print(f"  #{issue['number']} — {title}")
        for raw_url in urls:
            url      = raw_url.strip("()")
            filename = url.split("/")[-1]
            dest     = DOWNLOAD_DIR / filename

            if dest.exists():
                print(f"    ✅  Already downloaded: {filename}")
                downloaded.append(dest)
                continue

            print(f"    ⬇️   Downloading {filename}...")
            if download_file(url, dest, token):
                size_mb = dest.stat().st_size / 1_048_576
                print(f"    ✅  Saved {filename} ({size_mb:.1f} MB)")
                downloaded.append(dest)

    if not downloaded:
        sys.exit("\n❌  No files downloaded. Nothing to merge.")

    # ── Also pick up any files already in results/ not from issues ────────
    existing = [f for f in DOWNLOAD_DIR.glob("*.xz") if f not in downloaded]
    if existing:
        print(f"\n📂  Also found {len(existing)} existing file(s) in {DOWNLOAD_DIR}/")
        downloaded += existing

    downloaded = sorted(set(downloaded))

    # ── Merge ──────────────────────────────────────────────────────────────
    print(f"\n📦  Merging {len(downloaded)} file(s)...\n")
    total, good, errors, dupes = merge_files(downloaded)

    out_size_mb = Path(OUTPUT_FILE).stat().st_size / 1_048_576
    print(f"""
{'='*60}
  ✅  Done!
  Output         : {OUTPUT_FILE}  ({out_size_mb:.1f} MB)
  Total records  : {total:,}
  Clean records  : {good:,}  (written to output)
  Parse errors   : {errors:,}  (excluded)
  Duplicates     : {dupes:,}  (skipped)
{'='*60}
""")


if __name__ == "__main__":
    main()
