#!/opt/homebrew/bin/python3
"""
GK Daily Control Tower — P3 Topic Scout.

Nightly (fired by the tower's tick at scout.run_at, or by hand / from the
dashboard) this proposes new Special Edition topics:

  1. pull current headlines from Google News RSS verticals
  2. collect everything already covered or queued: produced episode slugs +
     titles, and the live lines of the "GK Daily Topics" gdoc (source of truth)
  3. one kimi-k2.6 call scores/proposes N fresh explainer topics in house style
  4. proposals land in queue.json (Drive, next to the gdoc) and a Telegram
     digest goes out

Approval closes the loop: approve (dashboard button, or auto after
auto_approve_hours) appends the topic's queue_line to the gdoc via
`gws docs +write` — the same doc the 5 AM script-writing task already reads,
so nothing downstream changes. Veto just retires the proposal.

Usage:
    scout.py --run             # full run: propose + queue.json + Telegram
    scout.py --dry             # propose and print; write/send nothing
    scout.py --approve SLUG    # push one proposal into the gdoc
    scout.py --veto SLUG
    scout.py --list
"""

import argparse
import json
import logging
import re
import subprocess
import sys
import threading
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import tower  # load_config / telegram / load_env_creds; tower imports us lazily

log = logging.getLogger("tower.scout")
QUEUE_LOCK = threading.Lock()
USER_AGENT = "gkdaily-tower-scout/1.0"


# ----------------------------------------------------------------- sources --

def fetch_headlines(cfg: dict) -> dict[str, list[str]]:
    """Top headlines per configured Google News vertical; failures tolerated."""
    out = {}
    for name, url in cfg["scout"]["news_feeds"].items():
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=20) as resp:
                root = ET.fromstring(resp.read())
            titles = [(el.text or "").strip() for el in root.iter("title")][1:]
            # Google News titles end with " - Source"; keep them, the model copes
            out[name] = [t for t in titles if t][:12]
        except Exception as exc:
            log.warning("feed %s failed: %s", name, exc)
    return out


def past_topics(cfg: dict) -> list[str]:
    """Slugs + titles of everything already produced."""
    seen = []
    processed = cfg["drive_gk_daily"] / "scripts" / "processed"
    if processed.is_dir():
        seen += [re.sub(r"^\d{4}-\d{2}-\d{2}_", "", p.stem)
                 for p in processed.glob("*.md")]
    meta = cfg["podcasts_root"] / "public" / "episodes" / "special_editions.json"
    if meta.exists():
        try:
            seen += [v.get("title", "") for v in json.loads(meta.read_text()).values()]
        except Exception:
            pass
    return seen


def _gdoc_topic_paragraphs(cfg: dict) -> list[tuple[int, str]]:
    """(startIndex, text) for every non-empty paragraph below the divider."""
    doc_id = cfg["scout"]["topic_doc_id"]
    proc = subprocess.run(
        ["/opt/homebrew/bin/gws", "docs", "documents", "get",
         "--params", json.dumps({"documentId": doc_id})],
        capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"gws docs get failed: {proc.stderr[-300:]}")
    doc = json.loads(proc.stdout)
    out, past_divider = [], False
    for el in doc.get("body", {}).get("content", []):
        if "paragraph" not in el:
            continue
        text = "".join(r.get("textRun", {}).get("content", "")
                       for r in el["paragraph"].get("elements", [])).strip()
        if text.startswith("----"):
            past_divider = True
            continue
        if past_divider and text:
            out.append((el["startIndex"], text))
    return out


def gdoc_lines(cfg: dict) -> list[str]:
    """Current topic lines from the source-of-truth gdoc (below the divider)."""
    return [text for _, text in _gdoc_topic_paragraphs(cfg)]


def gdoc_insert(cfg: dict, line: str, before_line: str | None = None) -> str:
    """Add a topic line to the gdoc queue.

    before_line None → append at the end (gws +write helper). Otherwise insert
    the new line (plus a blank paragraph, matching the doc's spacing) at the
    startIndex of the named existing line, so it takes that line's position.
    """
    line = " ".join(line.split())
    if before_line:
        for idx, text in _gdoc_topic_paragraphs(cfg):
            if text == before_line:
                body = {"requests": [{"insertText": {
                    "location": {"index": idx}, "text": line + "\n\n"}}]}
                proc = subprocess.run(
                    ["/opt/homebrew/bin/gws", "docs", "documents", "batchUpdate",
                     "--params", json.dumps(
                         {"documentId": cfg["scout"]["topic_doc_id"]}),
                     "--json", json.dumps(body)],
                    capture_output=True, text=True, timeout=60)
                if proc.returncode != 0:
                    raise RuntimeError(f"gdoc insert failed: {proc.stderr[-300:]}")
                return f"inserted before “{before_line[:50]}”"
        # target line vanished (doc edited meanwhile) — fall through to append
    proc = subprocess.run(
        ["/opt/homebrew/bin/gws", "docs", "+write",
         "--document", cfg["scout"]["topic_doc_id"],
         "--text", f"\n{line}\n"],
        capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"gdoc append failed: {proc.stderr[-300:]}")
    return "appended to the end of the queue"


# --------------------------------------------------------------------- llm --

PROMPT = """You are the topic scout for "GK Daily Special Edition", a daily \
12–15 minute researched explainer podcast. The host is an OBGYN physician; \
the audience is curious generalists. House style: "how does X actually work" \
deep dives on infrastructure, energy, medicine, economics, technology, and \
science — sparked by the news but NOT news recaps; each episode should still \
be worth hearing in a year. Medical topics get clinical rigor.

Propose exactly {n} NEW topics. Hard rules:
- No overlap with ALREADY PRODUCED or ALREADY QUEUED topics (not even a \
different angle on the same subject).
- Spread across at least 3 different domains.
- Prefer topics with a concrete "why now" hook from the HEADLINES, but \
evergreen mechanisms beat thin news pegs.

Return STRICT JSON: {{"candidates": [{{"title": "podcast-style title with an \
em-dash subtitle", "slug": "kebab-case-slug", "angle": "2-3 sentences on the \
questions the episode answers", "why_now": "1 sentence", "queue_line": \
"slug — three or four comma-separated angle phrases (matches the topics-doc \
format)"}}]}}

HEADLINES TODAY:
{headlines}

ALREADY PRODUCED:
{past}

ALREADY QUEUED:
{queued}
"""


def propose(cfg: dict, headlines: dict, past: list, queued: list) -> list[dict]:
    creds = tower.load_env_creds(Path(cfg["scout"]["minibot_env"]).expanduser())
    api_key = creds.get("KIMI_API_KEY")
    if not api_key:
        raise RuntimeError("KIMI_API_KEY not found in minibot .env")
    base = creds.get("KIMI_BASE_URL", "https://api.moonshot.ai/v1")

    body = {
        "model": cfg["scout"]["model"],
        # no temperature: kimi-k2.6 rejects anything but the default (1).
        # k2.6 is a reasoning model: reasoning_content counts toward
        # max_tokens, so leave generous headroom or content comes back empty.
        "max_tokens": 12000,
        "response_format": {"type": "json_object"},
        "messages": [{
            "role": "user",
            "content": PROMPT.format(
                n=cfg["scout"]["candidates_per_run"],
                headlines=json.dumps(headlines, indent=1),
                past=json.dumps(past, indent=1),
                queued=json.dumps(queued, indent=1)),
        }],
    }
    reply = None
    for attempt in (1, 2):  # kimi can be slow on long structured outputs
        req = urllib.request.Request(
            f"{base}/chat/completions", data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=420) as resp:
                reply = json.loads(resp.read())
            break
        except TimeoutError:
            log.warning("kimi call timed out (attempt %d)", attempt)
            if attempt == 2:
                raise

    choice = reply["choices"][0]
    content = choice["message"].get("content") or ""
    if not content.strip():
        raise RuntimeError(
            f"model returned empty content (finish_reason="
            f"{choice.get('finish_reason')!r}) — raise max_tokens?")
    content = re.sub(r"^```(json)?|```$", "", content.strip(), flags=re.M)
    cands = json.loads(content)["candidates"]

    cleaned = []
    for c in cands:
        slug = re.sub(r"[^a-z0-9-]", "", str(c.get("slug", "")).lower())
        if slug and c.get("title") and c.get("queue_line"):
            c["slug"] = slug
            cleaned.append(c)
    if not cleaned:
        raise RuntimeError(f"model returned no usable candidates: {content[:200]}")
    return cleaned


# ------------------------------------------------------------------- queue --

def load_queue(cfg: dict) -> dict:
    p = cfg["topic_queue_json"]
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            log.warning("queue.json unreadable; starting fresh")
    return {"updated": None, "candidates": []}


def save_queue(cfg: dict, queue: dict) -> None:
    queue["updated"] = _now(cfg).isoformat(timespec="seconds")
    p = cfg["topic_queue_json"]
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(queue, indent=2, ensure_ascii=False) + "\n")
    tmp.replace(p)


def _now(cfg: dict) -> datetime:
    return datetime.now(ZoneInfo(cfg["timezone"]))


def run(cfg: dict, dry: bool = False) -> str:
    headlines = fetch_headlines(cfg)
    past = past_topics(cfg)
    try:
        queued = gdoc_lines(cfg)
    except Exception as exc:
        log.warning("gdoc read failed (%s); proposing against queue.json only", exc)
        queued = []
    with QUEUE_LOCK:
        queue = load_queue(cfg)
    known_slugs = {c["slug"] for c in queue["candidates"]}
    queued += [c["queue_line"] for c in queue["candidates"]
               if c["status"] in ("proposed", "pushed")]

    cands = propose(cfg, headlines, past, queued)
    fresh = [c for c in cands if c["slug"] not in known_slugs]
    if dry:
        print(json.dumps(fresh, indent=2, ensure_ascii=False))
        return f"dry run: {len(fresh)} candidate(s), nothing written"

    now = _now(cfg)
    for c in fresh:
        c |= {"status": "proposed",
              "proposed_at": now.isoformat(timespec="seconds"),
              "decided_at": None, "decided_by": None}
    with QUEUE_LOCK:
        queue = load_queue(cfg)
        queue["candidates"] += [c for c in fresh
                                if c["slug"] not in
                                {x["slug"] for x in queue["candidates"]}]
        save_queue(cfg, queue)

    hours = cfg["scout"]["auto_approve_hours"]
    lines = [f"🔭 GK Daily Topic Scout — {len(fresh)} new candidate(s):"]
    lines += [f"{i}. {c['title']}\n    ({c['why_now']})"
              for i, c in enumerate(fresh, 1)]
    lines.append("Approve/veto on the tower dashboard: "
                 "http://geffreys-mac-mini.tail52e6e4.ts.net:8888/tower/")
    if hours:
        lines.append(f"Unreviewed proposals auto-approve into the topics doc "
                     f"after {hours} h.")
    tower.telegram(cfg, "\n".join(lines))
    return f"proposed {len(fresh)} candidate(s)"


# ---------------------------------------------------------------- approval --

def _find(queue: dict, slug: str) -> dict | None:
    return next((c for c in queue["candidates"] if c["slug"] == slug), None)


def approve(cfg: dict, slug: str, decided_by: str = "user") -> str:
    with QUEUE_LOCK:
        queue = load_queue(cfg)
        c = _find(queue, slug)
        if not c:
            raise RuntimeError(f"no candidate with slug {slug!r}")
        if c["status"] != "proposed":
            return f"{slug} already {c['status']}"
        proc = subprocess.run(
            ["/opt/homebrew/bin/gws", "docs", "+write",
             "--document", cfg["scout"]["topic_doc_id"],
             "--text", f"\n{c['queue_line']}\n"],
            capture_output=True, text=True, timeout=60)
        if proc.returncode != 0:
            raise RuntimeError(f"gdoc append failed: {proc.stderr[-300:]}")
        c["status"] = "pushed"
        c["decided_at"] = _now(cfg).isoformat(timespec="seconds")
        c["decided_by"] = decided_by
        save_queue(cfg, queue)
    if decided_by == "auto":
        tower.telegram(cfg, f"🔭 Topic Scout: auto-approved “{c['title']}” "
                            "into the topics doc (no review within the window).")
    return f"pushed to topics doc: {c['queue_line']}"


def veto(cfg: dict, slug: str) -> str:
    with QUEUE_LOCK:
        queue = load_queue(cfg)
        c = _find(queue, slug)
        if not c:
            raise RuntimeError(f"no candidate with slug {slug!r}")
        if c["status"] != "proposed":
            return f"{slug} already {c['status']}"
        c["status"] = "vetoed"
        c["decided_at"] = _now(cfg).isoformat(timespec="seconds")
        c["decided_by"] = "user"
        save_queue(cfg, queue)
    return f"vetoed {slug}"


def auto_approve_due(cfg: dict, now: datetime) -> None:
    hours = cfg["scout"]["auto_approve_hours"]
    if not hours:
        return
    with QUEUE_LOCK:
        queue = load_queue(cfg)
        due = [c["slug"] for c in queue["candidates"]
               if c["status"] == "proposed"
               and datetime.fromisoformat(c["proposed_at"]) +
               timedelta(hours=hours) < now]
    for slug in due:
        try:
            approve(cfg, slug, decided_by="auto")
        except Exception as exc:
            log.warning("auto-approve %s failed: %s", slug, exc)


# ----------------------------------------------------- tower tick entrypoint --

def maybe_run(cfg: dict, conn, now: datetime) -> None:
    """Fire the nightly run once per day at/after scout.run_at."""
    if now.strftime("%H:%M") < cfg["scout"]["run_at"]:
        return
    today = f"{now:%Y-%m-%d}"
    conn.execute("CREATE TABLE IF NOT EXISTS scout_runs "
                 "(date TEXT PRIMARY KEY, ts TEXT, result TEXT)")
    if conn.execute("SELECT 1 FROM scout_runs WHERE date=?", (today,)).fetchone():
        return
    conn.execute("INSERT INTO scout_runs VALUES (?,?,?)",
                 (today, now.isoformat(timespec="seconds"), "running"))
    conn.commit()

    def go():
        import sqlite3
        try:
            result = run(cfg)
        except Exception as exc:
            result = f"failed: {exc}"
            log.exception("nightly scout run failed")
            tower.telegram(cfg, f"🔭 Topic Scout nightly run FAILED: {exc}")
        c = sqlite3.connect(tower.DB_PATH)
        c.execute("UPDATE scout_runs SET result=? WHERE date=?", (result, today))
        c.commit()
        c.close()
        log.info("scout nightly: %s", result)

    threading.Thread(target=go, daemon=True).start()


# -------------------------------------------------------------------- main --

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--run", action="store_true")
    g.add_argument("--dry", action="store_true")
    g.add_argument("--approve", metavar="SLUG")
    g.add_argument("--veto", metavar="SLUG")
    g.add_argument("--list", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [scout] %(levelname)s: %(message)s")
    cfg = tower.load_config()

    if args.list:
        for c in load_queue(cfg)["candidates"]:
            print(f"{c['status']:9} {c['slug']:40} {c['title'][:60]}")
        return 0
    if args.approve:
        print(approve(cfg, args.approve))
        return 0
    if args.veto:
        print(veto(cfg, args.veto))
        return 0
    print(run(cfg, dry=args.dry))
    return 0


if __name__ == "__main__":
    sys.exit(main())
