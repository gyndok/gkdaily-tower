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
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from reliability import atomic_json

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
12–15 minute researched explainer podcast narrated in a reporter's voice. The \
show's owner is an OBGYN physician (so medical topics get clinical rigor); the \
audience is curious generalists. House style: "how does X actually work" \
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


def _extract_candidates(content: str) -> list[dict]:
    """Parse the model's JSON; if it is truncated, salvage the complete
    candidate objects rather than failing the whole run."""
    content = re.sub(r"^```(json)?|```$", "", content.strip(), flags=re.M).strip()
    try:
        data = json.loads(content)
        cands = data.get("candidates", data) if isinstance(data, dict) else data
        if isinstance(cands, list):
            return [c for c in cands if isinstance(c, dict)]
    except json.JSONDecodeError:
        pass
    salvaged = []
    for block in re.findall(r"\{[^{}]*\}", content):
        try:
            obj = json.loads(block)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("title"):
            salvaged.append(obj)
    if salvaged:
        log.warning("model JSON was malformed/truncated; salvaged %d candidate(s)",
                    len(salvaged))
    return salvaged


def _content_filtered(cfg: dict, text: str) -> bool:
    """True if Moonshot's content filter rejects this text outright."""
    creds = tower.load_env_creds(Path(cfg["scout"]["minibot_env"]).expanduser())
    body = {"model": cfg["scout"]["model"], "thinking": {"type": "disabled"},
            "max_tokens": 5, "messages": [{"role": "user", "content": text}]}
    req = urllib.request.Request(
        creds.get("KIMI_BASE_URL", "https://api.moonshot.ai/v1") + "/chat/completions",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {creds.get('KIMI_API_KEY', '')}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60):
            return False
    except urllib.error.HTTPError as exc:
        return "content_filter" in exc.read().decode(errors="replace")
    except Exception:
        return False


def screen_headlines(cfg: dict, headlines: dict) -> dict:
    """Drop only the headlines Moonshot's content filter refuses.

    The scout's model is Moonshot's Kimi, whose filter rejects the WHOLE
    request — HTTP 400, "considered high risk" — if any part trips it. On
    2026-09-23 a single top-news headline, "Trump greets Xi Jinping at plane
    as Chinese leader arrives for state visit", took the entire nightly
    proposal down, and every retry failed identically because retries only
    shrank the headline count. Checked feed by feed, then headline by
    headline within a flagged feed, so a normal night costs nothing extra
    and a bad one costs a handful of five-token calls.
    """
    clean = {}
    for feed, items in headlines.items():
        if not _content_filtered(cfg, json.dumps(items)):
            clean[feed] = items
            continue
        kept = [h for h in items if not _content_filtered(cfg, h)]
        for h in items:
            if h not in kept:
                log.warning("dropped headline the model provider refuses: %s", h[:120])
        clean[feed] = kept
    return clean


def propose(cfg: dict, headlines: dict, past: list, queued: list) -> list[dict]:
    creds = tower.load_env_creds(Path(cfg["scout"]["minibot_env"]).expanduser())
    api_key = creds.get("KIMI_API_KEY")
    if not api_key:
        raise RuntimeError("KIMI_API_KEY not found in minibot .env")
    base = creds.get("KIMI_BASE_URL", "https://api.moonshot.ai/v1")

    last_error = None
    screened = False
    for attempt in (1, 2, 3):
        # Attempt 2+ shrinks the input in case size was the problem.
        per_feed = 12 if attempt == 1 else 6
        hl = {k: v[:per_feed] for k, v in headlines.items()}
        body = {
            "model": cfg["scout"]["model"],
            # kimi-k2.6 is a reasoning model; for this structured-output task
            # thinking only burns the token budget (empty/truncated JSON on
            # 2026-08-19 and 08-21). Moonshot honors thinking: disabled.
            "thinking": {"type": "disabled"},
            "max_tokens": 8000,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "user", "content": PROMPT.format(
                n=cfg["scout"]["candidates_per_run"],
                headlines=json.dumps(hl, indent=1),
                past=json.dumps(past, indent=1),
                queued=json.dumps(queued, indent=1))}],
        }
        req = urllib.request.Request(
            f"{base}/chat/completions", data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=240) as resp:
                reply = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            last_error = f"attempt {attempt}: HTTP {exc.code} {detail[:160]}"
            log.warning("kimi call failed (%s)", last_error)
            if "content_filter" in detail and not screened:
                headlines = screen_headlines(cfg, headlines)
                screened = True
            time.sleep(5 * attempt)
            continue
        except Exception as exc:  # timeouts, 5xx, connection resets
            last_error = f"attempt {attempt}: {exc}"
            log.warning("kimi call failed (%s)", last_error)
            time.sleep(5 * attempt)
            continue
        choice = reply["choices"][0]
        content = choice["message"].get("content") or ""
        if choice.get("finish_reason") == "length":
            log.warning("attempt %d: output truncated (finish_reason=length)", attempt)
        cands = _extract_candidates(content)
        cleaned = []
        for c in cands:
            slug = re.sub(r"[^a-z0-9-]", "", str(c.get("slug", "")).lower())
            if slug and c.get("title") and c.get("queue_line"):
                c["slug"] = slug
                c.setdefault("angle", "")
                c.setdefault("why_now", "")
                cleaned.append(c)
        if cleaned:
            return cleaned
        last_error = (f"attempt {attempt}: no usable candidates "
                      f"(finish={choice.get('finish_reason')!r}, {len(content)} chars)")
        log.warning(last_error)
    raise RuntimeError(f"scout model failed after 3 attempts — {last_error}")


# ------------------------------------------------------------------- queue --

QUEUE_IO_TIMEOUT = 30   # seconds; a stalled Drive mount blocks forever otherwise


def _mirror_path(cfg: dict) -> Path:
    """Local mirror of the queue, on real disk rather than the Drive mount."""
    return Path(__file__).resolve().parent / "queue.mirror.json"


def _with_timeout(fn, seconds: int, what: str):
    """Run fn in a daemon thread; raise TimeoutError if it outlives `seconds`.

    Reads and writes under ~/Library/CloudStorage block indefinitely when
    DriveFS cannot hydrate a placeholder (e.g. its content cache is disabled
    because the disk is full), so every queue touch is bounded here.
    """
    box = {}

    def run():
        try:
            box["ok"] = fn()
        except BaseException as exc:      # noqa: BLE001 - reported to caller
            box["err"] = exc

    t = threading.Thread(target=run, daemon=True, name=f"queue-io:{what}")
    t.start()
    t.join(seconds)
    if t.is_alive():
        raise TimeoutError(f"{what} blocked for >{seconds}s (Drive mount stalled)")
    if "err" in box:
        raise box["err"]
    return box["ok"]


def load_queue(cfg: dict) -> dict:
    p = cfg["topic_queue_json"]
    mirror = _mirror_path(cfg)
    try:
        drive = _with_timeout(
            lambda: json.loads(p.read_text()) if p.exists() else None,
            QUEUE_IO_TIMEOUT, "read queue.json")
    except Exception as exc:
        log.warning("queue.json unreadable on Drive (%s); trying local mirror", exc)
    else:
        if drive is not None:
            try:                          # keep the mirror fresh for next time
                atomic_json(mirror, drive)
            except Exception as exc:
                log.warning("could not refresh queue mirror: %s", exc)
            return drive
        if not mirror.exists():
            return {"updated": None, "candidates": []}

    if mirror.exists():
        try:
            queue = json.loads(mirror.read_text())
        except Exception as exc:
            raise RuntimeError(
                f"queue.json unreadable and mirror {mirror} is corrupt; "
                "preserving both for recovery") from exc
        log.warning("running off local mirror %s (updated %s) — Drive copy "
                    "could not be read", mirror, queue.get("updated"))
        return queue

    raise RuntimeError("queue.json unreadable and no local mirror exists; "
                       "preserving it for recovery")


def save_queue(cfg: dict, queue: dict) -> None:
    queue["updated"] = _now(cfg).isoformat(timespec="seconds")
    # Local mirror first: it is the copy that cannot be lost to a stalled mount.
    atomic_json(_mirror_path(cfg), queue)
    p = cfg["topic_queue_json"]
    try:
        _with_timeout(lambda: (p.parent.mkdir(parents=True, exist_ok=True),
                               atomic_json(p, queue)),
                      QUEUE_IO_TIMEOUT, "write queue.json")
    except Exception as exc:
        log.warning("could not write queue.json to Drive (%s); mirror at %s "
                    "holds this run and will be pushed on the next good write",
                    exc, _mirror_path(cfg))


def _now(cfg: dict) -> datetime:
    return datetime.now(ZoneInfo(cfg["timezone"]))


# --------------------------------------------------------- subject dedupe --
#
# The scout kept proposing topics that had already aired, or near-duplicates of
# each other: by 2026-09-23 queue.json held SEVEN measles candidates under
# seven different slugs. Three gaps let them through. Dedupe compared exact
# slugs only; vetoed candidates were left out of the model's context, so a
# vetoed idea came straight back; and the only record of what had aired was
# the mini's own, so everything the MacBook pipeline produced was invisible.
#
# The fix: build one "taken" list from every source, then ask the model — in a
# single batched call — whether each proposal is the same SUBJECT as anything
# on it. A shared word is not a match ("honeybee colony collapse" is not
# "spelling bee"); the same disease, drug, technology, event or policy is.

SUBJECT_PROMPT = """You are deduplicating topic proposals for a daily deep-dive \
podcast against topics it has already published or already has queued.

For EACH numbered proposal, decide whether it is substantially the SAME SUBJECT \
as any item on the covered list — the same disease, drug, technology, event, \
institution, person, place or policy — even when the angle, wording or slug \
differs. "Measles herd immunity math" and "measles resurgence" are the SAME \
subject. Sharing a word is NOT enough: "honeybee colony collapse" and "the \
spelling bee industry" are different subjects.

A new angle on a covered subject still counts as a MATCH, unless the proposal \
explicitly describes itself as a follow-up AND names the earlier episode.

{batch_rule}PROPOSALS:
{proposals}

COVERED OR QUEUED:
{taken}

Reply with ONLY a JSON object, one result per proposal, in order:
{{"results": [{{"id": 1, "verdict": "NEW", "item": ""}}, \
{{"id": 2, "verdict": "MATCH", "item": "<the covered item it duplicates>"}}]}}"""


def published_titles(cfg: dict) -> list[str]:
    """Every entry in special-editions.md, the cross-machine master list.

    One line per episode, `YYYY-MM-DD — Title (optional subject tag)`, written
    by both the MacBook and (via tower.maybe_record_published) the mini. The
    date is stripped: the subject is what matters.
    """
    path = cfg["drive_gk_daily"] / "special-editions.md"
    try:
        text = _with_timeout(path.read_text, QUEUE_IO_TIMEOUT, "special-editions.md")
    except Exception as exc:
        log.warning("special-editions.md unreadable (%s)", exc)
        return []
    return [m.group(1).strip() for m in
            re.finditer(r"^\d{4}-\d{2}-\d{2}\s+—\s+(.+)$", text, re.M)]


def taken_topics(cfg: dict, queue: dict, pending: list[str]) -> list[str]:
    """Everything a new proposal must not duplicate, from every source.

    special-editions.md (both machines), the mini's own produced episodes
    (past_topics — kept because it covers episodes the master list misses:
    "GLP-1 Beyond Weight Loss" aired 2026-08-11 without the "Special Edition"
    title prefix, so the RSS-driven master list never picked it up), the
    Topics doc's pending lines, and every queue.json candidate whatever its
    status — vetoed ideas included, which is the point.
    """
    items = (published_titles(cfg) + past_topics(cfg) + list(pending)
             + [c.get("title") or c.get("queue_line", "")
                for c in queue.get("candidates", [])])
    seen, out = set(), []
    for it in items:
        it = " ".join(str(it).split())[:140]       # bound tokens per item
        key = re.sub(r"[^a-z0-9]", "", it.lower())
        if it and key not in seen:
            seen.add(key)
            out.append(it)
    return out


BATCH_RULE = ("Also treat a proposal as a MATCH if it is the same subject as an "
              "EARLIER-numbered proposal in this same list; name that proposal "
              "as the item.\n\n")


def subject_check(cfg: dict, proposals: list[str], taken: list[str],
                  batch_rule: bool = True) -> list[dict]:
    """One batched model call: MATCH or NEW for every proposal.

    Returns [{"id", "proposal", "verdict", "item"}] in proposal order. A
    proposal the model gives no verdict for comes back UNDECIDED; callers
    treat that as a duplicate, because proposing a repeat is the failure this
    exists to prevent and skipping one idea for a night costs nothing.
    """
    if not proposals:
        return []
    creds = tower.load_env_creds(Path(cfg["scout"]["minibot_env"]).expanduser())
    api_key = creds.get("KIMI_API_KEY")
    if not api_key:
        raise RuntimeError("KIMI_API_KEY not found in minibot .env")
    base = creds.get("KIMI_BASE_URL", "https://api.moonshot.ai/v1")
    prompt = SUBJECT_PROMPT.format(
        batch_rule=BATCH_RULE if batch_rule else "",
        proposals="\n".join(f"{i}. {p}" for i, p in enumerate(proposals, 1)),
        taken="\n".join(f"- {t}" for t in taken))
    body = {"model": cfg["scout"]["model"],
            "thinking": {"type": "disabled"},        # see propose(): keeps JSON whole
            "max_tokens": 4000,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "user", "content": prompt}]}
    last_error = None
    for attempt in (1, 2, 3):
        req = urllib.request.Request(
            f"{base}/chat/completions", data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=240) as resp:
                reply = json.loads(resp.read())
            content = reply["choices"][0]["message"].get("content") or ""
            m = re.search(r"\{.*\}", content, re.S)
            results = json.loads(m.group(0)).get("results", []) if m else []
            by_id = {int(r.get("id", 0)): r for r in results if str(r.get("id", "")).isdigit()}
            if not by_id:
                raise ValueError(f"no parseable results in {len(content)} chars")
            out = []
            for i, p in enumerate(proposals, 1):
                r = by_id.get(i, {})
                verdict = str(r.get("verdict", "")).upper()
                out.append({"id": i, "proposal": p,
                            "verdict": verdict if verdict in ("NEW", "MATCH") else "UNDECIDED",
                            "item": str(r.get("item", ""))[:140]})
            return out
        except Exception as exc:
            last_error = f"attempt {attempt}: {exc}"
            log.warning("subject check failed (%s)", last_error)
            time.sleep(5 * attempt)
    raise RuntimeError(f"subject check failed after 3 attempts — {last_error}")


VERIFY_PROMPT = """For each numbered pair, decide whether an episode on A would \
REPEAT an episode on B: the same specific disease, drug, technology, event, \
institution, person, place or policy.

Answer YES for the same specific subject under a different angle or wording — \
"measles herd immunity math" vs "measles resurgence" is YES (same disease).
Answer NO when they only share a broad category — "honeybee colony collapse" \
vs "CRISPR gene-drive mosquitoes" is NO (both insects, different subjects), and \
"a Lyme vaccine" vs "a measles vaccine" is NO (different diseases).

{pairs}

Reply with ONLY a JSON object, one result per pair, in order:
{{"results": [{{"id": 1, "same": "YES"}}, {{"id": 2, "same": "NO"}}]}}"""


def _kimi_json(cfg: dict, prompt: str, max_tokens: int = 4000) -> dict:
    """One JSON-mode Kimi call with the scout's proven settings and retries."""
    creds = tower.load_env_creds(Path(cfg["scout"]["minibot_env"]).expanduser())
    api_key = creds.get("KIMI_API_KEY")
    if not api_key:
        raise RuntimeError("KIMI_API_KEY not found in minibot .env")
    base = creds.get("KIMI_BASE_URL", "https://api.moonshot.ai/v1")
    body = {"model": cfg["scout"]["model"], "thinking": {"type": "disabled"},
            "max_tokens": max_tokens, "response_format": {"type": "json_object"},
            "messages": [{"role": "user", "content": prompt}]}
    last_error = None
    for attempt in (1, 2, 3):
        req = urllib.request.Request(
            f"{base}/chat/completions", data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=240) as resp:
                reply = json.loads(resp.read())
            content = reply["choices"][0]["message"].get("content") or ""
            m = re.search(r"\{.*\}", content, re.S)
            if m:
                return json.loads(m.group(0))
            raise ValueError(f"no JSON in {len(content)} chars")
        except Exception as exc:
            last_error = f"attempt {attempt}: {exc}"
            log.warning("kimi JSON call failed (%s)", last_error)
            time.sleep(5 * attempt)
    raise RuntimeError(f"kimi call failed after 3 attempts — {last_error}")


def verify_pairs(cfg: dict, pairs: list[tuple[str, str]]) -> list[bool]:
    """Confirm specific (proposal, covered item) pairs are the same subject.

    The list scan over-matches: checked against 521 covered topics, "honeybee
    colony collapse" was flagged as a duplicate of "CRISPR gene-drive
    mosquitoes", "The Screwworm Returns" and a Lyme disease episode — insects
    and parasites, not the same subject — and one reply even wrote "different
    subject" inside the item it was calling a match. That bias is systematic,
    so voting could not remove it. Judging one named pair is a far easier
    question than scanning a long list, and it is where the precision comes
    from. A pair the model gives no answer for is not confirmed.
    """
    if not pairs:
        return []
    text = "\n".join(f"{i}. A: {a}\n   B: {b}" for i, (a, b) in enumerate(pairs, 1))
    data = _kimi_json(cfg, VERIFY_PROMPT.format(pairs=text), max_tokens=2000)
    by_id = {int(r["id"]): str(r.get("same", "")).upper() == "YES"
             for r in data.get("results", []) if str(r.get("id", "")).isdigit()}
    return [by_id.get(i, False) for i in range(1, len(pairs) + 1)]


def subject_check_voted(cfg: dict, proposals: list[str], taken: list[str],
                        votes: int = 3) -> list[dict]:
    """Scan for candidate matches with high recall, then verify each one.

    Stage 1 — scan: `votes` independent calls, proposals shuffled differently
    each time, against the full covered list. Any vote naming a match adds
    that (proposal, item) pair to the candidates. Taking the UNION rather than
    a majority is deliberate: stage 1's job is to miss nothing.

    Stage 2 — verify: one call judges every candidate pair on its own. A
    proposal is a duplicate only if at least one of its pairs is confirmed.

    Why both: tested 2026-09-23 on the seven ideas in
    tower-dedupe-instructions.md, a single scan got all seven right on some
    passes and wrongly matched "honeybee colony collapse" on others — to
    mosquitoes, screwworm and Lyme, i.e. anything insect-shaped. Majority
    voting did not fix it (one pass had all three votes wrong), because the
    bias is systematic rather than random. The pairwise check is where the
    precision comes from; the scan is where the recall comes from.

    Within-batch duplicates ("same as an earlier proposal in this list") need
    a fixed order, which shuffling destroys, so they get one more fixed-order
    scan over the survivors, verified the same way.
    """
    import random
    n = len(proposals)
    if not n:
        return []
    candidates = [dict() for _ in range(n)]        # item -> vote count
    ok_votes = 0
    for k in range(votes):
        order = list(range(n))
        random.Random(k).shuffle(order)
        try:
            res = subject_check(cfg, [proposals[i] for i in order], taken,
                                batch_rule=False)
        except Exception as exc:
            log.warning("subject scan %d failed (%s)", k + 1, exc)
            continue
        ok_votes += 1
        for pos, r in enumerate(res):
            if r["verdict"] == "MATCH" and r["item"].strip():
                c = candidates[order[pos]]
                c[r["item"]] = c.get(r["item"], 0) + 1
    if not ok_votes:
        raise RuntimeError("every subject scan failed")

    pairs = [(i, item) for i in range(n) for item in candidates[i]]
    confirmed = {}
    if pairs:
        for (i, item), same in zip(pairs, verify_pairs(cfg, [(proposals[i], it)
                                                            for i, it in pairs])):
            if same and i not in confirmed:
                confirmed[i] = item

    out = []
    for i, p in enumerate(proposals):
        flagged = len(candidates[i])
        out.append({"id": i + 1, "proposal": p,
                    "verdict": "MATCH" if i in confirmed else "NEW",
                    "item": confirmed.get(i, ""),
                    "votes": (f"{flagged} flagged, confirmed" if i in confirmed else
                              f"{flagged} flagged, rejected" if flagged else "0 flagged")})

    survivors = [o for o in out if o["verdict"] == "NEW"]
    if len(survivors) > 1:
        try:
            batch = subject_check(cfg, [o["proposal"] for o in survivors], [],
                                  batch_rule=True)
            bpairs = [(o, r["item"]) for o, r in zip(survivors, batch)
                      if r["verdict"] == "MATCH" and r["item"].strip()]
            for (o, item), same in zip(bpairs, verify_pairs(
                    cfg, [(o["proposal"], it) for o, it in bpairs])):
                if same:
                    o.update(verdict="MATCH", item=f"same batch: {item}")
        except Exception as exc:
            log.warning("within-batch check failed (%s); keeping survivors", exc)
    return out


def dedupe_mode(cfg: dict) -> str:
    """'on' drops matches for real; 'dry-run' (the default) proposes nothing.

    Starts in dry-run so the check can be proven against known duplicates
    before it is trusted to decide what reaches the topics doc.
    """
    return cfg["scout"].get("subject_dedupe", "dry-run")


def run(cfg: dict, dry: bool = False) -> str:
    headlines = fetch_headlines(cfg)
    # The master list covers both machines; past_topics() only knew the mini.
    past = list(dict.fromkeys(past_topics(cfg) + published_titles(cfg)))
    try:
        pending = gdoc_lines(cfg)
    except Exception as exc:
        log.warning("gdoc read failed (%s); proposing against queue.json only", exc)
        pending = []
    with QUEUE_LOCK:
        queue = load_queue(cfg)
    known_slugs = {c["slug"] for c in queue["candidates"]}
    # EVERY status, vetoed included. Passing only proposed/pushed is how a
    # vetoed idea walked straight back into the next night's proposals.
    queued = pending + [c["queue_line"] for c in queue["candidates"]]

    cands = propose(cfg, headlines, past, queued)
    fresh = [c for c in cands if c["slug"] not in known_slugs]
    # Count exact-slug repeats as drops too. They were silent before, so a dry
    # run that discarded five re-proposed queue slugs reported "0 duplicate(s)
    # dropped" — true of the subject stage, false of the run.
    dropped = [(c, {"verdict": "EXACT", "item": f"slug already in queue: {c['slug']}"})
               for c in cands if c["slug"] in known_slugs]

    mode = dedupe_mode(cfg)
    if mode != "off" and fresh:
        taken = taken_topics(cfg, queue, pending)
        try:
            verdicts = subject_check_voted(
                cfg, [f"{c['title']} — {c.get('angle', '')}".strip(" —")
                      for c in fresh], taken)
        except Exception as exc:
            # Fail closed: a night with no proposals costs nothing; a night of
            # duplicates is the bug this exists to fix.
            log.error("subject check unavailable (%s); proposing nothing", exc)
            if not (dry or mode == "dry-run"):
                tower.telegram(cfg, "🔭 Topic Scout skipped tonight — could not "
                                    f"check proposals for duplicates ({str(exc)[:120]}).")
            return f"subject check failed; proposed nothing ({exc})"
        for c, v in zip(fresh, verdicts):
            log.info("dedupe %s: %s%s", c["slug"], v["verdict"],
                     f" (matches {v['item']!r})" if v["item"] else "")
        dropped += [(c, v) for c, v in zip(fresh, verdicts) if v["verdict"] != "NEW"]
        fresh = [c for c, v in zip(fresh, verdicts) if v["verdict"] == "NEW"]

    if dry or mode == "dry-run":
        print(json.dumps(fresh, indent=2, ensure_ascii=False))
        for c, v in dropped:
            print(f"DROPPED {c['slug']}: {v['verdict']} — {v['item']}")
        return (f"dry run ({mode}): {len(fresh)} new, {len(dropped)} duplicate(s) "
                "dropped, nothing written")

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
        due = []
        for c in queue["candidates"]:
            if c["status"] != "proposed":
                continue
            try:
                ts = datetime.fromisoformat(c["proposed_at"])
            except (KeyError, TypeError, ValueError):
                continue
            if ts.tzinfo is None:  # tolerate naive timestamps
                ts = ts.replace(tzinfo=now.tzinfo)
            if ts + timedelta(hours=hours) < now:
                due.append(c["slug"])
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
    g.add_argument("--check", nargs="+", metavar="IDEA",
                   help="subject-check ideas against everything taken; writes nothing")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [scout] %(levelname)s: %(message)s")
    cfg = tower.load_config()

    if args.check:
        # Test harness: the same taken-set and model call the nightly run uses,
        # applied to ideas you name. Slugs are fine; hyphens read as spaces.
        try:
            pending = gdoc_lines(cfg)
        except Exception as exc:
            log.warning("gdoc read failed (%s)", exc)
            pending = []
        taken = taken_topics(cfg, load_queue(cfg), pending)
        ideas = [i.replace("-", " ") for i in args.check]
        print(f"checking {len(ideas)} idea(s) against {len(taken)} taken topics")
        for v in subject_check_voted(cfg, ideas, taken):
            tail = f"  <- {v['item']}" if v["item"] else ""
            print(f"  {v['verdict']:9} {args.check[v['id'] - 1]:34} "
                  f"[{v['votes']}]{tail}")
        return 0
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
