#!/usr/bin/env python3
"""
GK Daily Control Tower — P4 Script Factory (failover-only).

If no special-edition script has landed in Drive by factory.failover_at on a
weekday, the tower runs this to write one ON the mini: pick the top uncovered
topic (same priority order as the cloud-side skill: topics gdoc, then
queue.json candidates), research and write it with Claude Opus 5 + web search,
and drop the finished YYYY-MM-DD_slug.md into GK Daily/scripts/ — where the
existing WatchPaths producer takes over untouched. The cloud-side scheduled
task remains the preferred author; this is the guarantee behind it.

The output honors the same contract the producer parses: "# Title" first line
(no "GK Daily Special Edition:" prefix — the producer adds it), spoken prose
with [pause] between sections, no headers/bullets in the body, and a trailing
"--- SOURCES ---" section that is never read aloud.

Runs under the podcasts venv (needs the anthropic SDK; ANTHROPIC_API_KEY comes
from ~/podcasts/.env). Imports tower/scout, which are stdlib-only, so any
Python ≥3.10 works.

Usage:
    factory.py --plan            # pick and print the topic; no API call
    factory.py --run             # full: research + write + deliver to Drive
    factory.py --run --stage     # full generation, but write to staging/
                                 #   (test mode: nothing reaches the pipeline)
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
import scout
import tower

log = logging.getLogger("tower.factory")

STOPWORDS = {"the", "a", "an", "and", "or", "of", "in", "on", "for", "to",
             "how", "why", "what", "its", "is", "are", "vs", "with"}


# --------------------------------------------------------------- topic pick --

def _words(text: str) -> set:
    return {w for w in re.findall(r"[a-z0-9]+", text.lower())
            if w not in STOPWORDS and len(w) > 2}


def covered_topics(cfg: dict) -> list[str]:
    """Everything already produced or already sitting in scripts/ today."""
    covered = []
    scripts = cfg["drive_gk_daily"] / "scripts"
    for d in (scripts, scripts / "processed"):
        if d.is_dir():
            covered += [re.sub(r"^\d{4}-\d{2}-\d{2}_", "", p.stem)
                        for p in d.glob("*.md")]
    eps = cfg["podcasts_root"] / "public" / "episodes"
    covered += [re.sub(r"-\d{4}-\d{2}-\d{2}$", "",
                       p.stem[len("special-edition-"):])
                for p in eps.glob("special-edition-*.mp3")]
    covered += ["desalination", "colorado-river-crisis"]  # pre-system era
    return covered


def is_covered(line: str, covered: list[str]) -> bool:
    lw = _words(line)
    if not lw:
        return True
    for slug in covered:
        sw = _words(slug.replace("-", " "))
        if sw and len(lw & sw) / len(sw) >= 0.6:
            return True
    return False


def slug_for(line: str) -> str:
    """The gdoc convention is 'slug — angle'; fall back to kebabing the head."""
    head = re.split(r"\s+—\s+|\s+-\s+", line, maxsplit=1)[0].strip()
    slug = re.sub(r"[^a-z0-9]+", "-", head.lower()).strip("-")
    return "-".join(slug.split("-")[:6]) or "untitled"


def pick_topic(cfg: dict) -> dict:
    """Top uncovered topic: gdoc queue first, then queue.json candidates."""
    covered = covered_topics(cfg)
    try:
        for line in scout.gdoc_lines(cfg):
            if not is_covered(line, covered):
                return {"line": line, "slug": slug_for(line), "source": "topics doc"}
    except Exception as exc:
        log.warning("gdoc read failed (%s); falling back to queue.json", exc)
    queue = scout.load_queue(cfg)["candidates"]
    for status in ("pushed", "approved", "proposed"):
        for c in queue:
            if c["status"] == status and not is_covered(c["queue_line"], covered):
                return {"line": c["queue_line"], "slug": c["slug"],
                        "source": f"queue.json ({status})"}
    raise RuntimeError("no uncovered topic available in the topics doc or queue.json")


# --------------------------------------------------------------- generation --

SYSTEM = """You research and write scripts for "GK Daily Special Edition", a \
daily deep-dive podcast. The host is an OBGYN physician; the audience is \
curious generalists. Voice: conversational but authoritative — explaining the \
topic to a smart friend over coffee. Use "you" and "we" naturally, vary \
sentence length, no jargon without explanation.

Research first: run 6-8 web searches covering different angles (current state, \
how it works, economics, policy/geopolitics, US developments, controversies, \
future outlook). Prioritize authoritative sources. Build the script on \
concrete facts — specific numbers, names, dates — not generalities.

Then produce the COMPLETE script file. Structure:
- First line: `# Title` — a compelling podcast title with an em-dash subtitle. \
Do NOT include a "GK Daily Special Edition:" prefix (automation adds it). \
This is the ONLY markdown header in the file.
- Cold open (2-3 sentences): a hook that makes the listener care.
- Intro: "Welcome to GK Daily Special Edition. I'm your host, and today we're \
diving deep into [topic]." plus a brief roadmap.
- 4-7 body sections, each opening with a transition, presenting 3-5 concrete \
facts, including a "here's what's interesting" moment, closing with a bridge.
- Future outlook, then an outro like "That's our deep dive into [topic]. If \
you found this valuable, share it with someone who'd want to hear it. Until \
next time, stay curious."

Formatting rules (the file is spoken by TTS):
- Insert [pause] on its own line between major sections.
- Numbers as words under 10, digits for 10 and above.
- Spell out abbreviations on first use ("reverse osmosis, or RO").
- No markdown headers in the body, no bullet points, no parenthetical asides — \
everything flows as natural speech.
- Target ~{words} words of spoken body (the renderer plays at 1.2x; that is \
about 15 minutes of listening).
- End the file with a `--- SOURCES ---` line followed by the key URLs used \
(never spoken).

Your FINAL message must contain ONLY the complete file content, starting with \
the `# Title` line — no commentary before or after."""


def generate(cfg: dict, topic: dict) -> str:
    import anthropic  # only available in the podcasts venv

    creds = tower.load_env_creds(Path(cfg["factory"]["podcasts_env"]).expanduser())
    api_key = creds.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not found")
    client = anthropic.Anthropic(api_key=api_key, timeout=900.0)

    now = datetime.now(ZoneInfo(cfg["timezone"]))
    user_prompt = (
        f"Today is {now:%A, %B %d, %Y}. Research and write today's GK Daily "
        f"Special Edition script.\n\nTopic (from the {topic['source']}): "
        f"{topic['line']}\n\nAlready-covered topics (do not drift into these): "
        f"{', '.join(covered_topics(cfg)[:40])}")

    tools = [{"type": "web_search_20260209", "name": "web_search", "max_uses": 10}]
    messages = [{"role": "user", "content": user_prompt}]

    response = None
    for round_no in range(8):  # server tool loop can pause; resume until done
        with client.messages.stream(
            model=cfg["factory"]["model"],
            max_tokens=32000,
            system=SYSTEM.replace("{words}", str(cfg["factory"]["target_words"])),
            tools=tools,
            messages=messages,
        ) as stream:
            response = stream.get_final_message()
        log.info("round %d: stop_reason=%s, output_tokens=%s", round_no,
                 response.stop_reason, response.usage.output_tokens)
        if response.stop_reason == "refusal":
            raise RuntimeError("model declined the request (stop_reason=refusal)")
        if response.stop_reason != "pause_turn":
            break
        messages = [{"role": "user", "content": user_prompt},
                    {"role": "assistant", "content": response.content}]
    else:
        raise RuntimeError("generation did not finish within 8 pause_turn rounds")

    text = "\n".join(b.text for b in response.content if b.type == "text")
    m = re.search(r"^# .+$", text, re.M)
    if not m:
        raise RuntimeError(f"no '# Title' line in model output: {text[:200]!r}")
    script = text[m.start():].strip() + "\n"

    body = re.split(r"^---\s*SOURCES\s*---", script, flags=re.M)[0]
    word_count = len(body.split())
    if word_count < 2000:
        raise RuntimeError(f"script too short ({word_count} words)")
    if "--- SOURCES ---" not in script:
        script += "\n--- SOURCES ---\n(sources unavailable)\n"
    if "[pause]" not in script:
        log.warning("script has no [pause] markers")
    log.info("script generated: %d words", word_count)
    return script


# ----------------------------------------------------------------- deliver --

def deliver(cfg: dict, topic: dict, script: str, stage: bool) -> Path:
    now = datetime.now(ZoneInfo(cfg["timezone"]))
    name = f"{now:%Y-%m-%d}_{topic['slug']}.md"
    if stage:
        out_dir = Path(cfg["factory"]["staging_dir"]).expanduser()
    else:
        out_dir = cfg["drive_gk_daily"] / "scripts"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / name
    if out.exists():
        raise RuntimeError(f"{out} already exists — refusing to overwrite")
    tmp = out.with_suffix(".md.tmp")
    tmp.write_text(script)
    tmp.replace(out)  # atomic, so the WatchPaths producer never sees a partial
    log.info("delivered %s (%s)", out, "staged" if stage else "live")
    return out


def run(cfg: dict, stage: bool = False) -> str:
    topic = pick_topic(cfg)
    log.info("topic [%s]: %s", topic["source"], topic["line"])
    if not stage:
        tower.telegram(cfg,
            "🏭 GK Daily Script Factory engaged — no script arrived by "
            f"{cfg['factory']['failover_at']}, generating one on the mini.\n"
            f"Topic ({topic['source']}): {topic['line']}")
    script = generate(cfg, topic)
    out = deliver(cfg, topic, script, stage)
    words = len(script.split())
    if not stage:
        tower.telegram(cfg,
            f"🏭 Script Factory wrote {out.name} ({words} words) to Drive — "
            "the producer will pick it up and publish automatically.")
    return f"{out} ({words} words)"


# ------------------------------------------------------ tower tick entrypoint --

def maybe_failover(cfg: dict, conn, now: datetime, data: dict) -> None:
    """Called from tower.tick(): fire once if no script arrived by deadline."""
    if now.weekday() >= 5:
        return
    if now.strftime("%H:%M") < cfg["factory"]["failover_at"]:
        return
    sp = data.get("special", {})
    if "error" in sp or sp.get("arrived_today"):
        return
    if not data.get("drive", {}).get("scripts_exists"):
        return  # Drive is down — the watchtower alert covers this, don't pile on
    today = f"{now:%Y-%m-%d}"
    conn.execute("CREATE TABLE IF NOT EXISTS factory_runs "
                 "(date TEXT PRIMARY KEY, ts TEXT, result TEXT)")
    if conn.execute("SELECT 1 FROM factory_runs WHERE date=?", (today,)).fetchone():
        return
    conn.execute("INSERT INTO factory_runs VALUES (?,?,?)",
                 (today, now.isoformat(timespec="seconds"), "running"))
    conn.commit()

    def go():
        import sqlite3
        import subprocess
        try:
            proc = subprocess.run(
                [str(Path(cfg["factory"]["python"]).expanduser()),
                 str(Path(__file__).resolve()), "--run"],
                capture_output=True, text=True, timeout=1800)
            result = ("ok: " + proc.stdout.strip()[-200:]) if proc.returncode == 0 \
                else f"failed rc={proc.returncode}: {proc.stderr.strip()[-300:]}"
        except Exception as exc:
            result = f"failed: {exc}"
        if not result.startswith("ok"):
            log.error("factory failover failed: %s", result)
            tower.telegram(cfg, f"🏭 Script Factory FAILED: {result[:300]}\n"
                                "No special edition today unless a script "
                                "arrives some other way.")
        c = sqlite3.connect(tower.DB_PATH)
        c.execute("UPDATE factory_runs SET result=? WHERE date=?", (result, today))
        c.commit()
        c.close()
        log.info("factory failover: %s", result)

    import threading
    threading.Thread(target=go, daemon=True).start()


# -------------------------------------------------------------------- main --

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--plan", action="store_true")
    g.add_argument("--run", action="store_true")
    ap.add_argument("--stage", action="store_true",
                    help="with --run: write to staging/, not Drive; no Telegram")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [factory] %(levelname)s: %(message)s")
    cfg = tower.load_config()

    if args.plan:
        topic = pick_topic(cfg)
        print(json.dumps(topic, indent=2, ensure_ascii=False))
        return 0
    print(run(cfg, stage=args.stage))
    return 0


if __name__ == "__main__":
    sys.exit(main())
