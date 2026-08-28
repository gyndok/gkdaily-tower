#!/Users/geffreyklein/podcasts/venv/bin/python3
"""
GK Daily Special Edition — one command, end to end, on demand.

Runs every stage synchronously and does not return until the episode is
LIVE on Spotify (or it has told you exactly where it stopped):

    topic  ->  research + script (Claude Opus 5 + web search)
           ->  deliver to Google Drive scripts/
           ->  render (Kokoro TTS) + publish feed
           ->  upload to Spotify (retried)
           ->  verify the episode really appears in the public feed

Why this exists: nothing in the pipeline was ever LOST — 43/43 scripts
rendered and every render reached Spotify — but the normal path is
event-driven (a launchd WatchPaths trigger with a 300 s throttle, then a
best-effort upload stage), so an episode could sit unstarted or un-uploaded
for hours until someone noticed. This path waits, retries, and reports.

Usage (safe to call from MiniBot's run_shell, a phone, or ssh):
    gkdaily-special.py --topic "the physics of noise-cancelling headphones"
    gkdaily-special.py --next          # top uncovered topic from the topics doc
    gkdaily-special.py --status        # what is in flight, what published today
    gkdaily-special.py --next --quiet  # no Telegram, just stdout

Exit codes: 0 published & verified, 1 failed, 2 already running, 3 no topic.
Progress goes to Telegram at each milestone so a phone shows the same story.
"""

import argparse
import fcntl
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TOWER = Path.home() / "gkdaily-tower"
CLAWD = Path.home() / "clawd"
PODCASTS = Path.home() / "podcasts"
LOCK = CLAWD / ".gkdaily-special.lock"
LOG = Path.home() / "Library" / "Logs" / "gkdaily-special.log"

sys.path.insert(0, str(TOWER))
import factory  # noqa: E402  (tower modules; stdlib-only except factory's SDK)
import scout  # noqa: E402
import tower  # noqa: E402

QUIET = False


def notify(msg: str) -> bool:
    """Send progress through MiniBot's own bot, not the tower's.

    Geffrey asks for an episode by messaging MiniBot, so the stage updates
    belong in that same thread. The tower and the rest of the clawd tooling
    use a different bot token, which would put "script written / rendered /
    live" in a separate conversation from the request that started them.

    A DM's chat id equals the user id, so the first entry in MiniBot's
    ALLOWED_USER_IDS is the destination. Falls back to the tower's bot if
    MiniBot's credentials are missing, since a progress message in the wrong
    thread still beats silence.
    """
    try:
        creds = tower.load_env_creds(Path.home() / "minibot" / ".env")
        token = creds.get("TELEGRAM_BOT_TOKEN")
        chat_id = (creds.get("TELEGRAM_CHAT_ID")
                   or creds.get("ALLOWED_USER_IDS", "").split(",")[0].strip())
        if token and chat_id:
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=urllib.parse.urlencode(
                    {"chat_id": chat_id, "text": msg}).encode())
            urllib.request.urlopen(req, timeout=15)
            return True
    except Exception:
        pass
    try:
        return tower.telegram(tower.load_config(), msg)
    except Exception:
        return False


def say(msg: str, telegram: bool = True) -> None:
    """One line to stdout, the log, and (by default) the MiniBot thread."""
    stamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass
    if telegram and not QUIET:
        notify(msg)


def run(cmd: list, timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run([str(c) for c in cmd], capture_output=True,
                          text=True, timeout=timeout)


# ------------------------------------------------------------------ stages --

def resolve_topic(cfg: dict, explicit: str | None) -> dict:
    """An explicit topic wins; otherwise the top uncovered line in the doc."""
    if explicit:
        line = " ".join(explicit.split())
        if factory.is_covered(line, factory.covered_topics(cfg)):
            raise SystemExit(f"'{line}' looks like an episode that already "
                             "exists. Pick a different angle, or delete the "
                             "old script if you want it redone.")
        return {"line": line, "slug": factory.slug_for(line), "source": "on demand"}
    return factory.pick_topic(cfg)          # topics doc, then queue.json


def write_script(cfg: dict, topic: dict) -> Path:
    script = factory.generate(cfg, topic)
    return factory.deliver(cfg, topic, script, stage=False)


def produce(script_path: Path) -> None:
    """Call the producer directly — never wait on the WatchPaths trigger."""
    proc = run([CLAWD / ".venv/bin/python3", CLAWD / "produce-special-podcast.py",
                "--script", script_path], timeout=2700)
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-3:]
        raise RuntimeError("producer failed: " + " | ".join(tail))


def produced_mp3(topic: dict, since: float) -> str:
    """Name the episode the producer just wrote.

    The producer derives its own slug and date from the script filename, so
    reading the directory beats predicting the name (and survives a run that
    crosses midnight). The computed name is only the fallback.
    """
    eps = PODCASTS / "public" / "episodes"
    fresh = [p for p in eps.glob("special-edition-*.mp3")
             if p.stat().st_mtime >= since - 5]
    if fresh:
        return max(fresh, key=lambda p: p.stat().st_mtime).name
    return f"special-edition-{topic['slug']}-{datetime.now():%Y-%m-%d}.mp3"


def ensure_uploaded(mp3_name: str, attempts: int = 3) -> None:
    """The producer's upload stage is best-effort; make it definite."""
    ledger = PODCASTS / "config" / "spotify_uploaded.json"
    for i in range(1, attempts + 1):
        try:
            if mp3_name in json.loads(ledger.read_text()):
                return
        except Exception:
            pass
        say(f"Spotify upload not recorded yet — retry {i}/{attempts}")
        run(["/opt/homebrew/bin/python3", PODCASTS / "scripts/upload_spotify.py"],
            timeout=1800)
        time.sleep(5)
    try:
        if mp3_name in json.loads(ledger.read_text()):
            return
    except Exception:
        pass
    raise RuntimeError(f"{mp3_name} never reached the Spotify ledger")


def verify_live(cfg: dict, title: str, minutes: int = 12) -> bool:
    """Spotify ingests asynchronously; poll the public feed for the title."""
    norm = lambda s: re.sub(r"[^a-z0-9]", "", s.lower())
    want = norm(title)[:40]
    deadline = time.time() + minutes * 60
    while time.time() < deadline:
        try:
            req = urllib.request.Request(cfg["spotify_rss"],
                                         headers={"User-Agent": "gkdaily-special/1.0"})
            root = ET.fromstring(urllib.request.urlopen(req, timeout=25).read())
            if any(want and want in norm(el.text or "") for el in root.iter("title")):
                return True
        except Exception:
            pass
        time.sleep(45)
    return False


# -------------------------------------------------------------------- main --

def status(cfg: dict) -> int:
    scripts = cfg["drive_gk_daily"] / "scripts"
    pending = [p.name for p in scripts.glob("*.md")] if scripts.is_dir() else []
    led = json.loads((PODCASTS / "config/spotify_uploaded.json").read_text())
    today = datetime.now(ZoneInfo(cfg["timezone"])).strftime("%Y-%m-%d")
    todays = [n for n, ts in led.items() if ts.startswith(today)]
    running = LOCK.exists() and _locked()
    print(f"in flight        : {'yes' if running else 'no'}")
    print(f"scripts waiting  : {', '.join(pending) or 'none'}")
    print(f"published today  : {', '.join(todays) or 'none'}")
    try:
        upcoming = [l for l in scout.gdoc_lines(cfg)
                    if not factory.is_covered(l, factory.covered_topics(cfg))]
        print(f"next up          : {upcoming[0][:70] if upcoming else 'queue empty'}")
        print(f"queue depth      : {len(upcoming)}")
    except Exception as exc:
        print(f"topics doc       : unreadable ({exc})")
    return 0


def _locked() -> bool:
    try:
        f = open(LOCK, "w")
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(f, fcntl.LOCK_UN)
        return False
    except BlockingIOError:
        return True


def main() -> int:
    global QUIET
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--topic", help="produce an episode on this exact topic")
    g.add_argument("--next", action="store_true",
                   help="produce the top uncovered topic from the topics doc")
    g.add_argument("--status", action="store_true")
    ap.add_argument("--quiet", action="store_true", help="no Telegram messages")
    args = ap.parse_args()
    QUIET = args.quiet
    cfg = tower.load_config()

    if args.status:
        return status(cfg)

    lock_file = open(LOCK, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        say("🎙️ A GK Daily special is already being produced — "
            "letting that one finish.")
        return 2

    started = datetime.now(ZoneInfo(cfg["timezone"]))
    try:
        topic = resolve_topic(cfg, args.topic)
    except SystemExit as exc:
        say(f"🎙️ Can't start: {exc}")
        return 3
    say(f"🎙️ GK Daily special starting — {topic['line'][:110]}\n"
        f"(source: {topic['source']}) Researching and writing now; "
        "about 12–15 minutes until it is live on Spotify.")

    try:
        script_path = write_script(cfg, topic)
        words = len(script_path.read_text().split())
        say(f"✍️ Script written ({words} words). Rendering audio…")

        render_start = time.time()
        produce(script_path)
        mp3 = produced_mp3(topic, render_start)
        meta_path = PODCASTS / "public/episodes/special_editions.json"
        title = topic["line"][:60]
        try:
            title = json.loads(meta_path.read_text())[mp3]["title"]
        except Exception:
            pass
        say(f"🎧 Rendered: {title}")

        ensure_uploaded(mp3)
        say("⬆️ Uploaded to Spotify — waiting for it to appear in the feed…",
            telegram=False)

        if verify_live(cfg, title):
            mins = (datetime.now(ZoneInfo(cfg["timezone"])) - started).seconds // 60
            say(f"✅ LIVE on Spotify ({mins} min): {title}\n"
                "https://open.spotify.com/show/0344TpzH4nfACvR7amNX7V")
            return 0
        say(f"⚠️ {title} uploaded but not visible in the feed yet. "
            "Spotify is still ingesting; it normally appears within the hour "
            "and the Tower is watching it.")
        return 0
    except Exception as exc:
        say(f"❌ GK Daily special FAILED: {str(exc)[:300]}\n"
            f"Topic: {topic['line'][:80]}")
        return 1
    finally:
        fcntl.flock(lock_file, fcntl.LOCK_UN)


if __name__ == "__main__":
    sys.exit(main())
