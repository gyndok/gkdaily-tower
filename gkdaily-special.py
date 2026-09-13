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

Exit codes: 0 verified (or durably queued with --detach), 1 failed, 2 busy,
3 no topic, 4 awaiting feed verification.
Progress goes to Telegram at each milestone so a phone shows the same story.
"""

import argparse
import fcntl
import json
import os
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

TOWER = Path(__file__).resolve().parent
CLAWD = Path.home() / "clawd"
PODCASTS = Path.home() / "podcasts"
LOCK = CLAWD / ".gkdaily-special.lock"
LOG = Path.home() / "Library" / "Logs" / "gkdaily-special.log"

sys.path.insert(0, str(TOWER))
import jobs
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
    # Only write the log file when stdout is NOT already pointed at it — the
    # dispatch/MiniBot launchers redirect into the same path, which otherwise
    # records every line twice.
    try:
        same = LOG.exists() and os.fstat(1).st_ino == os.stat(LOG).st_ino
    except Exception:
        same = False
    if not same:
        try:
            with open(LOG, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass
    if telegram and not QUIET:
        notify(msg)


def run(cmd: list, timeout: int) -> subprocess.CompletedProcess:
    sys.path.insert(0, str(PODCASTS / "scripts"))
    from runtime import run_managed
    return run_managed([str(c) for c in cmd], capture_output=True,
                          text=True, timeout=timeout)


# ------------------------------------------------------------------ stages --

def resolve_topic(cfg: dict, explicit: str | None) -> dict:
    """An explicit topic wins; otherwise the top uncovered line in the doc."""
    if explicit is not None:
        line = " ".join(explicit.split())
        if not line:
            raise RuntimeError("topic must not be empty")
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
                "--script", script_path], timeout=7200)
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-3:]
        raise RuntimeError("producer failed: " + " | ".join(tail))


def produced_mp3(script_path: Path) -> str:
    """Use the producer's filename contract, never another run's newest file."""
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})_(.+)", script_path.stem)
    if not match:
        raise RuntimeError(f"invalid script filename: {script_path.name}")
    name = f"special-edition-{match[2].lower()}-{match[1]}.mp3"
    path = PODCASTS / "public" / "episodes" / name
    ledger = PODCASTS / "config/spotify_uploaded.json"
    recorded = name in json.loads(ledger.read_text()) if ledger.exists() else False
    if not recorded and (not path.is_file() or path.stat().st_size == 0):
        raise RuntimeError(f"expected audio missing or empty: {name}")
    return name


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
        sys.path.insert(0, str(PODCASTS / "scripts"))
        from runtime import uploader_python
        run([uploader_python(PODCASTS), PODCASTS / "scripts/upload_spotify.py"],
            timeout=1800)
        time.sleep(5)
    try:
        if mp3_name in json.loads(ledger.read_text()):
            return
    except Exception:
        pass
    raise RuntimeError(f"{mp3_name} never reached the Spotify ledger")


LAST_VERIFIED_URL = ""


def verify_live(cfg: dict, title: str, minutes: int = 12) -> bool:
    """Spotify ingests asynchronously; poll the public feed for the title."""
    global LAST_VERIFIED_URL
    norm = lambda s: re.sub(r"[^a-z0-9]", "", s.lower())
    want = norm(title)
    deadline = time.time() + minutes * 60
    while time.time() < deadline:
        try:
            req = urllib.request.Request(cfg["spotify_rss"],
                                         headers={"User-Agent": "gkdaily-special/1.0"})
            root = ET.fromstring(urllib.request.urlopen(req, timeout=25).read())
            item = next((item for item in root.findall("./channel/item")
                         if want and want == norm(item.findtext("title") or "")), None)
            if item is not None:
                LAST_VERIFIED_URL = item.findtext("link") or ""
                return True
        except Exception:
            pass
        time.sleep(45)
    return False


# -------------------------------------------------------------------- main --

def status(cfg: dict) -> int:
    scripts = cfg["drive_gk_daily"] / "scripts"
    pending = [p.name for p in scripts.glob("*.md")] if scripts.is_dir() else []
    ledger_path = PODCASTS / "config/spotify_uploaded.json"
    led = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
    today = datetime.now(ZoneInfo(cfg["timezone"])).strftime("%Y-%m-%d")
    todays = [n for n, ts in led.items() if ts.startswith(today)]
    running = LOCK.exists() and _locked()
    with jobs.connect() as conn:
        for job in conn.execute("SELECT id,status,attempts FROM jobs WHERE status != 'done' ORDER BY created"):
            print(f"job {job['id'][:8]}: {job['status']} (attempts {job['attempts']})")
    print(f"in flight        : {'yes' if running else 'no'}")
    print(f"scripts waiting  : {', '.join(pending) or 'none'}")
    print(f"uploads recorded : {', '.join(todays) or 'none'} (not delivery proof)")
    delivery_path = PODCASTS / "config/delivery.json"
    records = json.loads(delivery_path.read_text()) if delivery_path.exists() else {}
    verified = [name for name in todays if records.get(name, {}).get('state') == 'verified_live']
    print(f"verified live    : {', '.join(verified) or 'none recorded here'}")
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
    g.add_argument("--job-id", help=argparse.SUPPRESS)
    ap.add_argument("--quiet", action="store_true", help="no Telegram messages")
    ap.add_argument("--detach", action="store_true",
                    help="run in a new session, surviving the caller's exit")
    args = ap.parse_args()

    QUIET = args.quiet or os.getenv("GK_QUIET") == "1"
    cfg = tower.load_config()
    saved = {}
    if args.job_id:
        job = jobs.get(args.job_id)
        request = json.loads(job['request'])
        args.topic = request['topic']
        QUIET = request['quiet'] or os.getenv("GK_QUIET") == "1"
        saved = json.loads(job['checkpoint'])
    if not args.job_id and not args.status:
        ident = jobs.enqueue(args.topic, args.quiet)
        print(f"Queued GK Daily job {ident}; Tower will process it and report progress.", flush=True)
        jobs.maybe_start(cfg)
        return 0

    if args.status:
        return status(cfg)

    LOCK.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(LOCK, "a")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        say("🎙️ A GK Daily special is already being produced — "
            "letting that one finish.")
        return 2

    started = datetime.now(ZoneInfo(cfg["timezone"]))
    try:
        topic = saved.get("topic") or resolve_topic(cfg, args.topic)
        if args.job_id:
            jobs.checkpoint(args.job_id, topic=topic)
    except (SystemExit, RuntimeError) as exc:
        say(f"🎙️ Can't start: {exc}")
        return 3
    say(f"🎙️ GK Daily special starting — {topic['line'][:110]}\n"
        f"(source: {topic['source']}) Researching and writing now; "
        "about 12–15 minutes until it is live on Spotify.")

    try:
        if args.job_id:
            jobs.checkpoint(args.job_id, stage="Researching and writing", phase="research")
        if args.job_id:
            # Archive before exposing to Drive. A retry uses this exact file,
            # including its original date, even if the producer moved its copy.
            archive = TOWER / 'job-scripts' / args.job_id
            archive.mkdir(parents=True, exist_ok=True)
            if not saved.get('script'):
                script_path = archive / f"{started:%Y-%m-%d}_{topic['slug']}.md"
                jobs.checkpoint(args.job_id, script=str(script_path))
            else:
                script_path = Path(saved['script'])
            if not script_path.exists():
                script = factory.generate(cfg, topic)
                tmp = script_path.with_suffix('.tmp')
                tmp.write_text(script)
                tmp.replace(script_path)
        else:
            script_path = write_script(cfg, topic)
        words = len(script_path.read_text().split())
        say(f"✍️ Script written ({words} words). Rendering audio…")

        if args.job_id:
            jobs.checkpoint(args.job_id, stage="Making audio and preparing upload", phase="audio", words=words)
        render_start = time.time()
        if args.job_id:
            # Producer archives/moves its input, so retain our canonical copy.
            import shutil
            try:
                mp3 = produced_mp3(script_path)
            except RuntimeError:
                produce(script_path)
            mp3 = produced_mp3(script_path)
        else:
            produce(script_path)
            mp3 = produced_mp3(script_path)
        meta_path = PODCASTS / "public/episodes/special_editions.json"
        title = topic["line"][:60]
        try:
            title = json.loads(meta_path.read_text())[mp3]["title"]
        except Exception:
            pass
        if args.job_id:
            jobs.checkpoint(args.job_id, episode=mp3, title=title)
        say(f"🎧 Rendered: {title}")

        if args.job_id:
            jobs.checkpoint(args.job_id, stage="Uploading to Spotify", phase="upload")
        ensure_uploaded(mp3)
        say("⬆️ Uploaded to Spotify — waiting for it to appear in the feed…",
            telegram=False)

        if args.job_id:
            jobs.checkpoint(args.job_id, stage="Waiting for Spotify confirmation", phase="verify")
        if verify_live(cfg, title):
            if args.job_id:
                jobs.checkpoint(args.job_id, phase="live", listen_url=LAST_VERIFIED_URL)
            mins = (datetime.now(ZoneInfo(cfg["timezone"])) - started).seconds // 60
            say(f"✅ LIVE on Spotify ({mins} min): {title}\n"
                "https://open.spotify.com/show/0344TpzH4nfACvR7amNX7V")
            return 0
        say(f"⚠️ {title} uploaded but not visible in the feed yet. "
            "Spotify is still ingesting; it normally appears within the hour "
            "and the Tower is watching it.")
        return 4  # awaiting verification is not verified success
    except Exception as exc:
        say(f"❌ GK Daily special FAILED: {str(exc)[:300]}\n"
            f"Topic: {topic['line'][:80]}")
        return 1
    finally:
        fcntl.flock(lock_file, fcntl.LOCK_UN)


if __name__ == "__main__":
    sys.exit(main())
