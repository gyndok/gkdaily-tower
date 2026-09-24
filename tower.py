#!/opt/homebrew/bin/python3
"""
GK Daily Control Tower — P1 Watchtower.

Supervises the two GK Daily podcast pipelines from the outside: it reads the
logs, ledgers, launchd state, Google Drive drop folder, and the public Spotify
RSS that the pipelines already produce — it never touches the pipelines
themselves. Its one job is to turn silence into signal:

  - absence detection: alerts when an expected event (briefing produced,
    uploaded, special-edition script arrived) has NOT happened by its deadline,
    which the pipelines' own failure-only alerts can never do
  - live verification: an upload only counts once the episode's title actually
    appears in the Spotify (anchor.fm) feed
  - a 07:00 green digest, so a quiet morning is a confirmed-good morning and a
    missing digest means the tower itself is down
  - a tailnet-only status page (nginx /tower/ on :8888 proxies to :8891)

Deliberately stdlib-only (Python 3.14): no venv to rot, nothing to pip install.
State lives in state.db (SQLite) next to this file; alerts are deduped one per
rule per day, with a "resolved" follow-up when a red condition clears.

Usage:
    tower.py --serve        # daemon: scheduler + HTTP (what launchd runs)
    tower.py --check        # one collection pass, print JSON, send nothing
    tower.py --once         # one full tick WITH alerts/digest, then exit
"""

import argparse
from reliability import atomic_json, exclusive_lock
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from http.server import ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "state.db"
USER_AGENT = "gkdaily-tower/1.0"

log = logging.getLogger("tower")


# ------------------------------------------------------------------ config --

def upload_python(cfg):
    path = cfg["podcasts_root"] / ".venv-upload/bin/python3"
    return str(path) if path.exists() else "/opt/homebrew/bin/python3"


def load_config() -> dict:
    cfg = json.loads((BASE_DIR / "config.json").read_text())
    for key in ("podcasts_root", "producer_log", "drive_gk_daily", "clawd_env",
                "topic_queue_json"):
        cfg[key] = Path(os.path.expanduser(cfg[key]))
    return cfg


def load_env_creds(env_path: Path) -> dict:
    creds = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                creds[k.strip()] = v.strip().strip('"').strip("'")
    return creds


# ---------------------------------------------------------------- database --

def db(path: str | Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE IF NOT EXISTS alerts (
        rule_key TEXT PRIMARY KEY,       -- "<date>:<rule_id>"
        severity TEXT, message TEXT,
        sent_at TEXT, resolved_at TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS digests (
        date TEXT PRIMARY KEY, sent_at TEXT, message TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS history (
        ts TEXT, status_json TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS actions (
        id INTEGER PRIMARY KEY, ts TEXT, name TEXT, arg TEXT,
        status TEXT, output TEXT)""")
    conn.commit()
    return conn


# ---------------------------------------------------------------- telegram --

def telegram(cfg: dict, message: str) -> bool:
    """Send a Telegram message via the MiniBot creds. Never raises."""
    try:
        creds = load_env_creds(cfg["clawd_env"])
        bot_env = Path(cfg.get("telegram_env", "~/minibot/.env")).expanduser()
        mini = load_env_creds(bot_env)
        token = mini.get("TELEGRAM_BOT_TOKEN") or creds.get("TELEGRAM_BOT_TOKEN")
        chat_id = creds.get("TELEGRAM_CHAT_ID")
        if mini.get("TELEGRAM_BOT_TOKEN"):
            owners = [x.strip() for x in mini.get("ALLOWED_USER_IDS", "").split(",") if x.strip()]
            if len(owners) == 1:
                chat_id = owners[0]
            elif chat_id not in owners:
                log.warning("MiniBot recipient is ambiguous; Telegram alert retained for retry")
                return False
        if not token or not chat_id:
            log.warning("no Telegram credentials; alert not sent: %s", message)
            return False
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=urllib.parse.urlencode(
                {"chat_id": chat_id, "text": message}).encode(),
        )
        urllib.request.urlopen(req, timeout=15)
        return True
    except Exception as exc:
        log.warning("telegram send failed: %s", exc)
        return False


# -------------------------------------------------------------- collectors --
# Each collector returns plain data and swallows its own errors into an
# "error" field — one broken probe must not blind the rest of the tower.

class Collectors:
    def __init__(self, cfg: dict, now: datetime):
        self.cfg = cfg
        self.now = now
        self._rss_cache: tuple[float, list] | None = None
        self._drive_probe: dict | None = None

    _probes = {}
    _probe_lock = threading.Lock()

    def _guard(self, fn):
        # A stalled Drive/FileProvider read must not stop scheduling. Keep at
        # most one in-flight probe per collector, even across supervisor ticks.
        name = fn.__name__
        with self._probe_lock:
            probe = self._probes.get(name)
            if probe is None or probe['event'].is_set():
                probe = {'event':threading.Event()}
                self._probes[name] = probe
                def run():
                    try: probe['value'] = fn()
                    except Exception as exc: probe['value'] = {'error':str(exc)}
                    finally: probe['event'].set()
                threading.Thread(target=run, daemon=True).start()
        if not probe['event'].wait(self.cfg.get('collector_timeout_seconds', 3)):
            log.warning('collector %s timed out; other checks continue', name)
            return {'error':f'{name} probe timed out'}
        return probe['value']

    def collect(self) -> dict:
        return {
            "briefing": self._guard(self.briefing),
            "special": self._guard(self.special),
            "ledger": self._guard(self.ledger),
            "launchd": self._guard(self.launchd),
            "drive": self._guard(self.drive),
            "drive_health": self._guard(self.drive_health),
            "session": self._guard(self.session),
            "substack": self._guard(self.substack),
            "disk": self._guard(self.disk),
            "volumes": self._guard(self.volumes),
            "scripts_archive": self._guard(self.scripts_archive),
            "log_errors": self._guard(self.log_errors),
        }

    # -- daily briefing ------------------------------------------------------
    def briefing(self) -> dict:
        root = self.cfg["podcasts_root"]
        ymd = self.now.strftime("%Y%m%d")
        mp3 = root / "public" / "episodes" / f"gk_daily_{ymd}_morning.mp3"
        day_log = root / "logs" / f"podcast_{self.now:%Y-%m-%d}.log"
        text = day_log.read_text(errors="replace") if day_log.exists() else ""
        return {
            "mp3_exists": mp3.exists(),
            "mp3_mtime": iso_mtime(mp3),
            "pipeline_complete": "PIPELINE COMPLETE" in text,
            "upload_logged": "Spotify upload complete" in text,
            "log_exists": day_log.exists(),
        }

    # -- special editions ----------------------------------------------------
    def special(self) -> dict:
        scripts = self.cfg["drive_gk_daily"] / "scripts"
        processed = scripts / "processed"
        today = f"{self.now:%Y-%m-%d}"
        pending = []
        if scripts.is_dir():
            for p in scripts.glob("*.md"):
                st = p.stat()
                age_min = (time.time() - st.st_mtime) / 60
                # A placeholder Drive will not hydrate looks identical to a
                # script the producer merely has not reached yet. Recording it
                # turns "stuck for 14 h" into "stuck because Drive is wedged"
                # (2026-09-10) — a different fix entirely.
                pending.append({"name": p.name, "age_minutes": round(age_min),
                                "cloud_only": st.st_size > 0 and st.st_blocks == 0})
        processed_today = (
            sorted(p.name for p in processed.glob(f"{today}_*.md"))
            if processed.is_dir() else [])
        arrived_today = bool(processed_today) or any(
            p["name"].startswith(today) for p in pending)
        return {
            "scripts_dir_exists": scripts.is_dir(),
            "pending": pending,
            "processed_today": processed_today,
            "arrived_today": arrived_today,
        }

    # -- spotify upload ledger ----------------------------------------------
    def ledger(self) -> dict:
        path = self.cfg["podcasts_root"] / "config" / "spotify_uploaded.json"
        state = json.loads(path.read_text()) if path.exists() else {}
        today = f"{self.now:%Y-%m-%d}"
        todays = {name: ts for name, ts in state.items()
                  if ts.startswith(today)}
        eps = self.cfg["podcasts_root"] / "public" / "episodes"
        pending = [{"name": p.name,
                    "age_minutes": round((time.time() - p.stat().st_mtime) / 60)}
                   for p in eps.glob("*.mp3") if p.name not in state]
        # upload_spotify.py tags an entry " UNVERIFIED" when the episode was
        # published but never appeared in the Creators list. It alerts once;
        # a one-time message is exactly what went unnoticed on 2026-08-29, so
        # the tag is surfaced here as a standing condition until resolved.
        unverified = [{"name": n, "ts": ts} for n, ts in state.items()
                      if "UNVERIFIED" in ts]
        return {
            "total": len(state),
            "today": todays,
            "pending": pending,
            "unverified": unverified,
            "briefing_uploaded_at": next(
                (ts for n, ts in todays.items() if n.startswith("gk_daily_")),
                None),
        }

    # -- launchd -------------------------------------------------------------
    def launchd(self) -> dict:
        out = subprocess.run(["launchctl", "list"], capture_output=True,
                             text=True, timeout=15).stdout
        loaded = {}
        for line in out.splitlines()[1:]:
            parts = line.split("\t")
            if len(parts) == 3:
                loaded[parts[2]] = parts[1]  # label -> last exit status
        return {label: loaded.get(label)  # None = not loaded
                for label in self.cfg["launchd_labels"]}

    # -- substack (specials only; dailies never go there) --------------------
    def substack(self) -> dict:
        """Compare specials on Spotify against the public Substack feed.

        Substack imports this show from the Spotify RSS feed, so episodes
        normally arrive on their own; this verifies they did rather than
        assuming it. Only episodes inside the feed's visible window (it
        returns ~20 posts) can be judged, so older ones are ignored.
        """
        cfg_s = self.cfg.get("substack", {})
        if not cfg_s.get("enabled"):
            return {"enabled": False}
        spot_path = self.cfg["podcasts_root"] / "config" / "spotify_uploaded.json"
        spot = json.loads(spot_path.read_text()) if spot_path.exists() else {}
        meta_path = (self.cfg["podcasts_root"] / "public" / "episodes"
                     / "special_editions.json")
        meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}

        titles, oldest = self.substack_feed()
        if titles is None:
            return {"enabled": True, "error": "substack feed unreachable"}
        norm = lambda x: re.sub(r"[^a-z0-9]", "", x.lower())
        feed_norm = [norm(t) for t in titles]

        missing, waiting = [], []
        grace = cfg_s.get("sync_grace_min", 120)
        for name, ts in spot.items():
            if not name.startswith("special-edition-"):
                continue
            try:
                up = datetime.fromisoformat(ts).replace(tzinfo=self.now.tzinfo)
            except ValueError:
                continue
            if oldest and up < oldest:
                continue  # outside the feed's visible window — can't judge
            age = (self.now - up).total_seconds() / 60
            title = meta.get(name, {}).get("title", "")
            # Substack sometimes shortens the title (full subtitle dropped),
            # so match on a short prefix and also on the episode slug.
            slug = re.sub(r"-\d{4}-\d{2}-\d{2}$", "",
                          name[len("special-edition-"):-4]).replace("-", "")
            keys = [k for k in (norm(title)[:28], norm(slug)) if len(k) > 8]
            if any(k in f or f[:28] in norm(title) for k in keys
                   for f in feed_norm if f):
                continue
            (missing if age > grace else waiting).append(
                {"name": name, "title": title[:70], "age_minutes": round(age)})
        return {"enabled": True, "missing": missing, "waiting": waiting,
                "feed_posts": len(titles),
                "session_exists": (self.cfg["podcasts_root"]
                                   / ".substack-session.json").exists()}

    def substack_feed(self):
        """(titles, oldest_pubdate) from the public Substack feed; cached."""
        if getattr(self, "_sub_cache", None) and (
                time.time() - self._sub_cache[0] < self.cfg.get(
                    "substack", {}).get("feed_cache_seconds", 900)):
            return self._sub_cache[1], self._sub_cache[2]
        url = self.cfg.get("substack", {}).get(
            "feed_url", "https://geffreyklein.substack.com/feed")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=25) as resp:
                root = ET.fromstring(resp.read())
            items = root.findall(".//item")
            titles = [(i.findtext("title") or "").strip() for i in items]
            oldest = None
            if items:
                from email.utils import parsedate_to_datetime
                try:
                    oldest = parsedate_to_datetime(
                        items[-1].findtext("pubDate")).astimezone(self.now.tzinfo)
                except Exception:
                    oldest = None
            self._sub_cache = (time.time(), titles, oldest)
            return titles, oldest
        except Exception as exc:
            log.warning("substack feed fetch failed: %s", exc)
            return None, None

    # -- google drive --------------------------------------------------------
    def drive_health(self) -> dict:
        """Probe Drive OUT OF PROCESS so a wedged provider cannot hang the tick.

        Google Drive's file provider fails in two ways while the account is
        perfectly healthy: reads of a dehydrated placeholder hang forever
        (2026-09-10) and stat() raises OSError EDEADLK, "Resource deadlock
        avoided" (2026-09-21). Each cost an episode until someone noticed.

        This used to be an inline `gk.is_dir()`, which meant a wedge could
        hang the whole collection pass — the tower falling silent exactly when
        it had something worth saying. A bounded subprocess cannot do that:
        the worst case is a timeout we report as the finding itself.
        """
        if self._drive_probe is not None:
            return self._drive_probe
        gk = self.cfg["drive_gk_daily"]
        probe = (
            "import sys,time\n"
            "from pathlib import Path\n"
            "p=Path(sys.argv[1]); t=time.time()\n"
            "root=1 if p.is_dir() else 0\n"
            "sc=p/'scripts'\n"
            "scripts=1 if sc.is_dir() else 0\n"
            "n=len(list(sc.glob('*.md'))) if scripts else -1\n"
            "chars=0\n"
            "for f in sorted(sc.glob('*.md'))[:1]:\n"
            "    chars=len(f.read_text(errors='replace'))\n"
            "print(root,scripts,n,chars,round(time.time()-t,2))\n")
        limit = self.cfg.get("drive_probe_timeout", 25)
        try:
            proc = subprocess.run([sys.executable, "-c", probe, str(gk)],
                                  capture_output=True, text=True, timeout=limit)
        except subprocess.TimeoutExpired:
            self._drive_probe = {"state": "wedged", "seconds": limit,
                                 "reason": f"no answer in {limit}s — reads are hanging"}
            return self._drive_probe
        if proc.returncode != 0:
            tail = (proc.stderr or "").strip().splitlines()
            msg = tail[-1] if tail else f"exit {proc.returncode}"
            wedged = "deadlock" in msg.lower() or "errno 11" in msg.lower()
            self._drive_probe = {"state": "wedged" if wedged else "error",
                                 "reason": msg[:170]}
            return self._drive_probe
        try:
            root, scripts, pending, chars, secs = (proc.stdout or "").split()
            self._drive_probe = {
                "state": "ok" if root == "1" and scripts == "1" else "missing",
                "gk_daily_exists": root == "1", "scripts_exists": scripts == "1",
                "pending_md": int(pending), "sample_chars": int(chars),
                "seconds": float(secs)}
        except ValueError:
            self._drive_probe = {"state": "error",
                                 "reason": f"unparseable probe output: {(proc.stdout or '')[:80]!r}"}
        return self._drive_probe

    def drive(self) -> dict:
        # Derived from the bounded probe — never touches Drive inline.
        h = self.drive_health()
        return {"gk_daily_exists": h.get("gk_daily_exists", False),
                "scripts_exists": h.get("scripts_exists", False)}

    # -- spotify session freshness -------------------------------------------
    def session(self) -> dict:
        path = self.cfg["podcasts_root"] / ".spotify-session.json"
        if not path.exists():
            return {"exists": False, "age_days": None}
        age = (time.time() - path.stat().st_mtime) / 86400
        return {"exists": True, "age_days": round(age, 1)}

    # -- disk ----------------------------------------------------------------
    def disk(self) -> dict:
        usage = shutil.disk_usage(Path.home())
        return {"free_gb": round(usage.free / 1e9, 1)}

    # -- script archive completeness -----------------------------------------
    def scripts_archive(self) -> dict:
        """Published episodes whose script never reached Drive.

        Episodes made through the older direct-render path write straight into
        special-editions/<slug>/ and never deliver markdown to the Drive drop
        folder, so the archive silently loses the source. Found 2026-08-30:
        five episodes going back to 08-16, spotted by eye rather than by any
        check. The mp3 is janitored after media_retention_days, so the episode
        METADATA is the durable list to compare against, not the audio.
        """
        meta_path = (self.cfg["podcasts_root"] / "public" / "episodes"
                     / "special_editions.json")
        meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
        proc = self.cfg["drive_gk_daily"] / "scripts" / "processed"
        have = set()
        if proc.is_dir():
            for f in proc.glob("*.md"):
                # The producer appends _HHMMSS when a same-named script is
                # already archived (2026-09-09_keytruda-..._095348.md), which
                # made this rule flag an episode whose script was right there.
                stem = re.sub(r"_\d{6}$", "", f.stem)
                have.add(re.sub(r"^\d{4}-\d{2}-\d{2}_", "", stem))
        missing = []
        for name in meta:
            m = re.match(r"special-edition-(.+)-(\d{4}-\d{2}-\d{2})\.mp3$", name)
            if not m:
                continue
            slug, date = m.group(1), m.group(2)
            if slug in have:
                continue
            # Recoverable while the rendered text survives locally. Check
            # BOTH workspaces: the pipeline moved from special-editions/ to
            # work-specials/, and checking only the old path reported 14
            # perfectly recoverable episodes as lost on 2026-09-22 — the
            # tower crying wolf about its own stale assumption.
            root = self.cfg["podcasts_root"]
            recoverable = any(
                (root / workspace / slug / name).is_file()
                for workspace in ("work-specials", "special-editions")
                for name in ("script.txt", f"{date}_{slug}.md"))
            missing.append({"slug": slug, "date": date,
                            "recoverable": recoverable})
        missing.sort(key=lambda x: x["date"])
        return {"episodes": len(meta), "missing": missing}

    # -- external volumes the pipeline now depends on ------------------------
    def volumes(self) -> dict:
        """Check that symlinked pipeline directories still resolve.

        On 2026-08-29 ~/podcasts/special-editions and ~/podcasts/public/episodes
        were moved to /Volumes/T7 Shield/Archives and symlinked back, to free
        disk. That silently made an external drive load-bearing: unplug it and
        renders fail, the briefing cannot write its mp3, and the symptom is an
        obscure ModuleNotFoundError rather than "the drive is gone". This turns
        that into a named cause.

        Only reports on paths that ARE symlinks, so it stays quiet — and
        correct — if the directories are ever moved back onto the internal
        disk.
        """
        out = []
        root = self.cfg["podcasts_root"]
        for rel in ("special-editions", "public/episodes"):
            path = root / rel
            if not path.is_symlink():
                continue
            target = Path(os.readlink(path))
            mount = None
            if str(target).startswith("/Volumes/"):
                mount = "/" + "/".join(str(target).split("/")[1:3])
            out.append({
                "path": rel,
                "target": str(target),
                "mount": mount,
                "mounted": Path(mount).is_mount() if mount else True,
                "readable": path.is_dir(),
            })
        return {"links": out}

    # -- error lines from today's logs (surface, don't re-alert) -------------
    def log_errors(self) -> dict:
        errors = []
        prod = self.cfg["producer_log"]
        if prod.exists():
            tail = prod.read_text(errors="replace").splitlines()[-300:]
            errors += [l for l in tail
                       if f"{self.now:%Y-%m-%d}" in l
                       and ("FAILED" in l or "NEEDS ATTENTION" in l)]
        day_log = (self.cfg["podcasts_root"] / "logs"
                   / f"podcast_{self.now:%Y-%m-%d}.log")
        if day_log.exists():
            errors += [l for l in day_log.read_text(errors="replace")
                       .splitlines() if " ERROR" in l or " CRITICAL" in l]
        return {"today": errors[-20:]}

    # -- spotify public feed (cached; separate because it's a network call) --
    def rss_items(self) -> list | None:
        """Feed items as {title, published}, or None if the feed is unreachable.

        Carries the publication date because a title alone cannot identify an
        episode: re-uploading a corrected render reuses the title, so the old
        entry — still in the feed, or merely cached — would answer for the new
        one. See reconcile_unverified.
        """
        if self._rss_cache and (time.time() - self._rss_cache[0]
                                < self.cfg["rss_cache_seconds"]):
            return self._rss_cache[1]
        try:
            req = urllib.request.Request(self.cfg["spotify_rss"],
                                         headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=25) as resp:
                root = ET.fromstring(resp.read())
            from email.utils import parsedate_to_datetime  # RFC-822 pubDate
            items = []
            for item in root.iter("item"):
                published = None
                raw = (item.findtext("pubDate") or "").strip()
                if raw:
                    try:
                        published = parsedate_to_datetime(raw)
                    except (TypeError, ValueError):
                        published = None
                items.append({"title": (item.findtext("title") or "").strip(),
                              "published": published})
            self._rss_cache = (time.time(), items)
            return items
        except Exception as exc:
            log.warning("spotify RSS fetch failed: %s", exc)
            return None

    def rss_titles(self) -> list | None:
        """Item titles only — what the live-verification rules compare against."""
        items = self.rss_items()
        return None if items is None else [i["title"] for i in items]


def ledger_ts(ts: str) -> str:
    """The bare ISO timestamp from a ledger value.

    upload_spotify.py appends " UNVERIFIED" to entries it published but could
    not confirm in Spotify Creators. The suffix keeps ts.startswith(date)
    working, but fromisoformat() rejects it — which broke the live-verification
    rules the moment the first tagged entry appeared (2026-08-30).
    """
    return (ts or "").split(" ")[0]


def iso_mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(
        timespec="seconds")


# ------------------------------------------------------------------- rules --
# A rule is satisfied (ok), pending (not ok, deadline not reached), firing
# (not ok, past deadline), or unknown (collector broke). Only red/yellow
# firing rules reach Telegram; info rules just color the status page.

def expected_title(mp3_name: str, special_meta: dict) -> str | None:
    """Mirror episode_metadata() in upload_spotify.py so RSS lookups match."""
    if mp3_name.startswith("special-edition-"):
        title = special_meta.get(mp3_name, {}).get("title")
        if title:
            return title
        slug = re.sub(r"-\d{4}-\d{2}-\d{2}$", "",
                      mp3_name[len("special-edition-"):-len(".mp3")])
        return f"GK Daily Special Edition: {slug.replace('-', ' ').title()}"
    m = re.match(r"gk_daily_(\d{8})_(morning|evening)", mp3_name)
    if m:
        friendly = datetime.strptime(m.group(1), "%Y%m%d").strftime(
            "%a %b %d, %Y")
        label = "Morning" if m.group(2) == "morning" else "Afternoon"
        return f"GK Daily — {label} — {friendly}"
    return None


def evaluate(cfg: dict, col: Collectors, data: dict, now: datetime) -> list:
    rules = []
    deadlines = cfg["deadlines"]

    def at(hhmm: str) -> datetime:
        h, m = map(int, hhmm.split(":"))
        return now.replace(hour=h, minute=m, second=0, microsecond=0)

    def add(rule_id, label, ok, due, severity="red", detail=""):
        state = ("unknown" if ok is None else
                 "ok" if ok else
                 "firing" if due is not None and now >= due else "pending")
        rules.append({"id": rule_id, "label": label, "state": state,
                      "severity": severity, "detail": detail,
                      "due": due.isoformat(timespec="minutes") if due else None})

    b, sp, led = data["briefing"], data["special"], data["ledger"]
    weekday = now.weekday() < 5

    # 1. briefing produced by 06:20
    ok = None if "error" in b else (b["mp3_exists"] or b["pipeline_complete"])
    add("briefing_produced", "Briefing episode produced",
        ok, at(deadlines["briefing_produced"]),
        detail=f"mp3 at {b.get('mp3_mtime')}" if ok else
        "no mp3 and no PIPELINE COMPLETE in today's log")

    # 2. briefing uploaded to Spotify by 06:35
    ok = None if "error" in led else bool(led["briefing_uploaded_at"])
    add("briefing_uploaded", "Briefing uploaded to Spotify",
        ok, at(deadlines["briefing_uploaded"]),
        detail=f"ledger: {led.get('briefing_uploaded_at') or 'no entry today'}")

    # 3. special-edition script arrived by deadline (weekdays) — this is the
    #    watch on the Claude scheduled task that writes the scripts.
    if weekday:
        ok = None if "error" in sp else sp["arrived_today"]
        add("special_script", "Special-edition script arrived in Drive",
            ok, at(deadlines["special_script_arrival"]),
            detail=(f"processed: {', '.join(sp.get('processed_today', [])) or '—'}; "
                    f"pending: {len(sp.get('pending', []))}"))

    # 4. no script stuck unprocessed in scripts/
    stuck = [] if "error" in sp else [
        p for p in sp["pending"]
        if p["age_minutes"] > cfg["stuck_script_minutes"]]
    add("no_stuck_scripts", "No scripts stuck unprocessed",
        None if "error" in sp else not stuck,
        now if stuck else None,
        detail="; ".join(
            f"{p['name']} ({p['age_minutes']} min"
            + (", Drive placeholder not hydrating" if p.get("cloud_only") else "")
            + ")" for p in stuck) or "clear")

    # 4b. every rendered episode reaches the ledger — the uploader's failure
    #     is non-fatal to the producer, so a rendered-but-never-uploaded
    #     episode was invisible here (stablecoin-economy, 2026-08-22 11:09)
    stuck_up = [] if "error" in led else [
        p for p in led.get("pending", [])
        if p["age_minutes"] > cfg.get("upload_stuck_minutes", 30)]
    add("uploads_pending", "Rendered episodes uploaded to Spotify",
        None if "error" in led else not stuck_up,
        now if stuck_up else None,
        detail="; ".join(f"{p['name']} ({p['age_minutes']} min, not in ledger)"
                         for p in stuck_up) or "nothing waiting")

    # 5. every episode uploaded today is actually live in the Spotify feed
    #    (checked once per episode after verify_window past its upload time)
    if "error" not in led and led["today"]:
        meta_path = (cfg["podcasts_root"] / "public" / "episodes"
                     / "special_editions.json")
        special_meta = (json.loads(meta_path.read_text())
                        if meta_path.exists() else {})
        titles = col.rss_titles()
        window = timedelta(minutes=cfg["verify_window_minutes"])
        for name, ts in sorted(led["today"].items()):
            due = (datetime.fromisoformat(ledger_ts(ts))
                   .replace(tzinfo=now.tzinfo) + window)
            want = expected_title(name, special_meta)
            ok = None if titles is None or want is None else want in titles
            add(f"live:{name}", f"Live on Spotify: {name}",
                ok, due, detail=f"expect title “{want}”")

    # 5b. specials reach Substack (normally via its Spotify-RSS import)
    sub = data.get("substack", {})
    if sub.get("enabled"):
        if "error" in sub:
            add("substack_synced", "Specials synced to Substack", None, None,
                severity="yellow", detail=sub["error"])
        else:
            miss = sub.get("missing", [])
            add("substack_synced", "Specials synced to Substack",
                not miss, now if miss else None,
                detail="; ".join(f"{m['title'] or m['name']} ({m['age_minutes']} min)"
                                 for m in miss)
                or (f"{len(sub.get('waiting', []))} still importing"
                    if sub.get("waiting") else "all recent specials present"))

    # 6. launchd jobs loaded
    ld = data["launchd"]
    missing = [] if "error" in ld else [k for k, v in ld.items() if v is None]
    add("launchd_loaded", "Pipeline launchd jobs loaded",
        None if "error" in ld else not missing, now if missing else None,
        detail=", ".join(missing) if missing else "all loaded")

    # 7. google drive mounted
    dr = data["drive"]
    ok = None if "error" in dr else dr["scripts_exists"]
    add("drive_mounted", "Google Drive GK Daily folder mounted", ok, now,
        detail=str(cfg["drive_gk_daily"]))

    # 8. spotify session freshness (yellow — proactive re-login warning)
    se = data["session"]
    ok = (None if "error" in se else
          se["exists"] and (se["age_days"] or 0) < cfg["session_warn_days"])
    add("session_fresh", "Spotify login session fresh", ok, now,
        severity="yellow",
        detail=f"age {se.get('age_days')} d (warn ≥ {cfg['session_warn_days']} d)"
        if se.get("exists") else "session file missing")

    # 9. disk space (yellow)
    dk = data["disk"]
    ok = None if "error" in dk else dk["free_gb"] > cfg["disk_min_free_gb"]
    add("disk_space", "Disk space", ok, now, severity="yellow",
        detail=f"{dk.get('free_gb')} GB free")

    # 9b. episodes published but never confirmed in Spotify Creators (red).
    # These are NOT auto-retried — a false negative would double-publish — so
    # they stay red until a human checks and clears the tag.
    unver = data["ledger"].get("unverified", []) if "error" not in data["ledger"] else []
    grace = timedelta(minutes=cfg.get("unverified_grace_min", 60))
    unver = [u for u in unver
             if datetime.fromisoformat(ledger_ts(u["ts"])).replace(
                 tzinfo=now.tzinfo) + grace < now]
    if unver:
        add("uploads_unverified", "Spotify uploads unconfirmed", False, now,
            severity="red",
            detail=f"{unver[0]['name']} published but not found in Creators"
                   + (f" (+{len(unver) - 1} more)" if len(unver) > 1 else "")
                   + " — check creators.spotify.com; not retried automatically")

    # 9c. published episodes with no script in the Drive archive (yellow —
    # nothing is broken, but the source is only recoverable while the local
    # rendered text survives).
    sa = data["scripts_archive"]
    miss = sa.get("missing", []) if "error" not in sa else []
    if "error" not in sa:
        lost = [m for m in miss if not m["recoverable"]]
        add("scripts_archived", "Scripts archived to Drive", not miss, now,
            severity="yellow",
            detail=(f"{len(miss)} episode(s) with no script in processed/ — "
                    f"oldest {miss[0]['date']} {miss[0]['slug']}; "
                    f"{len(lost)} unrecoverable" if miss else
                    f"all {sa.get('episodes', 0)} episodes have a script"))

    # 9d. the Google Drive file provider itself (red — no script can be read).
    # Distinct from volumes_mounted, which watches the T7 episode archive.
    dh = data["drive_health"]
    if "error" in dh:
        add("drive_responsive", "Google Drive responsive", None, now,
            severity="red", detail=f"probe failed: {str(dh['error'])[:90]}")
    else:
        state = dh.get("state")
        add("drive_responsive", "Google Drive responsive", state == "ok", now,
            severity="red",
            detail=(f"wedged — {dh.get('reason', 'unresponsive')}; scripts cannot "
                    "be read locally, the pipeline falls back to the Drive API"
                    if state == "wedged" else
                    ("GK Daily folder not visible — Drive answered but the "
                     "folder is gone or not synced") if state == "missing" else
                    f"probe error — {dh.get('reason', '')[:90]}" if state == "error" else
                    f"responded in {dh.get('seconds')}s, "
                    f"{dh.get('pending_md')} script(s) pending"))

    # 10. external volumes backing the pipeline (red — nothing can render)
    vol = data["volumes"]
    links = vol.get("links", []) if "error" not in vol else []
    broken = [l for l in links if not (l["mounted"] and l["readable"])]
    if links:  # only meaningful once something is symlinked off the internal disk
        add("volumes_mounted", "External episode volume",
            None if "error" in vol else not broken, now, severity="red",
            detail=(f"{broken[0]['mount'] or broken[0]['target']} not available "
                    f"— {broken[0]['path']} is unreachable, renders and the "
                    f"briefing will fail" if broken else
                    f"{links[0]['mount'] or 'symlinked'} mounted, "
                    f"{len(links)} path(s) resolving"))

    # 11. today's log errors — info only; the pipelines alert these themselves
    le = data["log_errors"]
    errs = le.get("today", []) if "error" not in le else []
    add("log_errors", "Today's log errors",
        None if "error" in le else not errs, None, severity="info",
        detail=f"{len(errs)} error line(s) — details in producer/pipeline logs"
        if errs else "clean")

    return rules


# ---------------------------------------------------------------- alerting --

def process_alerts(cfg: dict, conn: sqlite3.Connection, rules: list,
                   now: datetime, quiet: bool = False) -> None:
    today = f"{now:%Y-%m-%d}"
    for r in rules:
        if r["severity"] == "info":
            continue
        key = f"{today}:{r['id']}"
        row = conn.execute("SELECT resolved_at FROM alerts WHERE rule_key=?",
                           (key,)).fetchone()
        if r["state"] == "firing" and row is None:
            icon = "🔴" if r["severity"] == "red" else "🟡"
            msg = (f"{icon} GK Daily tower: {r['label']}\n{r['detail']}"
                   + (f"\n(deadline was {r['due']})" if r["due"] else ""))
            if not quiet:
                telegram(cfg, msg)
            conn.execute(
                "INSERT INTO alerts VALUES (?,?,?,?,NULL)",
                (key, r["severity"], msg, now.isoformat(timespec="seconds")))
            conn.commit()
            log.warning("ALERT %s: %s", r["id"], r["detail"])
        elif r["state"] == "ok" and row is not None and row[0] is None:
            if not quiet:
                telegram(cfg, f"✅ GK Daily tower: resolved — {r['label']}")
            conn.execute("UPDATE alerts SET resolved_at=? WHERE rule_key=?",
                         (now.isoformat(timespec="seconds"), key))
            conn.commit()
            log.info("RESOLVED %s", r["id"])


def maybe_digest(cfg: dict, conn: sqlite3.Connection, rules: list,
                 data: dict, now: datetime, quiet: bool = False) -> None:
    """One green-check digest per day at/after the digest time."""
    today = f"{now:%Y-%m-%d}"
    h, m = map(int, cfg["deadlines"]["digest"].split(":"))
    if now < now.replace(hour=h, minute=m, second=0, microsecond=0):
        return
    if conn.execute("SELECT 1 FROM digests WHERE date=?", (today,)).fetchone():
        return
    bad = [r for r in rules if r["state"] == "firing"]
    led_today = data["ledger"].get("today", {})
    if bad:
        lines = [f"⚠️ GK Daily digest for {today} — {len(bad)} issue(s):"]
        lines += [f"  • {r['label']}: {r['detail']}" for r in bad]
    else:
        lines = [f"✅ GK Daily digest for {today} — all green."]
        b = data["briefing"]
        if b.get("mp3_exists"):
            lines.append(f"  • briefing produced ({b['mp3_mtime']})"
                         + (", uploaded" if data['ledger'].get(
                             'briefing_uploaded_at') else ""))
        specials = [n for n in led_today if n.startswith("special-edition-")]
        lines.append(f"  • specials uploaded today: "
                     f"{', '.join(specials) if specials else 'none yet'}")
        sub_d = data.get("substack", {})
        if sub_d.get("enabled") and "error" not in sub_d:
            miss = sub_d.get("missing", [])
            lines.append("  • Substack: "
                         + (f"{len(miss)} special(s) missing — "
                            + ", ".join(m["name"] for m in miss[:2])
                            if miss else "specials in sync"))
        pend = data["special"].get("pending", [])
        if pend:
            lines.append(f"  • awaiting production: "
                         f"{', '.join(p['name'] for p in pend)}")
    msg = "\n".join(lines) + "\n— tower is alive; no digest = tower is down"
    if not quiet:
        telegram(cfg, msg)
    conn.execute("INSERT INTO digests VALUES (?,?,?)",
                 (today, now.isoformat(timespec="seconds"), msg))
    conn.commit()
    log.info("digest sent for %s", today)


# ----------------------------------------------------------------- janitor --

def media_cleanup(cfg: dict, now: datetime) -> str:
    """Prune local audio that Spotify already hosts.

    Every special exists twice locally (special-editions/ master + the
    public/episodes copy) even after upload. Keep media_retention_days of
    audio; delete older mp3s ONLY if they are in the upload ledger with a
    timestamp older than 3 days (i.e. verified long-since uploaded).
    Scripts and metadata are never touched. Daily-briefing retention is
    publish_feed()'s job (episode_max_count).
    """
    days = cfg.get("media_retention_days", 14)
    led_path = cfg["podcasts_root"] / "config" / "spotify_uploaded.json"
    try:
        ledger = json.loads(led_path.read_text())
    except Exception as exc:
        return f"skipped: ledger unreadable ({exc})"
    cutoff = (now - timedelta(days=days)).isoformat()
    safety = (now - timedelta(days=3)).isoformat()
    substack = {}
    if cfg.get("substack", {}).get("enabled"):
        sub_path = cfg["podcasts_root"] / "config" / "substack_uploaded.json"
        try:
            substack = json.loads(sub_path.read_text()) if sub_path.exists() else {}
        except Exception:
            return "skipped: substack ledger unreadable"
    freed, n = 0, 0

    eps = cfg["podcasts_root"] / "public" / "episodes"
    for mp3 in eps.glob("special-edition-*.mp3"):
        ts = ledger.get(mp3.name)
        if not ts or ts > cutoff or ts > safety:
            continue
        if cfg.get("substack", {}).get("enabled") and mp3.name not in substack:
            continue  # Substack still needs the local file
        slug = re.sub(r"-\d{4}-\d{2}-\d{2}$", "",
                      mp3.stem[len("special-edition-"):])
        master = (cfg["podcasts_root"] / "special-editions" / slug
                  / "episode.mp3")
        for f in (mp3, master):
            if f.exists():
                freed += f.stat().st_size
                f.unlink()
                n += 1

    cache = cfg["podcasts_root"] / "cache"
    dailies = sorted(cache.glob("*_audio.mp3"),
                     key=lambda f: f.stat().st_mtime, reverse=True)
    for f in dailies[2:]:  # match publish's episode_max_count
        freed += f.stat().st_size
        f.unlink()
        n += 1
    return f"removed {n} file(s), freed {freed / 1e6:.0f} MB"


def maybe_media_cleanup(cfg: dict, conn, now: datetime) -> None:
    today = f"{now:%Y-%m-%d}"
    conn.execute("CREATE TABLE IF NOT EXISTS media_cleanup "
                 "(date TEXT PRIMARY KEY, ts TEXT, result TEXT)")
    if conn.execute("SELECT 1 FROM media_cleanup WHERE date=?",
                    (today,)).fetchone():
        return
    result = media_cleanup(cfg, now)
    conn.execute("INSERT INTO media_cleanup VALUES (?,?,?)",
                 (today, now.isoformat(timespec="seconds"), result))
    conn.commit()
    log.info("media cleanup: %s", result)


# ------------------------------------------------------------- upload retry --

def maybe_retry_upload(cfg: dict, conn, now: datetime, data: dict) -> None:
    import jobs
    if not cfg.get('upload_retry', {}).get('enabled', True): return
    for entry in data.get('ledger', {}).get('pending', []):
        if entry['age_minutes'] >= cfg.get('upload_retry', {}).get('after_min', 20):
            jobs.enqueue_upload(entry['name'])


def maybe_nudge_producer(cfg: dict, conn, now: datetime, data: dict) -> None:
    # Intake is durable and deduplicated in tick; never spawn a competing producer.
    return


def maybe_substack_upload(cfg: dict, conn, now: datetime, data: dict) -> None:
    """Push new specials to Substack automatically: wait delay_min after the
    Spotify upload (so metadata is settled), then run the uploader; retry
    every retry_min while anything is pending. The stuck rule goes red at
    stuck_min. Needs the one-time --login session."""
    sub = data.get("substack", {})
    cfg_s = cfg.get("substack", {})
    if (not sub.get("enabled") or "error" in sub
            or cfg_s.get("mode") != "upload"      # default is verify-only
            or not sub.get("session_exists")):
        return
    due = [p for p in sub.get("missing", []) + sub.get("waiting", [])
           if p["age_minutes"] >= cfg_s.get("delay_min", 10)]
    if not due:
        return
    conn.execute("CREATE TABLE IF NOT EXISTS substack_runs "
                 "(ts TEXT, result TEXT)")
    row = conn.execute("SELECT ts, result FROM substack_runs "
                       "ORDER BY ts DESC LIMIT 1").fetchone()
    if row:
        mins = (now - datetime.fromisoformat(row[0])).total_seconds() / 60
        # A 429 means Substack is throttling this machine; retrying on the
        # normal cadence just extends the block.
        wait = (cfg_s.get("backoff_min", 120)
                if row[1] and ("rc=3" in row[1] or "rate limit" in row[1].lower())
                else cfg_s.get("retry_min", 30))
        if mins < wait:
            return
    conn.execute("INSERT INTO substack_runs VALUES (?,?)",
                 (now.isoformat(timespec="seconds"), "running"))
    conn.commit()
    ts_key = now.isoformat(timespec="seconds")

    def go():
        try:
            proc = subprocess.run(
                [upload_python(cfg),
                 str(cfg["podcasts_root"] / "scripts" / "upload_substack.py")],
                capture_output=True, text=True, timeout=1800)
            result = "ok" if proc.returncode == 0 else                 f"rc={proc.returncode}: {(proc.stderr or proc.stdout).strip()[-300:]}"
        except Exception as exc:
            result = f"failed: {exc}"
        c = sqlite3.connect(DB_PATH)
        c.execute("UPDATE substack_runs SET result=? WHERE ts=?", (result, ts_key))
        c.commit()
        c.close()
        log.info("substack auto-upload: %s", result)

    threading.Thread(target=go, daemon=True).start()
    log.info("substack auto-upload started for %d pending special(s)", len(due))


# ------------------------------------------------ special-editions.md --

def _episode_key(title: str) -> str:
    """Normalise a title to its main clause, for matching across sources.

    The same episode is written three ways: the mini's metadata ("GK Daily
    Special Edition: Microplastics and Human Health — What We Actually Know"),
    the feed ("GK Daily Special Edition: Microplastics Human Health") and the
    master list, which sometimes truncates a subtitle or appends a "(subject
    tag)". The clause before the first em dash is the stable part.
    """
    t = re.sub(r"^GK Daily Special Edition[:—\s-]*", "", title or "", flags=re.I)
    t = re.split(r"\s+—\s+|\s+\(", t, maxsplit=1)[0]
    return re.sub(r"[^a-z0-9]", "", t.lower())


def _topic_tag(name: str) -> str:
    """The on-demand request that produced an episode, as a subject tag.

    Titles are written to intrigue ("The Vaccine We Threw Away"), not to name
    their subject, and the subject is what the scout's duplicate check reads.
    The MacBook tags its lines by hand for that reason; the mini can recover
    the same thing from the job that made the episode.
    """
    try:
        c = sqlite3.connect(BASE_DIR / "jobs.db")
        for req, ck in c.execute("SELECT request, checkpoint FROM jobs WHERE status='done'"):
            if json.loads(ck or "{}").get("episode") == name:
                topic = (json.loads(req or "{}").get("topic") or "").strip()
                return re.split(r"\s+—\s+", topic, maxsplit=1)[0][:70]
    except Exception:
        pass
    return ""


def maybe_record_published(cfg: dict, conn, now: datetime) -> list[str]:
    """Add the mini's newly published episodes to special-editions.md.

    That file is the cross-machine do-not-repeat list the Topic Scout reads.
    The MacBook appends its own episodes and periodically re-syncs the whole
    file from the public feed; between syncs, anything the mini published was
    invisible to it. This closes that gap.

    Rules, from tower-dedupe-instructions.md and from the file itself:
      * mini episodes only, from its own metadata + upload ledger — the
        MacBook's audio/ledger.json is never read or written here;
      * only recent publishes (window_days), so a historical episode the file
        lists under a different title is never re-added as a duplicate — the
        08-21 microplastics episode is exactly that case;
      * the file is newest-first, so a new line goes in at its date position,
        not at the end;
      * everything else in the file is preserved byte-for-byte, and the write
        is atomic: full text to a temp file beside it, then rename.

    mode "dry-run" (the default) logs what it would insert and writes nothing.
    """
    cfg_s = cfg.get("special_editions_md", {})
    mode = cfg_s.get("mode", "dry-run")
    if mode == "off":
        return []
    conn.execute("CREATE TABLE IF NOT EXISTS se_md_sync (ts TEXT, added TEXT)")
    last = conn.execute("SELECT MAX(ts) FROM se_md_sync").fetchone()[0]
    if last and (now - datetime.fromisoformat(last)).total_seconds() / 60 < cfg_s.get("every_min", 30):
        return []
    conn.execute("INSERT INTO se_md_sync VALUES (?,?)", (now.isoformat(timespec="seconds"), ""))
    conn.commit()

    path = cfg["drive_gk_daily"] / "special-editions.md"
    try:
        text = path.read_text()
        meta = json.loads((cfg["podcasts_root"] / "public/episodes/special_editions.json").read_text())
        ledger = json.loads((cfg["podcasts_root"] / "config/spotify_uploaded.json").read_text())
    except Exception as exc:
        log.warning("special-editions.md sync skipped (%s)", exc)
        return []

    lines = text.split("\n")
    present = {_episode_key(m.group(1)) for l in lines
               if (m := re.match(r"^\d{4}-\d{2}-\d{2}\s+—\s+(.+)$", l))}
    horizon = (now - timedelta(days=cfg_s.get("window_days", 7))).date()
    new_entries = []
    for name, info in meta.items():
        stamp = ledger.get(name, "")
        if not stamp or "UNVERIFIED" in stamp:
            continue                                  # not confirmed published
        try:
            pub = datetime.fromisoformat(stamp.split(" ")[0]).date()
        except ValueError:
            continue
        if pub < horizon:
            continue
        title = re.sub(r"^GK Daily Special Edition[:—\s-]*", "", info.get("title", "")).strip()
        if not title or _episode_key(title) in present:
            continue
        tag = _topic_tag(name)
        entry = f"{pub.isoformat()} — {title}" + (f" ({tag})" if tag else "")
        new_entries.append((pub.isoformat(), entry))
        present.add(_episode_key(title))
    if not new_entries:
        return []

    for date, entry in sorted(new_entries, reverse=True):
        idx = next((i for i, l in enumerate(lines)
                    if re.match(r"^\d{4}-\d{2}-\d{2}\s+—", l) and l[:10] < date), None)
        if idx is None:                               # older than all: after the last entry
            idx = max((i for i, l in enumerate(lines) if re.match(r"^\d{4}-\d{2}-\d{2}\s+—", l)),
                      default=len(lines) - 1) + 1
        lines.insert(idx, entry)

    added = [e for _, e in new_entries]
    if mode != "on":
        log.info("special-editions.md (dry-run) would add: %s", " | ".join(added))
        return added
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("\n".join(lines))
    os.replace(tmp, path)
    conn.execute("UPDATE se_md_sync SET added=? WHERE ts=(SELECT MAX(ts) FROM se_md_sync)",
                 (" | ".join(added),))
    conn.commit()
    log.info("special-editions.md: added %s", " | ".join(added))
    return added


# ----------------------------------------------------------- stray Docs --

def maybe_import_stray_docs(cfg: dict, conn, now: datetime) -> None:
    """Import episode scripts that were saved as Google Docs, not .md files.

    Seen 2026-09-13: the skill created
    `2026-09-13_pacing-the-frontier-stepping-stone.md` as a Google Doc in My
    Drive root. The name matched the pipeline contract exactly, so nothing
    looked wrong — but a Doc has no readable body on disk and root is not
    watched, so the episode never existed as far as the pipeline was
    concerned, and no log anywhere said so. Silence is the failure mode this
    tower exists to break.
    """
    cfg_d = cfg.get("docs_import", {})
    if not cfg_d.get("enabled", True):
        return
    conn.execute("CREATE TABLE IF NOT EXISTS docs_sweeps (ts TEXT, found TEXT)")
    last = conn.execute("SELECT MAX(ts) FROM docs_sweeps").fetchone()[0]
    if last:
        mins = (now - datetime.fromisoformat(last)).total_seconds() / 60
        if mins < cfg_d.get("every_min", 30):
            return
    conn.execute("INSERT INTO docs_sweeps VALUES (?,?)",
                 (now.isoformat(timespec="seconds"), ""))
    conn.commit()

    def go():
        try:
            import docs_import
            found = docs_import.sweep(cfg)
            if found:
                log.info("imported stray Docs: %s", ", ".join(found))
        except Exception:
            log.exception("stray-Doc sweep failed")

    threading.Thread(target=go, daemon=True).start()


# --------------------------------------------------------------- main loop --

LATEST: dict = {}          # last tick's full status, served by HTTP
LATEST_LOCK = threading.Lock()


def reconcile_unverified(cfg: dict, col, data: dict, now: datetime) -> None:
    """Clear the UNVERIFIED tag once the episode really does show up.

    The uploader can only wait so long before returning, and Spotify's ingest
    is slower than that: the 2026-08-30 telescope episode published fine but
    took longer than the uploader's window to appear, so it got tagged. A tag
    that never clears is a false alarm, and false alarms are how a real one
    gets ignored.

    The tower already polls the public feed, so it is the right place to
    settle the question: found in the feed -> drop the tag; still absent after
    unverified_grace_min -> the rule stays red and means something.

    "Found" has to mean found *as this upload*, not merely a title match.
    Re-rendering an episode and re-uploading it reuses the title, so on
    2026-09-14 the tag for a re-uploaded rescue-dogs episode was cleared by
    the stale feed entry for the very episode it replaced — the check
    confirmed the wrong thing and happened to be right. The item's publication
    date has to be at or after the upload, with a little slack for clock and
    timezone skew between the ledger and Spotify.
    """
    led = data.get("ledger", {})
    unver = led.get("unverified", []) if "error" not in led else []
    if not unver:
        return
    items = col.rss_items()
    if items is None:
        return
    meta_path = (cfg["podcasts_root"] / "public" / "episodes"
                 / "special_editions.json")
    try:
        special_meta = json.loads(meta_path.read_text())
    except Exception:
        special_meta = {}
    path = cfg["podcasts_root"] / "config" / "spotify_uploaded.json"
    cleared = []
    try:
        with exclusive_lock(cfg["podcasts_root"] / ".spotify-upload.lock", blocking=False):
            state = json.loads(path.read_text())
            slack = timedelta(minutes=cfg.get("unverified_date_slack_min", 30))
            for entry in unver:
                want = expected_title(entry["name"], special_meta)
                current = state.get(entry["name"], "")
                if not want or not current.endswith(" UNVERIFIED"):
                    continue
                try:
                    uploaded = datetime.fromisoformat(ledger_ts(current))
                except ValueError:
                    continue
                if uploaded.tzinfo is None:
                    uploaded = uploaded.replace(tzinfo=now.tzinfo)
                match = next(
                    (i for i in items
                     if i["title"] == want and i["published"] is not None
                     and i["published"] >= uploaded - slack),
                    None)
                if match is None:
                    # A same-titled item published BEFORE this upload is the
                    # episode being replaced, not proof of the new one.
                    continue
                state[entry["name"]] = current.removesuffix(" UNVERIFIED")
                cleared.append(entry["name"])
            if cleared:
                atomic_json(path, state)
    except BlockingIOError:
        return  # uploader owns the ledger; reconcile on the next tick
    except (OSError, ValueError):
        log.exception("could not reconcile upload ledger")
        return
    if cleared:
        log.info("cleared UNVERIFIED tag (now live): %s", ", ".join(cleared))
        telegram(cfg, "✅ GK Daily: " + ", ".join(cleared) +
                 " turned up on Spotify after all — the unconfirmed-upload "
                 "flag is cleared. Slow ingest, not a lost episode.")


def tick(cfg: dict, conn: sqlite3.Connection, quiet: bool = False) -> dict:
    now = datetime.now(ZoneInfo(cfg["timezone"]))
    col = Collectors(cfg, now)
    data = col.collect()
    if not quiet:
        try:  # settle any stale UNVERIFIED tags before judging them
            reconcile_unverified(cfg, col, data, now)
            data = col.collect()
        except Exception:
            log.exception("unverified reconciliation failed")
    rules = evaluate(cfg, col, data, now)
    process_alerts(cfg, conn, rules, now, quiet=quiet)
    maybe_digest(cfg, conn, rules, data, now, quiet=quiet)

    if not quiet:
        try:
            import jobs
            import notifications
            notifications.maybe_poll(cfg)
            if "06:00" <= now.strftime("%H:%M") < cfg.get("daily_catchup_until", "12:00"):
                name = f"gk_daily_{now:%Y%m%d}_morning.mp3"
                ledger_path = cfg["podcasts_root"] / "config/spotify_uploaded.json"
                ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
                if name not in ledger: jobs.enqueue_daily("morning", f"{now:%Y-%m-%d}")
            for script in (cfg["drive_gk_daily"] / "scripts").glob("*.md"):
                jobs.enqueue_script(script)
            jobs.maybe_start(cfg)
            jobs.retry_archives(cfg)
            jobs.maybe_preflight(cfg, now)
        except Exception:
            log.exception("job worker start failed")

    if not quiet:  # topic scout: nightly proposals + auto-approve sweep
        try:
            import scout  # lazy: scout imports tower, so no import cycle
            scout.maybe_run(cfg, conn, now)
            scout.auto_approve_due(cfg, now)
        except Exception:
            log.exception("scout step failed")

    if not quiet:  # local-media janitor: prune audio Spotify already hosts
        try:
            maybe_media_cleanup(cfg, conn, now)
        except Exception:
            log.exception("media cleanup failed")

    if not quiet:  # spotify: re-upload episodes that rendered but never landed
        try:
            maybe_retry_upload(cfg, conn, now, data)
        except Exception:
            log.exception("upload retry failed")

    if not quiet:  # producer: run scripts the WatchPaths trigger missed
        try:
            maybe_nudge_producer(cfg, conn, now, data)
        except Exception:
            log.exception("producer nudge failed")

    if not quiet:  # keep the cross-machine do-not-repeat list current
        try:
            maybe_record_published(cfg, conn, now)
        except Exception:
            log.exception("special-editions.md sync failed")

    if not quiet:  # rescue scripts saved as Google Docs instead of .md files
        try:
            maybe_import_stray_docs(cfg, conn, now)
        except Exception:
            log.exception("stray-Doc import failed")

    if not quiet:  # substack: push new specials once Spotify has them
        try:
            maybe_substack_upload(cfg, conn, now, data)
        except Exception:
            log.exception("substack step failed")

    if not quiet:  # script factory: failover if no special-edition script
        try:
            import factory  # lazy for the same reason
            factory.maybe_failover(cfg, conn, now, data)
        except Exception:
            log.exception("factory step failed")

    status = {"ts": now.isoformat(timespec="seconds"),
              "overall": overall_state(rules),
              "rules": rules, "collectors": data}
    conn.execute("INSERT INTO history VALUES (?,?)",
                 (status["ts"], json.dumps(status)))
    conn.execute("DELETE FROM history WHERE ts < ?",
                 ((now - timedelta(days=30)).isoformat(),))
    conn.commit()
    with LATEST_LOCK:
        LATEST.clear()
        LATEST.update(status)
    return status


def overall_state(rules: list) -> str:
    states = {r["state"]: r for r in rules}
    if any(r["state"] == "firing" and r["severity"] == "red" for r in rules):
        return "red"
    if any(r["state"] == "firing" for r in rules):
        return "yellow"
    if "unknown" in states:
        return "yellow"
    return "green"


def scheduler(cfg: dict) -> None:
    conn = db()
    while True:
        try:
            tick(cfg, conn)
        except Exception:
            log.exception("tick failed")  # never die; next tick retries
        time.sleep(cfg["tick_seconds"])


# ------------------------------------------------------------------- http --
# The whole HTTP layer (dashboard, actions, screenshots) lives in
# dashboard.py; the tower only hands it a way to read the latest status.

def get_status() -> dict:
    with LATEST_LOCK:
        return dict(LATEST)


# ------------------------------------------------------------------- main --

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--serve", action="store_true")
    mode.add_argument("--check", action="store_true",
                      help="collect + evaluate, print JSON, send nothing")
    mode.add_argument("--once", action="store_true",
                      help="one real tick (alerts + digest enabled)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [tower] %(levelname)s: %(message)s")
    cfg = load_config()

    if args.check or args.once:
        # --check uses a throwaway DB so a dry run can't eat the dedup rows
        # that decide whether a real alert still needs to be sent.
        status = tick(cfg, db(":memory:") if args.check else db(),
                      quiet=args.check)
        print(json.dumps(status, indent=2))
        return 0 if status["overall"] != "red" else 1

    import dashboard
    dashboard.init(cfg, DB_PATH, get_status,
                   lambda name, meta: expected_title(name, meta))
    # A restart orphans any action thread that was mid-subprocess; its row
    # would sit at 'running' forever and block single-flight checks.
    conn = db()
    n = conn.execute(
        "UPDATE actions SET status='interrupted', "
        "output=COALESCE(output,'') || ' [tower restarted mid-run]' "
        "WHERE status='running'").rowcount
    conn.commit()
    conn.close()
    if n:
        log.warning("marked %d orphaned running action(s) as interrupted", n)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute('SELECT * FROM history ORDER BY ts DESC LIMIT 1').fetchone()
        if row: LATEST.update(json.loads(row[1]))
    except Exception: log.exception('could not load previous status')
    threading.Thread(target=scheduler, args=(cfg,), daemon=True).start()
    server = ThreadingHTTPServer((cfg["bind"], cfg["port"]), dashboard.Handler)
    log.info("serving on %s:%s", cfg["bind"], cfg["port"])
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
