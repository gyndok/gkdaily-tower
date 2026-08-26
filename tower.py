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
        token = creds.get("TELEGRAM_BOT_TOKEN")
        chat_id = creds.get("TELEGRAM_CHAT_ID")
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

    def _guard(self, fn):
        try:
            return fn()
        except Exception as exc:
            log.warning("collector %s failed: %s", fn.__name__, exc)
            return {"error": str(exc)}

    def collect(self) -> dict:
        return {
            "briefing": self._guard(self.briefing),
            "special": self._guard(self.special),
            "ledger": self._guard(self.ledger),
            "launchd": self._guard(self.launchd),
            "drive": self._guard(self.drive),
            "session": self._guard(self.session),
            "substack": self._guard(self.substack),
            "disk": self._guard(self.disk),
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
                age_min = (time.time() - p.stat().st_mtime) / 60
                pending.append({"name": p.name, "age_minutes": round(age_min)})
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
        return {
            "total": len(state),
            "today": todays,
            "pending": pending,
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
    def drive(self) -> dict:
        gk = self.cfg["drive_gk_daily"]
        return {"gk_daily_exists": gk.is_dir(),
                "scripts_exists": (gk / "scripts").is_dir()}

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
    def rss_titles(self) -> list | None:
        """Item titles from the anchor.fm feed, or None if unreachable."""
        if self._rss_cache and (time.time() - self._rss_cache[0]
                                < self.cfg["rss_cache_seconds"]):
            return self._rss_cache[1]
        try:
            req = urllib.request.Request(self.cfg["spotify_rss"],
                                         headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=25) as resp:
                root = ET.fromstring(resp.read())
            titles = [(el.text or "").strip()
                      for el in root.iter("title")][1:]  # [0] = channel title
            self._rss_cache = (time.time(), titles)
            return titles
        except Exception as exc:
            log.warning("spotify RSS fetch failed: %s", exc)
            return None


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
    m = re.match(r"gk_daily_(\d{8})_morning", mp3_name)
    if m:
        friendly = datetime.strptime(m.group(1), "%Y%m%d").strftime(
            "%a %b %d, %Y")
        return f"GK Daily — Morning — {friendly}"
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
        detail="; ".join(f"{p['name']} ({p['age_minutes']} min)"
                         for p in stuck) or "clear")

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
            due = datetime.fromisoformat(ts).replace(tzinfo=now.tzinfo) + window
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

    # 10. today's log errors — info only; the pipelines alert these themselves
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


# ------------------------------------------------------------ producer nudge --

def maybe_nudge_producer(cfg: dict, conn, now: datetime, data: dict) -> None:
    """Run the producer for a script it never picked up.

    The producer fires on a launchd WatchPaths trigger with a 300 s throttle,
    so a script landing just after a run (or during the window) can sit
    untouched until the next trigger — which may be the next weekday. Seen
    2026-08-26: the Mars script arrived the same minute the previous episode
    finished and waited for a manual run.
    """
    cfg_n = cfg.get("producer_nudge", {})
    if not cfg_n.get("enabled", True):
        return
    sp = data.get("special", {})
    if "error" in sp:
        return
    stale = [p for p in sp.get("pending", [])
             if p["age_minutes"] >= cfg_n.get("after_min", 8)]
    if not stale:
        return
    # Never start a second producer on top of a running one.
    try:
        running = subprocess.run(["pgrep", "-f", "produce-special-podcast.py"],
                                 capture_output=True, text=True, timeout=10)
        if running.returncode == 0 and running.stdout.strip():
            return
    except Exception:
        return
    conn.execute("CREATE TABLE IF NOT EXISTS producer_nudges (ts TEXT, result TEXT)")
    last = conn.execute("SELECT MAX(ts) FROM producer_nudges").fetchone()[0]
    if last:
        mins = (now - datetime.fromisoformat(last)).total_seconds() / 60
        if mins < cfg_n.get("retry_min", 20):
            return
    ts_key = now.isoformat(timespec="seconds")
    conn.execute("INSERT INTO producer_nudges VALUES (?,?)", (ts_key, "running"))
    conn.commit()
    names = ", ".join(p["name"] for p in stale)
    log.info("nudging producer for unprocessed script(s): %s", names)

    def go():
        clawd = Path.home() / "clawd"
        try:
            proc = subprocess.run(
                [str(clawd / ".venv" / "bin" / "python3"),
                 str(clawd / "produce-special-podcast.py")],
                capture_output=True, text=True, timeout=2400)
            result = ("ok" if proc.returncode == 0
                      else f"rc={proc.returncode}: {(proc.stderr or '').strip()[-200:]}")
        except Exception as exc:
            result = f"failed: {exc}"
        c = sqlite3.connect(DB_PATH)
        c.execute("UPDATE producer_nudges SET result=? WHERE ts=?", (result, ts_key))
        c.commit(); c.close()
        log.info("producer nudge: %s", result)
        if not result.startswith("ok"):
            telegram(cfg, f"⚠️ GK Daily: auto-run of the producer for {names} "
                          f"failed — {result[:200]}")

    threading.Thread(target=go, daemon=True).start()


# ---------------------------------------------------------- substack runner --

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
                ["/opt/homebrew/bin/python3",
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


# --------------------------------------------------------------- main loop --

LATEST: dict = {}          # last tick's full status, served by HTTP
LATEST_LOCK = threading.Lock()


def tick(cfg: dict, conn: sqlite3.Connection, quiet: bool = False) -> dict:
    now = datetime.now(ZoneInfo(cfg["timezone"]))
    col = Collectors(cfg, now)
    data = col.collect()
    rules = evaluate(cfg, col, data, now)
    process_alerts(cfg, conn, rules, now, quiet=quiet)
    maybe_digest(cfg, conn, rules, data, now, quiet=quiet)

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

    if not quiet:  # producer: run scripts the WatchPaths trigger missed
        try:
            maybe_nudge_producer(cfg, conn, now, data)
        except Exception:
            log.exception("producer nudge failed")

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
    threading.Thread(target=scheduler, args=(cfg,), daemon=True).start()
    server = ThreadingHTTPServer((cfg["bind"], cfg["port"]), dashboard.Handler)
    log.info("serving on %s:%s", cfg["bind"], cfg["port"])
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
