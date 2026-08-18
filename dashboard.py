"""
GK Daily Control Tower — P2 dashboard (HTTP layer).

Server-rendered HTML + a small set of POST actions, stdlib only. tower.py
wires this up at startup via init(); this module never imports tower, so
there is no circular dependency.

Routes (all also reachable behind nginx at /tower/... — links are relative):
    GET  /            dashboard
    GET  /status      last tick as JSON (unchanged from P1)
    GET  /shots/<f>   spotify-upload failure screenshot (png/jpg only)
    POST /action      name=<action> [arg=<value>] → 303 back to /

Actions are whitelisted, argv-only (never a shell), logged to the actions
table, and run in a background thread so the UI never blocks. Credentials
never pass through here — re-login stays a terminal job by design.
"""

import html
import json
import logging
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import urllib.parse
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from zoneinfo import ZoneInfo

log = logging.getLogger("tower.dash")

# set by init()
CFG: dict = {}
DB_PATH: Path = None
GET_STATUS = lambda: {}
EXPECTED_TITLE = lambda name, meta: name


def init(cfg: dict, db_path: Path, get_status, expected_title) -> None:
    global CFG, DB_PATH, GET_STATUS, EXPECTED_TITLE
    CFG, DB_PATH = cfg, db_path
    GET_STATUS, EXPECTED_TITLE = get_status, expected_title


def now_tz() -> datetime:
    return datetime.now(ZoneInfo(CFG["timezone"]))


def esc(s) -> str:
    return html.escape(str(s), quote=True)


# ----------------------------------------------------------------- actions --

SCRIPT_NAME_RE = re.compile(r"^[\w()\-. ]+\.md$")


def _scripts_dir() -> Path:
    return CFG["drive_gk_daily"] / "scripts"


def _valid_script(arg: str) -> Path | None:
    """A bare .md filename that really exists in the Drive scripts folder."""
    if not arg or Path(arg).name != arg or not SCRIPT_NAME_RE.match(arg):
        return None
    p = _scripts_dir() / arg
    return p if p.is_file() else None


def _rotate_logs() -> str:
    done = []
    for name in ("morning_stdout.log", "morning_stderr.log",
                 "evening_stdout.log", "evening_stderr.log"):
        p = CFG["podcasts_root"] / "logs" / name
        if p.exists() and p.stat().st_size > 1_000_000:
            tail = p.read_bytes()[-200_000:]
            p.write_bytes(b"[rotated by tower]\n" + tail)
            done.append(f"{name} truncated to 200 KB")
    shots = CFG["podcasts_root"] / "logs" / "spotify-upload"
    if shots.is_dir():
        cutoff = time.time() - 30 * 86400
        old = [p for p in shots.iterdir()
               if p.is_file() and p.stat().st_mtime < cutoff]
        for p in old:
            p.unlink()
        if old:
            done.append(f"pruned {len(old)} screenshot(s) older than 30 d")
    return "; ".join(done) or "nothing needed rotating"


def _mark_processed(script: Path) -> str:
    processed = _scripts_dir() / "processed"
    processed.mkdir(exist_ok=True)
    shutil.move(str(script), str(processed / script.name))
    return f"moved {script.name} to processed/"


def dispatch(name: str, arg: str, form: dict | None = None) -> str:
    """Start (or run) one whitelisted action; returns a short ack string."""
    global _UPCOMING_CACHE
    if name == "add_topic":
        line = " ".join((arg or "").split())[:200]
        if len(line) < 8:
            return _record(name, arg, "failed", "topic text too short")
        pos_raw = (form or {}).get("pos", [""])[0].strip()
        before = None
        if pos_raw:
            try:
                pos = int(pos_raw)
            except ValueError:
                return _record(name, arg, "failed",
                               f"position must be a number, got {pos_raw!r}")
            try:
                upcoming, _ = upcoming_topics()
            except Exception as exc:
                return _record(name, arg, "failed",
                               f"could not read topics doc: {exc}")
            if 1 <= pos <= len(upcoming):
                before = upcoming[pos - 1]
            # position past the end just appends
        import scout
        try:
            msg = scout.gdoc_insert(CFG, line, before_line=before)
        except Exception as exc:
            return _record(name, arg, "failed", str(exc))
        _UPCOMING_CACHE = None  # show the new line on the next page load
        return _record(name, arg, "done", msg)
    pods, clawd = CFG["podcasts_root"], Path.home() / "clawd"
    uploader = ["/opt/homebrew/bin/python3", str(pods / "scripts" / "upload_spotify.py")]
    producer = [str(clawd / ".venv" / "bin" / "python3"),
                str(clawd / "produce-special-podcast.py")]

    if name == "retry_upload":
        return _spawn(name, arg, uploader, 1800)
    if name == "producer_dry_run":
        return _spawn(name, arg, producer + ["--dry-run"], 600)
    if name == "force_produce":
        script = _valid_script(arg)
        if not script:
            return _record(name, arg, "failed", "unknown script name")
        return _spawn(name, arg, producer + ["--script", str(script), "--force"], 1800)
    if name == "mark_processed":
        script = _valid_script(arg)
        if not script:
            return _record(name, arg, "failed", "unknown script name")
        try:
            return _record(name, arg, "done", _mark_processed(script))
        except Exception as exc:
            return _record(name, arg, "failed", str(exc))
    if name == "rotate_logs":
        try:
            return _record(name, arg, "done", _rotate_logs())
        except Exception as exc:
            return _record(name, arg, "failed", str(exc))
    if name == "run_scout":
        return _spawn(name, arg,
                      ["/opt/homebrew/bin/python3",
                       str(Path(__file__).resolve().parent / "scout.py"),
                       "--run"], 300)
    if name == "run_factory_staged":
        # Full research+write on Claude Opus 5, but the script lands in
        # staging/ — nothing reaches the pipeline. For testing/previewing.
        return _spawn(name, arg,
                      [str(Path(CFG["factory"]["python"]).expanduser()),
                       str(Path(__file__).resolve().parent / "factory.py"),
                       "--run", "--stage"], 1800)
    if name == "produce_topic":
        # Full pipeline kickoff: Factory writes the script for this exact
        # queue line → Drive → the WatchPaths producer renders and uploads.
        try:
            upcoming, _ = upcoming_topics()
        except Exception as exc:
            return _record(name, arg, "failed", f"could not read topics doc: {exc}")
        if arg not in upcoming:
            # Only lines currently in the queue are producible — stops stale
            # pages, double-clicks on covered topics, and arbitrary input.
            return _record(name, arg, "failed",
                           "not an uncovered line in the current queue")
        conn = sqlite3.connect(DB_PATH)
        busy = conn.execute(
            "SELECT COUNT(*) FROM actions WHERE status='running' "
            "AND name IN ('produce_topic','run_factory_staged')").fetchone()[0]
        conn.close()
        if busy:
            return _record(name, arg, "failed",
                           "a script generation is already running — wait for it")
        _UPCOMING_CACHE = None  # the line is about to become covered
        return _spawn(name, arg,
                      [str(Path(CFG["factory"]["python"]).expanduser()),
                       str(Path(__file__).resolve().parent / "factory.py"),
                       "--run", "--topic", arg], 1800)
    if name in ("approve_topic", "veto_topic"):
        import scout
        try:
            fn = scout.approve if name == "approve_topic" else scout.veto
            return _record(name, arg, "done", fn(CFG, arg))
        except Exception as exc:
            return _record(name, arg, "failed", str(exc))
    return _record(name, arg, "failed", "unknown action")


def _record(name, arg, status, output) -> str:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO actions (ts,name,arg,status,output) VALUES (?,?,?,?,?)",
                 (now_tz().isoformat(timespec="seconds"), name, arg, status,
                  output[-2000:]))
    conn.commit()
    conn.close()
    log.info("action %s(%s): %s — %s", name, arg, status, output[:200])
    return status


def _spawn(name, arg, argv, timeout) -> str:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "INSERT INTO actions (ts,name,arg,status,output) VALUES (?,?,?,?,?)",
        (now_tz().isoformat(timespec="seconds"), name, arg, "running",
         " ".join(argv)))
    action_id = cur.lastrowid
    conn.commit()
    conn.close()

    def run():
        try:
            proc = subprocess.run(argv, capture_output=True, text=True,
                                  timeout=timeout)
            status = "done" if proc.returncode == 0 else f"failed rc={proc.returncode}"
            output = (proc.stdout + "\n" + proc.stderr).strip()[-2000:]
        except Exception as exc:
            status, output = "failed", str(exc)
        c = sqlite3.connect(DB_PATH)
        c.execute("UPDATE actions SET status=?, output=? WHERE id=?",
                  (status, output, action_id))
        c.commit()
        c.close()
        log.info("action %s finished: %s", name, status)

    threading.Thread(target=run, daemon=True).start()
    return "running"


# ------------------------------------------------------------ data helpers --

def pending_uploads() -> list[str]:
    led_path = CFG["podcasts_root"] / "config" / "spotify_uploaded.json"
    state = json.loads(led_path.read_text()) if led_path.exists() else {}
    eps = CFG["podcasts_root"] / "public" / "episodes"
    return sorted(p.name for p in eps.glob("*.mp3") if p.name not in state)


def special_meta() -> dict:
    p = CFG["podcasts_root"] / "public" / "episodes" / "special_editions.json"
    try:
        return json.loads(p.read_text()) if p.exists() else {}
    except Exception:
        return {}


def briefing_facts(date_str: str) -> dict:
    """Voice + word count parsed from that day's pipeline log, best effort."""
    p = CFG["podcasts_root"] / "logs" / f"podcast_{date_str}.log"
    facts = {}
    if p.exists():
        text = p.read_text(errors="replace")
        m = re.search(r"\(([\d.]+ MB), (\d+) words\)", text)
        if m:
            facts["size"], facts["words"] = m.group(1), m.group(2)
        m = re.search(r"Voice rotation.*?\b([ab][fm]_\w+)", text)
        if m:
            facts["voice"] = m.group(1)
    return facts


def episode_rows(days: int = 14) -> list[dict]:
    led_path = CFG["podcasts_root"] / "config" / "spotify_uploaded.json"
    state = json.loads(led_path.read_text()) if led_path.exists() else {}
    cutoff = (now_tz() - timedelta(days=days)).strftime("%Y-%m-%d")
    meta = special_meta()
    rows = []
    for name, ts in sorted(state.items(), key=lambda kv: kv[1], reverse=True):
        if ts[:10] < cutoff:
            continue
        row = {"name": name, "uploaded": ts,
               "title": EXPECTED_TITLE(name, meta) or name,
               "special": name.startswith("special-edition-")}
        if not row["special"]:
            row |= briefing_facts(f"{ts[:10]}")
        rows.append(row)
    return rows


def stage_timeline() -> list[tuple[str, str]]:
    """(time, event) pairs for today's briefing run."""
    p = (CFG["podcasts_root"] / "logs"
         / f"podcast_{now_tz():%Y-%m-%d}.log")
    if not p.exists():
        return []
    events = []
    pat = re.compile(
        r"^(\d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2})).*?"
        r"(STAGE \d(?:/5)?: .+?|PIPELINE COMPLETE.*?|Spotify upload complete)\s*$")
    for line in p.read_text(errors="replace").splitlines():
        m = pat.match(line)
        if m and (not events or events[-1][1] != m.group(3)):
            events.append((m.group(2), m.group(3)))
    return events


def log_tail(path: Path, n: int = 30) -> str:
    if not path.exists():
        return "(missing)"
    return "\n".join(path.read_text(errors="replace").splitlines()[-n:])


def screenshots(limit: int = 6) -> list[Path]:
    d = CFG["podcasts_root"] / "logs" / "spotify-upload"
    if not d.is_dir():
        return []
    pics = [p for p in d.iterdir() if p.suffix.lower() in (".png", ".jpg", ".jpeg")]
    return sorted(pics, key=lambda p: p.stat().st_mtime, reverse=True)[:limit]


_UPCOMING_CACHE: tuple[float, list, int] | None = None


def upcoming_topics() -> tuple[list[str], int]:
    """Uncovered gdoc queue lines in priority order (top = next up), plus a
    count of covered lines still sitting in the doc. Cached 5 min — the gdoc
    read is a gws subprocess and shouldn't run on every page load."""
    global _UPCOMING_CACHE
    if _UPCOMING_CACHE and time.time() - _UPCOMING_CACHE[0] < 300:
        return _UPCOMING_CACHE[1], _UPCOMING_CACHE[2]
    import factory
    import scout
    lines = scout.gdoc_lines(CFG)  # raises on gws failure; caller handles
    covered = factory.covered_topics(CFG)
    upcoming = [l for l in lines if not factory.is_covered(l, covered)]
    done_count = len(lines) - len(upcoming)
    _UPCOMING_CACHE = (time.time(), upcoming, done_count)
    return upcoming, done_count


def topic_candidates() -> dict:
    """queue.json split by status for the Topic Scout section."""
    try:
        import scout
        cands = scout.load_queue(CFG)["candidates"]
    except Exception:
        cands = []
    return {
        "proposed": [c for c in cands if c["status"] == "proposed"],
        "decided": sorted((c for c in cands if c["status"] != "proposed"),
                          key=lambda c: c.get("decided_at") or "", reverse=True)[:10],
    }


def recent_actions(limit: int = 10) -> list[tuple]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT ts,name,arg,status,output FROM actions "
        "ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return rows


# ------------------------------------------------------------------- html --

CSS = """
 body{font:15px/1.5 -apple-system,system-ui,sans-serif;margin:1.5rem auto;
      max-width:52rem;padding:0 1rem;background:#fff;color:#1a1d21}
 @media(prefers-color-scheme:dark){body{background:#14161a;color:#d8dde3}
      td,th{border-color:#333!important}
      pre,button,.thumb{background:#1d2026!important;border-color:#333!important}}
 h1{font-size:1.25rem} h2{font-size:1.05rem;margin:1.6rem 0 .3rem}
 small,.muted{opacity:.6}
 table{border-collapse:collapse;width:100%}
 td,th{text-align:left;padding:.4rem .55rem;border-bottom:1px solid #e3e6ea;
      vertical-align:top;font-variant-numeric:tabular-nums}
 .ok:before{content:"●";color:#2e9e5b;margin-right:.5rem}
 .firing:before{content:"●";color:#d43b3b;margin-right:.5rem}
 .pending:before{content:"●";color:#c99a2e;margin-right:.5rem}
 .unknown:before{content:"●";color:#8a8f98;margin-right:.5rem}
 .banner{padding:.55rem 1rem;border-radius:6px;font-weight:600;margin:.6rem 0}
 .green{background:#2e9e5b22}.red{background:#d43b3b22}.yellow{background:#c99a2e22}
 pre{background:#f4f5f7;border:1px solid #e3e6ea;border-radius:6px;
     padding:.6rem;overflow-x:auto;font-size:12px;line-height:1.45}
 button{font:inherit;padding:.3rem .7rem;border:1px solid #b9bec6;
     border-radius:6px;background:#f4f5f7;cursor:pointer}
 input:not([type=hidden]){font:inherit;padding:.25rem .5rem;
     border:1px solid #b9bec6;border-radius:6px;background:transparent;
     color:inherit}
 button:hover{border-color:#6a7f99}
 form{display:inline;margin-right:.4rem}
 details{margin:.5rem 0} summary{cursor:pointer}
 .thumb{max-width:180px;border:1px solid #e3e6ea;border-radius:6px;margin:.2rem}
 .tag{font-size:11px;padding:.05rem .45rem;border-radius:99px;
     background:#8a8f9822;margin-left:.4rem}
"""


def button(label: str, action: str, arg: str = "", confirm: str = "") -> str:
    onsubmit = f' onsubmit="return confirm(\'{esc(confirm)}\')"' if confirm else ""
    return (f'<form method="post" action="action"{onsubmit}>'
            f'<input type="hidden" name="name" value="{esc(action)}">'
            f'<input type="hidden" name="arg" value="{esc(arg)}">'
            f'<button>{esc(label)}</button></form>')


def render_page() -> str:
    status = GET_STATUS()
    rules = status.get("rules", [])
    overall = status.get("overall", "")
    labels = {"green": "All green ✅", "yellow": "Attention 🟡",
              "red": "Problem 🔴", "": "No data yet"}

    rule_rows = "".join(
        f'<tr><td class="{r["state"]}">{esc(r["label"])}</td>'
        f'<td>{esc(r["detail"])}</td></tr>' for r in rules)

    timeline = stage_timeline()
    timeline_html = ("".join(f"<tr><td>{esc(t)}</td><td>{esc(e)}</td></tr>"
                             for t, e in timeline)
                     or '<tr><td colspan="2" class="muted">no run yet today</td></tr>')

    pend_up = pending_uploads()
    pend_up_html = ("".join(f"<li>{esc(n)}</li>" for n in pend_up)
                    or '<li class="muted">none — everything on disk is uploaded</li>')

    sp = status.get("collectors", {}).get("special", {})
    pending_scripts = sp.get("pending", []) if isinstance(sp, dict) else []
    script_rows = "".join(
        f'<tr><td>{esc(p["name"])} <span class="tag">{p["age_minutes"]} min</span></td>'
        f'<td>{button("Mark processed", "mark_processed", p["name"], "Move " + p["name"] + " to processed/ without producing it?")}'
        f'{button("Force produce", "force_produce", p["name"], "Re-produce and re-publish " + p["name"] + "? This can double-publish an episode.")}'
        f'</td></tr>'
        for p in pending_scripts) or \
        '<tr><td colspan="2" class="muted">no scripts waiting in Drive</td></tr>'

    try:
        upcoming, done_count = upcoming_topics()
        upcoming_html = "".join(
            f'<tr><td class="muted" style="width:2rem">{i}</td>'
            f'<td>{esc(l)}{" <span class=tag>next up</span>" if i == 1 else ""}</td>'
            f'<td style="width:8rem">'
            + button("Produce now", "produce_topic", l,
                     "Write, render, and publish this episode to Spotify now? "
                     "Costs about $1 and takes ~20 minutes end to end.")
            + "</td></tr>"
            for i, l in enumerate(upcoming, 1)) or \
            '<tr><td colspan="3" class="muted">queue is empty — approve some Scout proposals</td></tr>'
        upcoming_note = (f"{len(upcoming)} queued · {done_count} line(s) in the "
                         "doc already produced")
    except Exception as exc:
        upcoming_html = (f'<tr><td colspan="3" class="muted">could not read the '
                         f'topics doc: {esc(exc)}</td></tr>')
        upcoming_note = ""

    topics = topic_candidates()
    prop_rows = "".join(
        f'<tr><td><b>{esc(c["title"])}</b><br>{esc(c["angle"])}<br>'
        f'<span class="muted">{esc(c.get("why_now", ""))}</span><br>'
        f'<span class="tag">{esc(c["queue_line"])}</span></td>'
        f'<td>{button("Approve → topics doc", "approve_topic", c["slug"], "Append to the GK Daily Topics doc: " + c["queue_line"] + "?")}'
        f'{button("Veto", "veto_topic", c["slug"])}</td></tr>'
        for c in topics["proposed"]) or \
        '<tr><td colspan="2" class="muted">no proposals waiting — Scout runs nightly at ' \
        + esc(CFG["scout"]["run_at"]) + '</td></tr>'
    decided_rows = "".join(
        f'<tr><td>{esc((c.get("decided_at") or "")[:16].replace("T", " "))}</td>'
        f'<td>{esc(c["status"])}{" (auto)" if c.get("decided_by") == "auto" else ""}</td>'
        f'<td>{esc(c["title"])}</td></tr>' for c in topics["decided"])

    ep_rows = "".join(
        f'<tr><td>{esc(r["uploaded"][:16].replace("T", " "))}</td>'
        f'<td>{"Special" if r["special"] else "Briefing"}</td>'
        f'<td>{esc(r["title"])}'
        + (f' <span class="tag">{esc(r.get("voice", ""))}</span>' if r.get("voice") else "")
        + (f' <span class="tag">{esc(r.get("words", ""))} words</span>' if r.get("words") else "")
        + "</td></tr>"
        for r in episode_rows())

    err_lines = (status.get("collectors", {}).get("log_errors", {}) or {}).get("today", [])
    err_html = esc("\n".join(err_lines)) if err_lines else "clean"

    shots_html = "".join(
        f'<a href="shots/{esc(p.name)}"><img class="thumb" src="shots/{esc(p.name)}" '
        f'alt="{esc(p.name)}" loading="lazy"></a>' for p in screenshots()) \
        or '<span class="muted">no failure screenshots</span>'

    act_rows = "".join(
        f"<tr><td>{esc(ts[:16].replace('T', ' '))}</td><td>{esc(nm)}"
        + (f" <span class='tag'>{esc(arg)}</span>" if arg else "")
        + f"</td><td>{esc(st)}</td>"
        f"<td><details><summary>output</summary><pre>{esc(out)}</pre></details></td></tr>"
        for ts, nm, arg, st, out in recent_actions()) or \
        '<tr><td colspan="4" class="muted">none yet</td></tr>'

    pods = CFG["podcasts_root"]
    logs_html = "".join(
        f"<details><summary>{esc(title)}</summary><pre>{esc(log_tail(p))}</pre></details>"
        for title, p in [
            (f"pipeline log ({now_tz():%Y-%m-%d})",
             pods / "logs" / f"podcast_{now_tz():%Y-%m-%d}.log"),
            ("special-edition producer log", CFG["producer_log"]),
            ("tower log", Path.home() / "Library" / "Logs" / "gkdaily-tower.log"),
        ])

    return f"""<!doctype html><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="120">
<title>GK Daily Tower</title><style>{CSS}</style>
<h1>GK Daily Control Tower <small>P2</small></h1>
<div class="banner {overall}">{labels.get(overall, "?")} — checked {esc(status.get("ts", "never"))}</div>

<h2>Checks</h2>
<table><tr><th>Check</th><th>Detail</th></tr>{rule_rows}</table>

<h2>Today's briefing run</h2>
<table>{timeline_html}</table>

<h2>Queue &amp; actions</h2>
<p><b>Pending Spotify uploads</b></p><ul>{pend_up_html}</ul>
<p>{button("Retry Spotify upload", "retry_upload", "",
           "Run the Spotify uploader for all pending episodes now?")}
{button("Producer dry-run", "producer_dry_run")}
{button("Rotate logs", "rotate_logs")}
{button("Script Factory test (staged)", "run_factory_staged", "",
        "Generate a full script with Claude Opus 5 into staging/ (costs ~$1, publishes nothing)?")}</p>
<p><b>Scripts in Drive</b></p>
<table>{script_rows}</table>

<h2>Upcoming topics</h2>
<p class="muted">From the <a href="{esc(CFG.get("topic_queue_doc", "#"))}">GK Daily
Topics doc</a>, in order — the top uncovered line is what the 5 AM task (or the
Script Factory failover) produces next. {esc(upcoming_note)}</p>
<table>{upcoming_html}</table>
<form method="post" action="action" style="margin:.6rem 0">
<input type="hidden" name="name" value="add_topic">
<input name="arg" size="44" maxlength="200" required
 placeholder="new-topic — angle, angle, angle">
 at position <input name="pos" size="3" inputmode="numeric" placeholder="end">
 <button>Add to queue</button>
<br><small class="muted">Position = row number above (1 = next up); leave blank
to add at the end. Writes straight into the topics doc.</small>
</form>

<h2>Topic Scout</h2>
<p class="muted">Proposals from the nightly scan; approving appends the topic
to the <a href="{esc(CFG.get("topic_queue_doc", "#"))}">GK Daily Topics doc</a>
(source of truth). {button("Run Scout now", "run_scout")}</p>
<table>{prop_rows}</table>
<details><summary>recent decisions</summary>
<table>{decided_rows or ""}</table></details>

<h2>Episodes (last 14 days)</h2>
<table><tr><th>Uploaded</th><th>Type</th><th>Title</th></tr>{ep_rows}</table>

<h2>Errors &amp; logs</h2>
<p><b>Today's error lines</b></p><pre>{err_html}</pre>
<p><b>Upload failure screenshots</b></p><p>{shots_html}</p>
{logs_html}

<h2>Recent actions</h2>
<table><tr><th>When</th><th>Action</th><th>Status</th><th></th></tr>{act_rows}</table>

<p><small>Auto-refreshes every 2 min · JSON at <a href="status">status</a>
· re-login (never via web): <code>cd ~/podcasts && python3 scripts/upload_spotify.py --login</code></small></p>
"""


# ----------------------------------------------------------------- handler --

class Handler(BaseHTTPRequestHandler):
    def _send(self, body: bytes, ctype: str, code: int = 200,
              extra: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path.endswith("/status"):
            self._send(json.dumps(GET_STATUS(), indent=2).encode(),
                       "application/json")
            return
        if "/shots/" in path:
            name = Path(path.rsplit("/", 1)[-1]).name
            f = CFG["podcasts_root"] / "logs" / "spotify-upload" / name
            if f.is_file() and f.suffix.lower() in (".png", ".jpg", ".jpeg"):
                ctype = "image/png" if f.suffix.lower() == ".png" else "image/jpeg"
                self._send(f.read_bytes(), ctype)
            else:
                self._send(b"not found", "text/plain", 404)
            return
        try:
            self._send(render_page().encode(), "text/html; charset=utf-8")
        except Exception as exc:
            log.exception("render failed")
            self._send(f"tower render error: {esc(exc)}".encode(),
                       "text/plain", 500)

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if not path.endswith("/action"):
            self._send(b"not found", "text/plain", 404)
            return
        length = int(self.headers.get("Content-Length", 0))
        form = urllib.parse.parse_qs(self.rfile.read(length).decode())
        name = (form.get("name") or [""])[0]
        arg = (form.get("arg") or [""])[0]
        dispatch(name, arg, form)
        self._send(b"", "text/plain", 303, {"Location": "."})

    def log_message(self, fmt, *args):
        pass
