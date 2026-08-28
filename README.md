# GK Daily Control Tower

Supervisor for the GK Daily podcast pipelines on the Mac mini. Stdlib-only
Python (no venv for the tower itself), SQLite state, launchd-managed
(`com.geffrey.gkdaily-tower`, KeepAlive). Dashboard at `/tower/` on the
tailnet nginx (:8888 → 127.0.0.1:8891).

| Module | Job |
|---|---|
| `tower.py` | Watchtower daemon: collectors over the pipelines' logs/ledgers, absence-detection rules with deadlines, Spotify live verification against the anchor.fm feed, Telegram alerts + 07:00 daily digest, HTTP server. `--check` = dry run. |
| `dashboard.py` | Web UI: checks, stage timeline, upcoming-topics queue (from the topics gdoc), episode history, merged logs, screenshot gallery, whitelisted one-click actions. |
| `scout.py` | P3 Topic Scout: nightly (21:00) kimi-k2.6 pass over news feeds + back-catalogue + gdoc → 5 candidates in Drive `queue.json` + Telegram digest. Approve (button or 24 h auto) appends to the topics gdoc via `gws docs +write`. |
| `factory.py` | P4 Script Factory (failover-only): weekdays, if no special-edition script is in Drive by 07:30 CT, picks the top uncovered topic and writes the script with Claude Opus 5 + web search, delivering to `GK Daily/scripts/` for the untouched producer. Runs under the podcasts venv (needs `anthropic`). `--plan` / `--run [--stage]`. |
| `gkdaily-special.py` | **On-demand producer.** One command from a topic to a verified-live Spotify episode, synchronously. Runs under the podcasts venv. Symlinked as `~/clawd/gkdaily-special.py`. |
| `docs/dispatch-route.sh` | The GK Daily block for `~/clawd/dispatch.sh` (that file is untracked, so this is the reconstructible copy). |
| `config.json` | All paths, deadlines, models, feeds. |
| `launchd/` | Copy of the LaunchAgent plist (live copy: `~/Library/LaunchAgents/`). |

Design doc: `~/podcasts/docs/PRD-gk-daily-control-tower.md`.

The tower only **reads** the pipelines (logs, ledgers, folders, launchctl);
the sole write paths are the explicit dashboard actions and the Factory's
script delivery. Credentials never pass through the web layer.

---

## On-demand special editions

Added 2026-08-28. Before this, every path into the special-edition pipeline
was event-driven: a script landed in Drive and a launchd `WatchPaths` trigger
with a 300 s throttle eventually started the producer, whose Spotify upload
stage is deliberately non-fatal. Nothing was ever lost — an audit of the full
history found 43/43 scripts rendered and every render on Spotify — but an
episode could sit unstarted or un-uploaded for hours until someone noticed.
Two concrete cases: maternal-mortality retried 18 times across 33 hours, and
the Mars episode sat until it was run by hand.

`gkdaily-special.py` is the synchronous answer. It does not return until the
episode is verified live in the public RSS feed:

```
gkdaily-special.py --topic "the physics of fusion ignition"   # any subject
gkdaily-special.py --next        # top unproduced line from the topics gdoc
gkdaily-special.py --status      # in flight / published today / queue depth
gkdaily-special.py --next --quiet    # stdout only, no Telegram
```

Stages: resolve topic (explicit, else `factory.pick_topic`) → `factory.generate`
(Opus 5 + web search) → `factory.deliver` to Drive → **producer called directly
with `--script`**, bypassing the WatchPaths throttle entirely → verify the mp3
reached the Spotify ledger, re-running the uploader up to 3× → poll the
anchor.fm feed for the title (12 min). Telegram gets a line at each milestone.
An `flock` at `~/clawd/.gkdaily-special.lock` prevents two concurrent runs.
Exit codes: 0 published, 1 failed, 2 already running, 3 no usable topic.

It reads the produced filename off disk rather than predicting it, because the
producer derives its own slug and date from the script filename.

### Three front ends, one script

1. **Telegram → MiniBot** — "make the next gk daily special", "I want a gk
   daily special on undersea cables", "gk daily status". The skill block and
   the hard-trigger words (`podcast`, `episode`, `gkdaily`) live in
   `~/minibot/brain.py` so these reach the tool loop instead of falling to the
   free, tool-less chat rung. MiniBot fires it detached and does not wait.
2. **`~/clawd/dispatch.sh`** — see `docs/dispatch-route.sh`.
3. **Direct / ssh.**

Progress goes through **MiniBot's own bot** (`@Gyndok_notes_bot`, credentials
from `~/minibot/.env`), so the stage updates land in the same thread the
request was typed into. A DM's chat id equals the user id, so the first entry
in `ALLOWED_USER_IDS` is the destination when no `TELEGRAM_CHAT_ID` is set.
If those credentials are missing it falls back to the tower's bot
(`@gyndokFinance_bot`, `~/clawd/.env`) — a message in the wrong thread still
beats silence. Tower alerts and the 07:00 digest stay on the tower bot.

## Upload auto-retry

The `uploads_pending` rule used to alert and wait for a human. It now acts: an
episode that rendered but never reached the ledger triggers a re-run of
`upload_spotify.py` after 20 minutes, rate-limited to one attempt per 30
minutes, `pgrep`-guarded so it never stacks on a running uploader, with the
outcome reported to Telegram either way. Config: `upload_retry` in
`config.json`. This covers episodes made by *any* path, not just the on-demand
one. Companion to `producer_nudge`, which starts a script the WatchPaths
trigger missed.
