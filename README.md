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
| `config.json` | All paths, deadlines, models, feeds. |
| `launchd/` | Copy of the LaunchAgent plist (live copy: `~/Library/LaunchAgents/`). |

Design doc: `~/podcasts/docs/PRD-gk-daily-control-tower.md`.

The tower only **reads** the pipelines (logs, ledgers, folders, launchctl);
the sole write paths are the explicit dashboard actions and the Factory's
script delivery. Credentials never pass through the web layer.
