# GK Daily reliability audit — September 9, 2026

The running Mac was inspected and the fixes below were installed with backups. The Tower returned **green** at 10:43:59 Central. There were 114 upload-ledger entries, none marked unverified. Today's morning briefing and two specials passed the Tower's feed checks. A headless browser successfully opened the existing Spotify session. No episode was generated or published as an audit test.

## How production currently works

| Entry | Script | Audio and distribution |
|---|---|---|
| Daily news, 06:00 | Mac launchd runs `podcasts/run_pipeline.py --edition morning` | Local pipeline renders, then invokes the Spotify uploader |
| Scheduled special | External scheduled author reads **GK Daily Topics**, expected around 05:00; Drive holds the script | Producer has weekday 05:30 and Drive-change triggers; Tower nudges missed scripts |
| Missing scheduled special | Tower's weekday 08:05 Factory fallback reads the topic document | Factory delivers to Drive; same producer/uploader |
| Telegram special | MiniBot invokes `gkdaily-special.py --detach` | Now enters a durable local queue, saves its script, invokes producer, checks exact audio, uploader ledger and public feed |
| Topic discovery | Tower runs Scout at 21:00; proposals can be auto-approved after 24 hours | Approved topics are appended to the Google Doc |

The live Google Doc and the Mac's own `gws` credentials were both readable. The external 05:00 author's actual scheduler configuration is outside this repository and was not independently verified. The README and topic document describe that schedule; the local 08:05 fallback is verified in code. The document is a topic queue, not itself a scheduler.

## Installed fixes

| Finding | Change |
|---|---|
| A detached Telegram request vanished on process loss or was refused when busy | SQLite FIFO, active-request deduplication, persisted script checkpoints, worker recovery, three bounded attempts and explicit needs-attention state |
| Concurrent producers could move the same script out from under one another | Shared process lock covering scheduled, direct and on-demand producer invocations; recognizes an input already archived by a preceding run |
| On-demand code selected the newest audio, possibly another episode | Exact date/slug filename matching; missing or empty expected audio fails |
| Feed check accepted a title prefix and returned success even without verification | Exact normalized RSS item title; exit 4 for awaiting verification |
| Crash after Spotify Publish could trigger a duplicate | Persist `UNVERIFIED` before the click; do not automatically resubmit ambiguous publication |
| Uploader read its ledger before acquiring the lock | Lock now precedes ledger reads, including seeding |
| Tower reconciliation could overwrite uploader state | Same uploader lock plus reread inside the lock; atomic ledger replacement |
| Routine uploader launched a visible browser | Headless for uploads; interactive login remains available |
| Dashboard actions lacked cross-site request protection | Per-process form token, request-size limit, socket timeout, no-store and anti-framing headers |
| `--check` could reconcile ledger state and notify | Quiet checks skip reconciliation and worker startup |
| Truncated model replies could become episodes | Reject token-limit termination; preserve continuation history |
| A model outage caused automatic publication without live research | Default is to hold; `allow_unresearched_fallback` is an explicit opt-in |
| Unreadable Scout queue silently became an empty queue | Fail while preserving existing data; atomic replacement on successful writes |

The queue preserves detached requests once the Mac receives them. It does not turn Telegram into an off-machine job server. Direct synchronous invocations and the existing scheduled author still use their established paths. Scripts saved by the new queue live in `job-scripts/`; protect these and `jobs.db` in backups.

## Verification

13 offline regression tests passed. They exercise restart recovery, bounded retries, busy-worker handling, queue deduplication and checkpoints, atomic-write failures, exact audio identity, RSS matching, empty topics, research failures, dry-check side effects, dashboard request protection, shared ledger locking and corrupt Scout data. Python parsing and diff whitespace checks passed. Live HTTP dashboard returned 200 with protected forms; the Tower returned green; local Google Docs access and headless Spotify login succeeded.

No end-to-end TTS/publication test, reboot test, network-outage test or offsite phone test was performed. A successful headless login does not prove every step of Spotify's upload wizard. Observe the next normal episode for that final confirmation.

## Recommended next work, in priority order

1. **Move audio distribution to a host/feed with a supported publishing interface.** Keep Spotify as a listening destination while removing its browser wizard from the critical path. Spotify supports shows hosted elsewhere ([official guidance](https://support.spotify.com/us/creators/article/claiming-your-podcast-on-spotify-for-creators/)). Select the host after checking its upload API, storage, cost and migration support; preserve the existing show and episode identifiers. No hosting migration was made.
2. **Add an off-machine dead-man monitor.** The Tower cannot report its own power or internet failure. An independent service should alert only when a heartbeat or expected public episode is missing. No monitoring service/account was selected or created.
3. **Make restart recovery independent of desktop login.** Tower, Drive sync and MiniBot are currently login services. The Mac has sleep disabled and automatic power restart enabled, but that does not prove recovery to a usable user session. Use a tested remote-login recovery procedure and UPS, or move the queue/scheduler to an always-on service. FileVault was already off; no encryption or login settings were changed.
4. **Unify all production around episode IDs and stage checkpoints.** Extend the durable queue to the external scheduled author and daily briefing, with daily catch-up after missed schedules. Current morning launchd has no `RunAtLoad` catch-up; Factory's date row also prevents a same-day retry after its first failure. These paths still rely on existing alerts/nudges rather than the new on-demand queue.
5. **Remove desktop Drive synchronization from the production handoff.** Use Drive/Docs APIs to fetch scripts into a local spool; retain remote archives. Existing Drive streaming placeholders can temporarily fail reads. The Scout warning observed during audit was intermittent: the file subsequently parsed with 52 candidates.
6. **Harden MiniBot's command boundary.** It currently permits everyone if `ALLOWED_USER_IDS` is empty. Make the default deny access and add deterministic podcast commands that bypass model availability and shell quoting. Its shared general-purpose bot was inspected but not modified by this change.
7. **Back up production state off the Mac.** Include scripts with sources, metadata, upload ledger, queue database and configuration. Keep active production on internal storage and use the external T7 for archives; a disconnected archive volume is currently load-bearing. Do not back up session tokens to a public repository.

Exact title verification reduces false positives but is not an episode-ID reconciliation protocol. Ambiguous Spotify uploads intentionally require attention rather than a risky duplicate. A request can finish rendering and remain in needs-attention while Spotify ingestion is slow; Tower feed reconciliation still clears its upload tag when visible.

## Using it away from the desk

Keep using the existing Telegram requests: “make the next gk daily special,” “gk daily special on [topic],” and “gk daily status.” Detached requests now receive a queue ID; status includes unfinished jobs and their attempt count. The Mac must be powered, online, and running its user services.

For an operator, `python3 jobs.py` lists recent jobs. `python3 jobs.py --retry FULL_JOB_ID` requeues a needs-attention job, retaining the original script and upload ledger. Never delete an ambiguous upload's ledger entry just to force a retry; first establish whether Spotify already published it. Reauthentication remains the existing uploader's `--login` flow, accessible using secure remote desktop.

## Deployment and rollback

Installed Tower files: `tower.py`, `dashboard.py`, `factory.py`, `scout.py`, `gkdaily-special.py`, `jobs.py`, `reliability.py`, `config.json`. Companion fixes are captured as `companion/producer.patch` and `companion/uploader.patch` against the inspected live sources outside this repository.

Original files are saved locally under the audit workspace's `work/pre-hardening-backup/`; deployment targets and original SHA-256 values are in `work/deploy-manifest.json` (Scout is the additional `9-scout.py` backup). Runtime state and account credentials were not replaced. To roll back, stop new requests, let active production finish, restore those original files to their corresponding targets, and restart the Tower. Retain the job database and archived scripts for recovery; do not replay completed jobs. The installed live checkout contains these uncommitted application changes; the review branch is the reproducible source of the Tower changes.
