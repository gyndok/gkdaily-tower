"""Durable episode notifications; failed Telegram sends remain pending."""
import json
import time
import fcntl
import jobs


def poll(cfg):
    import tower
    # Worker and supervisor can both poll. Only one may send at a time.
    with open(jobs.DB.with_suffix('.notifications.lock'), 'a') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: return
        with jobs.connect() as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS notification_watch (job_id TEXT PRIMARY KEY, state TEXT)')
            conn.execute('''CREATE TABLE IF NOT EXISTS notification_outbox (
                id INTEGER PRIMARY KEY, job_id TEXT, message TEXT, sent REAL,
                attempts INTEGER DEFAULT 0, ready REAL DEFAULT 0)''')
            rows=conn.execute('''SELECT j.*, w.state AS previous FROM jobs j
                LEFT JOIN notification_watch w ON w.job_id=j.id
                WHERE j.status IN ('queued','running','retry','verifying') OR w.job_id IS NOT NULL''').fetchall()
            for row in rows:
                req=json.loads(row['request'])
                if req.get('quiet'): continue
                state=f"{row['status']}:{row['attempts']}"
                if state==row['previous']: continue
                conn.execute('INSERT OR REPLACE INTO notification_watch VALUES (?,?)',(row['id'],state))
                saved=json.loads(row['checkpoint'])
                title=saved.get('title') or req.get('topic') or req.get('source') or 'Daily episode'
                text={'queued':'Request saved. Waiting for the production worker.',
                      'running':'Production is underway.' if not row['attempts'] else 'Production is underway again using saved work.',
                      'retry':'Production hit a problem. Tower will retry automatically using saved work.',
                      'verifying':'Audio is uploaded. Waiting for public feed confirmation.',
                      'done':'Verified live on Spotify.',
                      'needs_attention':'Production needs attention: '+(row['error'] or 'Open Tower for details.'),
                      'cancelled':'Request cancelled.'}.get(row['status'])
                if text:
                    conn.execute('INSERT INTO notification_outbox(job_id,message) VALUES (?,?)',(row['id'],f'GK Daily: {title}\n{text}'))
            pending=conn.execute('SELECT * FROM notification_outbox WHERE sent IS NULL AND ready<=? ORDER BY id LIMIT 10',(time.time(),)).fetchall()
        for item in pending:
            ok=tower.telegram(cfg,item['message'])
            with jobs.connect() as conn:
                conn.execute('UPDATE notification_outbox SET sent=?,attempts=attempts+1,ready=? WHERE id=?',
                    (time.time() if ok else None,time.time()+min(1800,60*2**min(item['attempts'],5)),item['id']))
                conn.execute('INSERT INTO events(job_id,ts,kind,detail) VALUES (?,?,?,?)',
                    (item['job_id'],time.time(),'notification','Telegram accepted notification' if ok else 'Telegram send failed; queued for retry'))


def safe_poll(cfg):
    try: poll(cfg)
    except Exception:
        import logging
        logging.getLogger(__name__).exception('Episode notification check failed')


def maybe_poll(cfg):
    import threading
    if getattr(maybe_poll,'running',False):return
    maybe_poll.running=True
    def go():
        try:poll(cfg)
        except Exception:
            import logging
            logging.getLogger(__name__).exception('Episode notification check failed')
        finally:maybe_poll.running=False
    threading.Thread(target=go,daemon=True).start()
