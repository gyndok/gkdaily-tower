"""Durable FIFO for detached requests. Tower restarts resume persisted scripts."""
import json
from contextlib import contextmanager
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path

BASE = Path(__file__).resolve().parent
DB = BASE / 'jobs.db'


@contextmanager
def connect():
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, created REAL, updated REAL, request TEXT,
        status TEXT, attempts INTEGER DEFAULT 0, ready REAL DEFAULT 0,
        checkpoint TEXT DEFAULT '{}', error TEXT DEFAULT '')''')
    conn.commit()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def enqueue(topic=None, quiet=False):
    request = json.dumps({'topic': topic, 'quiet': quiet}, sort_keys=True)
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute("SELECT id FROM jobs WHERE request=? AND status IN ('queued','running','retry')", (request,)).fetchone()
        if row:
            return row['id']
        ident = uuid.uuid4().hex
        conn.execute('INSERT INTO jobs (id,created,updated,request,status) VALUES (?,?,?,?,?)',
                     (ident, time.time(), time.time(), request, 'queued'))
    return ident


def get(ident):
    with connect() as conn:
        row = conn.execute('SELECT * FROM jobs WHERE id=?', (ident,)).fetchone()
    if row is None:
        raise RuntimeError('unknown job')
    return dict(row)


def checkpoint(ident, **values):
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('SELECT checkpoint FROM jobs WHERE id=?', (ident,)).fetchone()
        state = json.loads(row[0])
        state.update(values)
        conn.execute('UPDATE jobs SET checkpoint=?,updated=? WHERE id=?',
                     (json.dumps(state), time.time(), ident))


def work():
    import tower
    cfg = tower.load_config()
    # The child inherits the worker lock: killing just the worker cannot
    # let another worker retry while the existing producer is still alive.
    with open(BASE / '.jobs.lock', 'a') as lock:
        import fcntl
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        with connect() as conn:
            conn.execute("UPDATE jobs SET status='retry' WHERE status='running'")
            row = conn.execute("SELECT * FROM jobs WHERE status IN ('queued','retry') AND ready<=? ORDER BY created LIMIT 1", (time.time(),)).fetchone()
            if row is None:
                return
            conn.execute("UPDATE jobs SET status='running',attempts=attempts+1,updated=? WHERE id=?", (time.time(), row['id']))
        try:
            proc = subprocess.run([str(Path(cfg['factory']['python']).expanduser()),
                                   str(BASE / 'gkdaily-special.py'), '--job-id', row['id']],
                                  pass_fds=(lock.fileno(),))
            code = proc.returncode
            attempts = row['attempts'] + (0 if code == 2 else 1)
            status = 'done' if code == 0 else ('needs_attention' if attempts >= 3 or code == 3 else 'retry')
            error = '' if code == 0 else f'pipeline exit {code}'
        except Exception as exc:
            attempts = row['attempts'] + 1
            status = 'needs_attention' if attempts >= 3 else 'retry'
            error = type(exc).__name__
        with connect() as conn:
            conn.execute('UPDATE jobs SET status=?,attempts=?,ready=?,updated=?,error=? WHERE id=?',
                         (status, attempts, time.time()+300, time.time(), error, row['id']))
        if status == 'needs_attention':
            request = json.loads(row['request'])
            if not request['quiet']:
                tower.telegram(cfg, f'GK Daily job {row["id"][:8]} needs attention after {attempts} attempts. Saved script/checkpoint retained. {error}')


def maybe_start(cfg):
    with connect() as conn:
        pending = conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running','retry') AND ready<=? LIMIT 1", (time.time(),)).fetchone()
    if pending:
        log = Path.home() / 'Library/Logs/gkdaily-special.log'
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open('a') as stream:
            subprocess.Popen([sys.executable, str(BASE / 'jobs.py'), '--work'],
                             stdin=subprocess.DEVNULL, stdout=stream, stderr=stream,
                             start_new_session=True)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--work', action='store_true')
    parser.add_argument('--retry', metavar='JOB_ID')
    args = parser.parse_args()
    if args.work:
        work()
    elif args.retry:
        with connect() as conn:
            count = conn.execute("UPDATE jobs SET status='retry',attempts=0,ready=0 WHERE id=? AND status='needs_attention'", (args.retry,)).rowcount
        print(f'{count} job(s) queued for retry; existing script and upload ledger retained.')
    else:
        with connect() as conn:
            for row in conn.execute('SELECT id,status,attempts,error FROM jobs ORDER BY created DESC LIMIT 20'):
                print(dict(row))
