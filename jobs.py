"""Durable production requests shared by launchd, Drive, Telegram and the UI."""
import json
import re
import sqlite3
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent
DB = BASE / 'jobs.db'
ACTIVE = ('queued', 'running', 'retry', 'verifying')

@contextmanager
def connect():
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY, created REAL, updated REAL, request TEXT,
        status TEXT, attempts INTEGER DEFAULT 0, ready REAL DEFAULT 0,
        checkpoint TEXT DEFAULT '{}', error TEXT DEFAULT '')''')
    conn.execute('CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, job_id TEXT, ts REAL, kind TEXT, detail TEXT)')
    conn.commit()
    try:
        with conn: yield conn
    finally: conn.close()


def _enqueue(request, permanent=False):
    encoded = json.dumps(request, sort_keys=True)
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        query = "SELECT id FROM jobs WHERE request=?"
        if not permanent: query += " AND status IN ('queued','running','retry','verifying')"
        row = conn.execute(query, (encoded,)).fetchone()
        if row: return row['id']
        ident = uuid.uuid4().hex
        conn.execute('INSERT INTO jobs (id,created,updated,request,status) VALUES (?,?,?,?,?)',
                     (ident,time.time(),time.time(),encoded,'queued'))
        conn.execute('INSERT INTO events(job_id,ts,kind,detail) VALUES (?,?,?,?)', (ident,time.time(),'queued','Request saved'))
    return ident


def enqueue(topic=None, quiet=False):
    return _enqueue({'topic':topic, 'quiet':quiet})


def enqueue_daily(edition='morning', day=None):
    if edition not in ('morning','evening'): raise ValueError('Invalid edition')
    day = day or datetime.now().strftime('%Y-%m-%d')
    datetime.strptime(day,'%Y-%m-%d')
    return _enqueue({'kind':'daily','edition':edition,'date':day,'quiet':False}, permanent=True)


def enqueue_upload(name):
    if Path(name).name != name or not name.endswith(".mp3"): raise ValueError("Invalid episode name")
    return _enqueue({"kind":"upload","episode":name,"quiet":True}, permanent=True)


def enqueue_script(path):
    # Intake persists only the path; worker hydrates with a bounded read before staging.
    return _enqueue({'kind':'script','source':str(Path(path).absolute()),'quiet':False}, permanent=True)


def get(ident):
    with connect() as conn:
        row=conn.execute('SELECT * FROM jobs WHERE id=?',(ident,)).fetchone()
    if row is None: raise RuntimeError('Unknown job')
    return dict(row)


def checkpoint(ident, **values):
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        row=conn.execute('SELECT checkpoint FROM jobs WHERE id=?',(ident,)).fetchone()
        state=json.loads(row[0]); state.update(values)
        if 'stage' in values: state['stage_at']=time.time()
        conn.execute('UPDATE jobs SET checkpoint=?,updated=? WHERE id=?',(json.dumps(state),time.time(),ident))
        if 'stage' in values:
            conn.execute('INSERT INTO events(job_id,ts,kind,detail) VALUES (?,?,?,?)',(ident,time.time(),'stage',values['stage']))


def action(ident, name):
    with connect() as conn:
        if name=='retry':
            sql="UPDATE jobs SET status='retry',attempts=0,ready=0,error='' WHERE id=? AND status='needs_attention'"
        elif name=='cancel':
            sql="UPDATE jobs SET status='cancelled' WHERE id=? AND status IN ('queued','retry')"
        else: raise ValueError('Unknown action')
        changed=conn.execute(sql,(ident,)).rowcount
        if changed: conn.execute('INSERT INTO events(job_id,ts,kind,detail) VALUES (?,?,?,?)',(ident,time.time(),name,'Requested from dashboard'))
        return bool(changed)


def work():
    import fcntl
    import os
    import tower
    cfg=tower.load_config()
    sys.path.insert(0,str(Path(cfg['podcasts_root'])/'scripts'))
    from runtime import run_managed
    with open(BASE/'.jobs.lock','a') as lock:
        try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: return
        with connect() as conn:
            conn.execute("UPDATE jobs SET status='retry' WHERE status='running'")
            row=conn.execute("SELECT * FROM jobs WHERE status IN ('queued','retry','verifying') AND ready<=? ORDER BY created LIMIT 1",(time.time(),)).fetchone()
            if row is None:return
            conn.execute("UPDATE jobs SET status='running',updated=? WHERE id=?",(time.time(),row['id']))
        saved=json.loads(row['checkpoint'])
        if not saved.get('started_at'): checkpoint(row['id'],started_at=time.time())
        req=json.loads(row['request'])
        import notifications
        notifications.safe_poll(cfg)
        try:
            refusal=''   # set only by a refusing run; must exist on every path
            if row['status']=='verifying':
                code=verify_job(dict(row),cfg)
            else:
                if req.get('kind')=='daily':
                    argv=[str(Path(cfg['factory']['python']).expanduser()),str(Path(cfg['podcasts_root'])/'run_pipeline.py'),'--direct','--edition',req['edition'],'--date',req['date']]
                elif req.get('kind')=='upload':
                    argv=[str(Path(cfg['factory']['python']).expanduser()),str(BASE/'upload_job.py'),row['id']]
                elif req.get('kind')=='script':
                    argv=[str(Path(cfg['factory']['python']).expanduser()),str(BASE/'script_job.py'),row['id']]
                else:
                    argv=[str(Path(cfg['factory']['python']).expanduser()),str(BASE/'gkdaily-special.py'),'--job-id',row['id']]
                proc=run_managed(argv,timeout=10800,pass_fds=(lock.fileno(),),
                                 env={**os.environ,'GK_JOB_ID':row['id'],'GK_QUIET':'1'})
                code=proc.returncode
                if code not in (0,2,4):
                    checkpoint(row['id'],last_error=(proc.stderr or proc.stdout or '')[-2000:])
                # Exit 3 is a REFUSAL, not a failure: the topic is already
                # covered, or no usable topic was given. Retrying can never
                # change that, so the reason has to reach the dashboard —
                # otherwise it reads as "Pipeline exit 3", indistinguishable
                # from a transient fault. On 2026-09-21 two Starlink requests
                # were refused (an episode existed since 09-03) and retried
                # three times from the dashboard because the screen gave no
                # reason to stop.
                if code==3:
                    m=re.search(r"Can't start: (.+)", (proc.stdout or '')+(proc.stderr or ''))
                    refusal=m.group(1).strip() if m else ''
            if code==0: checkpoint(row['id'],stage='verified_live',phase='live',verified_at=time.time())
            attempts=row['attempts']+(0 if code in (2,4) else 1)
            status='done' if code==0 else ('verifying' if code==4 else ('needs_attention' if attempts>=3 or code==3 else 'retry'))
            error=('' if code==0 else
                   'Awaiting public feed confirmation' if code==4 else
                   f'Refused — {refusal}' if code==3 and refusal else
                   'Refused: no usable topic (already covered, or empty)' if code==3 else
                   f'Pipeline exit {code}')
            if status=='verifying':
                saved=json.loads(get(row['id'])['checkpoint'])
                since=saved.get('verification_started') or time.time()
                checkpoint(row['id'],verification_started=since)
                if time.time()-since>7200:
                    status='needs_attention';error='Not confirmed live after two hours; reconcile before uploading again'
        except Exception as exc:
            attempts=row['attempts']+1; status='needs_attention' if attempts>=3 else 'retry';error=str(exc)[:500]
        with connect() as conn:
            conn.execute('UPDATE jobs SET status=?,attempts=?,ready=?,updated=?,error=? WHERE id=?',
                         (status,attempts,time.time()+min(1800,300*2**min(attempts,3)),time.time(),error,row['id']))
            conn.execute('INSERT INTO events(job_id,ts,kind,detail) VALUES (?,?,?,?)',(row['id'],time.time(),status,error))
        import notifications
        notifications.safe_poll(cfg)



def verify_job(row,cfg):
    sys.path.insert(0,str(Path(cfg['podcasts_root'])/'scripts'))
    from delivery import check
    saved=json.loads(row['checkpoint'])
    if not saved.get('episode') or not saved.get('title'): return 1
    result=check(cfg['podcasts_root'],saved['episode'],saved['title'],cfg['spotify_rss'])
    checkpoint(row['id'],delivery=result,stage=result['state'])
    return 0 if result['state']=='verified_live' else 4


def maybe_preflight(cfg, now):
    """Once daily after 05:30, off the supervisor thread; retain the result."""
    day = now.strftime('%Y-%m-%d')
    if now.strftime('%H:%M') < '05:30': return
    path = BASE / 'preflight.json'
    if path.exists():
        try:
            saved = json.loads(path.read_text())
            if saved.get('date') == day and all(c['ok'] for c in saved.get('checks',[])): return
        except ValueError: pass
    if getattr(maybe_preflight, 'running', False): return
    maybe_preflight.running=True
    import threading
    def go():
        try:
            sys.path.insert(0,str(Path(cfg['podcasts_root'])/'scripts'))
            from runtime import run_managed,atomic_json
            result=run_managed([str(Path(cfg['factory']['python']).expanduser()),Path(cfg['podcasts_root'])/'scripts/preflight.py'],timeout=90)
            try: checks=json.loads(result.stdout)
            except ValueError: checks=[{'name':'Preflight execution','ok':False,'detail':f'Exit {result.returncode}: '+(result.stderr or result.stdout or 'No diagnostic output')[-1200:]}]
            atomic_json(path,{'date':day,'checked_at':time.time(),'checks':checks})
        finally:maybe_preflight.running=False
    threading.Thread(target=go,daemon=True).start()


def retry_archives(cfg):
    """A separate bounded task; Drive availability never blocks delivery."""
    import threading
    if getattr(retry_archives, 'running', False): return
    retry_archives.running = True
    def go():
        try:
            sys.path.insert(0, str(Path(cfg['podcasts_root'])/'scripts'))
            from runtime import run_managed
            with connect() as conn:
                rows=conn.execute("SELECT id,checkpoint FROM jobs WHERE status IN ('done','verifying') ORDER BY created DESC LIMIT 30").fetchall()
            for row in rows:
                saved=json.loads(row['checkpoint'])
                source=saved.get('script')
                if not source or not str(source).endswith('.md') or saved.get('archived'):continue
                if time.time()-saved.get('archive_attempt',0)<1800:continue
                checkpoint(row['id'],archive_attempt=time.time())
                result=run_managed([sys.executable,BASE/'archive_script.py',source,Path(cfg['drive_gk_daily'])/'scripts/processed'],timeout=90)
                checkpoint(row['id'],archived=result.returncode==0,archive_error='' if result.returncode==0 else 'Drive archive unavailable; local source retained')
                break
        except Exception:
            # Source remains local; next tick may retry after the saved cooldown.
            pass
        finally: retry_archives.running=False
    threading.Thread(target=go,daemon=True).start()


def maybe_start(cfg):
    with connect() as conn:
        pending=conn.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running','retry','verifying') AND ready<=? LIMIT 1",(time.time(),)).fetchone()
    if pending:
        log=Path.home()/'Library/Logs/gkdaily-special.log'
        log.parent.mkdir(parents=True,exist_ok=True)
        with log.open('a') as stream:
            subprocess.Popen([sys.executable,str(BASE/'jobs.py'),'--work'],stdin=subprocess.DEVNULL,stdout=stream,stderr=stream,start_new_session=True)


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--work',action='store_true');parser.add_argument('--retry')
    args=parser.parse_args()
    if args.work:work()
    elif args.retry:print(action(args.retry,'retry'))
    else:
        with connect() as conn:
            for row in conn.execute('SELECT id,status,attempts,error FROM jobs ORDER BY created DESC LIMIT 20'):print(dict(row))
