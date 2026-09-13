"""Small JSON boundary for Tower's static browser UI; no process execution on GET."""
import json
import time
from pathlib import Path
import jobs
from progress import describe
import logging


def snapshot(dashboard):
    with jobs.connect() as conn:
        rows=conn.execute('SELECT * FROM jobs ORDER BY created DESC LIMIT 100').fetchall()
        events=conn.execute('SELECT * FROM events ORDER BY id DESC LIMIT 200').fetchall()
    result=[]
    for row in rows:
        req=json.loads(row['request']);state=json.loads(row['checkpoint'])
        result.append({k:row[k] for k in ('id','created','updated','status','attempts','error')} | {
            'title':state.get('title') or req.get('topic') or Path(req.get('source','')).name or f"{req.get('date','')} {req.get('edition','Special')} edition",
            'kind':req.get('kind','special'), 'stage':state.get('stage','Queued'),
            'verified':bool(state.get('verified_at') or state.get('delivery',{}).get('verified_at')),
            'episode':state.get('episode'), 'delivery':state.get('delivery'),
            'last_error':state.get('last_error'),
            'progress':describe(dict(row), state),
        })
    status=dashboard.GET_STATUS()
    records_path=dashboard.CFG['podcasts_root']/'config/delivery.json'
    records=json.loads(records_path.read_text()) if records_path.exists() else {}
    episodes=dashboard.episode_rows()
    for ep in episodes:
        ep['delivery']=records.get(ep['name'])
        ep['late_upload']=not ep['special'] and ep['uploaded'][11:16] > '06:35'
        # Preserve stored Tower feed evidence for pre-migration episodes.
        if not ep['delivery']:
            rule=next((r for r in status.get('rules',[]) if r['id']=='live:'+ep['name']),None)
            if rule and rule['state']=='ok':ep['delivery']={'state':'verified_live','checked_at':status.get('ts')}
    preflight_path=jobs.BASE/'preflight.json'
    preflight=json.loads(preflight_path.read_text()) if preflight_path.exists() else {}
    return {'preflight':preflight,'csrf':dashboard.CSRF_TOKEN,'server_time':time.time(),'status':status,
            'jobs':result,'events':[dict(e) for e in events], 'episodes':episodes}


def mutate(body,dashboard):
    action=body.get('action')
    if action=='enqueue':
        topic=' '.join(str(body.get('topic','')).split())
        if not 3<=len(topic)<=1000:raise ValueError('Enter a topic between 3 and 1,000 characters')
        ident=jobs.enqueue(topic)
        try: jobs.maybe_start(dashboard.CFG)
        except Exception: logging.getLogger(__name__).exception('Request saved; worker will start on next tick')
        return {'id':ident,'message':'Request saved. Tower will start it automatically.'}
    if action=='daily':return {'id':jobs.enqueue_daily(),'message':'Morning edition saved.'}
    if action in ('retry','cancel'):
        if not jobs.action(str(body.get('id','')),action):raise ValueError('This job changed; refresh and try again')
        return {'message':'Job '+('queued for retry' if action=='retry' else 'cancelled')}
    if action=='reconcile':
        ident=str(body.get('id',''));row=jobs.get(ident)
        if row['status'] not in ('needs_attention','verifying'):raise ValueError('Job is not awaiting reconciliation')
        state=json.loads(row['checkpoint'])
        if not state.get('episode'):raise ValueError('No rendered episode to verify')
        with jobs.connect() as conn:
            changed=conn.execute("UPDATE jobs SET status='verifying',ready=0 WHERE id=? AND status IN ('needs_attention','verifying')",(ident,)).rowcount
            if not changed:raise ValueError('Job changed; refresh')
        jobs.checkpoint(ident,verification_started=time.time())
        return {'message':'Publication check queued. This does not upload another copy.'}
    if action in ('edit_topic','swap_topic','add_topic'):
        if action=='add_topic':
            import scout
            line=' '.join(str(body.get('topic','')).split())
            if not 3<=len(line)<=1000:raise ValueError('Enter a topic between 3 and 1,000 characters')
            message=('Topic is already in the queue.' if line in scout.gdoc_lines(dashboard.CFG) else scout.gdoc_insert(dashboard.CFG,line))
        else:
            import topic_editor
            message=topic_editor.edit(dashboard.CFG,body.get('old'),
                new=body.get('topic') if action=='edit_topic' else None,
                swap=body.get('swap') if action=='swap_topic' else None)
        dashboard._mark_upcoming_stale()
        return {'message':message}
    raise ValueError('Unknown action')
