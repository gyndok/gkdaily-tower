"""Retry delivery without regenerating any source or audio."""
import json
import sys
from pathlib import Path
import jobs
import tower

if __name__=='__main__':
    ident=sys.argv[1];cfg=tower.load_config();root=Path(cfg['podcasts_root'])
    sys.path.insert(0,str(root/'scripts'))
    from runtime import run_managed,uploader_python
    req=json.loads(jobs.get(ident)['request']);name=req['episode']
    path=root/'public/episodes/special_editions.json'
    meta=json.loads(path.read_text()) if path.exists() else {}
    title=tower.expected_title(name,meta)
    if not title:raise RuntimeError('Cannot identify episode title; refusing ambiguous delivery')
    jobs.checkpoint(ident,episode=name,title=title,stage='Retrying upload')
    result=run_managed([uploader_python(root),root/'scripts/upload_spotify.py'],timeout=1800)
    if result.returncode not in (0,75):sys.exit(1)
    sys.exit(jobs.verify_job(jobs.get(ident),cfg))
