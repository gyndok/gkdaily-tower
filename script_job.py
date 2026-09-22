"""Hydrate once into local staging, then run the compatible special producer."""
import importlib.util
import json
import os
import sys
from pathlib import Path
import jobs
import tower


def execute(ident):
    cfg=tower.load_config(); root=Path(cfg['podcasts_root'])
    sys.path.insert(0,str(root/'scripts'))
    from runtime import run_managed,atomic_json
    row=jobs.get(ident);req=json.loads(row['request']);saved=json.loads(row['checkpoint'])
    source=Path(req['source'])
    spec=importlib.util.spec_from_file_location('producer',Path.home()/'clawd/produce-special-podcast.py')
    producer=importlib.util.module_from_spec(spec);spec.loader.exec_module(producer)
    match=producer.NAME_RE.match(source.stem)
    if not match:
        if source.exists(): producer.quarantine(source,'Invalid script filename')
        jobs.checkpoint(ident,stage='Invalid input',last_error='Invalid script filename; moved to rejected')
        return 3
    date,slug=match.groups()
    name=f'special-edition-{slug.lower()}-{date}.mp3'
    staging=jobs.BASE/'job-scripts'/ident
    staging.mkdir(parents=True,exist_ok=True)
    local=staging/source.name
    meta_path=root/'public/episodes/special_editions.json'
    metadata=json.loads(meta_path.read_text()) if meta_path.exists() else {}
    if name not in metadata:
        jobs.checkpoint(ident,stage='Reading Drive script')
        if not local.exists():
            # Google Drive's file provider wedges periodically while the
            # account is perfectly healthy — a hanging read on 2026-09-10, and
            # OSError EDEADLK "Resource deadlock avoided" on 2026-09-21, which
            # cost the Bennu episode ten hours. The bytes stay reachable over
            # the Drive API throughout, so a local read failure must not end
            # the episode. produce-special-podcast.py grew this fallback on
            # 09-10, but this module is the path that actually runs now and
            # had its own unprotected /bin/cat.
            result=run_managed(['/bin/cat',source],timeout=180)
            text=result.stdout if result.returncode==0 else None
            if text is None:
                jobs.checkpoint(ident,stage='Reading Drive script over the API')
                text=producer.fetch_from_drive_api(source.name)
            if not text:
                raise RuntimeError('Cannot read Drive script: the local file '
                                   'provider failed and the Drive API could '
                                   'not supply it either')
            if len(text.split())<50:
                producer.quarantine(source,'Script contains fewer than 50 words')
                return 3
            temp=local.with_suffix('.tmp');temp.write_text(text);temp.replace(local)
        jobs.checkpoint(ident,script=str(local),stage='Rendering special edition')
        result=run_managed([Path.home()/'clawd/.venv/bin/python3',Path.home()/'clawd/produce-special-podcast.py','--script',local],timeout=7200,env={**os.environ,'GK_QUIET':'1'})
        if result.returncode: raise RuntimeError((result.stderr or '')[-1500:])
        metadata=json.loads(meta_path.read_text())
    title=metadata[name]['title']
    jobs.checkpoint(ident,episode=name,title=title,stage='Verifying publication')
    # Archive source only if unchanged since staging, or already recorded as duplicate.
    if source.exists() and not local.exists():
        producer.archive_duplicate(source)
    elif source.exists() and local.exists():
        result=run_managed(['/bin/cat',source],timeout=180)
        if result.returncode==0 and result.stdout==local.read_text():producer.archive_duplicate(source)
    return jobs.verify_job(jobs.get(ident),cfg)

if __name__=='__main__':sys.exit(execute(sys.argv[1]))
