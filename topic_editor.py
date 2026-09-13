"""Revision-guarded topic text edits. Preserve document structure and other content."""
import json
import subprocess


def edit(cfg, old, new=None, swap=None, delete=False):
    if delete and (new is not None or swap is not None):raise ValueError("Delete cannot be combined with an edit or swap")
    if not isinstance(old,str) or not old.strip():raise ValueError('Select a topic first')
    if new is not None:
        new=' '.join(str(new).split())
        if not 3<=len(new)<=1000:raise ValueError('Topic must contain 3–1,000 characters')
    doc_id=cfg['scout']['topic_doc_id']
    result=subprocess.run(['/opt/homebrew/bin/gws','docs','documents','get','--params',json.dumps({'documentId':doc_id})],capture_output=True,text=True,timeout=60)
    if result.returncode:raise RuntimeError('Could not read the topic document')
    doc=json.loads(result.stdout);revision=doc.get('revisionId')
    if not revision:raise RuntimeError('Document did not supply a revision; no edit made')
    paragraphs=[];past=False
    for element in doc.get('body',{}).get('content',[]):
        paragraph=element.get('paragraph')
        if not paragraph:continue
        raw=''.join(part.get('textRun',{}).get('content','') for part in paragraph.get('elements',[]))
        if raw.strip().startswith('----'):past=True;continue
        if past and raw.strip():paragraphs.append((element['startIndex'],raw.rstrip('\n')))
    def one(text):
        matches=[p for p in paragraphs if p[1].strip()==text]
        if len(matches)!=1:raise ValueError('Topic changed or is ambiguous; reload the queue')
        return matches[0]
    first=one(old)
    replacements=[(first,new)] if swap is None else [(first,one(swap)[1]),(one(swap),first[1])]
    requests=[]
    for (index,text),replacement in sorted(replacements,reverse=True):
        if replacement is None and not delete:raise ValueError('No replacement supplied')
        end=index+len(text.encode('utf-16-le'))//2
        requests.append({'deleteContentRange':{'range':{'startIndex':index,'endIndex':end}}})
        if not delete:
            requests.append({'insertText':{'location':{'index':index},'text':replacement}})
    body={'writeControl':{'requiredRevisionId':revision},'requests':requests}
    result=subprocess.run(['/opt/homebrew/bin/gws','docs','documents','batchUpdate','--params',json.dumps({'documentId':doc_id}),'--json',json.dumps(body)],capture_output=True,text=True,timeout=60)
    if result.returncode:raise RuntimeError('Topic update failed or document changed. Reload before trying again.')
    return 'Topic deleted.' if delete else 'Topic queue updated.'
