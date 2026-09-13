"""Translate durable evidence into UI progress without invented percentages."""
PHASES = ('research', 'audio', 'upload', 'verify')


def describe(row, checkpoint):
    status=row['status'];stage=str(checkpoint.get('stage','')).lower()
    verified=bool(checkpoint.get('verified_at') or checkpoint.get('delivery',{}).get('verified_at'))
    phase=checkpoint.get('phase')
    if status=='verifying':phase='verify'
    elif status=='done':phase='live' if verified else 'legacy'
    elif status=='queued':phase='queued'
    elif phase not in PHASES:
        if 'verif' in stage or 'confirmation' in stage or 'publication_uncertain' in stage:phase='verify'
        elif 'upload' in stage and 'audio' not in stage:phase='upload'
        elif any(word in stage for word in ('audio','render','packag')):phase='audio'
        elif any(word in stage for word in ('research','writ','script','news')):phase='research'
        else:phase='queued'
    total=checkpoint.get('audio_total');complete=checkpoint.get('audio_completed')
    audio=None
    if isinstance(total,int) and total>0 and isinstance(complete,int):
        audio={'completed':max(0,min(complete,total)),'total':total}
    return {'phase':phase,'audio':audio,'words':checkpoint.get('words'),
            'started_at':checkpoint.get('started_at'),
            'activity_at':checkpoint.get('stage_at',row['updated']),
            'retry_at':row.get('ready') if status=='retry' else None,
            'verified':verified,
            'listen_url':checkpoint.get('listen_url') or checkpoint.get('delivery',{}).get('url')}
