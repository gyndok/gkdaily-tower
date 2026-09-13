import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
import jobs
import tower

class WorkerTests(unittest.TestCase):
    def test_restart_resumes_abandoned_job_and_keeps_checkpoint(self):
        with tempfile.TemporaryDirectory() as d, patch.object(jobs,'BASE',Path(d)), patch.object(jobs,'DB',Path(d)/'jobs.db'), patch.object(tower,'load_config',return_value={'factory':{'python':'/usr/bin/python3'},'podcasts_root':Path(d),'spotify_rss':'https://example.com/rss'}), patch('runtime.run_managed',return_value=Mock(returncode=0)) as run:
            ident=jobs.enqueue('topic',True)
            jobs.checkpoint(ident,script='/saved/original.md')
            with jobs.connect() as conn:
                conn.execute("UPDATE jobs SET status='running' WHERE id=?",(ident,))
            jobs.work()
            row=jobs.get(ident)
            self.assertEqual(row['status'],'done')
            self.assertEqual(json.loads(row['checkpoint'])['script'],'/saved/original.md')
            self.assertEqual(run.call_args.args[0][-2:],['--job-id',ident])
            self.assertTrue(run.call_args.kwargs['pass_fds'])

    def test_verification_does_not_repeat_production(self):
        with tempfile.TemporaryDirectory() as d, patch.object(jobs,'BASE',Path(d)), patch.object(jobs,'DB',Path(d)/'jobs.db'), patch.object(tower,'load_config',return_value={'factory':{'python':'/usr/bin/python3'},'podcasts_root':Path(d),'spotify_rss':'https://example.com/rss'}), patch('runtime.run_managed',return_value=Mock(returncode=4)) as produce, patch.object(jobs,'verify_job',return_value=4), patch.object(tower,'telegram') as telegram:
            ident=jobs.enqueue('topic',True)
            for attempt in range(3):
                with jobs.connect() as conn:
                    conn.execute('UPDATE jobs SET ready=0 WHERE id=?',(ident,))
                jobs.work()
            self.assertEqual(jobs.get(ident)['status'],'verifying')
            self.assertEqual(jobs.get(ident)['attempts'],0)
            self.assertEqual(produce.call_count,1)
            telegram.assert_not_called()

    def test_busy_does_not_exhaust_attempts(self):
        with tempfile.TemporaryDirectory() as d, patch.object(jobs,'BASE',Path(d)), patch.object(jobs,'DB',Path(d)/'jobs.db'), patch.object(tower,'load_config',return_value={'factory':{'python':'/usr/bin/python3'},'podcasts_root':Path(d),'spotify_rss':'https://example.com/rss'}), patch('runtime.run_managed',return_value=Mock(returncode=2)):
            ident=jobs.enqueue('topic',True);jobs.work()
            self.assertEqual(jobs.get(ident)['attempts'],0)
            self.assertEqual(jobs.get(ident)['status'],'retry')
