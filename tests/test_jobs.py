import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
import jobs
import tower

class WorkerTests(unittest.TestCase):
    def test_restart_resumes_abandoned_job_and_keeps_checkpoint(self):
        with tempfile.TemporaryDirectory() as d, patch.object(jobs,'BASE',Path(d)), patch.object(jobs,'DB',Path(d)/'jobs.db'), patch.object(tower,'load_config',return_value={'factory':{'python':'/usr/bin/python3'}}), patch.object(jobs.subprocess,'run',return_value=Mock(returncode=0)) as run:
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

    def test_unverified_job_has_bounded_retries(self):
        with tempfile.TemporaryDirectory() as d, patch.object(jobs,'BASE',Path(d)), patch.object(jobs,'DB',Path(d)/'jobs.db'), patch.object(tower,'load_config',return_value={'factory':{'python':'/usr/bin/python3'}}), patch.object(jobs.subprocess,'run',return_value=Mock(returncode=4)), patch.object(tower,'telegram') as telegram:
            ident=jobs.enqueue('topic',True)
            for attempt in range(3):
                with jobs.connect() as conn:
                    conn.execute('UPDATE jobs SET ready=0 WHERE id=?',(ident,))
                jobs.work()
            self.assertEqual(jobs.get(ident)['status'],'needs_attention')
            self.assertEqual(jobs.get(ident)['attempts'],3)
            telegram.assert_not_called()

    def test_busy_does_not_exhaust_attempts(self):
        with tempfile.TemporaryDirectory() as d, patch.object(jobs,'BASE',Path(d)), patch.object(jobs,'DB',Path(d)/'jobs.db'), patch.object(tower,'load_config',return_value={'factory':{'python':'/usr/bin/python3'}}), patch.object(jobs.subprocess,'run',return_value=Mock(returncode=2)):
            ident=jobs.enqueue('topic',True);jobs.work()
            self.assertEqual(jobs.get(ident)['attempts'],0)
            self.assertEqual(jobs.get(ident)['status'],'retry')
