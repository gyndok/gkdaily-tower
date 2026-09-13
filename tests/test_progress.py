import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch,Mock
import jobs
import api
from progress import describe

class ProgressTests(unittest.TestCase):
    def test_verification_wait_overrides_old_audio_checkpoint(self):
        data=describe({'status':'verifying','updated':10}, {'phase':'audio','audio_completed':3,'audio_total':8})
        self.assertEqual(data['phase'],'verify')
        self.assertFalse(data['verified'])

    def test_legacy_completion_does_not_claim_verified_publication(self):
        data=describe({'status':'done','updated':10},{'stage':'Making audio'})
        self.assertEqual(data['phase'],'legacy')
        self.assertFalse(data['verified'])

    def test_audio_counter_is_bounded_and_only_present_with_real_counts(self):
        row={'status':'running','updated':10}
        self.assertIsNone(describe(row,{'phase':'audio'})['audio'])
        self.assertEqual(describe(row,{'audio_total':4,'audio_completed':6})['audio'],{'completed':4,'total':4})

    def test_delayed_archive_update_does_not_reset_activity_clock(self):
        data=describe({'status':'running','updated':100},{'phase':'audio','stage_at':20})
        self.assertEqual(data['activity_at'],20)

    def test_request_remains_durable_if_immediate_start_fails(self):
        with tempfile.TemporaryDirectory() as d,patch.object(jobs,'DB',Path(d)/'jobs.db'),patch.object(jobs,'maybe_start',side_effect=OSError('unavailable')):
            with self.assertLogs('api',level='ERROR'):
                result=api.mutate({'action':'enqueue','topic':'Undersea cables'},Mock(CFG={}))
            self.assertEqual(jobs.get(result['id'])['status'],'queued')

    def test_enqueue_starts_worker_after_saving(self):
        with tempfile.TemporaryDirectory() as d,patch.object(jobs,'DB',Path(d)/'jobs.db'):
            def start(cfg):
                with jobs.connect() as conn:self.assertEqual(conn.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],1)
            with patch.object(jobs,'maybe_start',side_effect=start) as worker:
                api.mutate({'action':'enqueue','topic':'Undersea cables'},Mock(CFG={}))
                worker.assert_called_once()
