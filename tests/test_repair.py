import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch,Mock
import runtime
import delivery
import jobs
import api
import dashboard

class RepairTests(unittest.TestCase):
    def test_timeout_terminates_child_process_group(self):
        with tempfile.TemporaryDirectory() as d:
            pidfile=Path(d)/'pid'
            code="import subprocess,time,pathlib; p=subprocess.Popen(['sleep','60']);pathlib.Path(%r).write_text(str(p.pid));time.sleep(60)" % str(pidfile)
            with self.assertRaises(subprocess.TimeoutExpired):
                runtime.run_managed([sys.executable,'-c',code],timeout=1)
            pid=int(pidfile.read_text())
            result=subprocess.run(['ps','-o','stat=','-p',str(pid)],capture_output=True,text=True)
            self.assertTrue(result.returncode or result.stdout.strip().startswith('Z'),result.stdout)

    def test_upload_record_is_not_verified_delivery(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'config').mkdir()
            (root/'config/spotify_uploaded.json').write_text(json.dumps({'x.mp3':'2026-09-12 UNVERIFIED'}))
            with patch.object(delivery.urllib.request,'urlopen',return_value=io.BytesIO(b'<rss><channel><item><title>Wrong title</title></item></channel></rss>')):
                self.assertEqual(delivery.check(root,'x.mp3','Right title')['state'],'publication_uncertain')
            xml=b'<rss><channel><item><title>Right title</title><link>https://example.com/episode</link></item></channel></rss>'
            with patch.object(delivery.urllib.request,'urlopen',return_value=io.BytesIO(xml)):
                self.assertEqual(delivery.check(root,'x.mp3','Right title')['state'],'verified_live')
            with patch.object(delivery.urllib.request,'urlopen',side_effect=OSError('offline')):
                self.assertEqual(delivery.check(root,'x.mp3','Right title')['state'],'verified_live')

    def test_daily_identity_survives_completion_and_repeated_click(self):
        with tempfile.TemporaryDirectory() as d,patch.object(jobs,'DB',Path(d)/'jobs.db'):
            one=jobs.enqueue_daily('morning','2026-09-12')
            with jobs.connect() as c:c.execute("UPDATE jobs SET status='done' WHERE id=?",(one,))
            self.assertEqual(one,jobs.enqueue_daily('morning','2026-09-12'))
            self.assertNotEqual(one,jobs.enqueue_daily('evening','2026-09-12'))

    def test_active_job_cannot_be_cancelled_or_retried(self):
        with tempfile.TemporaryDirectory() as d,patch.object(jobs,'DB',Path(d)/'jobs.db'):
            one=jobs.enqueue('Test')
            with jobs.connect() as c:c.execute("UPDATE jobs SET status='running' WHERE id=?",(one,))
            self.assertFalse(jobs.action(one,'cancel'))
            self.assertFalse(jobs.action(one,'retry'))
            self.assertEqual(jobs.get(one)['status'],'running')

    def test_json_api_rejects_missing_csrf_without_mutation(self):
        handler=object.__new__(dashboard.Handler);handler.path='/api/action'
        payload=b'{"action":"daily"}';handler.headers={'Content-Length':str(len(payload))}
        handler.rfile=io.BytesIO(payload);handler.connection=Mock();handler._send=Mock()
        with patch.object(api,'mutate') as mutate:
            handler.do_POST();mutate.assert_not_called()
        self.assertEqual(handler._send.call_args.args[2],403)

    def test_snapshot_does_not_expose_script_or_payload(self):
        with tempfile.TemporaryDirectory() as d,patch.object(jobs,'DB',Path(d)/'jobs.db'):
            ident=jobs.enqueue('Test');jobs.checkpoint(ident,script='PRIVATE SCRIPT',payload={'secret':'private'})
            dash=Mock(CFG={'podcasts_root':Path(d)},CSRF_TOKEN='token')
            dash.GET_STATUS.return_value={};dash.episode_rows.return_value=[]
            data=api.snapshot(dash)
            self.assertNotIn('PRIVATE SCRIPT',json.dumps(data));self.assertNotIn('secret',json.dumps(data))

if __name__=='__main__':unittest.main()

class ProbeTests(unittest.TestCase):
    def test_stalled_collector_does_not_accumulate_threads(self):
        import threading
        import tower
        release=threading.Event();calls=[]
        def stuck_probe():calls.append(1);release.wait(2);return {'ok':True}
        col=tower.Collectors({'collector_timeout_seconds':0.01},None)
        try:
            self.assertIn('error',col._guard(stuck_probe))
            self.assertIn('error',col._guard(stuck_probe))
            self.assertEqual(len(calls),1)
        finally:release.set()
