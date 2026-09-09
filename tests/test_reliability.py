import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import factory
import scout
import jobs
import tower
import dashboard
from reliability import atomic_json, exclusive_lock
spec = importlib.util.spec_from_file_location('special', Path(__file__).resolve().parents[1] / 'gkdaily-special.py')
special = importlib.util.module_from_spec(spec)
spec.loader.exec_module(special)


class ReliabilityTests(unittest.TestCase):
    def test_corrupt_scout_queue_is_not_treated_as_empty(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'queue.json'
            path.write_text('{broken')
            with self.assertRaises(RuntimeError):
                scout.load_queue({'topic_queue_json':path})
            self.assertEqual(path.read_text(), '{broken')

    def test_atomic_write_preserves_old_file_on_replace_failure(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state.json'
            atomic_json(p, {'old': 1})
            with patch('reliability.os.replace', side_effect=OSError('disk')):
                with self.assertRaises(OSError):
                    atomic_json(p, {'new': 2})
            self.assertEqual(json.loads(p.read_text()), {'old': 1})
            self.assertEqual(len(list(Path(d).iterdir())), 1)

    def test_exact_audio_not_newest_audio(self):
        with tempfile.TemporaryDirectory() as d, patch.object(special, 'PODCASTS', Path(d)):
            eps = Path(d)/'public/episodes'; eps.mkdir(parents=True)
            (eps/'special-edition-unrelated-2026-09-09.mp3').write_bytes(b'audio')
            with self.assertRaises(RuntimeError):
                special.produced_mp3(Path('2026-09-09_requested.md'))
            name='special-edition-requested-2026-09-09.mp3'
            (eps/name).write_bytes(b'audio')
            self.assertEqual(special.produced_mp3(Path('2026-09-09_requested.md')),name)

    def test_feed_requires_exact_item_title(self):
        xml=b'<rss><channel><title>Wanted</title><item><title>Wanted sequel</title></item></channel></rss>'
        with patch.object(special.urllib.request, 'urlopen', return_value=io.BytesIO(xml)), patch.object(special.time, 'time', side_effect=[0,0,1000]), patch.object(special.time, 'sleep'):
            self.assertFalse(special.verify_live({'spotify_rss':'https://example.com/rss'}, 'Wanted', 1))

    def test_blank_topic_rejected(self):
        with self.assertRaises(RuntimeError):
            special.resolve_topic({}, '  ')

    def test_research_failure_does_not_publish_fallback(self):
        cfg={'timezone':'America/Chicago','factory':{}}
        with patch.object(factory, 'covered_topics', return_value=[]), patch.object(factory, '_generate_via_claude', side_effect=RuntimeError('outage')), patch.object(factory,'_generate_via_kimi') as fallback:
            with self.assertRaises(RuntimeError):
                factory.generate(cfg, {'source':'test','line':'test'})
            fallback.assert_not_called()

    def test_queue_deduplicates_and_persists_checkpoints(self):
        with tempfile.TemporaryDirectory() as d, patch.object(jobs,'DB',Path(d)/'jobs.db'):
            one=jobs.enqueue('topic', True)
            self.assertEqual(one,jobs.enqueue('topic', True))
            jobs.checkpoint(one, topic={'line':'topic'}, script='/saved.md')
            self.assertEqual(json.loads(jobs.get(one)['checkpoint'])['script'],'/saved.md')
            with jobs.connect() as c:
                c.execute("UPDATE jobs SET status='done' WHERE id=?", (one,))
            self.assertNotEqual(one,jobs.enqueue('topic', True))

    def test_dry_check_does_not_reconcile_or_spawn(self):
        col=MagicMock(); col.collect.return_value={}
        with patch.object(tower,'Collectors',return_value=col), patch.object(tower,'evaluate',return_value=[]), patch.object(tower,'process_alerts'), patch.object(tower,'maybe_digest'), patch.object(tower,'reconcile_unverified') as reconcile, patch.object(jobs,'maybe_start') as start:
            tower.tick({'timezone':'America/Chicago'},tower.db(':memory:'),quiet=True)
            reconcile.assert_not_called(); start.assert_not_called()

    def test_csrf_and_request_size(self):
        for payload,expected in [(b'name=run',403),(b'X'*16385,400),(f'csrf={dashboard.CSRF_TOKEN}&name=run'.encode(),303)]:
            handler=object.__new__(dashboard.Handler)
            handler.path='/action'; handler.headers={'Content-Length':str(len(payload))}
            handler.rfile=io.BytesIO(payload); handler.connection=MagicMock(); handler._send=MagicMock()
            with patch.object(dashboard,'dispatch') as dispatch:
                handler.do_POST()
                self.assertEqual(handler._send.call_args.args[2], expected)
                self.assertEqual(dispatch.called,expected==303)

    def test_reconciliation_respects_uploader_lock(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); (root/'config').mkdir(); p=root/'config/spotify_uploaded.json'
            atomic_json(p,{'episode.mp3':'2026-09-09T10:00:00 UNVERIFIED'})
            cfg={'podcasts_root':root}; col=MagicMock(); col.rss_titles.return_value=['Title']
            with exclusive_lock(root/'.spotify-upload.lock'), patch.object(tower,'expected_title',return_value='Title'), patch.object(tower,'telegram') as telegram:
                tower.reconcile_unverified(cfg,col,{'ledger':{'unverified':[{'name':'episode.mp3','ts':'2026-09-09T10:00:00 UNVERIFIED'}]}},None)
                self.assertIn('UNVERIFIED',p.read_text()); telegram.assert_not_called()


if __name__=='__main__': unittest.main()
