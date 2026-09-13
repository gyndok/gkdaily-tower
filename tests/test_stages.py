import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock,patch
import runtime
import topic_editor

ROOT=Path(__file__).resolve().parents[2]

class StageTests(unittest.TestCase):
    def test_packaged_daily_retries_upload_without_regenerating(self):
        spec=importlib.util.spec_from_file_location('daily_runner',ROOT/'podcasts/run_pipeline.py')
        daily=importlib.util.module_from_spec(spec);spec.loader.exec_module(daily)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'public/episodes').mkdir(parents=True)
            (root/'public/episodes/gk_daily_20260912_morning.mp3').write_bytes(b'packaged audio')
            ingestion=Mock();publish=Mock()
            with patch.object(daily,'BASE_DIR',root),patch.object(daily,'setup_logging',return_value=Mock()),patch.object(daily.subprocess,'Popen'),patch.object(runtime,'run_managed',return_value=Mock(returncode=1,stderr='Browser unavailable')),patch('delivery.check',return_value={'state':'upload_pending'}),patch.dict(sys.modules,{'ingest':ingestion,'publish':publish}),patch.object(daily,'send_telegram_notification') as notify:
                self.assertEqual(daily.run_pipeline('morning','2026-09-12'),4)
                ingestion.run_ingestion.assert_not_called();notify.assert_not_called()
            state=json.loads((root/'state/2026-09-12-morning.json').read_text())
            self.assertEqual(state['stage'],'upload_pending')
            self.assertEqual(state['episode'],'gk_daily_20260912_morning.mp3')

    def test_invalid_script_is_quarantined_without_overwrite(self):
        spec=importlib.util.spec_from_file_location('special_producer_test',ROOT/'clawd/produce-special-podcast.py')
        producer=importlib.util.module_from_spec(spec);spec.loader.exec_module(producer)
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'bad 2.md';p.write_text('one')
            dest=producer.quarantine(p,'Invalid filename');self.assertFalse(p.exists())
            p.write_text('two');second=producer.quarantine(p,'Invalid filename')
            self.assertEqual(dest.read_text(),'one');self.assertEqual(second.read_text(),'two')
            self.assertEqual(json.loads(dest.with_suffix('.reason.json').read_text())['reason'],'Invalid filename')

    def test_topic_edit_uses_revision_and_preserves_other_paragraphs(self):
        doc={'revisionId':'r1','body':{'content':[
            {'startIndex':1,'paragraph':{'elements':[{'textRun':{'content':'Header\n'}}]}},
            {'startIndex':8,'paragraph':{'elements':[{'textRun':{'content':'----\n'}}]}},
            {'startIndex':13,'paragraph':{'elements':[{'textRun':{'content':'Topic 😀\n'}}]}},
            {'startIndex':22,'paragraph':{'elements':[{'textRun':{'content':'Other topic\n'}}]}}]}}
        with patch.object(topic_editor.subprocess,'run',side_effect=[Mock(returncode=0,stdout=json.dumps(doc)),Mock(returncode=0)]) as run:
            topic_editor.edit({'scout':{'topic_doc_id':'doc'}},'Topic 😀',new='Better topic')
        args=run.call_args.args[0];body=json.loads(args[args.index('--json')+1])
        self.assertEqual(body['writeControl'],{'requiredRevisionId':'r1'})
        self.assertEqual(body['requests'][0]['deleteContentRange']['range'],{'startIndex':13,'endIndex':21})
        self.assertEqual(len(body['requests']),2)

if __name__=='__main__':unittest.main()
