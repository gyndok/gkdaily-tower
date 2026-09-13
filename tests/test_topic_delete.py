import json,unittest
from unittest.mock import patch,Mock
import topic_editor
class TopicDeleteTests(unittest.TestCase):
 def test_delete_only_selected_text_preserves_revision_and_final_newline(self):
  text='Ocean 🐳'
  doc={'revisionId':'rev1','body':{'content':[{'startIndex':1,'paragraph':{'elements':[{'textRun':{'content':'----\n'}}]}},{'startIndex':6,'paragraph':{'elements':[{'textRun':{'content':text+'\n'}}]}}]}}
  with patch.object(topic_editor.subprocess,'run',side_effect=[Mock(returncode=0,stdout=json.dumps(doc)),Mock(returncode=0)]) as run:
   self.assertEqual(topic_editor.edit({'scout':{'topic_doc_id':'doc'}},text,delete=True),'Topic deleted.')
   args=run.call_args.args[0];body=json.loads(args[args.index('--json')+1])
   self.assertEqual(body,{'writeControl':{'requiredRevisionId':'rev1'},'requests':[{'deleteContentRange':{'range':{'startIndex':6,'endIndex':14}}}]})
 def test_missing_topic_never_writes(self):
  with patch.object(topic_editor.subprocess,'run',return_value=Mock(returncode=0,stdout=json.dumps({'revisionId':'r','body':{'content':[]}}))) as run:
   with self.assertRaises(ValueError):topic_editor.edit({'scout':{'topic_doc_id':'doc'}},'Missing',delete=True)
   self.assertEqual(run.call_count,1)
