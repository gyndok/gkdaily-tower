import unittest,tempfile
from pathlib import Path
from unittest.mock import patch
import jobs,notifications,tower
class NotificationsTests(unittest.TestCase):
 def test_failed_send_retries_and_success_is_not_repeated(self):
  with tempfile.TemporaryDirectory() as d,patch.object(jobs,'DB',Path(d)/'jobs.db'),patch.object(tower,'telegram',side_effect=[False,True]) as send:
   ident=jobs.enqueue('Ocean currents');notifications.poll({})
   with jobs.connect() as c:c.execute('UPDATE notification_outbox SET ready=0')
   notifications.poll({});notifications.poll({});self.assertEqual(send.call_count,2)
 def test_running_retry_and_completion_are_announced(self):
  with tempfile.TemporaryDirectory() as d,patch.object(jobs,'DB',Path(d)/'jobs.db'),patch.object(tower,'telegram',return_value=True) as send:
   ident=jobs.enqueue('Ocean currents')
   for status in ('running','retry','running','done'):
    with jobs.connect() as c:c.execute('UPDATE jobs SET status=? WHERE id=?',(status,ident))
    notifications.poll({})
   self.assertEqual(send.call_count,4)
   self.assertIn('Verified live',send.call_args.args[1])
 def test_quiet_and_unwatched_historical_jobs_are_not_announced(self):
  with tempfile.TemporaryDirectory() as d,patch.object(jobs,'DB',Path(d)/'jobs.db'),patch.object(tower,'telegram',return_value=True) as send:
   jobs.enqueue('Quiet',quiet=True);old=jobs.enqueue('Old')
   with jobs.connect() as c:c.execute("UPDATE jobs SET status='done' WHERE id=?",(old,))
   notifications.poll({});send.assert_not_called()
 def test_tower_uses_minibot_token_and_owner(self):
  with patch.object(tower,'load_env_creds',side_effect=[{'TELEGRAM_BOT_TOKEN':'old','TELEGRAM_CHAT_ID':'10'},{'TELEGRAM_BOT_TOKEN':'mini','ALLOWED_USER_IDS':'20'}]),patch.object(tower.urllib.request,'urlopen') as send:
   self.assertTrue(tower.telegram({'clawd_env':Path('/unused')},'Status'))
   req=send.call_args.args[0]
   self.assertIn('botmini/sendMessage',req.full_url)
   self.assertIn('chat_id=20',req.data.decode())
