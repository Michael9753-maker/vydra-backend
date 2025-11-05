# dm_test.py
from time import sleep
from download_manager import get_default_manager

dm = get_default_manager()
url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"   # or your test url
job_id, err = dm.start_download_job(url=url, user_id="local-test@example.com", mode="video", requested_quality="360p")
print("start returned:", job_id, err)
import pprint, time
for i in range(120):   # poll 120s
    s = dm.get_job_status(job_id)
    print(i, "status:", s.get("status") if s else None, "progress:", s.get("progress") if s else None)
    if s and s.get("status") in ("finished","error","cancelled"):
        break
    time.sleep(1)
print("final job:", dm.get_job_status(job_id))
