import time
import isodate
import traceback
from datetime import datetime, timedelta


def schedule_work(duration_iso, do_work):
    while(True):
        try:
            duration = isodate.parse_duration(duration_iso)
            duration_seconds =  duration.total_seconds()
            
            now = datetime.utcnow()
            then = now + duration
            rounded_then = int(then.timestamp() / duration_seconds) * duration_seconds
            seconds_to_sleep = rounded_then - now.timestamp()
            time.sleep(seconds_to_sleep)
            
            do_work(datetime.fromtimestamp(rounded_then))

        except KeyboardInterrupt:
            raise
        except Exception:
            traceback.print_exc()
            pass
        
        