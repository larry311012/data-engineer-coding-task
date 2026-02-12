from datetime import datetime, timezone
import uuid
from dataclasses import dataclass

@dataclass
class RunContext:
    run_id:str
    load_ts: datetime
    load_date: str

def new_run_context(run_id: str = None) -> RunContext:
    ts = datetime.now(timezone.utc)
    return RunContext(
        run_id =run_id if run_id else str(uuid.uuid4()),
        load_ts = ts,
        load_date = ts.strftime('%Y-%m-%d'),
    )