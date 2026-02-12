from __future__ import annotations
import json
import pandas as pd 
from pipeline.config import api_json_path
from pipeline.db import get_conn
from pipeline.run_context import new_run_context
from pipeline.load import delete_by_load_date, insert_rows

def run(run_id: str = None, load_date: str = None):
    ctx = new_run_context(run_id=run_id)
    if load_date:
          ctx.load_date = load_date

    with open(api_json_path, 'r') as f:
        payload = json.load(f)
    
    out_rows = []
    row_num = 0
    for camp in payload['campaigns']:
        for details in camp['daily_metrics']:
            row_num += 1
            out_rows.append({
                'row_num': row_num,
                'campaign_id' : str(camp['campaign_id'] or ''),
                'campaign_name' : str(camp['campaign_name'] or ''),
                'campaign_type': str(camp['campaign_type'] or ''),
                'status' : str(camp['status'] or ''),
                'date' : str(details['date'] or ''),
                'impressions' : str(details['impressions'] or ''),
                'clicks' : str(details['clicks'] or ''),
                'cost_micros' : str(details['cost_micros'] or ''),
                'conversions' : str(details['conversions'] or ''),
                'conversion_value' : str(details['conversion_value'] or ''),
                'run_id': ctx.run_id,
                'load_ts': ctx.load_ts.isoformat(),
                'load_date': ctx.load_date,
            })
    
    df = pd.DataFrame(out_rows)
    cols = list(df.columns)
    rows = df.itertuples(index=False, name=None)

    with get_conn() as conn:
        delete_by_load_date(conn, 'raw', 'google_ads_raw', ctx.load_date)
        insert_rows(conn, 'raw', 'google_ads_raw', cols, rows)
        conn.commit()

    print(f"[google_raw] row={len(df)} run_id={ctx.run_id}")