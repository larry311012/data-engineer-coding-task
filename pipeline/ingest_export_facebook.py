from __future__ import annotations
import pandas as pd 
from pipeline.config import export_csv_path
from pipeline.db import get_conn
from pipeline.run_context import new_run_context
from pipeline.load import delete_by_load_date, insert_rows

def run(run_id: str = None, load_date: str = None):
    ctx = new_run_context(run_id=run_id)
    if load_date:
          ctx.load_date = load_date

    df = pd.read_csv(export_csv_path, dtype=str, keep_default_na=False, engine='python')
    df.columns = [c.strip() for c in df.columns]
    df.insert(0, 'row_num', range(1, len(df)+1))
    df['run_id'] = ctx.run_id
    df['load_ts'] = ctx.load_ts.isoformat()
    df['load_date'] = ctx.load_date

    expected_columns = [
        'row_num',
        'campaign_id', 
        'campaign_name', 
        'date',
        'impressions', 
        'clicks', 
        'spend', 
        'purchases', 
        'purchase_value', 
        'reach', 
        'frequency',
        'run_id',
        'load_ts',
        'load_date'
    ]
    df = df.reindex(columns=expected_columns, fill_value='')

    cols = list(df.columns)
    rows = df.itertuples(index=False, name=None)

    with get_conn() as conn:
        delete_by_load_date(conn, 'raw', 'facebook_export_raw', ctx.load_date)
        insert_rows(conn, 'raw', 'facebook_export_raw', cols, rows)
        conn.commit()

    print(f"[facebook_raw] row={len(df)} run_id={ctx.run_id}")