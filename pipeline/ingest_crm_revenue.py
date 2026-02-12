from __future__ import annotations
import pandas as pd 
import re
from io import StringIO
from pipeline.config import crm_csv_path
from pipeline.db import get_conn
from pipeline.run_context import new_run_context
from pipeline.load import delete_by_load_date, insert_rows

def _fix_date_commas(filepath):
    """Fix unquoted commas inside month-name dates like 'January 4, 2024'."""
    fixed_lines = []
    month_pattern = re.compile(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)'
        r'\s+\d{1,2},\s*\d{4}'
    )
    with open(filepath, 'r') as f:
        for line in f:
            # Quote any month-name date that contains a comma
            line = month_pattern.sub(lambda m: f'"{m.group(0)}"', line)
            fixed_lines.append(line)
    return fixed_lines

def run(run_id: str = None, load_date: str = None):
    ctx = new_run_context(run_id=run_id)
    if load_date:
          ctx.load_date = load_date
          
    fixed_lines = _fix_date_commas(crm_csv_path)
    df = pd.read_csv(StringIO(''.join(fixed_lines)), dtype=str, keep_default_na=False, engine='python')
    df.columns = [c.strip() for c in df.columns]
    df.insert(0, 'row_num', range(1, len(df)+1))
    df['run_id'] = ctx.run_id
    df['load_ts'] = ctx.load_ts.isoformat()
    df['load_date'] = ctx.load_date

    expected_columns = [
        'row_num',
        'order_id', 
        'customer_id', 
        'order_date',
        'revenue', 
        'channel_attributed', 
        'campaign_source', 
        'product_category', 
        'region', 
        'run_id',
        'load_ts',
        'load_date'
    ]
    df = df.reindex(columns=expected_columns, fill_value='')
    cols = list(df.columns)
    rows = df.itertuples(index=False, name=None)

    with get_conn() as conn:
        delete_by_load_date(conn, 'raw', 'crm_revenue_raw', ctx.load_date)
        insert_rows(conn, 'raw', 'crm_revenue_raw', cols, rows)
        conn.commit()

    print(f"[crm_raw] row={len(df)} run_id={ctx.run_id}")