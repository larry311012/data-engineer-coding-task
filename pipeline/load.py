from __future__ import annotations
from psycopg2 import sql
from psycopg2.extras import execute_values

def delete_by_load_date(conn, schema, table, load_date):
    with conn.cursor() as cur:
        cur.execute(
              sql.SQL("DELETE FROM {}.{} WHERE load_date = %s").format(
                  sql.Identifier(schema), sql.Identifier(table)
              ),
              (load_date,),
          )


def insert_rows(conn, schema, table, cols, rows):
    with conn.cursor() as cur:
          stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
              sql.Identifier(schema),
              sql.Identifier(table),
              sql.SQL(', ').join(map(sql.Identifier, cols)),
          )
          execute_values(cur, stmt, list(rows), page_size=2000)