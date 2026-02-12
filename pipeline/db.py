import psycopg2

from pipeline.config import(
    warehouse_host,
    warehouse_port,
    warehouse_db,
    warehouse_user,
    warehouse_password,
)

def get_conn():
    return psycopg2.connect(
        host=warehouse_host,
        port=warehouse_port,
        dbname=warehouse_db,
        user=warehouse_user,
        password=warehouse_password
    )