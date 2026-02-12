import os
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
data_dir = project_root / 'data'

warehouse_host = os.getenv('WAREHOUSE_HOST', 'localhost')
warehouse_port = int(os.getenv('WAREHOUSE_PORT', '5432'))
warehouse_db = os.getenv('WAREHOUSE_DB', 'warehouse')
warehouse_user = os.getenv('WAREHOUSE_USER', 'postgres')
warehouse_password = os.getenv('WAREHOUSE_PASSWORD', 'postgres')

crm_csv_path = data_dir / 'crm_revenue.csv'
api_json_path = data_dir / 'google_ads_api.json'
export_csv_path = data_dir / 'facebook_export.csv'

