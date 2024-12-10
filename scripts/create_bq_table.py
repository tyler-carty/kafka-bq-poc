from google.cloud import bigquery
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import PROJECT_ID, DATASET_ID, TABLE_ID

def create_table():
    client = bigquery.Client()

    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("user_id", "INTEGER"),
        bigquery.SchemaField("action", "STRING"),
        bigquery.SchemaField("value", "INTEGER"),
    ]

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

if __name__ == "__main__":
    create_table()