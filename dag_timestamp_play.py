from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from google.cloud import storage
import mysql.connector
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import json

PROJECT_ID = "your-project"
BUCKET = "your-bucket"
DATAFLOW_TEMPLATE = "gs://your-bucket/templates/xml_to_bq_flex"
CLOUD_SQL_CONFIG = {
    "user": "root",
    "password": "password",
    "host": "your-cloudsql-ip",
    "database": "testdb"
}

TABLE = "dataset.customer_staging"
FINAL_TABLE = "dataset.customer"

default_args = {
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    dag_id="cloudsql_to_gcs_to_dataflow",
    schedule_interval="*/10 * * * *",  # every 10 mins
    default_args=default_args,
    catchup=False
)


# --------------------------------------------------------
# 1. GET LAST UPDATED TIMESTAMP FROM BIGQUERY
# --------------------------------------------------------
def fetch_latest_timestamp(**kwargs):
    hook = BigQueryHook()
    sql = f"""
        SELECT MAX(last_updated) AS ts 
        FROM `{FINAL_TABLE}`
    """
    rows = hook.get_client().query(sql).result()
    ts = None
    for r in rows:
        ts = r.ts
    if ts is None:
        ts = "1900-01-01 00:00:00"
    kwargs['ti'].xcom_push("latest_ts", str(ts))


# --------------------------------------------------------
# 2. LOAD NEW ROWS FROM CLOUD SQL & GENERATE XML
# --------------------------------------------------------
def extract_new_rows_to_xml(**kwargs):
    latest_ts = kwargs['ti'].xcom_pull(key="latest_ts")

    conn = mysql.connector.connect(**CLOUD_SQL_CONFIG)
    cur = conn.cursor(dictionary=True)

    cur.execute(f"""
        SELECT id, name, email, last_updated
        FROM customer
        WHERE last_updated > '{latest_ts}'
    """)

    rows = cur.fetchall()

    # XML root tag
    root = ET.Element("records")

    for row in rows:
        rec = ET.SubElement(root, "record")
        for key, val in row.items():
            child = ET.SubElement(rec, key)
            child.text = str(val)

    xml_data = ET.tostring(root, encoding="utf-8")

    # Upload the XML to GCS
    filename = f"cdc/xml_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(xml_data, content_type="application/xml")

    kwargs['ti'].xcom_push("xml_path", f"gs://{BUCKET}/{filename}")


# --------------------------------------------------------
# 3. TRIGGER DATAFLOW JOB (FLEX TEMPLATE)
# --------------------------------------------------------
trigger_dataflow = DataflowStartFlexTemplateOperator(
    task_id="run_dataflow",
    project_id=PROJECT_ID,
    body={
        "launchParameter": {
            "containerSpecGcsPath": DATAFLOW_TEMPLATE,
            "jobName": "xml-to-bq-job",
            "parameters": {
                "input": "{{ ti.xcom_pull('extract_new_rows_to_xml', key='xml_path') }}",
                "output_table": TABLE
            }
        }
    },
    location="us-central1",
    dag=dag
)


# --------------------------------------------------------
# 4. MERGE INTO FINAL BIGQUERY TABLE
# --------------------------------------------------------
def merge_to_final_bq(**kwargs):
    hook = BigQueryHook()
    sql = f"""
    MERGE `{FINAL_TABLE}` T
    USING `{TABLE}` S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET T.name = S.name,
                 T.email = S.email,
                 T.last_updated = S.last_updated
    WHEN NOT MATCHED THEN
      INSERT (id, name, email, last_updated)
      VALUES (S.id, S.name, S.email, S.last_updated)
    """
    hook.get_client().query(sql)


# TASK DEFINITIONS
t1 = PythonOperator(task_id="get_latest_timestamp", python_callable=fetch_latest_timestamp, dag=dag)
t2 = PythonOperator(task_id="extract_new_rows_to_xml", python_callable=extract_new_rows_to_xml, dag=dag)
t3 = trigger_dataflow
t4 = PythonOperator(task_id="merge_into_final", python_callable=merge_to_final_bq, dag=dag)

t1 >> t2 >> t3 >> t4
