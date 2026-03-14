from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import logging

# Rules
default_args = {
    'owner': 'Will',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # retry 1 times if Error
    'retry_delay': timedelta(minutes=5),  # sleep 5 minutes before retry
}

# Main Function
def analyze_daily_whales(**kwargs):
    # Connect to google bigquery
    hook = BigQueryHook(gcp_conn_id='gcp_bigquery_conn', use_legacy_sql=False)
    
    # SQL query to get the count and total volume of BUY and SELL transactions from yesterday
    sql = """
        SELECT
            side,
            COUNT(*) as tx_count,
            SUM(total_value_usd) as total_volume
        FROM `cryptods.crypto_ds.raw_whale_trades`
        WHERE DATE(TIMESTAMP(timestamp)) = CURRENT_DATE() - 1
        GROUP BY side
    """
    
    logging.info("🚀 กำลังดึงข้อมูลสรุปจาก BigQuery...")
    records = hook.get_records(sql)

    summary = {"BUY": {"count": 0, "vol": 0}, "SELL": {"count": 0, "vol": 0}}
    
    # store data from SQL in dict
    for row in records:
        side = row[0]
        summary[side]["count"] = row[1]
        summary[side]["vol"] = row[2]
        
    # Summary
    logging.info(f"📊 --- สรุปสถานการณ์ปลาวาฬเมื่อวานนี้ ---")
    logging.info(f"🟢 BUY : {summary['BUY']['count']} ครั้ง | มูลค่ารวม ${summary['BUY']['vol']:,.2f}")
    logging.info(f"🔴 SELL: {summary['SELL']['count']} ครั้ง | มูลค่ารวม ${summary['SELL']['vol']:,.2f}")

    if summary['BUY']['vol'] > summary['SELL']['vol']:
        logging.info("🔥 สรุป: ฝั่ง BUY มีแรงซื้อมากกว่า!")
    elif summary['SELL']['vol'] > summary['BUY']['vol']:
        logging.info("🩸 สรุป: ฝั่ง SELL มีแรงเทขายมากกว่า!")
    else:
        logging.info("⚖️ สรุป: แรงซื้อและขายเท่ากันพอดี")

# Init DAG obj
with DAG(
    dag_id='daily_crypto_whale_summary',
    default_args=default_args,
    description='สรุปข้อมูลการเทรดของปลาวาฬรายวันจาก BigQuery',
    schedule='0 1 * * *', # Run at 1.00 AM every day
    start_date=datetime(2026, 3, 13),
    catchup=False,
    tags=['crypto', 'bigquery', 'summary'],
) as dag:

    # create a task
    summarize_task = PythonOperator(
        task_id='analyze_and_summarize',
        python_callable=analyze_daily_whales,
    )

    # Set task dependencies (we have inly one task, so no dependencies to set)
    summarize_task