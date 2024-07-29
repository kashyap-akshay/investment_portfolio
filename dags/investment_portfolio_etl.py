import pandas as pd
import sqlite3
import smtplib
from datetime import timedelta
from email.mime.text import MIMEText
from src.data_pipeline import DataPipeline
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtSeedOperator, DbtTestOperator
from airflow.operators.sensors import FileSensor


def run_data_pipeline(**kwargs):
    pipeline = DataPipeline('investment_portfolio.db')

    holdings_df = pd.read_csv('holdings_data_1.csv')
    portfolio_stats_df = pd.read_csv('portfolio_stats_1.csv')
    trades_df = pd.read_csv('trades_data_1.csv')

    holdings_df_clean = pipeline.transform_data_in_python(holdings_df, 'holdings')
    portfolio_stats_df_clean = pipeline.transform_data_in_python(portfolio_stats_df, 'portfolio_stats')
    trades_df_clean = pipeline.transform_data_in_python(trades_df, 'trades')

    pipeline.upload_data_to_sql(holdings_df_clean, 'holdings_stage')
    pipeline.upload_data_to_sql(portfolio_stats_df_clean, 'portfolio_stats_stage')
    pipeline.upload_data_to_sql(trades_df_clean, 'trades_stage')

    pipeline.transform_data_in_sql()


def data_quality_checks(**kwargs):
    conn = sqlite3.connect('investment_portfolio.db')
    cursor = conn.cursor()

    # Example data quality check: Ensure holdings have been loaded
    cursor.execute("SELECT COUNT(*) FROM holdings")
    count = cursor.fetchone()[0]
    if count == 0:
        raise ValueError("Data quality check failed: No data found in holdings table")

    # Example data quality check: Ensure portfolio stats have been loaded
    cursor.execute("SELECT COUNT(*) FROM portfolio_stats")
    count = cursor.fetchone()[0]
    if count == 0:
        raise ValueError("Data quality check failed: No data found in portfolio_stats table")

    conn.close()


def send_email_notification(**kwargs):
    msg = MIMEText('Data pipeline has successfully loaded the data.')
    msg['Subject'] = 'Data Pipeline Success'
    msg['From'] = 'your_email@example.com'
    msg['To'] = 'distribution_list@example.com'

    with smtplib.SMTP('localhost') as server:
        server.send_message(msg)


default_args = {
    'owner': 'akshay',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'investment_portfolio_etl',
    default_args=default_args,
    description='Investment Portfolio Data ETL Pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

# Define the files to process
files_to_process = {
    'holdings': '/path/to/shared/drive/holdings_data_1.csv',
    'portfolio_stats': '/path/to/shared/drive/portfolio_stats_1.csv',
    'trades': '/path/to/shared/drive/trades_data_1.csv'
}

for file_type, file_path in files_to_process.items():

    file_sensor_task_id = f"file_sensor_{file_type}"

    file_sensor_task_id = FileSensor(
        task_id=file_sensor_task_id,
        filepath=file_path,
        poke_interval=60 * 60,
        timeout=60 * 60 * 8,
        mode='reschedule',
        dag=dag
    )

    run_pipeline_task = PythonOperator(
        task_id='run_data_pipeline',
        python_callable=run_data_pipeline,
        dag=dag,
    )

    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        dir='../dbt/models/',  #ToDo: Adjust this to the path where your dbt project is located
        profiles_dir='../dbt/',  #ToDo: Adjust this to the path where your dbt profiles.yml is located
        dag=dag,
    )

    data_quality_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks,
        dag=dag,
    )

    email_notification_task = PythonOperator(
        task_id='send_email_notification',
        python_callable=send_email_notification,
        dag=dag,
    )

    file_sensor_task_id >> run_pipeline_task >> dbt_run >> data_quality_task >> email_notification_task