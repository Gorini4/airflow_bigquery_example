import datetime
from airflow import models
from airflow.contrib.operators import bigquery_operator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# [START composer_notify_failure]
default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': 'thermal-loop-237313'
}

with models.DAG(
        'batman_bi_daily_report',
        schedule_interval=datetime.timedelta(hours=1),
        default_args=default_dag_args) as dag:

        bq_recent_questions_query = bigquery_operator.BigQueryOperator(
            task_id='bq_count_crimes_by_locations',
            bql="""
            SELECT 
                DATE(`date`, "America/Chicago") as report_day, 
                primary_type, 
                location_description, 
                count(*) as crime_count
            FROM `batman_bi.crime_logs`
            GROUP BY 1, 2, 3
            """,
            use_legacy_sql=False,
            destination_dataset_table='batman_bi.crime_count_by_location',
            write_disposition='WRITE_TRUNCATE')