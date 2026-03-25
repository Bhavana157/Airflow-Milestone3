from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import smtplib
import logging

# 🔹 Default arguments
default_args = {
    'owner': 'ownername',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# 🔹 Reusable Email Function
def send_email(subject, body):
    sender = "yourmailid@gmail.com"
    receiver = "yourmailid@gmail.com"
    password = "AppPassword"   # ⚠️ Use env variable in real projects

    message = f"Subject: {subject}\n\n{body}"

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, receiver, message)

        logging.info("Email sent successfully")

    except Exception as e:
        logging.error(f"Email failed: {e}")

# 🔹 Success Email (ONLY ONCE - DAG LEVEL)
def send_success_email(context):
    dag_id = context.get('dag').dag_id

    subject = f"DAG Success: {dag_id}"
    body = f"""
DAG: {dag_id}
Status: SUCCESS
All tasks executed successfully
"""
    send_email(subject, body)

# 🔹 Failure Email (ONLY ONCE - TASK LEVEL CONTROLLED)
def send_failure_email(context):
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id

    subject = f"DAG Failed: {dag_id}"
    body = f"""
DAG: {dag_id}
Failed Task: {task_instance.task_id}
Error: {context.get('exception')}
"""
    send_email(subject, body)

# 🔹 Extract Task
def extract():
    logging.info("Starting Extract Task")

    df = pd.read_csv('/opt/airflow/data/employee.csv')
    df = df.head(100)

    df.to_csv('/opt/airflow/data/data.csv', index=False)

    logging.info("Data Extracted Successfully")

# 🔹 Transform Task
def transform():
    logging.info("Starting Transform Task")

    try:
        df = pd.read_csv('/opt/airflow/data/data.csv')

        # ✅ Data Cleaning
        df = df.dropna()
        df = df.drop_duplicates()

        # ✅ Encoding
        df["Gender"] = df["Gender"].map({"Male": 1, "Female": 0})
        df["EverBenched"] = df["EverBenched"].map({"Yes": 1, "No": 0})

        # ✅ Feature Engineering
        df["ExperienceLevel"] = pd.cut(
            df["ExperienceInCurrentDomain"],
            bins=[-1, 1, 3, 5],
            labels=["Fresher", "Mid", "Senior"]
        )

        current_year = 2024
        df["YearsInCompany"] = current_year - df["JoiningYear"]

        # ✅ Save transformed data
        df.to_csv('/opt/airflow/data/transformed_data.csv', index=False)

        logging.info("Data Transformed Successfully")

    except Exception as e:
        logging.error(f"Transform failed: {e}")
        raise   # 🔥 triggers failure email

# 🔹 Load Task
def load():
    logging.info("Starting Load Task")

    df = pd.read_csv('/opt/airflow/data/transformed_data.csv')

    logging.info("Data Loaded Successfully")
    logging.info("\n" + str(df.head()))

# 🔹 DAG Definition
with DAG(
    dag_id='etl_pipeline_single_email_final',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='ETL pipeline with single email alert',

    on_success_callback=send_success_email   # ✅ only once

) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        on_failure_callback=send_failure_email   # ❌ only one failure email
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        on_failure_callback=send_failure_email
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        on_failure_callback=send_failure_email
    )

    extract_task >> transform_task >> load_task