import pandas as pd
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from reportlab.pdfgen import canvas

load_dotenv()
    
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_port = os.getenv("POSTGRES_PORT")
postgres_db = os.getenv("POSTGRES_DB")
    
DB_CONNECTION = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@localhost:{postgres_port}/{postgres_db}"
  

def ingest_data():
    df = pd.read_csv('./resources/port_operations.csv')
    df.to_csv('./data/bronze/raw_port_operations.csv', mode='w', index=False)

def transform_data():

    df = pd.read_csv('./data/bronze/raw_port_operations.csv')
    avg_time_per_ship = df.groupby('ship_id')['operation_time'].mean()
    worst_crane = df.groupby('crane_id')['operation_time'].mean()
    worst_crane_top = worst_crane.nlargest(1) 
    print(worst_crane_top)
    print(avg_time_per_ship)
    print(worst_crane_top)
    avg_time_per_ship.reset_index().to_csv('./data/silver/avg_time_per_ship.csv', mode='w+', index=False)
    worst_crane.reset_index().to_csv('./data/silver/crane_operation.csv', mode='w+', index=False)
    
def load_to_postgres():
    
    engine = PostgresHook("postgresql_job_bidding").get_sqlalchemy_engine()
    
    df1_path = './data/silver/avg_time_per_ship.csv'
    df2_path = './data/silver/crane_operation.csv'
    
    avg_time_per_ship_df = pd.read_csv(df1_path)
    crane_operation_df = pd.read_csv(df2_path)
    
    with engine.connect() as conn:
       avg_time_per_ship_df.to_sql('avg_time_per_ship', conn, schema="gold", index=False, if_exists='replace')
       crane_operation_df.to_sql('crane_operation', conn, schema="gold", index=False, if_exists='replace')
       
    avg_time_per_ship_df.to_sql('avg_time_per_ship', con=engine, if_exists='replace', index=False)
    crane_operation_df.to_sql('crane_operation', con=engine, if_exists='replace', index=False)

def generated_pdf_report():
    engine = PostgresHook("postgresql_job_bidding").get_sqlalchemy_engine()
    
    with engine.connect() as conn:
            longest_ship_time_df = pd.read_sql(
                sql = "SELECT * FROM gold.avg_time_per_ship \
                WHERE \
                operation_time = (SELECT operation_time AS operation_time2 FROM gold.avg_time_per_ship ORDER BY operation_time DESC LIMIT 1)",
                con = conn,
            )
            worst_crane_operation_time_df = pd.read_sql(
                sql = "SELECT * FROM gold.crane_operation \
                    WHERE \
                    operation_time = (SELECT operation_time AS operation_time2 FROM gold.crane_operation ORDER BY operation_time DESC LIMIT 1)",
                con = conn,
            )
            
            best_crane_operation_time_df = pd.read_sql(
                sql = "SELECT * FROM gold.crane_operation \
                    WHERE \
                    operation_time = (SELECT operation_time AS operation_time2 FROM gold.crane_operation ORDER BY operation_time ASC LIMIT 1)",
                con = conn,
            )
            
    print("longest ship time")
    print(longest_ship_time_df)
    print("worst ship time")
    print(worst_crane_operation_time_df)
    print("best ship time")
    print(best_crane_operation_time_df)

    
    pdf_file = "./data/report/laporan_mingguan.pdf"
    c = canvas.Canvas(pdf_file)

    c.drawString(100, 800, "Laporan Mingguan Pelabuhan")
    c.drawString(100, 770, f"Kapal Terlama: {longest_ship_time_df.iloc[0]['ship_id']} ({longest_ship_time_df.iloc[0]['operation_time']} jam waktu operasi)")
    c.drawString(100, 740, f"Crane Terbaik: {best_crane_operation_time_df.iloc[0]['crane_id']} ({best_crane_operation_time_df.iloc[0]['operation_time']} jam jam waktu operasi)")
    c.drawString(100, 710, f"Crane Terburuk: {worst_crane_operation_time_df.iloc[0]['crane_id']} ({worst_crane_operation_time_df.iloc[0]['operation_time'].round(1)} jam jam waktu operasi)")
    
    c.save()

default_args = {
    'owner': 'Fitran',
    'start_date': datetime(2025, 2, 2),
    'retries': 1,
}

@dag(
    dag_id="etl_pipeline",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def etl_pipeline():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")
    ingest_task = PythonOperator(task_id="ingest_task", python_callable=ingest_data)
    transform_task = PythonOperator(task_id="transform_task", python_callable=transform_data)
    load_postgres_task = PythonOperator(task_id="load_postgres_task", python_callable=load_to_postgres)
    generated_pdf_report_task = PythonOperator(task_id="generated_pdf_report", python_callable=generated_pdf_report)

    start_task >> ingest_task >> transform_task >> load_postgres_task >> generated_pdf_report_task >> end_task

etl_pipeline()
