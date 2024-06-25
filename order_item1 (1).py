# Impor modul dan library yang diperlukan
import pandas as pd
import fastavro
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Mendefinisikan fungsi utama untuk mengolah data order_item
def order_item_funnel():
    try:
        # Menginisialisasi hook PostgreSQL dan engine SQLAlchemy
        hook = PostgresHook(postgres_conn_id="postgres_dw")
        engine = hook.get_sqlalchemy_engine()

        # Membaca file Avro berisi data order_item dan menyimpannya ke dalam tabel 'order_item' di PostgreSQL
        with open("data/order_item.avro", 'rb') as f:
            reader = fastavro.reader(f)
            records = [r for r in reader]
            df = pd.DataFrame(records)

        # Menulis data ke tabel PostgreSQL
        df.to_sql("order_item", engine, if_exists="replace", index=False)

        # Jika eksekusi sampai di sini tanpa error, maka dianggap berhasil
        print("Data order_item berhasil dimasukkan ke dalam tabel 'order_item'.")
    except Exception as e:
        # Tangani error jika terjadi
        print(f"Terjadi kesalahan: {str(e)}")

# Mendefinisikan argumen default untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Membuat instance DAG dengan parameter yang telah ditentukan
dag = DAG(
    "ingest_order_item",
    default_args=default_args,
    description="Order_item Data Ingestion",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Mendefinisikan tugas (task) untuk mengolah data order_item menggunakan PythonOperator
task_load_order_item = PythonOperator(
    task_id="ingest_order_item",
    python_callable=order_item_funnel,
    dag=dag,
)

# Menjalankan tugas (task) load order_item
task_load_order_item