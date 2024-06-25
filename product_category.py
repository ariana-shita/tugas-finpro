# Impor modul dan library yang diperlukan
import pandas as pd  # Mengimpor pandas untuk mengelola data dalam format DataFrame
from datetime import datetime  # Mengimpor modul datetime untuk mengatur tanggal mulai DAG
from airflow import DAG  # Mengimpor kelas DAG dari Airflow untuk membuat alur kerja (workflow)
from airflow.operators.python_operator import PythonOperator   # Mengimpor PythonOperator dari Airflow untuk menjalankan fungsi Python sebagai tugas (task)
from airflow.hooks.postgres_hook import PostgresHook  # Mengimpor PostgresHook dari Airflow untuk menghubungkan Airflow dengan PostgreSQL

# Mendefinisikan fungsi utama untuk mengolah data product category
def product_category_funnel():

    try:
        # Menginisialisasi hook PostgreSQL dan engine SQLAlchemy
        hook = PostgresHook(postgres_conn_id="postgres_dw")
        engine = hook.get_sqlalchemy_engine()

        # Membaca file Excel berisi data product category dan menyimpannya ke dalam tabel 'product_categories' di PostgreSQL
        df = pd.read_excel("data/product_category.xls")
        df.to_sql("product_categories", engine, if_exists="replace", index=False)
        
        # Jika eksekusi sampai di sini tanpa error, maka dianggap berhasil
        print("Data product category berhasil dimasukkan ke dalam tabel 'product_categories'.")
    except Exception as e:
        # Tangani error jika terjadi
        print(f"Terjadi kesalahan: {str(e)}")

# Mendefinisikan argumen default untuk DAG
default_args = {
    "owner": "airflow",                     # Pemilik DAG
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,                           # Jumlah pengulangan jika tugas gagal
}

# Membuat instance DAG dengan parameter yang telah ditentukan
dag = DAG(                                 
    "ingest_product_category",              # ID DAG
    default_args=default_args,              # Menggunakan argumen default yang telah didefinisikan
    description="Product Category Data Ingestion",  # Deskripsi DAG
    schedule_interval="@once",              # Jadwal eksekusi DAG (sekali saja)
    start_date=datetime(2023, 1, 1),        # Tanggal mulai eksekusi DAG
    catchup=False,                          # Tidak menjalankan eksekusi tertunda
)

# Mendefinisikan tugas (task) untuk mengolah data product category menggunakan PythonOperator
task_load_product_category = PythonOperator(            
     task_id="ingest_product_category",      # ID tugas
     python_callable=product_category_funnel, # Fungsi Python yang akan dijalankan
     dag=dag,                                # DAG tempat tugas ini akan dijalankan
)

# Menjalankan tugas (task) load product category
task_load_product_category