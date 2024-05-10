'''
=================================================
Milestone 3

Nama  : Vania Alya Qonita
Batch : FTDS-029-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. 
Adapun dataset yang dipakai adalah dataset mengenai penjualan sebuah supermarket selama 2019.
=================================================
'''

# Import libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2 as db
import pandas as pd
import datetime as dt
from elasticsearch import Elasticsearch

def fetch_from_postgresql():
    '''
    Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.

    Parameters:
    Tidak menerima input parameter. Sudah memiliki beberapa input existing pada fungsi sebagai berikut.
    - dbname : string - nama database tempat penyimpanan data ['airflow']
    - host : string - host dari database ['postgres']
    - table : string - nama tabel penyimpanan postgres ['table_m3']

    Return
    Tidak melakukan return data. Hanya menampilkan "data saved"
        
    Contoh penggunaan:
    get_data_from_postgresql()
    '''
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)

    df=pd.read_sql("select * from table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_vania_alya_data_raw.csv', sep=',', index=False)
    print("-------Data Saved------")

def data_cleaning():
    '''
    Fungsi ini ditujukan untuk melakukan cleaning data terhadap data input. Cleaning dilakukan terhadap penamaan kolom dan isi row.

    Parameters:
    Tidak menerima input parameter. Sudah memiliki beberapa input existing pada fungsi sebagai berikut.
    - data : string - path terhadap data yang akan dilakukan cleaning [P2M3_vania_alya_data_raw.csv]

    Proses:
    Beberapa proses cleaning data yang sudah didefinisikan di dalam fungsi ini adalah sebagai berikut.
    - Pembersihan terhadap nama kolom : Lowercasing, pembersihan whitespace, dan perubahan spasi menjadi underscore(_).
    - Pembersihan data : Drop data null, drop data duplikat, dan perubahan format data date dan time.
    
    Return
    Tidak melakukan return data. Data sudah dilakukan export ke csv di dalam fungsi.
        
    Contoh penggunaan:
    data_cleaning()
    '''
    # Loading data
    data = pd.read_csv("/opt/airflow/dags/P2M3_vania_alya_data_raw.csv")

    # Update penamaan kolom
    ## Lowercasing nama
    data.columns=[x.lower() for x in data.columns]
    ## Menghilangkan whitespace sebelum dan setelah nama kolom
    data.columns = data.columns.str.strip()
    ## Mengganti whitespace pada tengah nama kolom dengan underscore
    data.columns = data.columns.str.replace(' ', '_')

    # Pembersihan data
    ## Melakukan dropping terhadap nilai null
    data.dropna(inplace=True)
    ## Melakukan dropping duplikat
    data.drop_duplicates(inplace=True)
    ## Mengganti tipe data kolom
    data['date'] = pd.to_datetime(data['date'],format='%m/%d/%Y')
    data['time'] = pd.to_datetime(data['time'],format='%H:%M').dt.time
    # Export data ke dalam csv
    data.to_csv('/opt/airflow/dags/P2M3_vania_alya_data_clean.csv', index=False)

def post_to_elasticsearch():
    '''
    Fungsi ini ditujukan untuk mengirimkan data ke ElasticSearch untuk diteruskan dan digunakan oleh Kibana sebagai data visualisasi.

    Parameters:
    Tidak menerima input parameter. Sudah memiliki beberapa input existing pada fungsi sebagai berikut.
    - data : string - path terhadap data yang akan dilakukan cleaning [P2M3_vania_alya_data_clean.csv]
    - index : string - penamaan index untuk data yang dilakukan upload [sales_data_m3]
    - id : int - pendetailan dari index dapat berupa penomoran [i+1]

    Return
    Tidak melakukan return data. Hanya menampilkan respoinse dari ElasticSearch.
        
    Contoh penggunaan:
    post_to_elasticsearch()
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_vania_alya_data_clean.csv')

    for i, r in df.iterrows():
        doc = r.to_dict() # Konversi row ke dictionary
        res = es.index(index="sales_data_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")


default_args = {
    'owner' : 'Vania',
    'start_date': dt.datetime(2024, 4, 27, 0, 0, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    }

with DAG(
    "Sales_data_processing",
    description="Milestone 3 Vania",
    schedule_interval='30 6 * * *',
    default_args= default_args,
    catchup=False
) as dag:
    # Task 1 : Load data
    fetch_data_postgres = PythonOperator(
        task_id='fetch_data_postgres',
        python_callable=fetch_from_postgresql
    )

    # Task 2 : Pembersihan data
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=data_cleaning
    )

    # Task 3 : Upload to Elasticsearch
    post_data = PythonOperator(
        task_id='post_data',
        python_callable=post_to_elasticsearch
    )

    # Menentukan alur proses
    fetch_data_postgres >> clean_data >> post_data

