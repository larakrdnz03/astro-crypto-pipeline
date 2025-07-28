from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine
import ta

NEON_DB_URL = "postgresql://<username>:<password>@<host>:5432/<dbname>?sslmode=require"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'crypto_pipeline_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

def fetch_process_store():
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=100"
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        '_1', '_2', '_3', '_4', '_5', '_6'
    ])
    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']]
    df = df.astype(float)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')

    df['sma'] = ta.trend.sma_indicator(df['close'], window=14)
    df['ema'] = ta.trend.ema_indicator(df['close'], window=14)
    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
    
    engine = create_engine(NEON_DB_URL)
    df.to_sql('btc_usdt_technical', engine, if_exists='append', index=False)

with dag:
    fetch_store = PythonOperator(
        task_id='fetch_process_store',
        python_callable=fetch_process_store
    )












