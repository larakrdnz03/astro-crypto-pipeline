from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine

NEON_DB_URL = "postgresql://neondb_owner:npg_IRiYav7TKA1Z@ep-divine-mouse-a29jty4e-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def fetch_process_store():
    print("🚀 Starting data fetch...")
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=100"
    response = requests.get(url)
    print(f"✅ API status: {response.status_code}")
    
    data = response.json()
    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        '_1', '_2', '_3', '_4', '_5', '_6'
    ])
    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']]
    df = df.astype(float)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')

    if df.empty:
        print("❗ DataFrame is empty. Exiting task.")
        return

    # SMA
    df['sma'] = df['close'].rolling(window=14).mean()

    # EMA
    df['ema'] = df['close'].ewm(span=14, adjust=False).mean()

    # RSI
    delta = df['close'].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14).mean()
    avg_loss = pd.Series(loss).rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # Sadece son 1 satırı yazdır ve yaz
    last_row = df.iloc[[-1]]
    print("Last row to be written:")
    print(last_row)

    try:
        engine = create_engine(NEON_DB_URL)
        last_row.to_sql('btc_usdt_technical', engine, if_exists='append', index=False)
        print("Last row successfully written to Neon DB.")
    except Exception as e:
        print(f"Failed to write to Neon DB: {e}")

with DAG(
    dag_id='crypto_pipeline_dag',
    default_args=default_args,
    schedule='*/5 * * * *',  # ← every 5 minutes
    catchup=False
) as dag:

    fetch_store = PythonOperator(
        task_id='fetch_process_store',
        python_callable=fetch_process_store
    )






    













