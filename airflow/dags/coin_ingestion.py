import json
import requests
import numpy as np
import pandas as pd
from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def ingest_coinmarketcap_category_data(**kwargs):
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/categories"
    
    headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '76d4d417-3475-4bd4-809d-b4b58f590e49',
    }

    response = requests.get(url, headers=headers)

    df = pd.DataFrame(response.json()['data'])

    df = df[['id', 'name','market_cap', 'market_cap_change', 'volume']]
    df['top_3_coins_id'] = None
    df['top_3_coins'] = None
    df['content'] = None
    df = df.rename(columns={
        "market_cap_change": "market_cap_change_24h",  "volume": "volume_24h"
    })

    conditions = [
        df['name'].str.contains('Portfolio', case=False, na=False),
        df['name'].str.contains('EcoSystem', case=False, na=False),
        df['name'].str.contains('Meme', case=False, na=False),
        df['name'].str.contains('Gaming', case=False, na=False),
        df['name'].str.contains('AI', case=False, na=False)
    ]

    # Define corresponding values for the 'type' column
    types = ['Portfolio', 'EcoSystem', 'Meme', 'Gaming', 'AI']

    # Create the 'type' column based on the conditions
    df['type'] = np.select(conditions, types, default='Other')
    df['source'] = 'coinmarketcap'
    execution_date = kwargs['execution_date']
    execution_date += timedelta(days=2)
    execution_date_str = execution_date.strftime('%Y-%m-%d')
    start_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = execution_date_str
    df['updated_at'] = execution_date.strftime('%Y-%m-%d %H:%M:%S')
     # Use PostgresHook to connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='crypto_postgres_conn_id')  # Ensure you have the connection setup in Airflow

    # Get the connection and cursor
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Step 1: Delete data within the range of updated_at between (execution_date-1) and (execution_date)
    delete_query = f"""
    DELETE FROM public.coin_category 
    WHERE 
        source ='coingecko' 
        AND updated_at >= '{start_date}' 
        AND updated_at < '{end_date}'
    """
    cursor.execute(delete_query)
    conn.commit()  # Commit the delete operation

   # Step 2: Insert new data
    insert_query = """
    INSERT INTO public.coin_category (id, name, market_cap, market_cap_change_24h, content, 
                                    top_3_coins_id, top_3_coins, volume_24h, updated_at, type, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare data for insertion
    insert_data = [
        (
            row['id'], row['name'], row['market_cap'], row['market_cap_change_24h'],
            row['content'], row['top_3_coins_id'], row['top_3_coins'], 
            row['volume_24h'], row['updated_at'], row['type'], row['source']
        )
        for _, row in df.iterrows()
    ]

    # Execute insertion in batches to optimize performance
    cursor.executemany(insert_query, insert_data)

    # Commit the insert operation
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    print(f"Data ingestion completed for execution_date {start_date}")


def ingest_coingecko_category_data(**kwargs):
    # Fetch data from CoinGecko API
    url = "https://api.coingecko.com/api/v3/coins/categories"
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": "CG-sjmGY4mc3Gk3BwFeCfXNe1vV"
    }
    response = requests.get(url, headers=headers)

    # Convert response to DataFrame
    df = pd.DataFrame(response.json())

    # Define conditions for each type
    conditions = [
        df['name'].str.contains('Portfolio', case=False, na=False),
        df['name'].str.contains('EcoSystem', case=False, na=False),
        df['name'].str.contains('Meme', case=False, na=False),
        df['name'].str.contains('Gaming', case=False, na=False),
        df['name'].str.contains('AI', case=False, na=False)
    ]

    # Define corresponding values for the 'type' column
    types = ['Portfolio', 'EcoSystem', 'Meme', 'Gaming', 'AI']

    # Create the 'type' column based on the conditions
    df['type'] = np.select(conditions, types, default='Other')
    df['source'] = 'coingecko'

    # Filter rows to keep only those where 'updated_at' is not NaN
    df = df[df['updated_at'].notna()]

    # Get the execution_date from the context (for the current DAG run)
    execution_date = kwargs['execution_date']
    execution_date += timedelta(days=2)
    execution_date_str = execution_date.strftime('%Y-%m-%d')

    # Calculate the date range for the DELETE (from execution_date-1 to execution_date)
    start_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = execution_date_str

    # Use PostgresHook to connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='crypto_postgres_conn_id')  # Ensure you have the connection setup in Airflow

    # Get the connection and cursor
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Step 1: Delete data within the range of updated_at between (execution_date-1) and (execution_date)
    delete_query = f"""
    DELETE FROM public.coin_category 
    WHERE 
        source ='coingecko' 
        AND updated_at >= '{start_date}' 
        AND updated_at < '{end_date}'
    """
    cursor.execute(delete_query)
    conn.commit()  # Commit the delete operation

   # Step 2: Insert new data
    insert_query = """
    INSERT INTO public.coin_category (id, name, market_cap, market_cap_change_24h, content, 
                                    top_3_coins_id, top_3_coins, volume_24h, updated_at, type, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare data for insertion
    insert_data = [
        (
            row['id'], row['name'], row['market_cap'], row['market_cap_change_24h'],
            row['content'], row['top_3_coins_id'], row['top_3_coins'], 
            row['volume_24h'], row['updated_at'], row['type'], row['source']
        )
        for _, row in df.iterrows()
    ]

    # Execute insertion in batches to optimize performance
    cursor.executemany(insert_query, insert_data)

    # Commit the insert operation
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    print(f"Data ingestion completed for execution_date {start_date}")

dag = DAG(
    'coin_gecko_category_ingestion',
    description='Ingest CoinGecko category data into PostgreSQL',
    schedule_interval='0 8,20 * * *',  # Run at 8 AM and 8 PM daily
    start_date=days_ago(1),
    catchup=False,  # To avoid backfilling, run only for the current day
)

# Define the PythonOperator task for data ingestion
ingest_coingecko = PythonOperator(
    task_id='ingest_coingecko_category_data',
    python_callable=ingest_coingecko_category_data,
    provide_context=True,  # Pass Airflow context to the function
    dag=dag,
)

# Define the PythonOperator task for data ingestion
ingest_coinmarketcap = PythonOperator(
    task_id='ingest_coinmarketcap_category_data',
    python_callable=ingest_coinmarketcap_category_data,
    provide_context=True,  # Pass Airflow context to the function
    dag=dag,
)


[ingest_coingecko, ingest_coinmarketcap]