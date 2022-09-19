from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'Hige',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['hige@fakeemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=35),
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'pool',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}



dag = DAG('Cripto_Scraper',
          description="A demo dag for 75.06",
          default_args=default_args,
          schedule_interval=timedelta(minutes=60),
          catchup=False,
          #on_success_callback=get_logging_callback("Mission success :)"),
          #on_failure_callback=get_logging_callback("Mission failed  :(", True),
          concurrency=1,
          max_active_runs=1)



#------------------------------------------------------------------------------
import requests
import pandas as pd

def fetch_data(**kwargs):
    response = requests.get("https://api2.binance.com/api/v3/ticker/24hr")
    json = response.json()

    df = pd.DataFrame(json)
    df = df[[ "symbol","openPrice","lastPrice","highPrice","lowPrice"]]

    df.to_parquet("/data/fetched.pqt")


from datetime import date

def convert_data(**kwargs):
    df = pd.read_parquet("/data/fetched.pqt")

    df = df.set_index("symbol")
    df = df.astype(float)
    df["fetch_date"] = date.today()
    df = df.reset_index()

    df.to_parquet("/data/converted.pqt")

def acumulate_raw_data(**kwargs):
    try:
        db = pd.read_parquet("/data/raw_data.pqt")
    except:
        db = pd.DataFrame([])

    df = pd.read_parquet("/data/converted.pqt")

    df = pd.concat([db,df])
    df = df.drop_duplicates(["symbol","fetch_date"],keep="last")
    df = df.reset_index()
    df.pop("index")

    df.to_parquet("/data/raw_data.pqt")


fetcher = PythonOperator(task_id='fetcher',python_callable=fetch_data,dag=dag)
converter = PythonOperator(task_id='converter',python_callable=convert_data,dag=dag)
raw_data_storer = PythonOperator(task_id='raw_data_storer',python_callable=acumulate_raw_data,dag=dag)

fetcher >> converter
converter >> raw_data_storer


#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

procesors = {}

def get_processor(symbol):
    def procesor(**kwargs):
        df = pd.read_parquet("/data/converted.pqt")

        starts = df[df.symbol.map(lambda x: x.startswith(symbol))]
        ends = df[df.symbol.map(lambda x: x.endswith(symbol))]

        starts["base"] = symbol
        starts["conversion"] = starts.symbol.map(lambda x: x.replace(symbol,""))


        ends["base"] = symbol
        ends["conv"] = ends.symbol.map(lambda x: x.replace(symbol,""))
        ends.lowPrice = 1 / ends.lowPrice
        ends.openPrice = 1 / ends.openPrice
        ends.lastPrice = 1 / ends.lastPrice
        ends.highPrice = 1 / ends.highPrice

        df = pd.concat([starts,ends]).dropna()
        df.to_parquet(f"/data/{symbol}_procesed.pqt")

    return procesor

for symbol in  Variable.get("procesing_symbols").split(","):

    procesor = get_processor(symbol)
    procesors[symbol] = PythonOperator(task_id=f'procesor_{symbol}',python_callable=procesor,dag=dag)


def store_processed_data(**kwargs):
    data = [pd.read_parquet(f"/data/{symbol}_procesed.pqt") for symbol in Variable.get("procesing_symbols").split(",")]
    try:
        data = [pd.read_parquet("/data/data.pqt")]+data
    except:
        pass

    data = pd.concat(data)

    data = data.drop_duplicates(["base","conv","fetch_date"],keep="last")
    data = data.reset_index()

    data.to_parquet("/data/data.pqt")

data_storer = PythonOperator(task_id=f'data_storer',python_callable=store_processed_data,dag=dag)

converter >> list(procesors.values())
list(procesors.values()) >> data_storer

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------


if "ETH" in procesors:

    def show_eth_value(**kwargs):
        df = pd.read_parquet(f"/data/ETH_procesed.pqt")

        df = df.set_index("conv") 
        price = df.loc["DAI"].lastPrice

        with open("/data/eth_value.txt") as out:
            out.write(f"Eth value in USD: {price}")

    PythonOperator(task_id=f'eth_informer',python_callable=show_eth_value,dag=dag).set_upstream(procesors["ETH"])