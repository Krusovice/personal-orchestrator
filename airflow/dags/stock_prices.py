from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import yfinance as yf
from datetime import datetime
import pandas as pd
import yaml
import logging
log = logging.getLogger(__name__)

"""
This dag fetches stockprices from yfinance and stores them in the postgres database.
"""

def fetch_stock_data(ticker, start_date):
    stock = yf.Ticker(ticker)
    stock_data = stock.history(start=start_date, end=datetime.today())
    if stock_data.empty:
        return [] 

    stock_data.reset_index(inplace=True)
    stock_data = stock_data[['Date', 'Close']]
    stock_data['ticker'] = ticker
    stock_data['Date'] = stock_data['Date'].dt.strftime('%Y-%m-%d')

    # lowering all first letters in columns.
    stock_data.columns = stock_data.columns.str.lower()

    # Reanming the column close to closing_price to make it more descriptive
    stock_data.rename(columns={'close': 'closing_price'}, inplace=True)
    
    return stock_data

@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["stock_prices"],
)


def fetch_and_store_stock_data():
    @task(multiple_outputs=True)
    def load_tickers():
        with open('/inputs/stocks_list.yml', 'r') as f:
            stock_fetching_config = yaml.safe_load(f)

        ticker_list = list(stock_fetching_config['metadata'].keys())

        start_date = stock_fetching_config['defaults']['start_date']

        log.info(f'ticker_list: {ticker_list}')

        return {"ticker_list": ticker_list, "start_date": start_date}


    @task()
    def fetch_data(ticker_list, start_date):
        stock_data_list = []

        for ticker in ticker_list:
            stock_data = fetch_stock_data(ticker, start_date)
            stock_data_list.append(stock_data)

        stock_data_df = pd.concat(stock_data_list, ignore_index=True)

        log.info(f'stock_data_df:\n {stock_data_df}')
        return stock_data_df.to_dict(orient="records")


    @task()
    def store_data(stock_data_dict):
        hook = PostgresHook(postgres_conn_id="webpage_postgres_db")
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
        insert into stockmarket.stock_prices (date, ticker, closing_price)
        VALUES (%s, %s, %s)
        ON CONFLICT (date,ticker) DO UPDATE
        SET closing_price = EXCLUDED.closing_price
        """

        stock_data_df = pd.DataFrame(stock_data_dict)
        for row in stock_data_df.itertuples(index=False):  # or index=True if you need the index
            cursor.execute(insert_query, (row.date, row.ticker, row.closing_price))
        
        conn.commit()
        cursor.close()
        conn.close()


    output = load_tickers()
    ticker_list = output['ticker_list']
    start_date = output['start_date']
    
    stock_data_dict = fetch_data(ticker_list, start_date)
    
    store_data(stock_data_dict)

fetch_and_store_stock_data()