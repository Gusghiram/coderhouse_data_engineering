import yfinance as yf
import pandas as pd
import psycopg2
from getpass import getpass
import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from psycopg2.extras import execute_values

tickers = yf.Tickers('msft aapl goog tsla amzn meta vz cost wmt nke')
companies = ('MSFT','AAPL','GOOG','TSLA','AMZN','META','VZ','COST','WMT','NKE')
historical = pd.DataFrame()

host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user=""
pwd = '' #getpass()

def yahoo_finance_extraction():

    for i in  range(len(companies)):
        df = tickers.tickers[companies[i]].history(period="5y")
        df['symbol'] = tickers.tickers[companies[i]].info['symbol']
        df['short_name'] = tickers.tickers[companies[i]].info['shortName']
        df['long_name'] = tickers.tickers[companies[i]].info['longName']
        df['address'] = tickers.tickers[companies[i]].info['address1']
        df['city'] = tickers.tickers[companies[i]].info['city']
        df['state'] = tickers.tickers[companies[i]].info['state']
        df['zip'] = tickers.tickers[companies[i]].info['zip']
        df['country'] = tickers.tickers[companies[i]].info['country']
        df['phone'] = tickers.tickers[companies[i]].info['phone']
        df['website'] = tickers.tickers[companies[i]].info['website']
        df['industry'] = tickers.tickers[companies[i]].info['industry']
        df['sector'] = tickers.tickers[companies[i]].info['sector']
        historical = pd.concat([historical,df], ignore_index=False)

    historical = historical.reset_index()
    subrogate_key = historical['Date'].apply(lambda x: dt.datetime.strftime(x,"%Y%m%d%H%M%S")) + historical['symbol']
    historical['stock_id'] = subrogate_key
    historical['upload_date'] = dt.datetime.date(dt.datetime.now())
    historical['Date'] = historical['Date'].apply(lambda x: dt.datetime.strptime(dt.datetime.strftime(x,"%Y-%m-%d"),"%Y-%m-%d"))
    historical['Date'] = historical['Date'].apply(lambda x: dt.datetime.date(x))
    historical = historical.rename(columns={'Open':'open_value', 'Close':'close_value','High':'high_value', 'Low':'low_value', 'Stock Splits':'stock_splits'})

def create_table():
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conexión a Redshift Existosa!")
        
    except Exception as e:
        print("No es posible conectarse a Redshift")
        print(e)

    create_table = ''' CREATE TABLE IF NOT EXISTS gustavo_hiram_gutierrez_coderhouse.stock_market  (
        stock_id varchar(25) NOT NULL PRIMARY KEY,
        symbol VARCHAR(100) NOT NULL,
        short_name VARCHAR(100) NOT NULL,
        long_name VARCHAR(100) NOT NULL,
        address VARCHAR(100) NOT NULL,
        city VARCHAR(100) NOT NULL,
        state VARCHAR(100) NOT NULL,
        zip VARCHAR(100) NOT NULL,
        country VARCHAR(100) NOT NULL,
        phone VARCHAR(100) NOT NULL,
        website VARCHAR(100) NOT NULL,
        industry VARCHAR(100) NOT NULL,
        sector VARCHAR(100) NOT NULL,
        date date NOT NULL,
        open_value DECIMAL NOT NULL,
        high_value DECIMAL NOT NULL,
        low_value DECIMAL NOT NULL,
        close_value DECIMAL NOT NULL,
        volume integer NOT NULL,
        dividends DECIMAL NOT NULL,
        stock_splits DECIMAL NOT NULL,
        upload_date date NOT NULL
    )'''

    with conn.cursor() as cur:
        cur.execute(create_table)
        conn.commit()
    print('Tabla Creada de Manera Exitosa!')



def insert_data():
    
    conn = psycopg2.connect(
        host=host,
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
        )
    
    with conn.cursor() as cur:
        execute_values(
            cur,
            '''
            INSERT INTO gustavo_hiram_gutierrez_coderhouse.stock_market (Date, open_value, high_value, low_value,
            close_value, Volume, Dividends, stock_splits,symbol,short_name,long_name,address,city,state,zip,country,phone,
            website,industry,sector,stock_id,upload_date)
            VALUES %s
            ''',
            [tuple(row) for row in historical.values],
            page_size=len(historical)
        )
        conn.commit()
    print('Data correctamente insertada!')
