##########################################################################
#      PACKAGES
##########################################################################

from sys import argv

import pandas as pd
import yfinance as yf
import sqlite3

from datetime import date

today=date.today()

##########################################################################
#      LECTURA DE DATOS
##########################################################################


# Function that downloads data from YF
def get_stock_data(symbol, start, end):
    data = yf.download(symbol, start=start, end=end)
    data.reset_index(inplace=True)
    data.rename(columns={
        "Date": "date",
        "Open": "open",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume"
    }, inplace=True)
    data['symbol'] = symbol
    return data

# Function that save the data in the DataFrame into the database
def save_data_range(symbol, start, end, con):
    data = get_stock_data(symbol, start, end)
    data.to_sql(
        "stock_data", 
        con, 
        if_exists="append", 
        index=False
    )
    

# Function that grabs data from today and inserts it into the database
def save_last_trading_session(symbol, con):
    today = pd.Timestamp.today()
    data = get_stock_data(symbol, today, today)
    data.to_sql(
        "stock_data", 
        con, 
        if_exists="append", 
        index=False
    )
    


# Choose stocks to follow
stocks=["AAPL", "META", "XOM", "QQQ", "SQQQ"]



#Get historical data from those stocks

# create an empty dictionary to store the dataframes
dfs = {}


# Read all tickers we want:
for stock in stocks:
    df=get_stock_data(stock, start="1990-01-01", end=str(today))
    dfs[stock]=df

#Save it to the SQL database

# Create the conection to SQL

con = sqlite3.connect("market_data.sqlite")

for stock in stocks:
    df=save_data_range(stock, start="1990-01-01", end=str(today), con=con)
    dfs[stock]=df

# In case I would like to update and add data from today
for stock in stocks:
    df=save_last_trading_session(stock, con=con)
    dfs[stock]=df























