import sqlite3
import pandas as pd

# connect to the database
con = sqlite3.connect("market_data.sqlite")

# simple select statement
df_1 = pd.read_sql_query("SELECT * from stock_data where symbol='QQQ'", con)
