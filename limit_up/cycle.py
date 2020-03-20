import pandas as pd
import datetime
import tushare as ts
# from numpy import arange
# import stock.util.sheep as sheep
# import stock.limit_up.get_limit_stock as gls

from stock.sql.data import save_data, read_data
df=read_data('daily',start_date='20200101').iloc[:,:6]
df.sort_values('trade_date',ascending=True,inplace=True)
df['5_close']=df.groupby('ts_code')['close'].shift(4)
df['next_4_days']=df.groupby('ts_code')['trade_date'].shift(4)
df['a']=df['trade_date']+pd.tseries.offsets.MonthOffset
df['week']=pd.to_datetime(df['trade_date']).dt.dayofweek
df['month']=pd.to_datetime(df['trade_date']).dt.month

