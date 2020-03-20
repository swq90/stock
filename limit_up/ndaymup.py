import pandas as pd
from functools import partial
from stock.sql.data import read_data, save_data,cal
import stock.util.basic as basic

# N天M板
N,M=7,3
limit_up=read_data('stk_limit')
daily=read_data('daily')
data=daily.merge(limit_up,on=['ts_code','trade_date'])
print(limit_up.shape,daily.shape,data.shape)
data['UP']=data.apply(lambda x :1 if x['close']==x['up_limit'] else 0,axis=1)
print(data.shape,data.dropna().shape)

data.sort_values(['ts_code','trade_date'],inplace=True)
data.reset_index(drop=True,inplace=True)

data['times']=data.groupby('ts_code')['UP'].rolling(7).sum().reset_index(drop=True)

# df=data.groupby('ts_code')['UP'].rolling(7).sum().reset_index()

print()
