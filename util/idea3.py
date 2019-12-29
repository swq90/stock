# 前n-5到n-1日连续上涨，n到n+1两日连续跌下跌，跌幅小于n-1日close的10%，n+2日涨停的概率

import os
import datetime
import pandas as pd

import util.basic as tool
import tushare as ts

# import util.basic as basic
import util.sheep as sheep
pro=ts.pro_api()
today=datetime.datetime.today().date()

path_source=os.getcwd() +'\\stockdata\\'+str(today)+'\\'

# print(os.path.dirname(os.getcwd()))
data=pd.read_csv(path_source + 'data.csv', index_col=0, dtype={'trade_date': object})
print(data.info())


up_data=tool.basic().up_info(data, days=5, up_range=0.5, pct=1,revise=0)
up_data.rename(columns={'trade_date':'up_date'},inplace=1)
up_data.to_csv('up.csv')

down_data=tool.basic().up_info(data,days=2, up_range=-0.1,up_range_top=-0.01,limit=1,revise=0,pct=0)
down_data=down_data[['ts_code','trade_date','up_pct']]
down_data.rename(columns={'up_pct':'down_pct'},inplace=1)
down_data.to_csv('down.csv')

down_data=down_data.merge(up_data,left_on=['ts_code','trade_date'],right_on=['ts_code','up_date'])
down_data.to_csv('aaa.csv')
print(down_data)

# pre_data=pre_data[pre_data['up_n_pct']<0.1]
# print(up_data)

