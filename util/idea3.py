# 前n-5到n-1日连续上涨，n到n+1两日连续跌下跌，跌幅小于n-1日close的10%，n+2日涨停的概率

import os
import datetime
import pandas as pd

import util.basic as basic
import tushare as ts

# import util.basic as basic
import util.sheep as sheep


pro=ts.pro_api()
tool = basic.basic()



path = 'D:\\workgit\\stock\\util\\stockdata\\'

today=datetime.datetime.today().date()
while (not os.path.isfile(path + str(today) + '\data.csv')) or (
not os.path.isfile(path + str(today) + '\daily-basic.csv')):
    today = today - datetime.timedelta(1)

path_source=os.getcwd() +'\\stockdata\\'+str(today)+'\\'

# print(os.path.dirname(os.getcwd()))
data=pd.read_csv(path_source + 'data.csv', index_col=0, dtype={'trade_date': object})
print(data.info())


up_data=tool.up_info(data, days=5, up_range=0.2, pct=0,limit=1,revise=0)
up_data=up_data.iloc[:,[0,1,-3,-1]]
up_data.rename(columns={'trade_date':'up_date'},inplace=1)
up_data.to_csv('up.csv')

down_data=tool.up_info(data,days=2, up_range=-0.1,up_range_top=-0.09,limit=1,revise=0,pct=0)
down_data=down_data[['ts_code','trade_date','up_pct']]
down_data.rename(columns={'up_pct':'down_pct'},inplace=1)
down_data.to_csv('down.csv')
pre_date=tool.pre_date(down_data[['trade_date']],days=2)
down_data=down_data.merge(pre_date.rename(columns={'pre_2_date':'up_date'}),on='trade_date')

data_need=down_data.merge(up_data,on=['ts_code','up_date'])

down_data.to_csv('aaa.csv')

# pre_data=pre_data[pre_data['up_n_pct']<0.1]
# print(up_data)

