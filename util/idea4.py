# 个股连续5日上涨，最近2天阴险，下跌小于10，下一天涨停概率
import datetime
import os

# import numpy as np
# import math
import pandas as pd
# # import stockfilter
#
import tushare as ts

# import util.sheep as sheep
# import util.fuquan as fuquan
#
#
# # ts.set_token('006b49622d70edc237ab01340dc210db15d9580c59b40d028e34e015')
z=pd.date_range(start='20191202',end='20191230')


pro = ts.pro_api()
# tool = basic.basic()
# data=pro.daily(ts_code='002943.SH')
stock_filter=pro.namechange()
stock_filter=stock_filter[stock_filter['name'].str.contains('ST|退') == True]
# stock_filter['end_date'].fillna(str(datetime.datetime.today()+datetime.timedelta(2)).replace('-',''))
stock_filter['end_date']=stock_filter['end_date'].fillna(str(datetime.datetime.today()+datetime.timedelta(2)).replace('-','')[:8])
print(stock_filter.shape)
print(type(stock_filter.iloc[0]['start_date']))
# print(stock_filter.loc[11])
# print(stock_filter.iloc[11])


res=pd.DataFrame()
for i in range(stock_filter.shape[0]):
    df=pd.DataFrame()
    source=stock_filter.iloc[i]
    print(source)
    df['trade_date']=pd.date_range(start=source['start_date'],end=source['end_date'])
    df['trade_date2']=pd.to_datetime(df['trade_date'])
    df['ts_code']=source['ts_code']
    df['name']=source['name']
    print(type(df.iloc[0]['trade_date2']))
    res=pd.concat([df,res],ignore_index=True)

data=pd.DataFrame()

stock_filter.to_csv('namechange2.csv',encoding='utf_8_sig')
today=datetime.datetime.today().date()

path = 'D:\\workgit\\stock\\util\\stockdata\\'

while (not os.path.isfile(path + str(today) + '\data.csv')) or (
not os.path.isfile(path + str(today) + '\daily-basic.csv')):
    today = today - datetime.timedelta(1)

path_source=os.getcwd() +'\\stockdata\\'+str(today)+'\\'




path_source=os.getcwd() +'\\stockdata\\'+str(today)+'\\'

# print(os.path.dirname(os.getcwd()))
data=pd.read_csv(path_source + 'data.csv', index_col=0, dtype={'trade_date': object})
# data=data[data['trade_date']>='20191212']
print(data.shape)

# tool.pre_data(data,label=list('abcd'))
t1=datetime.datetime.today()
z=tool.pre_data(data)
print(datetime.datetime.today()-t1)
data.to_csv('data.csv')
z.to_csv('dz.csv')
