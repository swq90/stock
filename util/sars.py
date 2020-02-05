import os
import numpy as np
import pandas as pd
import tushare as ts
from pandas import DataFrame

path = os.getcwd()
START = '20200101'
END = '20200204'
pro = ts.pro_api()

date_list: DataFrame = pd.DataFrame()
raw_data = pd.DataFrame()
limit_list = pd.DataFrame()
stock_basic = pd.DataFrame()
if os.path.isfile(path + '%s-%srawdata.csv' % (START, END)):
    raw_data = pd.read_csv(path + '%s-%srawdata.csv' % (START, END), index_col=0, dtype={'trade_date': object})
else:
    date_list['trade_date'] = pd.date_range(start=START, end=END)
    date_list['trade_date'] = date_list['trade_date'].astype(str).apply(lambda x: x.replace('-', ''))
    # print(pro.limit_list(trade_date='20161229'))
    for date in date_list['trade_date']:
        # z = pro.limit_list(trade_date=date)
        # limit_list = pd.concat([limit_list, pro.limit_list(trade_date=date)], ignore_index=True)
        raw_data = pd.concat([raw_data, pro.daily(trade_date=date)], ignore_index=True)
        # print(raw_data.shape)
        # stock_baisc=pd.concat([stock_baisc,pro.stock_basic(trade_date=date)],ignore_index=True)
    raw_data.to_csv(path + '%s-%srawdata.csv' % (START, END))
print(raw_data.shape, limit_list.shape)

for status in list('LDP'):
    # stock_baisc = pro.stock_basic(trade_date=END,tradelist_status=status)
    stock_basic = pd.concat([pro.stock_basic(trade_date=END, list_status=status), stock_basic], ignore_index=True)

stock_basic.drop_duplicates(inplace=True)
print(stock_basic.shape)
data = raw_data.merge(stock_basic, on='ts_code', how='outer')
data = data[data['pct_chg'] >= 9.90]

up_dis = data.groupby(by=['trade_date', 'industry'])['ts_code'].size()
# z=up_dis.get_loc_level('20020226')
up_dis=pd.DataFrame(up_dis)

up_dis.reset_index(inplace=True)
up_dis.sort_values(by='ts_code',ascending=False,inplace=True)
# up_dis.droplevel('trade_date')
print(up_dis.index)

