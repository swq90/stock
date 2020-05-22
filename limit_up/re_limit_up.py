#开盘涨停破板后回封次日表现

import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data ,read_data
from stock.util.basic import basic
def re_limit_up(start_date,end_date):
    rawdata=read_data('daily',start_date=start_date,end_date=end_date)
    # list_days=basic().list_days(rawdata)
    limit=read_data('stk_limit',start_date=start_date,end_date=end_date)
    data=rawdata.merge(limit[['ts_code','trade_date','up_limit']],on=['ts_code','trade_date'])
    print(data.shape)
    data['red_line']=data.apply(lambda x:1 if x['low']==x['up_limit'] else 0,axis=1)
    data['reback_limit']=data.apply(lambda x:1 if (x['open']==x['close'])&(x['open']==x['up_limit'])&(x['low']<x['close']) else 0,axis=1)
    print(data.shape)
    pre=basic().pre_data(data,label=['red_line'],pre_days=2)
    data=data.merge(pre[['ts_code','trade_date','pre_2_red_line']],on=['ts_code','trade_date'])
    pre=basic().pre_data(data,label=['reback_limit'],pre_days=1)
    data=data.merge(pre[['ts_code','trade_date','pre_1_reback_limit']],on=['ts_code','trade_date'])
    print(data.shape)
    data=data.loc[(data['pre_2_red_line']==1)&(data['pre_1_reback_limit']==1)]
    print(data.shape)
    #
    save_data('破板后回封表现.csv')
    # data=data.loc[(data['up_limit']==data['close'])&(data['open']==data['close'])&(data['low']<data['up_limit'])]
    # print(data.shape)
    data=data.merge(basic().list_days(data,list_days=15))
    print(data.shape)
    wool=sheep.wool(data,rawdata)
    return wool
re_limit_up('20190101','20191231')