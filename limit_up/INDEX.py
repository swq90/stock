import os
import sys
import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data
from stock.util.basic import basic
import stock.limit_up.get_limit_stock as gls
import tushare as ts

from stock.sql.data import save_data, read_data

pro = ts.pro_api()
START, END = '20170101', '20200401'
# #股票公告
# df = pro.anns(ts_code='600627.SH')
# #股票更名记录
# namechange=pro.query('namechange')
# 股票基本信息
# basic=read_data('stock_basic')
# 纳斯达克指数
def index_cycle(ts_code,start_date=None,end_date=None):
    # IXIC = pro.query('index_global', ts_code=ts_code, start_date=START)
    # # index_basic=pro.query('index_basic',market='data')
    # # 上证指数
    data = pro.query('index_daily',ts_code='000001.SH', start_date=START,end_date=END)
    data['today'] = pd.to_datetime(data['trade_date']).dt.dayofweek
    df=data[['trade_date','pct_chg','today','close','pre_close']].copy()
    df['today']=df['today']+1
    df['pct']=df['close']/df['pre_close']
    save_data(df,'周一周五%s-%s.csv'%(start_date,end_date))
    # save_data(df,'2019-周一周五.csv')
    #
    # data['pre_4_close'] = data['close'].shift(-4)
    # data['pre_1_date'] = data['today'].shift(-1)
    # data['pre_4_date'] = data['today'].shift(-4)
    # # 周一与上周五
    # # data.dropna(inplace=True)
    # # df1 = data.loc[(data['today'] == 0) & (data['pre_1_date'] == 4)].copy()
    # # save_data(df1,'周一数据%s-%s.csv'%(start_date,end_date))
    # # print(df1['pct_chg'].describe()
    # #       )
    # # # 本周五与本周一
    # # # df2 = data.loc[(data['today'] == 4) & (data['pre_4_date'] == 0)].copy()
    # # # df2['pct_chg2'] = (df2['close'] / df2['pre_4_close'] - 1) * 100
    # # df2 = data.loc[(data['today'] == 4) ].copy()
    # # save_data(df2,'周五数据%s-%s.csv'%(start_date,end_date))
    # # # print(df2['pct_chg2'].describe())
    # # print(df2['pct_chg'].describe())
    # df1 = data.loc[(data['today'] == 0)].copy()
    # save_data(df1,'周一数据%s-%s.csv'%(start_date,end_date))
    # df2 = data.loc[(data['today'] == 4) ].copy()
    # save_data(df2,'周五数据%s-%s.csv'%(start_date,end_date))





# data['lastday_week']=pd.to_datetime(data['trade_date']).dt.dayofweek
# data['monday/friday']=data['close']/data['5_close']
index_cycle('IXIC',start_date=START,end_date=END)
print()
