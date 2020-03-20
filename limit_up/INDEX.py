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
# START, END = '20170101', '20200401'
# #股票公告
# df = pro.anns(ts_code='600627.SH')
# #股票更名记录
# namechange=pro.query('namechange')
# 股票基本信息
# basic=read_data('stock_basic')
# 纳斯达克指数
def index_cycle(ts_code='000001.SH',start_date=None,end_date=None):
    # IXIC = pro.query('index_global', ts_code=ts_code, start_date=START)
    # # index_basic=pro.query('index_basic',market='data')
    # # 上证指数
    data = pro.query('index_daily',ts_code=ts_code, start_date=start_date,end_date=end_date)
    data['today'] = pd.to_datetime(data['trade_date']).dt.dayofweek
    df=data[['trade_date','pct_chg','today','close','pre_close']].copy()
    df['today']=df['today']+1
    df['pct']=df['close']/df['pre_close']

    # save_data(df,'周一周五%s-%s.csv'%(start_date,end_date))
    # save_data(df,'2019-周一周五.csv')
    res=pd.DataFrame()
    for i in range(1,6):
        dw= df.loc[(df['today'] == i)].copy()
        dw.sort_values('trade_date',inplace=True)
        dw['pro']= dw['pct'].cumprod()
        res.loc[i,start_date]=dw.iloc[-1,-1]

    return res
    # save_data(dw, '周%s-%s-%s.csv' % (i,start_date, end_date))
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

def index_jump(ts_code='000001.SH',start_date=None,end_date=None):
    data = pro.query('index_daily',ts_code=ts_code, start_date=start_date,end_date=end_date)
    df=data.copy().sort_values('trade_date')

    df['pre_2_close']=df['close'].shift(2)
    df['pct_2_chg']=df['close']/df['pre_2_close']-1
    df['pre_low']=df['low'].shift(1)
    df['pre_pct_chg']=df['pct_chg'].shift(1)
    df['next_open']=df['open'].shift(-1)
    df['next_close'] = df['close'].shift(-1)
    df['next_2_open']=df['open'].shift(-2)
    df['next_2_close'] = df['close'].shift(-2)
    df=df.loc[(df['high']<df['pre_low'])&(df['pre_pct_chg']<0)]
    df=df.loc[df['pct_2_chg']<-0.05]
    df['next_red_or_green']=df.apply(lambda x: 'red' if x['next_close']>x['next_open'] else 'green',axis=1)
    # df['next_up']=df.apply(lambda x:1 if x['next_close']>x['close'] else 0,axis=1)
    R=df.loc[df['next_red_or_green']=='red'].shape[0]/df.shape[0]
    U=df.loc[df['next_up']==1].shape[0]/df.shape[0]
    z=df.shape
    df['pct:t-o-c']=df['next_close']/df['next_open']
    df['pct:o-c']=df['next_2_close']/df['next_open']
    df['pct:o-o'] = df['next_2_open'] / df['next_open']
    # df['pct:c-c'] = df['next_2_close'] / df['next_close']
    # df['pct:c-o'] = df['next_2_open'] / df['next_close']
    df.dropna(inplace=True)
    t=df.iloc[:,-3:].describe()
    print(t)
    df['t-o-c']=df['pct:t-o-c'].cumprod()
    df['o-c']=df['pct:o-c'].cumprod()
    df['o-o'] = df['pct:o-o'].cumprod()
    # df['c-c'] = df['pct:c-c'].cumprod()
    # df['c-o'] = df['pct:c-o'].cumprod()

    print(df.iloc[-1,-3:])
    # for i in range(1,5):
    #     df.iloc[-i]=df.iloc[-i].cumprod()
    print()



START, END = '20180101', '20201231'
#
# # data['lastday_week']=pd.to_datetime(data['trade_date']).dt.dayofweek
# # data['monday/friday']=data['close']/data['5_close']
#
# # index_cycle('IXIC',start_date=START,end_date=END)
# res=pd.DataFrame()
# for year in range(7,10):
#     START, END = '201%s0101'%year, '201%s1231'%year
#
#     res=pd.concat([res,index_cycle( start_date=START, end_date=END)],axis=1)
# save_data(res,'sh周期日回溯.csv')
# print()
res=index_jump( start_date=START, end_date=END)
print()