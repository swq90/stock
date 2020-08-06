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
from stock.subject.ml_find_similiar import cos_sim
from stock.sql.data import save_data, read_data
from stock import vars

pro = ts.pro_api()
# START, END = '20170101', '20200401'
# #股票公告
# df = pro.anns(ts_code='600627.SH')
# #股票更名记录
# namechange=pro.query('namechange')
# 股票基本信息
# basic=read_data('stock_basic')
# 纳斯达克指数
def get_index(ts_code,start_date,end_date):
    data = pro.query('index_daily',ts_code=ts_code, start_date=start_date,end_date=end_date)
    return data
def index_cycle(ts_code='000001.SH',start_date=None,end_date=None):
    # IXIC = pro.query('index_global', ts_code=ts_code, start_date=START)
    # # index_basic=pro.query('index_basic',market='data')
    # # 上证指数
    data = get_index(ts_code, start_date,end_date)
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

def index_slope(data,N,K):
    """

    @param data: datafarme,指数数据
    @param N: int，几天内
    @param K: float，斜率
    @return: dataframe,符合斜率条件的股票
    """
    data.sort_values('trade_date',inplace=True)
    data['%s_pct'%N]=data['close'].rolling(N).apply(lambda x:100*(x[N-1]/x[0]-1))
    data['%s_amount'%(N-1)]=data['amount'].rolling(N-1).sum()
    return data
# # data['lastday_week']=pd.to_datetime(data['trade_date']).dt.dayofweek
# # data['monday/friday']=data['close']/data['5_close']
#
# index_cycle(start_date=START,end_date=END)
# res=pd.DataFrame()
# for year in range(7,10):
#     START, END = '201%s0101'%year, '201%s1231'%year
#
#     res=pd.concat([res,index_cycle( start_date=START, end_date=END)],axis=1)
# save_data(res,'sh周期日回溯.csv')
# print()
# res=index_jump( start_date=START, end_date=END)
# def cos_aux(df):
#     print(df)
#     return [[df.iloc[0,vars.TRADE_DATE],df.iloc[-1,vars.TRADE_DATE]],cos_sim(df[vector],sample[vector])]
def cos_cycel(data,N):
    res=[]
    data.sort_values(vars.TRADE_DATE,inplace=True)
    for l in range(data.shape[0]-N):
        vector2=data.iloc[l:l+N,:]

        res.append([vector2.iloc[0][vars.TRADE_DATE],vector2.iloc[-1][vars.TRADE_DATE],cos_sim(vector1[cols].values.reshape(1,-1),vector2[cols].values.reshape(1,-1))])
    return pd.DataFrame(res,columns=['start','end','sim'])
START, END = '20150101', '20201231'
ts_code='000001.SH'
# cols=[vars.OPEN,vars.CLOSE,vars.AMOUNT,vars.VOL]
cols=[vars.AMOUNT,vars.VOL]
#
file_name='%s_%s.csv'
N=10
data = get_index(ts_code, START, END)

# data = index_slope(data, 4, 1)
vector1 = data.iloc[-N-1:-1, :]
if __name__=='__main__':

    save_data(cos_cycel(data,N),file_name%(N,'-'.join(cols)),fp_date=True)
print()