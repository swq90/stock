
import sys
import pandas as pd
import datetime
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic
import stock.vars as sv
def slope(s):
    return s.iloc[0]/s.iloc[-1] if s.iloc[-1] else -1
def asymptote(df,N,similarity=0.8,labels=[sv.CLOSE,sv.OPEN,sv.MA]):
    """
    @param df: datafram，传入股票数据
    @param N: int，最短天数
    @param similarity: float，落在线内的股票占比
    @return: dataframe，股票代码，符合条件的开始结束时间
    """
    df.sort_values('trade_date',ascending=False,inplace=True)
    for l in labels:
        if l in df.columns:
            z=df.groupby('ts_code')[l].expanding(N).apply(slope,raw=False)
    return df

if __name__=='__main__':
    df=read_data('daily',start_date='20200601',end_date='20201231')
    res=asymptote(df,2,labels=[sv.CLOSE])