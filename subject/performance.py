import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data,save_data
from stock.util.basic import basic

def func1(data,modes,day):
    '''
    给入股票数据，返回后续表现
    @param data: dataframe，股票代码，交易日期
    @param mode: 表现方式
    @return: dataframe股票后续表现
    '''
    start,end=data['trade_date'].min()
    raw_data=read_data('daily',start_date=start)
    # mode=[close,open,high,mean,low,pct_chg,low_open,high_open]
    for mode in modes:
        if mode  in ['open','close','high','low']:


        else:
            # if mode not in ['low_open', 'high_open']:
            pre = basic().pre_data(data, label=[mode], pre_days=day)
            raw_data = raw_data.merge(pre[['ts_code', 'trade_date', 'pre_%s_%s'%(day,mode)]], on=['ts_code', 'trade_date'])
            print('%s/pre_close'%mode,raw_data['pre'])


data=pd.DataFrame()
if