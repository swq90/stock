import os
import sys
import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data
from stock.util.basic import basic
import stock.limit_up.get_limit_stock as gls

# from stock.limit_up import performance_of_first_limit_up as pf

fp = os.path.join(os.path.dirname(os.getcwd()), 'data', os.path.basename(sys.argv[0]).split('.py')[0])
fp=os.path.join(fp,'首板涨停回溯.csv')
def pro(start_date=None,end_date=None,limit_type='up',days=2):
    data=gls.first_limit(start_date=start_date, end_date=end_date, limit_type=limit_type,  days=days)
    data=data.loc[(data['pct_chg']>=-11)&(data['pct_chg']<=11)]
    fl=data.loc[(data['pre_1_is_roof']==1) & (data['pre_2_is_roof']==0)]
    return sheep.wool2(fl,fl,days=0,PRICEB='open',PRICES='close')


if __name__=='__main__':
    if os.path.isfile(fp):
        t=pro()
        save_data(t,'首板涨停回溯.csv',mode='a')
    else:
        t=pro(start_date='20190101')
        save_data(t,'首板涨停回溯.csv')
    # t=pro()
    # print()

