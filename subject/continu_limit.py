# 利用匿名函数,动态传参给filter
import sys
import pandas as pd
import datetime
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic
# 所有条件以list[lambda1,lambda2,……，func1)

start_date = '20200101'
end_date = '20201231'
OPEN,CLOSE,HIGH,LOW,PCT_CHG,PRE_CLOSE,UP_LIMIT,DOWN_LIMIT='open','close','high','low','pct_chg','pre_close','up_limit','down_limit'
red_line_limit=lambda x:x[LOW]==x[UP_LIMIT]
red_t_limit=lambda x:(x[OPEN]==x[CLOSE]==x[UP_LIMIT])&(x[LOW]<x[CLOSE])
UP_limit=lambda x:x[CLOSE]==x[UP_LIMIT]
red_block=lambda x:(x[CLOSE]>=x[OPEN])&(x[LOW]>x[DOWN_LIMIT])&(x[HIGH]<x[UP_LIMIT])
open_up_limit=lambda x:x[OPEN]==x[UP_LIMIT]

n_n=lambda x:(x[LOW]>x[DOWN_LIMIT])&(x[HIGH]<x[UP_LIMIT])
green_line_limit=lambda x:x[HIGH]==x[DOWN_LIMIT]
green_t_limit=lambda x:(x[OPEN]==x[CLOSE]==x[DOWN_LIMIT])&(x[HIGH]>x[CLOSE])
green_block=lambda x:(x[CLOSE]<x[OPEN])&(x[LOW]>x[DOWN_LIMIT])&(x[HIGH]<x[UP_LIMIT])
open_down_limit=lambda x:x[OPEN]==x[DOWN_LIMIT]
down_limit=lambda x:x[CLOSE]==x[DOWN_LIMIT]

open_green=lambda x:(x[OPEN]<x[PRE_CLOSE])&(x[OPEN]!=x[UP_LIMIT])
open_red=lambda x:(x[OPEN]>=x[PRE_CLOSE])&(x[OPEN]!=x[UP_LIMIT])
open_not_limit=lambda x:(x[OPEN]!=x[UP_LIMIT])&(x[OPEN]!=x[DOWN_LIMIT])

# df=pd.DataFrame({'a':[1.2,2.1,1,5,7,6,2,7,5,7],'b':[1.2,2.1,3,6,7,1.2,2.1,1,5,7],'c':[6,2,7,5,7,1.2,2.1,3,6,7],'trade_date':[2,1,0,0,1,0,1,2,2,1],'tc':[2,1,0,0,1,0,1,2,2,1]})
# print(df)


def process(df,expressions):
    # print(len(expressions))
    # df.sort_values('trade_date',inplace=True )
    # print(df.iloc[0],type(df.iloc[0]))

    # df=df.loc[expressions[0]]
    # print(df)
    # for i in range(len(expressions)):
    #     if not expressions:continue
    #     ds=df.iloc[i].copy()
    #     print(ds)
    #     # print(ds['a']==ds['b'])
    #     ds[i]=expressions[i](ds)
    #     # if expressions[i](ds):
    #     #     print(ds)
    #     #     continue
    #     # else:
    #     #     return
    #     print(ds)
    ds=df.copy()
    ds.sort_values(['ts_code','trade_date'],inplace=True)
    for i in range(len(expressions)):
        if not expressions[i]:continue
        ds[i]=expressions[i](ds)
        ds[i]=ds.groupby('ts_code')[i].shift(len(expressions)-i-1)

    for i in  range(len(expressions)):
        ds=ds.loc[ds[i]==True]
    return ds

if __name__=='__main__':
    df=read_data('daily',start_date=start_date,end_date=end_date).merge(read_data('stk_limit', start_date=start_date, end_date=end_date)[['ts_code', 'trade_date', 'up_limit', 'down_limit']],
                              on=['ts_code', 'trade_date'])
    pre=[open_up_limit,open_down_limit,]
    process(df,[n_n,red_line_limit,red_line_limit])