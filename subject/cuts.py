import sys
import pandas as pd
import datetime
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic
from stock import vars
from stock.subject import continu_limit as cl

def open_cut(df, cuts_format, limit=None,bins=2,PRICEB=vars.CLOSE,PRICES=vars.CLOSE,days=1):
    res = pd.DataFrame()
    if cuts_format not in df.columns:

        if cuts_format.split('=')[0] in df.columns:
            cuts_format = cuts_format.split('=')[0]
        else:
            df[cuts_format.split('=')[0]] = df.eval(cuts_format.split('=')[1])
            cuts_format = cuts_format.split('=')[0]
    if limit:
        df['cate'] = pd.cut(
            df.apply(lambda x: 99 if x[limit] == x[vars.UP_LIMIT] else -99 if x[limit] == x[vars.DOWN_LIMIT] else x[cuts_format],
                     axis=1), [-99] + list(range(-10, 11, bins)) + [100], right=False)
    else:
        df['cate'] = pd.cut(df[cuts_format],  list(range(-10, 11, bins)) , right=False)
    # save_data(df[[vars.TS_CODE,vars.TRADE_DATE,vars.PCT_CHG,vars.CLOSE,vars.OPEN,vars.UP_LIMIT,vars.DOWN_LIMIT,'cate']],'%sopen切分data3.csv'%start_date)

    # res=pd.DataFrame(columns=pd.MultiIndex.from_product([df['cate'].sort_values().unique(),
    #                                                      ['pct', 'n']]))
    for cate in df['cate'].sort_values().unique():
        data=sheep.avg_roi(df[df['cate']==cate],df,PRICEB=PRICEB,PRICES=PRICES,days=days).reset_index()
        data['cate']=str(cate)
        # print(data.info())
        data.set_index([vars.TRADE_DATE,'cate'],inplace=True)
        data=data.unstack(level=0)
        res=pd.concat([res,data])
    # res['product']=[res.loc[:,'pct']].cumprod()
    return res
start_date = '20200701'
end_date = '20201231'
FORMAT = lambda x: '%.3f' % x
# cuts_format = 'open/pre_close=100*(open/pre_close-1)'
cuts_format='pct_chg'
limit=vars.CLOSE
buy,sell,days,bins=vars.CLOSE,vars.OPEN,1,1
raw_data = read_data('daily', start_date=start_date, end_date=end_date).merge(
    read_data('stk_limit', start_date=start_date, end_date=end_date)[
        ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
    on=['ts_code', 'trade_date'])
list_days = basic().list_days(raw_data, list_days=30)
raw_data = raw_data.merge(list_days, on=['ts_code', 'trade_date'])
# ds=open_cut(raw_data,cuts_format=vars.PCT_CHG,limit=vars.CLOSE,bins=1)
# ds=open_cut(raw_data,limit=vars.OPEN,bins=1,PRICEB=vars.OPEN,PRICES=vars.CLOSE,days=0)
ds=open_cut(raw_data,cuts_format,limit=limit,bins=bins,PRICEB=buy,PRICES=sell,days=days)

save_data(ds,'%s-%s-%s-%s切分回溯%s.csv'%(limit,buy,days,sell,start_date))