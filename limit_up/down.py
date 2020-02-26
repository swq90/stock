import pandas as pd
import tushare as ts
# from numpy import arange
# import stock.util.sheep as sheep
# import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data ,read_data
pro=ts.pro_api()
start,end='20190101','20200231'
save_data(pro.index_daily(ts_code='000001.SH',start_date=start,end_date=end),'上证指数.csv')
data=read_data('daily',start_date=start,end_date=end)
limit=read_data('stk_limit',start_date=start,end_date=end)
data=data.merge(limit,on=['ts_code','trade_date'])
data['limit']=data.apply(lambda x: 99 if x['close']==x['up_limit'] else -99 if x['close']==x['down_limit'] else x['pct_chg'],axis=1)

df=pd.DataFrame({'up':data.loc[data['limit']==99].groupby('trade_date')['ts_code'].count(),'down':data.loc[data['limit']==-99].groupby('trade_date')['ts_code'].count()})
save_data(df,'涨跌停数据汇总.csv')
