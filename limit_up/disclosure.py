import pandas as pd
import tushare as ts
from stock.sql.data import read_data,save_data
import stock.util.sheep as sheep
# 年报计划
buy_date='20190101'
need_date='20191231'
end_date='20190630'
disclosure=ts.pro_api().disclosure_date(end_date='20181231',fields='ts_code,ann_date,end_date,pre_date,actual_date,modify_date')
share_float=ts.pro_api().share_float(ts_code='300617.SZ' )
disclosure=disclosure.loc[(disclosure['pre_date']>=need_date)&(disclosure['pre_date']<=end_date)].copy()
print(disclosure.shape,disclosure['ts_code'].unique().shape)
data=read_data('daily',start_date=buy_date,end_date=end_date)
# disclosure
# # 分红送股
# dividend=ts.pro_api().dividend()
res=pd.DataFrame()
# for ts_code in disclosure['ts_code'].unique():
#     df=data.loc[data['ts_code']==ts_code].copy()
#     df.sort_values('trade_date',inplace=True)
#     df['buy_date']=df['trade_date'].shift(5)
#     df['buy_price']=df['close'].shift(5)
#     res=pd.concat([res,df],ignore_index=True)

disclosure['trade_date']=disclosure['pre_date']
disclosure.sort_values('trade_date',ascending=False,inplace=True)
print(disclosure.shape)
disclosure=disclosure[['ts_code','trade_date']].drop_duplicates()
print(disclosure.shape,disclosure['ts_code'].unique().shape)
sheep.wool2(disclosure,res,PRICEB='buy_price')
print()




