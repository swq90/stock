import pandas as pd
import datetime
import tushare as ts
# from numpy import arange
# import stock.util.sheep as sheep
# import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data, read_data

pro = ts.pro_api()
START, END = '20170101', '20200331'
# #股票公告
# df = pro.anns(ts_code='600627.SH')
# #股票更名记录
# namechange=pro.query('namechange')
# 股票基本信息
basic=read_data('stock_basic')
# 纳斯达克指数
IXIC=pro.query('index_global',ts_code='IXIC',start_date=START)
# index_basic=pro.query('index_basic',market='SSE')
#上证指数
SSE = pro.index_daily(ts_code='000001.SH', start_date=START)
save_data(SSE, '上证指数.csv')
save_data(IXIC, '纳斯达克.csv')
IXIC['sz_date']=IXIC['trade_date'].shift(1)
IXIC.loc[0,'sz_date']=datetime.date.today().strftime('%Y%m%d')
IXIC=IXIC.iloc[:,1:]
# print(IXIC.columns)
columns=dict.fromkeys(IXIC.columns[:-1])
for key in columns.keys():
    columns[key]='us_%s'%key

IXIC.rename(columns=columns,inplace=True)
mix=SSE.merge(IXIC,left_on='trade_date',right_on='sz_date')
save_data(mix,'sz&nasdq.csv')

data = read_data('daily', start_date=START)
data = data[(data['pct_chg'] >= -11) & (data['pct_chg'] <= 11)]
# 涨>=5 vs 跌 <=-5
u_vs_d = pd.DataFrame({'up_5': data.loc[data['pct_chg'] >= 5].groupby('trade_date')['ts_code'].count(),
                       'down_-5': data.loc[data['pct_chg'] <= -5].groupby('trade_date')['ts_code'].count()})
u_vs_d['trade_date'] = u_vs_d.index
u_vs_d.fillna(1,inplace=True)
u_vs_d['rate'] = u_vs_d['up_5'] / u_vs_d['down_-5']
# u_vs_d.fillna(1,inplace=True)
# save_data(u_vs_d, '涨vs跌对比.csv')
limit = read_data('stk_limit', start_date=START, end_date=END)
data = data.merge(limit, on=['ts_code', 'trade_date'])
data['limit'] = data.apply(
    lambda x: 99 if x['close'] == x['up_limit'] else -99 if x['close'] == x['down_limit'] else x['pct_chg'], axis=1)

df = pd.DataFrame({'up_limit': data.loc[data['limit'] == 99].groupby('trade_date')['ts_code'].count(),
                   'down_limit': data.loc[data['limit'] == -99].groupby('trade_date')['ts_code'].count()})
df.fillna(0,inplace=True)
df['trade_date'] = df.index
# save_data(df.reset_index(drop=True), '涨跌停数据汇总.csv')
df = df.merge(SSE, on='trade_date')
df = df.merge(u_vs_d, on='trade_date', )
save_data(df, '涨跌停&上证汇总.csv')
