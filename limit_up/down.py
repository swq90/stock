import pandas as pd
import tushare as ts
# from numpy import arange
# import stock.util.sheep as sheep
# import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data, read_data

pro = ts.pro_api()
start, end = '20190101', '20200231'
index = pro.index_daily(ts_code='000001.SH', start_date=start, end_date=end)
save_data(index, '上证指数.csv')
data = read_data('daily', start_date=start, end_date=end)
data = data[(data['pct_chg'] >= -11) & (data['pct_chg'] <= 11)]
u_vs_d = pd.DataFrame({'up_5': data.loc[data['pct_chg'] >= 5].groupby('trade_date')['ts_code'].count(),
                       'down_-5': data.loc[data['pct_chg'] <= -5].groupby('trade_date')['ts_code'].count()})
u_vs_d['trade_date'] = u_vs_d.index
u_vs_d.fillna(0,inplace=True)
u_vs_d['rate'] = u_vs_d['up_5'] / u_vs_d['down_-5']
u_vs_d.fillna(1,inplace=True)
save_data(u_vs_d, '涨跌5对比.csv')
limit = read_data('stk_limit', start_date=start, end_date=end)
data = data.merge(limit, on=['ts_code', 'trade_date'])
data['limit'] = data.apply(
    lambda x: 99 if x['close'] == x['up_limit'] else -99 if x['close'] == x['down_limit'] else x['pct_chg'], axis=1)

df = pd.DataFrame({'up': data.loc[data['limit'] == 99].groupby('trade_date')['ts_code'].count(),
                   'down': data.loc[data['limit'] == -99].groupby('trade_date')['ts_code'].count()})
df.fillna(0,inplace=True)
df['trade_date'] = df.index
save_data(df.reset_index(drop=True), '涨跌停数据汇总.csv')
df = df.merge(index, on='trade_date')
df = df.merge(u_vs_d, on='trade_date', )
save_data(df, '涨跌停&上证汇总.csv')
