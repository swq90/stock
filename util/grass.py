import os
import datetime
import numpy as np
import math
import pandas as pd

import tushare as ts
import util.basic as basic

ma = [1, 5, 10]
period = 5
up_cal = 245
temp=50
pre = 5
label = 'low'

pro = ts.pro_api()
tool = basic.basic()

data = tool.trade_daily(cal=up_cal +temp).reset_index(drop=True)
# up = data[data["trade_date"].isin(tool.tradeCal(cal=up_cal))]
up = tool.up_info(data, days=period, up_range=0.5, pct=0)
print('up',up.shape)
up = up[up["trade_date"].isin(tool.tradeCal(cal=up_cal))]
print('up',up.shape)
# if label not in list(data):
#

df = data[['ts_code', 'trade_date', label]]
print('df', df.shape)

# df.loc[:,['count']] = 0
for i in range(1, pre + 1):

    df = tool.pre_label(df, label=label, days=i)
    if i == 1:
        df['count'] = df.apply(lambda x: 1 if x['low'] >= x['pre_%s_low' % i] else 0, axis=1)
    else:
        df['count'] = df.apply(
            lambda x: 1 + x['count'] if x['pre_%s_low' % (i - 1)] >= x['pre_%s_low' % i] else x['count'], axis=1)
    print('df', df.shape)
df.to_csv('alllow0000.csv')

df=df.dropna()
up_pre = up[['ts_code', 'pre_n_date']]
up_pre.rename(columns={'pre_n_date': 'trade_date'}, inplace=True)
up_pre = up_pre.merge(df, on=['ts_code', 'trade_date'])
df=df[df["trade_date"].isin(tool.tradeCal(cal=up_cal))]
df.to_csv('alllow1111.csv')
print('df', df.shape)

print('uppre',up_pre.shape)
up_pre.to_csv('up_pre.csv')


# up_pre = up_pre.groupby(by='count').size()
# df = df.groupby(by='count').size()

up_pre = pd.DataFrame(up_pre.groupby(by='count').size(),columns=['count'])
up_pre['pct']=up_pre['count']/up_pre['count'].sum()
df = pd.DataFrame(df.groupby(by='count').size(),columns=['count'])
df['pct']=df['count']/df['count'].sum()



print(up_pre)
print(df)
print(list(up_pre))
print(list(df))
