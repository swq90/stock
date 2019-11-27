import os
import datetime
import numpy as np
import math
import pandas as pd

import tushare as ts
import util.basic as basic

ma = [1, 5, 10]
period = 5
up_cal = 240
temp = 50
pre = 5
labels = ['low_ma5', 'low', 'ma1', 'ma5']

pro = ts.pro_api()
tool = basic.basic()
res=pd.DataFrame()
data = tool.trade_daily(cal=up_cal + temp).reset_index(drop=True)
print(data.shape)
data = data.merge(tool.get_all_ma(data, ma=[1, 5], dis_pct=False), on=['ts_code', 'trade_date'])
data['low_ma5'] = data.apply(lambda x: 1 if x['low'] > x['ma5'] else 0, axis=1)
print(data.shape)
# up = data[data["trade_date"].isin(tool.tradeCal(cal=up_cal))]
up = tool.up_info(data, days=period, up_range=0.5, pct=0)
print('up', up.shape)
up = up[up["trade_date"].isin(tool.tradeCal(cal=up_cal))]
print('up', up.shape)
# if label not in list(data):
#
for label in labels:
    df = data[['ts_code', 'trade_date', label]]
    print('df', df.shape)

    # df.loc[:,['count']] = 0
    for i in range(1, pre + 1):
        df = tool.pre_label(df, label=label, days=i)

        if label == 'low_ma5':
            if i == 1:
                df['count'] = df['pre_1_low_ma5']
            else:
                df['count'] = df.apply(
                    lambda x: x['count'] + x['pre_%s_%s' % (i, label)], axis=1)

        else:
            if i == 1:
                df['count'] = df.apply(lambda x: 1 if x[label] >= x['pre_%s_%s' % (i, label)] else 0, axis=1)
            else:
                df['count'] = df.apply(
                    lambda x: 1 + x['count'] if x['pre_%s_%s' % ((i - 1), label)] >= x['pre_%s_%s' % (i, label)] else x[
                        'count'], axis=1)
        print('df', df.shape)
    df.to_csv('all%s.csv' % label)

    df = df.dropna()
    up_pre = up[['ts_code', 'pre_n_date']]
    up_pre.rename(columns={'pre_n_date': 'trade_date'}, inplace=True)
    up_pre = up_pre.merge(df, on=['ts_code', 'trade_date'])
    df = df[df["trade_date"].isin(tool.tradeCal(cal=up_cal))]
    df.to_csv('all%sindays.csv' % label)
    print('df', df.shape)
    print('uppre', up_pre.shape)
    up_pre.to_csv('up_pre%s.csv' % label)
    up_pre = pd.DataFrame(up_pre.groupby(by='count').size(), columns=['count'])
    up_pre['pct'] = up_pre['count'] / up_pre['count'].sum()
    df = pd.DataFrame(df.groupby(by='count').size(), columns=['count'])
    df['pct'] = df['count'] / df['count'].sum()

    print('up:%s>pre_%s' % (label, label), up_pre)
    print('all:%s>pre_%s' % (label, label), df)
    up_pre.rename(columns={'pct':'up_pct'},inplace=True)
    df.rename(columns={'pct':'all_pct'},inplace=True)
    g=up_pre[['up_pct']].merge(df[['all_pct']],left_index=True,right_index=True)
    g['div']=g['up_pct']/g['all_pct']
    g['%sscore'%label]=g.apply(lambda x:10*x['div'],axis=1)/g['div'].sum()
    print(g)
    if res.empty:
        res=g[['%sscore'%label]]
    else:
        res=res.merge(g[['%sscore'%label]],left_index=True,right_index=True)
        print(res)

res.reset_index(drop=True)
res.to_csv('score.csv')