import os
import datetime
import numpy as np
import math
import pandas as pd
import stockfilter
import matplotlib.pyplot as plt
import tushare as ts
import util.basic as basic
import util.sheep as sheep

#
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)

day = 240
inter = 0.02
pre_bins = np.arange(0, 4, inter)
pro = ts.pro_api()
tool = basic.basic()
new_dir = '\\stockdata\\' + str(datetime.datetime.today().date()) + '\\'
path = os.getcwd() + new_dir

# data = pd.read_csv(path + '2019-12-13-data.csv', index_col=0, dtype={'trade_date': object})
# today = str(datetime.datetime.today())[:10]
# today = '2019-12-13'
mv_bins = list(range(0, 101, 20)) + [150, 200, 400, 30000]
# mv_bins=list([40,60,80,])
df_res = pd.DataFrame()
for bin in range(len(mv_bins) - 1):
    if mv_bins[bin] not in list([40, 80]):
        continue
    interval = str((mv_bins[bin], mv_bins[bin + 1]))

    if not os.path.isfile(path + interval + "pct-wool.csv"):
        continue
    df = pd.DataFrame()
    df = pd.read_csv(path + interval + "pct-wool.csv", index_col=0,
                     dtype={'trade_date': object})
    df.rename(columns={'pct': '%spct' % interval, 'n': '%sn' % interval, 'all_pct': '%sall_pct' % interval},
              inplace=True)
    print(df.mean())
    if df_res.empty:
        df_res = df.copy()
    else:
        df_res = df_res.merge(df, on='sell_date', how='outer')
    df_res['%spct' % interval] = df_res['%spct' % interval].fillna(1)
    df_res['%sall_pct' % interval] = df_res['%sall_pct' % interval].fillna(method='ffill')
    df_res['%sn' % interval] = df_res['%sn' % interval].fillna(0)

print(df_res)
df_res.to_csv(path + '最好最差.csv')
