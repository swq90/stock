import os
import datetime
import numpy as np
import math
import pandas as pd
import stock.basic.stockfilter
import matplotlib.pyplot as plt
import tushare as ts
import stock.util.basic as basic
import stock.util.sheep as sheep

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
today=str(datetime.datetime.today().date()-datetime.timedelta(1))

# today=str(datetime.datetime.today().date())
print(today)
new_dir = '\\stockdata\\' + today + '\\'
path = os.getcwd() + new_dir

# data = pd.read_csv(path + '2019-12-13-data.csv', index_col=0, dtype={'trade_date': object})
# today = str(datetime.datetime.today())[:10]
# today = '2019-12-13'
mv_bins = list(range(0, 101, 20)) + [150, 200, 400, 30000]

daily_basic = pd.read_csv(path + 'daily-basic.csv', index_col=0,
                          dtype={'trade_date': object})


# 比较换手率的影响
for item in ['turnover_rate']:
    df_res = pd.DataFrame()

    for bin in range(len(mv_bins) - 1):
        if mv_bins[bin] not in list([40, 80]):
            continue
        interval = str((mv_bins[bin], mv_bins[bin + 1]))

        if not os.path.isfile(path + interval +  "-30-of-bins.csv"):
            continue
        df = pd.DataFrame()
        df = pd.read_csv(path + interval + "-30-of-bins.csv", index_col=0,
                         dtype={'trade_date': object})[['ts_code', 'trade_date']]
        df = df[['ts_code', 'trade_date']].merge(daily_basic[['ts_code', 'trade_date', item]],
                                                       on=['ts_code', 'trade_date'])
        df_bins=pd.DataFrame()
        df_bins[interval+item+"-mean"] = df.groupby(by='trade_date')[item].mean()
        # # df.rename(columns={'trade_date':'buy})
        # # df_pct = pd.read_csv(path + interval + "pct-wool.csv", index_col=0,
        # #                  dtype={'trade_date': object})
        # print(type(df))
        # print(df)
        # df=pd.DataFrame([df])
        # print(list(df))
        # print(df)
        # print(type(df))
        #
        # df.columns=["trade_date",interval+item+"-mean"]
        # print(df)
        df_bins.to_csv(interval+'.csv')
        if df_res.empty:
            df_res = df_bins.copy()
        else:
            df_res = df_res.merge(df_bins,left_index=True, right_index=True, how='outer')
        df_res.fillna(0)
        df_res.to_csv(item+'.csv')
        # df_res['%sall_pct' % interval] = df_res['%sall_pct' % interval].fillna(method='ffill')
        # df_res['%sn' % interval] = df_res['%sn' % interval].fillna(0)

#





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
# df_res.to_csv(path + '最好最差.csv')


