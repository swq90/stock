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

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)

day=100
inter=0.02
pre_bins = np.arange(0, 4, inter)
pro = ts.pro_api()
tool = basic.basic()
path = os.getcwd() + '\\data\\'
data = pd.read_csv(path + '2019-12-05-data.csv', index_col=0, dtype={'trade_date': object})
today = str(datetime.datetime.today())[:10]
today = '2019-12-05'
mv_bins = list(range(0, 101, 20)) + [150, 200, 400, 30000]

if os.path.isfile(str((0,20))+'-%spre%s.csv'%(inter,day)):
    pct=pd.DataFrame()
    for i in range(1, 1+day):
        for bin in range(len(mv_bins) - 1):
            pct[str((mv_bins[bin], mv_bins[bin + 1]))]=pd.read_csv(str((mv_bins[bin], mv_bins[bin + 1]))+'-%spre%s.csv'%(inter,day),index_col=0)['pre_%s'%i]

        # pct.set_index(pd.read_csv(str((mv_bins[bin], mv_bins[bin + 1]))+'-%spre%s.csv'%(inter,day),index_col=0).index)
        # print(pct.info())
        # pct['all']=pct.apply(lambda x:x.sum(),axis=0)
        # pct=pct[pct.iloc[:,:]>0]
        # d=pct.iloc[,1]
        pct=pct[pct>0]
        # how='all" or 'any',all删除全为nan的行，any：删除任何含有nan从行
        pct.dropna(how='all',inplace=True)
        pct.fillna(0)
        pct.plot(title='pre%s_geshizhifenbuqingkuang'%i,grid=True)
        plt.show()
else:
    for bin in range(len(mv_bins) - 1):
        df = pd.read_csv(path + today + '-' + str((mv_bins[bin], mv_bins[bin + 1])) + "-30-of-bins.csv", index_col=0,
                         dtype={'trade_date': object})

        res = pd.DataFrame()

        for i in range(1, 1+day):
            # df_p=pd.DataFrame()

            pre_date = tool.pre_date(df[["trade_date"]], days=i).reset_index(drop=True)
            pre_data = data[['ts_code', 'trade_date', 'ma1']]
            pre_data.columns = ['ts_code', 'pre_%s_date' % i, 'pre_%s_ma1' % i]
            df_p = df[['ts_code', 'trade_date']].merge(data[['ts_code', 'trade_date', 'ma1']], on=['ts_code', 'trade_date'])
            df_p = df_p.merge(pre_date, on='trade_date')
            df_p = df_p.merge(pre_data, on=['ts_code', 'pre_%s_date' % i])

            df_p['pre_%s_ma1_pct' % i] = df_p['pre_%s_ma1' % i] / df_p['ma1']

            print(df_p.shape)
            res['pre_%s' % i] = pd.cut(df_p['pre_%s_ma1_pct' % i], bins=pre_bins)

        res = res.apply(pd.value_counts)
        res = res.apply(lambda x:  x / x.sum())
        # res = res.apply(lambda x:  x / df_p.shape[0])
        #
        print(res)
        res.to_csv(str((mv_bins[bin], mv_bins[bin + 1]))+'-%spre%s.csv'%(inter,day))



        # for item in list(res):
        #     res[item].plot(label=item, title=str((mv_bins[bin], mv_bins[bin + 1]))+'pre_n_dis', grid=True)
        # plt.legend(loc="best")
        # # print(np.argmax(res))
        # plt.show()
        # print(res.shape)
