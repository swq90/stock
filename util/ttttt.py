import os
import datetime
import numpy as np
import math
import pandas as pd
import stockfilter

import tushare as ts
import util.basic as basic
import util.sheep as sheep

pro = ts.pro_api()
tool = basic.basic()
path = os.getcwd() + '\\data\\'
data=pd.read_csv(path+'2019-12-05-data.csv',index_col=0,dtype={'trade_date': object})

mv_bins = list(range(0, 101, 20)) + [150, 200, 400, 30000]

for bin in range(len(mv_bins) - 1):
    df=pd.read_csv(path+str(datetime.datetime.today())[:10]+'-'+str((mv_bins[bin], mv_bins[bin + 1])) + "-30-of-bins.csv",index_col=0,dtype={'trade_date': object})


    for i in [1,5,10,20,30]:
        df_p=pd.DataFrame()
        pre_date = tool.pre_date(df[["trade_date"]], days=i).reset_index(drop=True)

        df_p=df.merge(pre_date,on='trade_date')
        df_p=df_p[['ts_code','pre_%s_date'%i]]
        df_p.columns=['ts_code','trade_date']
        df_p.to_csv(str((mv_bins[bin], mv_bins[bin + 1]))+str(i)+'pre-data.csv')

        df_p=sheep.wool(df_p,data)
        df_p.to_csv(str((mv_bins[bin], mv_bins[bin + 1]))+str(i)+'pre-wool.csv')

        print((mv_bins[bin], mv_bins[bin + 1]),i,df_p.iloc[-1]['all_pct'])
        print(df_p.iloc[-1])
