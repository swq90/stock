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
pre_bins=list(range(-100,101))*0.1
for bin in range(len(mv_bins) - 1):
    df=pd.read_csv(path+str(datetime.datetime.today())[:10]+'-'+str((mv_bins[bin], mv_bins[bin + 1])) + "-30-of-bins.csv",index_col=0,dtype={'trade_date': object})

    res=pd.DataFrame()

    for i in range(50):
        # df_p=pd.DataFrame()
        pre_date = tool.pre_date(df[["trade_date"]], days=i).reset_index(drop=True)
        pre_data=data[['ts_code','trade_date','ma1']]
        pre_data.columns=['ts_code','pre_%s_date'%i,'pre_%s_ma1'%i]

        df_p=df[['ts_code','trade_date']].merge(pre_date,on='trade_date')
        df_p=df_p.merge(pre_data,on=['ts_code','pre_%s_date'%i])

        df_p['pre_%s_ma1_pct'%i]=df_p['pre_%s_ma1'%i]/df_p['ma1']
        df_count=pd.cut(df_p['pre_%s_ma1_pct'%i],bins=pre_bins)
        print(df_count)
