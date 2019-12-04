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

df=pd.read_csv(path+'2019-12-04-20-46-21-150---30ofall_marks.csv',dtype={'trade_date': object})
print(df.shape)
pre_date = tool.pre_date(df[["trade_date"]], days=-10).reset_index(drop=True)
print(pre_date)
pre_date.to_csv('predate.csv')
df.to_csv('df1.csv')

df=df.merge(pre_date,on='trade_date')
df.to_csv('df2.csv')

print(df)
print(df.shape)
