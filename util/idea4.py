# 个股连续5日上涨，最近2天阴线，下跌小于10，下一天涨停概率
import datetime
import os

# import numpy as np
# import math
import pandas as pd
# # import stockfilter
#
import tushare as ts

# import util.sheep as sheep
# import util.fuquan as fuquan
#
#
# # ts.set_token('006b49622d70edc237ab01340dc210db15d9580c59b40d028e34e015')

import util.basic as basic
# z=pd.date_range(start='20191202',end='20191230')


pro = ts.pro_api()
tool = basic.basic()
# data=pro.daily(ts_code='002943.SH')
z=tool.history_name()
print(z.info())
# z.loc['info']=['st','a','b',1,2,3]
# print(z)
today=datetime.datetime.today().date()

path = 'D:\\workgit\\stock\\util\\stockdata\\'

while (not os.path.isfile(path + str(today) + '\data.csv')) or (
not os.path.isfile(path + str(today) + '\daily-basic.csv')):
    today = today - datetime.timedelta(1)

path_source=os.getcwd() +'\\stockdata\\'+str(today)+'\\'




path_source=os.getcwd() +'\\stockdata\\'+str(today)+'\\'

data=pd.read_csv(path_source + 'data.csv', index_col=0, dtype={'trade_date': object})

z=tool.pre_data(data)

