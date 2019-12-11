import pandas as pd
import numpy as np
import time
import tushare as ts
pro = ts.pro_api()
# change_info=pro.namechange()
# for key in ('L','D','P'):
#     stock_basic = pro.stock_basic(list_status=key,fields='ts_code,name,industry,list_date,delist_date')
#     print(stock_basic.shape)
#
#     stock_basic=stock_basic[stock_basic['name'].str.contains('退') == True].sort_values(by='delist_date',ascending=False)
#     print(stock_basic)
pd.set_option('display.max_rows', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)
daily=pro.daily(trade_date='20191128')
print(daily)