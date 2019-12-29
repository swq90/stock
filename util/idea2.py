import  os
import datetime
import pandas as pd
import tushare as ts

# import util.basic as basic
import util.sheep as sheep
pro=ts.pro_api()
today=datetime.datetime.today().date()

path_source=os.getcwd() +'\\stockdata\\'+str(today)+'\\'
data=pd.read_csv(path_source + 'data.csv', index_col=0, dtype={'trade_date': object})
mv_bins = list(range(0, 101, 20)) + [150, 200, 400, 30000]

stock=pd.read_csv(path_source + 'data.csv', index_col=0, dtype={'trade_date': object})