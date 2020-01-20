import os
import datetime
import pandas as pd
import tushare as ts
import basic.basic as basic
tool=basic.basic()

label_roof='high'
label_floor='low'
periods=20
times=17
others={}
path = 'D:\\workgit\\stock\\util\\stockdata\\'
today = datetime.datetime.today().date()
tool = basic.basic()
pro = ts.pro_api()
while (not os.path.isfile(path + str(today) + '\data.csv')) :
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，

data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})
