import os
import numpy as np
import pandas as pd
import datetime
import util.sheep as sheep
import util.basic as basic
import tushare as ts

pro=ts.pro_api()
FORMAT = lambda x: '%.4f' % x
NDAY=2
swings=20
slected_date='20200203'
label = ['low_ma5']
path = 'D:\\workgit\\stock\\util\\stockdata\\'
# pct=list(range(-11,11))

today = datetime.datetime.today().date()
tool = basic.basic()
while (not os.path.isfile(path + str(today) + '\data.csv')):
    today = today - datetime.timedelta(1)

raw_data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'high','low','close','pct_chg','pre_close']]
data=raw_data[raw_data['trade_date']==slected_date].copy()


data.loc[:,'swing']=data.apply(lambda x:(x['high']-x['low'])/x['pre_close']*100,axis=1)
res=pd.DataFrame()
t=1
for swing in range(0,swings+t):
    df=data[data['swing']>=swing]

    print(df.shape)
    if swing==9:
        df[['ts_code']].reset_index(drop=True).to_csv('振幅大于10.txt')
    for days in range(1,NDAY+1):
        data_res=sheep.wool(df,raw_data,days=days)
        if data_res.empty:
            continue
        res.loc[swing, 'huisu%s' % days] = data_res.iloc[-1, 0]
        res.loc[swing, 'count%s' % days] = data_res.iloc[-1, 1]

print()