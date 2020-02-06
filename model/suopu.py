import os
import numpy as np
import pandas as pd
import datetime
import util.sheep as sheep
import util.basic as basic
import tushare as ts
import util.stockfilter as filter
pro=ts.pro_api()
FORMAT = lambda x: '%.4f' % x
t=1

NDAY=3
slected_date='20200203'
label = ['low_ma5']
path = 'D:\\workgit\\stock\\util\\stockdata\\'
# pct=list(range(-11,11))

today = datetime.datetime.today().date()
tool = basic.basic()
while (not os.path.isfile(path + str(today) + '\data.csv')):
# or (
#         not os.path.isfile(path + str(today) + '\daily-basic.csv')) or (
#         not os.path.isfile(path + str(today) + '\stock-label.csv')):
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，

raw_data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'close','pct_chg']]
limit_list=pro.stk_limit(trade_date=slected_date)
print(limit_list.shape)

df=raw_data[raw_data['trade_date']==slected_date].merge(limit_list,on=['ts_code','trade_date'])

up_data=df[df['close']==df['up_limit']]
down_data=df[df['close']==df['down_limit']]
print(df.shape,down_data.shape,up_data.shape)
df=df[df['ts_code'].isin(down_data['ts_code'])==False]
print(df.shape)

# median_list=filter.StockFilter().stock_basic(name="st|ST|药",market="科创板",industry='生物制药|医药商业|医疗保健|中成药|化学制药')
# print(median_list)
# df[df["ts_code"].isin(median_list['ts_code']) == False][['ts_code']].to_csv('0203非跌停非医药.txt',index=False)
df[['ts_code']].to_csv('0203非跌停.txt',index=False)


df=df[df['ts_code'].isin(up_data['ts_code'])==False]
print(df.shape)


res=pd.DataFrame()


for days in range(1,NDAY+1):
    for pct in np.arange(-10,11,t):
        data=df[(df['pct_chg']>=pct)&(df['pct_chg']<(pct+t))]
        if pct==9:
            print(data)
        data_res = sheep.wool(data, raw_data, days=days)
        if data_res.empty:
            continue
        res.loc[pct,'huisu%s'%days]=data_res.iloc[-1,0]
        res.loc[pct, 'count%s'%days] = data_res.iloc[-1, 1]
    down_res=sheep.wool(down_data,raw_data,days=days)
    if down_res.empty:
        continue
    up_res=sheep.wool(up_data,raw_data,days=days)
    res.loc['down','huisu%s'%days]=down_res.iloc[-1,0]
    res.loc['down', 'count%s'%days] = down_res.iloc[-1, 1]
    res.loc['up', 'huisu%s'%days] = up_res.iloc[-1, 0]
    res.loc['up', 'count%s'%days] = up_res.iloc[-1, 1]
    others_res=sheep.wool(df,raw_data,days=days)
    res.loc['others', 'huisu%s'%days] = others_res.iloc[-1, 0]
    res.loc['others', 'count%s'%days] = others_res.iloc[-1, 1]
# res.sort_values(by='huisu',ascending=False,inplace=True)
if 'huisu2'  in list(res):
    res['pct']=res['huisu2']/res['huisu1']
res.to_csv('%s市场%s日回溯cut%s.csv'%(slected_date,NDAY,t))
print()

