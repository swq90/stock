# p=12,0<up<=2,sort by up_range,end_date close<limit_up
# 10天内满足7次上涨，且市值小于30亿 # 最后一天均价大于10日均价的1%且小于10%,换手率1.5%
# t = f5(period=10, avg_up_times=7, total_mv=3000000000, turnover_rate=1.5, up_range=[0.01, 0.1])

# 1.10天上涨大于等于7次的股票

# 2.所有股票筛选100*(ma1/ma10-1)在[1,10]之间
# 3.筛选换手率
# 4.去除st，退市股票


import os
import datetime
import time

import pandas as pd
import tushare as ts

import util.basic as basic

pro = ts.pro_api()
tool = basic.basic()

path = 'D:\\workgit\\stock\\util\\stockdata\\'
today = datetime.datetime.today().date()
up_n_pct=[1,10]
PERIOD= 10
TIMES= 7
LABEL='ma1'
OTHERS={'total_mv':[-10,30],'turnover_rate':[1.5,float('inf')]}


while (not os.path.isfile(path + str(today) + '\data.csv')) or (
not os.path.isfile(path + str(today) + '\daily-basic.csv')):
    today = today - datetime.timedelta(1)
path=path + str(today)
data = pd.read_csv(path  + '\data.csv', index_col=0,
                   dtype={'trade_date': object})[['ts_code', 'trade_date', 'ma1','ma10']]
daily_basic = pd.read_csv(path  + '\daily-basic.csv', index_col=0,
                          dtype={'trade_date': object})

path=path+str(today)+'model4\\'
print(data.shape)
print(daily_basic.shape)
data['ma1/ma10pct']=100*(data['ma1']/data['ma10']-1)
print(data.shape)

# 上涨次数
data=data.merge(tool.up_times(data,period=PERIOD,label=LABEL,up_times=TIMES),on=['ts_code','trade_date'],how='left')
print(data.shape)
up_times=tool.up_times(data,period=PERIOD,label=LABEL,up_times=TIMES)
up_times.to_csv('up_time.csv')
print(up_times.shape)
print(data.shape)
# daily_baisc 用于筛选市值，换手率，
for k,v in OTHERS.items():
    daily_basic=daily_basic[(daily_basic[k]>=v[0])&(daily_basic[k]<=v[1])]
    print(daily_basic.shape)

# 去除st
history = tool.history_name(start_date=data['trade_date'].min())
history['name'] = 'st'
data = data.merge(history, on=['ts_code', 'trade_date'], how='left')
print(data.shape)
data = data[data['name'].isna()]
data.drop(columns=['name'],inplace=True)

print(daily_basic.shape)
print(data.shape)

# 1.10天上涨大于等于7次的股票

# 2.所有股票筛选100*(ma1/ma10-1)在[1,10]之间
# 3.筛选换手率,市值


data=data[data['count_%s' %LABEL ]>=TIMES]
print(data.shape)
data=data[(data['ma1/ma10pct']>=up_n_pct[0])&(data['ma1/ma10pct']<=up_n_pct[1])]
print(data.shape)
data=data.merge(daily_basic[['ts_code', 'trade_date']],on=['ts_code', 'trade_date'])
print(data.shape)

data[:,0:2].to_csv(path+'%daysup%stimes.csv')


