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
import util.sheep as sheep
pro = ts.pro_api()
tool = basic.basic()

path = 'D:\\workgit\\stock\\util\\stockdata\\'
today = datetime.datetime.today().date()
up_n_pct=[1,10]
PERIOD= 10
TIMES= 6
LABEL='ma1'
OTHERS={'total_mv':[-10,30],'turnover_rate':[1.5,float('inf')]}


while (not os.path.isfile(path + str(today) + '\data.csv')) or (
not os.path.isfile(path + str(today) + '\daily-basic.csv')):
    today = today - datetime.timedelta(1)

path=path + str(today)
data = pd.read_csv(path  + '\data.csv', index_col=0,
                   dtype={'trade_date': object})[['ts_code', 'close','trade_date', 'ma1','ma10']]
daily_basic = pd.read_csv(path  + '\daily-basic.csv', index_col=0,
                          dtype={'trade_date': object})

path=path+'model4\\'
if not os.path.isdir(path):
    os.makedirs(path)
    print(path)



print(data.shape)
print(daily_basic.shape)
data['ma1/ma10pct']=100*(data['ma1']/data['ma10']-1)
print(data.shape)

# 上涨次数
df=data.merge(tool.up_times(data,period=PERIOD,label=LABEL,up_times=TIMES),on=['ts_code','trade_date'],how='left')
print(df.shape)


# daily_baisc 用于筛选市值，换手率，
for k,v in OTHERS.items():
    daily_basic=daily_basic[(daily_basic[k]>=v[0])&(daily_basic[k]<=v[1])]
    print(daily_basic.shape)

# 去除st
history = tool.history_name(start_date=df['trade_date'].min())
history['name'] = 'st'
df = df.merge(history, on=['ts_code', 'trade_date'], how='left')
print(df.shape)
df = df[df['name'].isna()]
df.drop(columns=['name'],inplace=True)

print(daily_basic.shape)
print(df.shape)
df.to_csv(path+'data.csv')
# 1.10天上涨大于等于7次的股票
# 2.所有股票筛选100*(ma1/ma10-1)在[1,10]之间
# 3.筛选换手率,市值


df=df[df['count_%s' %LABEL ]>=TIMES]
print(df.shape)
df=df[(df['ma1/ma10pct']>=up_n_pct[0])&(df['ma1/ma10pct']<=up_n_pct[1])]
print(df.shape)
df=df.merge(daily_basic[['ts_code', 'trade_date']],on=['ts_code', 'trade_date'])
print(df.shape)

df.to_csv(path+'%daysup%stimes.csv'%(PERIOD,TIMES))
sheep.wool(df,data).to_csv(path+'%daysup%stimeshuisu.csv'%(PERIOD,TIMES))
df[df['trade_date']==(str(today).replace('-',''))][['ts_code']].reset_index(drop=True).to_csv('%sdaysup%stimes%snew.txt'%(PERIOD,TIMES,str(today)))


