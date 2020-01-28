import os
import datetime
import pandas as pd
import util.sheep as sheep

path = 'D:\\workgit\\stock\\util\\stockdata\\'
# pct=list(range(-11,11))

today = datetime.datetime.today().date()

while (not os.path.isfile(path + str(today) + '\data.csv')) or (
        not os.path.isfile(path + str(today) + '\history_name.csv')):
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，

data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'close', 'pct_chg', 'low', 'ma5']]
history = pd.read_csv(path + str(today) + '\history_name.csv', index_col=0, dtype={'trade_date': object})
history['name'] = 'st'
data = data.merge(history, on=['ts_code', 'trade_date'], how='left')
print(data.shape)
data = data[data['name'].isna()]
data.drop(columns='name', inplace=True)
print(data.shape)
data.dropna(inplace=True)
for i in [18,19]:
    res=sheep.wool(data[(data['trade_date']>='20%s0101'%i)&(data['trade_date']<='20%s1231'%i)],data)
    res.to_csv(path+'%squanbuhuisuxiaoguo.csv'%i)