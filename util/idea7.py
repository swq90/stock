# 连续出现n日low>ma5的数据回溯效果
import os
import datetime
import pandas as pd
import tushare as ts
import util.basic as basic
import util.sheep as sheep


FORMAT = lambda x: '%.4f' % x
label = ['low_ma5']
path = 'D:\\workgit\\stock\\util\\stockdata\\'
# pct=list(range(-11,11))


today = datetime.datetime.today().date()
tool=basic.basic()
pro=ts.pro_api()
# 获得基础数据

XISHU=0.998
PERIOD=5
TIMES=5
CHGPCT=[1,2]

while (not os.path.isfile(path + str(today) + '\data.csv')) or (
        not os.path.isfile(path + str(today) + '\daily-basic.csv')) or (
        not os.path.isfile(path + str(today) + '\stock-label.csv')):
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，

data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})[['ts_code','trade_date','close','pct_chg','low','ma5']]
# data=data[data['trade_date']>='20191201']
data['lowma5']=data.apply(lambda x:1 if x['low']>(XISHU*x['ma5']) else 0 ,axis=1)
stock_baisc = pd.read_csv(path + str(today) + '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'turnover_rate','total_mv']]
history = pd.read_csv(path + str(today) + '\history_name.csv', index_col=0, dtype={'trade_date': object})
history['name'] = 'st'
path=path+str(today)+'idea7\\'
if not os.path.isdir(path):
    os.makedirs(path)
    print(path)

df=data.copy()

df.sort_values(by='trade_date',inplace=True)
# df.to_csv(path+'2019datalowma5.csv')
c_times = df.groupby('ts_code')['lowma5'].rolling(PERIOD).sum()
c_times.index = c_times.index.droplevel()
c_times = pd.DataFrame(c_times)
c_times.rename(columns={'lowma5': 'count'}, inplace=True)
# count_times=count_times[count_times['%s_%s_uptimes'%(label,period)]>=up_period]
df = df.join(c_times)
df.to_csv('idea77.csv')

print('777',df.shape)
df = df.merge(history, on=['ts_code', 'trade_date'], how='left')
print(df.shape)
df = df[df['name'].isna()]
df.drop(columns='name',inplace=True)
print(df.shape)
df.dropna(inplace=True)

print(df.shape)





df.to_csv(path+'data.csv')


df = df[(df['count'] >= TIMES)&(df['pct_chg']>=CHGPCT[0])&(df['pct_chg']<=CHGPCT[1])][['ts_code', 'trade_date']]
print(df.shape)
df.to_csv(path+'low%sma%stimes%s(%s-%s).csv'%(XISHU,PERIOD,TIMES,CHGPCT[0],CHGPCT[1]))

wool=sheep.wool(df,data)
wool.to_csv(path+'low%sma%stimes%s(%s-%s)huisuxiaoguo.csv'%(XISHU,PERIOD,TIMES,CHGPCT[0],CHGPCT[1]))