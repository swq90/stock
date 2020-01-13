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
data['lowma5']=data.apply(lambda x:1 if x['low']>(XISHU*x['ma5']) else 0 ,axis=1)
stock_baisc = pd.read_csv(path + str(today) + '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'turnover_rate','total_mv']]
path=path+str(today)+'idea7\\'
if not os.path.isdir(path):
    os.makedirs(path)
    print(path)

history = tool.history_name(start_date=data['trade_date'].min())
history['name'] = 'st'
data = data.merge(history, on=['ts_code', 'trade_date'], how='left')
print(data.shape)
data = data[data['name'].isna()]
data.drop(columns='name',inplace=True)
print(data.shape)
data.dropna(inplace=True)
print(data.shape)
data.sort_values(by='trade_date',inplace=True)
# data.to_csv(path+'2019datalowma5.csv')
c_times = data.groupby('ts_code')['lowma5'].rolling(PERIOD).sum()
c_times.index = c_times.index.droplevel()
c_times = pd.DataFrame(c_times)
c_times.rename(columns={'lowma5': 'count'}, inplace=True)
# count_times=count_times[count_times['%s_%s_uptimes'%(label,period)]>=up_period]
data = data.join(c_times)
data.to_csv(path+'data.csv')
print(data.shape)


stock_label = data[(data['count'] >= TIMES)&(data['pct_chg']>=CHGPCT[0])&(data['pct_chg']<=CHGPCT[1])][['ts_code', 'trade_date']]
print(stock_label.shape)
stock_label.to_csv(path+'low%sma%stimes%s(%s-%s).csv'%(XISHU,PERIOD,TIMES,CHGPCT[0],CHGPCT[1]))

wool=sheep.wool(stock_label,data)
wool.to_csv(path+'low%sma%stimes%s(%s-%s)huisuxiaoguo.csv'%(XISHU,PERIOD,TIMES,CHGPCT[0],CHGPCT[1]))