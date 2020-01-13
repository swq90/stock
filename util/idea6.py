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
pct=list(range(-11,11))
# pct=list(range(12))

today = datetime.datetime.today().date()
tool=basic.basic()
pro=ts.pro_api()
# 获得基础数据
t=pro.stk_limit(ts_code='')


while (not os.path.isfile(path + str(today) + '\data.csv')) or (
        not os.path.isfile(path + str(today) + '\daily-basic.csv')) or (
        not os.path.isfile(path + str(today) + '\stock-label.csv')):
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，

data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})[['ts_code','trade_date','close','pct_chg','low','ma5','low_ma5']]

stock_baisc = pd.read_csv(path + str(today) + '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'turnover_rate','total_mv']]
path=path+str(today)+'idea6\\'
if not os.path.isdir(path):
    os.makedirs(path)
    print(path)

# limit_price=tool.pre_data(data,['ma1'])
#
# limit_price['limit_up'] = (limit_price['pre_1_ma1']*1.1).map(FORMAT)
# limit_price['limit_down'] = (limit_price['pre_1_ma1']*0.9).map(FORMAT)
# limit_price['limit_up'] = limit_price['limit_up'].map(FORMAT)
# limit_price['limit_down']=limit_price['limit_down'].astype(float)


history = tool.history_name(start_date=data['trade_date'].min())
history['name'] = 'st'
data = data.merge(history, on=['ts_code', 'trade_date'], how='left')
print(data.shape)
data = data[data['name'].isna()]
data.drop(columns='name',inplace=True)
print(data.shape)
data.dropna(inplace=True)
print(data.shape)

data.to_csv(path+'2019datalowma5.csv')

res=pd.DataFrame()



for day in range(12):

    for t in label:
        l = []
        if day==0:
            stock_label = data[data['low_ma5']==0][['ts_code','trade_date']]
        else:
            stock_label = sheep.sheep(data, pre=day, labels=label)
            stock_label = stock_label[stock_label['count_%s' % t] == day]

        stock_label.to_csv(path+'2019low_ma5_continue_%slist.csv'%day)

        for k in pct:
            stock=stock_label.merge(data[(data['pct_chg']<=k+1)&(data['pct_chg']>k)][['ts_code','trade_date']])
            # stock=stock_label.merge(data[abs(data['pct_chg'])<=k])

            stock = stock[(stock['trade_date'] >= '20190101') & (stock['trade_date'] <= '20191231')]
            print(stock.shape)
            stock.to_csv(path+'2019data-pct%sday%s.csv'%(k,day))
            df=sheep.wool(stock,data)

            if df.empty:
                l.append(0)
            else:
                df.to_csv(path + '2019data-%swoolpct%sday%s.csv' % (t, k, day))
                l.append(df.iloc[-1,-1])
    res['%s-%s'%(t,day,)]=l
res.to_csv(path+'huisuxiaoguo2019.csv')