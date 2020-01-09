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
pct=list(range(-12,12))
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

data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})




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
data.to_csv('datalowma5.csv')

stock_baisc = pd.read_csv(path + str(today) + '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'turnover_rate','total_mv']]
res=pd.DataFrame()

for day in range(12):

    for t in label:
        l = []
        if day==0:
            stock_label = data[data['low_ma5']==0][['ts_code','trade_date']]
        else:
            stock_label = sheep.sheep(data, pre=day, labels=label)
            stock_label = stock_label[stock_label['count_%s' % t] == day]

        for k in pct:
            stock=stock_label.merge(data[(data['pct_chg']<=k+1)&(data['pct_chg']>k)][['ts_code','trade_date']])
                # stock_label.to_csv(path+'low_ma5_continue_%sstock.csv'%day)
            stock = stock[(stock['trade_date'] > '20180101') & (stock['trade_date'] < '20190101')]
            print(stock.shape)
            stock.to_csv('2018datapct%sday%s.csv'%(k,day))
            df=sheep.wool(stock,data)
            df.to_csv('datawoolpct%sday%s.csv'%(k,day))

            # df.to_csv(path+'%s_continue_%swoll.csv'%(label,day))
            if df.empty:
                l.append(0)
            else:
                l.append(df.iloc[-1,-1])
    res['%s-%s'%(t,day,)]=l
res.to_csv('huisuxiaoguo500.csv')