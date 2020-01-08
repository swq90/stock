# 连续出现n日low>ma5的数据回溯效果
import os
import datetime
import pandas as pd

import util.basic as basic
import util.sheep as sheep



label = ['low_ma5']
path = 'D:\\workgit\\stock\\util\\stockdata\\'
pct=list(range(12))
today = datetime.datetime.today().date()
tool=basic.basic()

# 获得基础数据
while (not os.path.isfile(path + str(today) + '\data.csv')) or (
        not os.path.isfile(path + str(today) + '\daily-basic.csv')) or (
        not os.path.isfile(path + str(today) + '\stock-label.csv')):
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，
data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})
print(data.shape)
history = tool.history_name(start_date=data['trade_date'].min())
history['name'] = 'st'
data = data.merge(history, on=['ts_code', 'trade_date'], how='left')
print(data.shape)
data = data[data['name'].isna()]


stock_baisc = pd.read_csv(path + str(today) + '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'turnover_rate','total_mv']]
res=pd.DataFrame()

for k in pct:

    for t in label:
        l = []
        for day in range(1,11):
            stock_label=sheep.sheep(data,pre=day,labels=label)

            stock_label=stock_label.merge(data[abs(data['pct_chg'])<=k][['ts_code','trade_date']])
            stock_label=stock_label[stock_label['count_%s' % t] == day]
            stock_label.to_csv(path+'low_ma5_continue_%sstock.csv'%day)
            print(stock_label.shape)
            df=sheep.wool(stock_label,data)
            # df.to_csv(path+'%s_continue_%swoll.csv'%(label,day))
            l.append(df.iloc[-1,-1])
    res['%s%s'%(label,k)]=l