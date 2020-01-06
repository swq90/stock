
import os
import datetime
import pandas as pd
import util.sheep as sheep
import util.basic as basic

tool=basic.basic()


labels = ['low_ma5', 'low', 'ma1', 'ma5']

path = 'D:\\workgit\\stock\\util\\stockdata\\'

today=datetime.datetime.today().date()
while (not os.path.isfile(path + str(today) + '\data.csv')) or (
# not os.path.isfile(path + str(today) + '\daily-basic.csv')):
not os.path.isfile(path + str(today) + '\stock-label.csv')):
    today = today - datetime.timedelta(1)
stock_label=pd.read_csv(path + str(today) + '\stock-label.csv', index_col=0, dtype={'trade_date': object})
print(stock_label.shape)
data=pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})
history=tool.history_name(start_date=stock_label['trade_date'].min())
history['name']='st'
stock_label=stock_label.merge(history,on=['ts_code', 'trade_date'],how='left')
print(stock_label.shape)
stock_label=stock_label[stock_label['name'].isna()]
print(stock_label.shape)
stock_label.drop(columns='name',inplace=True)
print(stock_label.shape)


for label in labels:
    for i in range(6):
        print('%s出现%s次'%(label,i))
        df=sheep.wool(stock_label[stock_label['count_%s'%label]==i],data)
        print('共%s个交易日，最终回溯结果%s'%(df.shape[0],df.iloc[-1, -1]))