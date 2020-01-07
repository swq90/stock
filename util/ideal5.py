import os
import datetime
import pandas as pd
import matplotlib.pyplot as plt

import util.sheep as sheep
import util.basic as basic

tool = basic.basic()

labels = ['low_ma5', 'low', 'ma1', 'ma5']

path = 'D:\\workgit\\stock\\util\\stockdata\\'

today = datetime.datetime.today().date()
while (not os.path.isfile(path + str(today) + '\data.csv')) or (
        not os.path.isfile(path + str(today) + '\daily-basic.csv')) or (
        not os.path.isfile(path + str(today) + '\stock-label.csv')):
    today = today - datetime.timedelta(1)
print(today)
stock_label = pd.read_csv(path + str(today) + '\stock-label.csv', index_col=0, dtype={'trade_date': object})
data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})
stock_baisc = pd.read_csv(path + str(today) + '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'turnover_rate','total_mv']]
print(stock_label.shape)

history = tool.history_name(start_date=stock_label['trade_date'].min())
history['name'] = 'st'
stock_label = stock_label.merge(history, on=['ts_code', 'trade_date'], how='left')
print(stock_label.shape)
stock_label = stock_label[stock_label['name'].isna()]
print(stock_label.shape)
stock_label.drop(columns='name', inplace=True)
print(stock_label.shape)

for label in labels:
    for i in range(6):
        print('%s出现%s次' % (label, i))
        stock = stock_label[stock_label['count_%s' % label] == i]
        print(stock.shape)
        df = sheep.wool(stock, data)
        print('共%s个交易日，最终回溯结果%s' % (df.shape[0], df.iloc[-1, -1]))
        stock_dtl = stock.merge(stock_baisc, on=['ts_code', 'trade_date'], how='left')
        print(stock_dtl['total_mv'].decribe())
        print(stock_dtl['turnover_rate'].decribe())

        dis=stock_dtl.groupby('total_mv')['ts_code'].count()
        dis.plt.scat
        #
        # plt.scatter(stock_dtl['ts_code'],stock_dtl['total_mv'])
        # plt.title('共%s个交易日，最终回溯结果%s' % (df.shape[0], df.iloc[-1, -1]))
        # plt.show()
