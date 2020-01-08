import os
import datetime
import pandas as pd
import matplotlib.pyplot as plt

import util.sheep as sheep
import util.basic as basic

tool = basic.basic()

labels = ['low_ma5', 'low', 'ma1', 'ma5']
# labels = ['low_ma5']


path = 'D:\\workgit\\stock\\util\\stockdata\\'

today = datetime.datetime.today().date()
while (not os.path.isfile(path + str(today) + '\data.csv')) or (
        not os.path.isfile(path + str(today) + '\daily-basic.csv')) or (
        not os.path.isfile(path + str(today) + '\stock-label.csv')):
    today = today - datetime.timedelta(1)
print(today)
stock_label = pd.read_csv(path + str(today) + '\stock-label.csv', index_col=0, dtype={'trade_date': object})
data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})
print(data.shape[0],data.shape[0]/3600)


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
for j in range(-10,12):
    res = pd.DataFrame()
    pct_chg = data[(data['pct_chg'] >= j)&(data['pct_chg']<=j+1)]
    for label in labels:
        l=[]
        for i in range(6):

            stock = stock_label[stock_label['count_%s' % label] == i]
            print(stock.shape)

            print(pct_chg.shape[0], pct_chg.shape[0] / 3600)
            df = sheep.wool(stock, pct_chg)
            if df.empty:
                l.append(0)
            else:
                l.append(df.iloc[-1, -1])
        print('%s出现%s次,共%s个交易日，股价变动小于%s-%s最终回溯结果%s' % (label, i, df.shape[0], j, j + 1, df.iloc[-1, -1]))

        res[label]=l
    res.to_csv('float%sresult.csv'%j)
        # stock_dtl = stock.merge(stock_baisc, on=['ts_code', 'trade_date'], how='left')

          # dis=stock_dtl.groupby('total_mv')['ts_code'].count()

        #
        # plt.scatter(stock_dtl['ts_code'],stock_dtl['total_mv'])
        # plt.title('共%s个交易日，最终回溯结果%s' % (df.shape[0], df.iloc[-1, -1]))
        # plt.show()
