import os
import datetime
import numpy as np
import math
import pandas as pd
import stockfilter

import tushare as ts
import util.basic as basic
import util.sheep as sheep

# ts.set_token('006b49622d70edc237ab01340dc210db15d9580c59b40d028e34e015')
pro = ts.pro_api()
tool = basic.basic()
ma = [1, 5]
period = 5
up_cal = 240
temp = 0
pre = 5
labels = ['low_ma5', 'low', 'ma1', 'ma5']
path = os.getcwd() + '\\data\\'
if not os.path.isdir(path):
    os.mkdir('data')
    print(path)
filename = path + str(datetime.datetime.today())[:10] + '-'
score = pd.DataFrame()
data = pd.DataFrame()
daily_basic = pd.DataFrame()
stock_label = pd.DataFrame()
stock_score = pd.DataFrame()
stock_marks = pd.DataFrame()

if os.path.isfile(filename + 'data.csv'):
    data = pd.read_csv(filename + 'data.csv', index_col=0, dtype={'trade_date': object})
    # data['trade_date']=data['trade_date'].astype('str')
else:
    data = tool.trade_daily(cal=up_cal + temp).reset_index(drop=True)
    data = data.merge(tool.get_all_ma(data, ma=ma, dis_pct=False), on=['ts_code', 'trade_date'])
    # 要修改Lget_all_ma 返回只保留ma，code，date，其他删除
    data['low_ma5'] = data.apply(lambda x: 1 if x['low'] > x['ma5'] else 0, axis=1)
    data.to_csv(filename + 'data.csv')

print("基础数据")

if os.path.isfile(filename + 'score.csv'):
    score = pd.read_csv(filename + 'score.csv', index_col=0, dtype=np.float64)
else:
    score = sheep.grass(data)
    score.to_csv(filename + 'score.csv')
#     调用方法获得得分表
print('对应分数')

if os.path.isfile(filename + 'stock-label.csv'):
    stock_label = pd.read_csv(filename + 'stock-label.csv', index_col=0, dtype={'trade_date': object})
else:
    stock_label = sheep.sheep(data)
    stock_label.to_csv(filename + 'stock-label.csv')
print('各项满足情况')

if os.path.isfile(filename + 'stock_marks.csv'):
    stock_marks = pd.read_csv(filename + 'stock_marks.csv', index_col=0, dtype={'trade_date': object})
else:
    stock_marks = sheep.marks(stock_label, score)
    stock_marks.to_csv(filename + 'stock_marks.csv')
print('marks', stock_marks.shape)

if os.path.isfile(filename + 'daily-basic.csv'):
    daily_basic = pd.read_csv(filename + 'daily-basic.csv', index_col=0, dtype={'trade_date': object})
else:
    for i in data['trade_date'].unique():
        daily_basic = pd.concat([pro.daily_basic(trade_date=i), daily_basic], ignore_index=True)
    for key in ["total_share", "float_share", "free_share", "total_mv", "circ_mv"]:
        daily_basic[key] = daily_basic[key] / 10000
    daily_basic.to_csv(filename + 'daily-basic.csv')

run_time = datetime.datetime.today()
FILENAME = path + str(run_time).replace(":", "-").replace(' ', '-')[:10] + '-'

# if os.path.isfile(filename+'stock-score.csv'):
#     stock_score = pd.read_csv(filename + 'stock-score.csv', index_col=0)
# else:
#     stock_score = marks(stock_label,score)
#     stock_score.to_csv(filename + 'stock-score.csv')

stock_marks = stock_marks[stock_marks['score'] >= 10]
print('marks1', stock_marks.shape)
stock_need = data[(data['close'] >= (0.97 * data['pre_close'])) & (data['close'] <= (1.03 * data['pre_close'])) & (
        abs(data['open'] - data['close']) <= (0.04 * data['pre_close']))]
stock_need = stock_need[stock_need['close'] < (1.1 * stock_need['pre_close'])]
# print(stock_need.info())
# print(stock_marks.info())
stock_marks = stock_marks.merge(stock_need[['ts_code', 'trade_date']], on=['ts_code', 'trade_date'])
print('marks2', stock_marks.shape)

# data_m = data[((data["low"] == data["high"])) == False]
stock_marks[['ts_code', 'trade_date', 'score']].to_csv(filename + '50ofall.csv')
# 保存当天前五十
print('marks3', stock_marks.shape)
mv_bins = []
mv_bins = list(range(0, 101, 20)) + [150, 200, 400, 30000]
if mv_bins:
    print(mv_bins)
    for i in range(len(mv_bins) - 1):
        CONTAIN = daily_basic[(daily_basic['total_mv'] >= mv_bins[i]) & (daily_basic['total_mv'] < mv_bins[i + 1])]
        print((mv_bins[i], mv_bins[i + 1]))
        stock_data1 = stock_marks.merge(CONTAIN[['ts_code', 'trade_date']], on=['ts_code', 'trade_date'])
        df = pd.DataFrame()
        for day in stock_data1['trade_date'].unique():
            # df=pd.concat([data[data['trade_date']==day].sort_values(by='trade_date',ascending=False).head(30),df])
            df = pd.concat(
                [stock_data1[stock_data1['trade_date'] == day].sort_values(by='score', ascending=False).head(30), df])
            # stock.to_csv(FILENAME + str((mv_bins[i], mv_bins[i + 1])) + "30ofall_marks.csv")
        df.to_csv(FILENAME + str((mv_bins[i], mv_bins[i + 1])) + "-30-of-bins.csv")

        stock = sheep.wool(df, data)
        stock.to_csv(FILENAME + str((mv_bins[i], mv_bins[i + 1])) + "pct-wool.csv")
        print(stock)



#所有股票排名回溯
df = pd.DataFrame()
for day in stock_marks['trade_date'].unique():
    # df=pd.concat([data[data['trade_date']==day].sort_values(by='trade_date',ascending=False).head(30),df])
    df = pd.concat(
        [stock_marks[stock_marks['trade_date'] == day].sort_values(by='score', ascending=False).head(50), df])
df.to_csv(filename+'sort_data_score.csv')
stock = sheep.wool(df, data)
stock.to_csv(FILENAME + "all-bins-pct_wool.csv")
print(stock)


























#
# print(list(data))
# print(list(daily_basic))
# print(list(stock_label))
# print(list(stock_score))
# print(list(stock_marks))
#
# # print('jieshulallllllaaaaa')
# 过滤


# data.to_csv(FILENAME + "all_marks.csv")
# df = df[df['score'] >= 10]


# stock_del=data[(abs(data['open']-data['close'])<=(0.05*data['pre_close']))&(data['close']>=(0.97*data['preclose']))]

# stock_data=sheep(data)
# print('stockdata',stock_data.shape)

# # stock_need=stock_need[]
# print(stock_need)
# print(stock_need.shape)

# stock_data=stock_data.merge(stock_need[['ts_code','trade_date']],on=['ts_code','trade_date'],how='inner')
# print('filter',stock_data.shape)


# CONTAIN = stockfilter.StockFilter().stock_basic('20191127', total_mv=[5000000000,20000000000])
# stock_data=stock_data[stock_data['ts_code'].isin(CONTAIN)]
# print(stock_data)
# stock_data=marks(stock_data,score)
# stock=wool(stock_data,data)
# print(stock)
