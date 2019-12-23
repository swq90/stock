import os
import datetime
import numpy as np
import math
import pandas as pd
import stockfilter

import tushare as ts
import util.basic as basic
import util.sheep as sheep
import util.fuquan as fuquan


# ts.set_token('006b49622d70edc237ab01340dc210db15d9580c59b40d028e34e015')
pro = ts.pro_api()
tool = basic.basic()
ma = [1, 5]
period = 5
up_cal = 240
temp = 240
pre = 5
labels = ['low_ma5', 'low', 'ma1', 'ma5']
new_dir='\\stockdata\\'+str(datetime.datetime.today().date())+'\\'
path = os.getcwd() + new_dir
# os.makedirs(new_dir)

print(path)
datelist=tool.tradeCal(cal=up_cal)
if not os.path.isdir(path):
    os.makedirs(path)
    print(path)

score = pd.DataFrame()
data = pd.DataFrame()
daily_basic = pd.DataFrame()
stock_label = pd.DataFrame()
stock_score = pd.DataFrame()
stock_marks = pd.DataFrame()

if os.path.isfile(path + 'data.csv'):
    data = pd.read_csv(path + 'data.csv', index_col=0, dtype={'trade_date': object})
    # data['trade_date']=data['trade_date'].astype('str')
else:
    data = tool.trade_daily(cal=up_cal + temp).reset_index(drop=True)
    print('daily')
    data=fuquan.fuqan(data)
    print('fuquan')
    data = data.merge(tool.get_all_ma(data, ma=ma, dis_pct=False), on=['ts_code', 'trade_date'])
    print('ma')
    # 要修改Lget_all_ma 返回只保留ma，code，date，其他删除
    data['low_ma5'] = data.apply(lambda x: 1 if x['low'] > x['ma5'] else 0, axis=1)
    data.to_csv(path + 'data.csv')

print("基础数据")
print(data['trade_date'].unique().shape)
if os.path.isfile(path + 'score.csv'):
    score = pd.read_csv(path + 'score.csv', index_col=0, dtype=np.float64)
else:
    score = sheep.grass(data[data['trade_date'].isin(datelist)==True])
    score.to_csv(path + 'score.csv')
#     调用方法获得得分表
print('对应分数')
data=data[data['trade_date'].isin(datelist)==True]
# print(data['trade_date'].unique().shape)

if os.path.isfile(path + 'stock-label.csv'):
    stock_label = pd.read_csv(path + 'stock-label.csv', index_col=0, dtype={'trade_date': object})
else:
    stock_label = sheep.sheep(data)
    # stock_label =stock_label[stock_label['trade_date'].isin(datelist)==True]
    stock_label.to_csv(path + 'stock-label.csv')
print('各项满足情况')

if os.path.isfile(path + 'stock_marks.csv'):
    stock_marks = pd.read_csv(path + 'stock_marks.csv', index_col=0, dtype={'trade_date': object})
else:
    stock_marks = sheep.marks(stock_label, score)
    stock_marks.to_csv(path + 'stock_marks.csv')
print('marks', stock_marks.shape)

if os.path.isfile(path + 'daily-basic.csv'):
    daily_basic = pd.read_csv(path + 'daily-basic.csv', index_col=0, dtype={'trade_date': object})
else:
    for i in data['trade_date'].unique():
        daily_basic = pd.concat([pro.daily_basic(trade_date=i), daily_basic], ignore_index=True)
    for key in ["total_share", "float_share", "free_share", "total_mv", "circ_mv"]:
        daily_basic[key] = daily_basic[key] / 10000
    daily_basic.to_csv(path + 'daily-basic.csv')

run_time = datetime.datetime.today()
# path = path + str(run_time).replace(":", "-").replace(' ', '-')[:10] + '-'

# if os.path.isfile(path+'stock-score.csv'):
#     stock_score = pd.read_csv(path + 'stock-score.csv', index_col=0)
# else:
#     stock_score = marks(stock_label,score)
#     stock_score.to_csv(path + 'stock-score.csv')

stock_marks = stock_marks[stock_marks['score'] >= 10]
print('marks1', stock_marks.shape)
stock_need = data[(data['close'] >= (0.97 * data['pre_close'])) & (data['close'] <= (1.03 * data['pre_close'])) & (
        abs(data['open'] - data['close']) <= (0.04 * data['pre_close']))]
# print( stock_need.shape)
#
# stock_need = stock_need[stock_need['close'] < (1.1 * stock_need['pre_close'])]
# print( stock_need.shape)

# print(stock_need.info())
# print(stock_marks.info())
stock_marks = stock_marks.merge(stock_need[['ts_code', 'trade_date']], on=['ts_code', 'trade_date'])
# print('marks2', stock_marks.shape)

# data_m = data[((data["low"] == data["high"])) == False]
# stock_marks[['ts_code', 'trade_date', 'score']].to_csv(path + '50ofall.csv')
# 保存当天前五十
print('marks3', stock_marks.shape)
mv_bins = []
mv_bins = list(range(0, 101, 20)) + [150, 200, 400, 30000]
if mv_bins:
    for i in range(len(mv_bins) - 1):
        CONTAIN = daily_basic[(daily_basic['total_mv'] >= mv_bins[i]) & (daily_basic['total_mv'] < mv_bins[i + 1])]
        print('市值',(mv_bins[i], mv_bins[i + 1]))
        stock_data1 = stock_marks.merge(CONTAIN[['ts_code', 'trade_date']], on=['ts_code', 'trade_date'])
        df = pd.DataFrame()
        for day in stock_data1['trade_date'].unique():
            # df=pd.concat([data[data['trade_date']==day].sort_values(by='trade_date',ascending=False).head(30),df])
            df = pd.concat(
                [stock_data1[stock_data1['trade_date'] == day].sort_values(by='score', ascending=False).head(30), df])
            # stock.to_csv(path + str((mv_bins[i], mv_bins[i + 1])) + "30ofall_marks.csv")
        df.to_csv(path + str((mv_bins[i], mv_bins[i + 1])) + "-30-of-bins.csv")

        stock = sheep.wool(df, data)

        stock.to_csv(path + str((mv_bins[i], mv_bins[i + 1])) + "pct-wool.csv")
        print(stock)



#所有股票排名回溯
df = pd.DataFrame()
top_n=[50]
for i in top_n:
    if df.empty:
        for day in stock_marks['trade_date'].unique():
            # df=pd.concat([data[data['trade_date']==day].sort_values(by='trade_date',ascending=False).head(30),df])
            df = pd.concat(
                [stock_marks[stock_marks['trade_date'] == day].sort_values(by='score', ascending=False).head(i), df])
        df.to_csv(path+'sort_data_score.csv')
        stock = sheep.wool(df, data)
        stock.to_csv(path + "all-pct-wool-head%s.csv"%i)
        print(i,stock)





























#
# print(list(data))
# print(list(daily_basic))
# print(list(stock_label))
# print(list(stock_score))
# print(list(stock_marks))
#
# # print('jieshulallllllaaaaa')
# 过滤


# data.to_csv(path + "all_marks.csv")
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