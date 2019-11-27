import os
import datetime
import numpy as np
import math
import pandas as pd

import tushare as ts
import util.basic as basic



score=pd.read_csv('D:\\workgit\\stock\\util\\score.csv',dtype=np.float64)
ma = [1, 5, 10]
period = 5
up_cal = 240
temp = 0
pre = 5
# labels = ['low_ma5', 'low']

labels = ['low_ma5', 'low', 'ma1', 'ma5']
# score=pd.DataFrame()
pro = ts.pro_api()
tool = basic.basic()
data = tool.trade_daily(cal=up_cal + temp).reset_index(drop=True)
data = data.merge(tool.get_all_ma(data, ma=[1, 5], dis_pct=False), on=['ts_code', 'trade_date'])
data['low_ma5'] = data.apply(lambda x: 1 if x['low'] > x['ma5'] else 0, axis=1)
def grass(data):

    up = tool.up_info(data, days=period, up_range=0.5, pct=0)
    print('up', up.shape)
    up = up[up["trade_date"].isin(tool.tradeCal(cal=up_cal))]
    print('up', up.shape)

    for label in labels:
        df = data[['ts_code', 'trade_date', label]]
        print('df', df.shape)

        # df.loc[:,['count']] = 0
        for i in range(1, pre + 1):
            df = tool.pre_label(df, label=label, days=i)

            if label == 'low_ma5':
                if i == 1:
                    df['count_%s' % label] = df['pre_1_low_ma5']
                else:
                    df['count_%s' % label] = df.apply(
                        lambda x: x['count_%s' % label] + x['pre_%s_%s' % (i, label)], axis=1)

            else:
                if i == 1:
                    df['count'] = df.apply(lambda x: 1 if x[label] >= x['pre_%s_%s' % (i, label)] else 0, axis=1)
                else:
                    df['count'] = df.apply(
                        lambda x: 1 + x['count'] if x['pre_%s_%s' % ((i - 1), label)] >= x[
                            'pre_%s_%s' % (i, label)] else x[
                            'count'], axis=1)
            print('df', df.shape)
        df.to_csv('all%s.csv' % label)

        df = df.dropna()
        up_pre = up[['ts_code', 'pre_n_date']]
        up_pre.rename(columns={'pre_n_date': 'trade_date'}, inplace=True)
        up_pre = up_pre.merge(df, on=['ts_code', 'trade_date'])
        df = df[df["trade_date"].isin(tool.tradeCal(cal=up_cal))]
        df.to_csv('all%sindays.csv' % label)
        print('df', df.shape)
        print('uppre', up_pre.shape)
        up_pre.to_csv('up_pre%s.csv' % label)
        up_pre = pd.DataFrame(up_pre.groupby(by='count').size(), columns=['count'])
        up_pre['pct'] = up_pre['count'] / up_pre['count'].sum()
        df = pd.DataFrame(df.groupby(by='count').size(), columns=['count'])
        df['pct'] = df['count'] / df['count'].sum()

        print('up:%s>pre_%s' % (label, label), up_pre)
        print('all:%s>pre_%s' % (label, label), df)
        up_pre.rename(columns={'pct': 'up_pct'}, inplace=True)
        df.rename(columns={'pct': 'all_pct'}, inplace=True)
        g = up_pre[['up_pct']].merge(df[['all_pct']], left_index=True, right_index=True)
        g['div'] = g['up_pct'] / g['all_pct']
        g['%sscore' % label] = g.apply(lambda x: 10 * x['div'], axis=1) / g['div'].sum()
        print(g)
        if score.empty:
            score = g[['%sscore' % label]]
        else:
            score = score.merge(g[['%sscore' % label]], left_index=True, right_index=True)

def sheep(data):

    print(data.shape)
    res=pd.DataFrame()
    for label in labels:


        df = data[['ts_code', 'trade_date', label]]
        df['count_%s' % label] =0
        print('df%s'%label, df.shape)

        # df.loc[:,['count']] = 0
        for i in range(1, pre + 1):
            df = tool.pre_label(df, label=label, days=i)

            if label == 'low_ma5':
                df['count_%s'%label] = df.apply(
                    lambda x: x['count_%s'%label] + x['pre_%s_%s' % (i, label)], axis=1)
                # df['count_%s'%label]=df['count_%s'%label].astype('int')

            else:
                if i==1:
                    df['count_%s'%label] = df.apply(
                        lambda x: 1 + x['count_%s'%label] if x[label] >= x['pre_%s_%s' % (i, label)] else x[
                            'count_%s'%label], axis=1)

                else:
                    df['count_%s'%label] = df.apply(
                        lambda x: 1 + x['count_%s'%label] if x['pre_%s_%s' % ((i - 1), label)] >= x['pre_%s_%s' % (i, label)] else x[
                            'count_%s'%label], axis=1)
            print('df', df.shape)
        df.to_csv('all%s.csv' % label)
        df = df.dropna()

        if res.empty:
            res=df[['ts_code','trade_date','count_%s'%label]]
        else:
            res=res.merge(df[['ts_code','trade_date','count_%s'%label]],on=['ts_code','trade_date'])
    res['count_low_ma5'] = res['count_%s' % label].astype('int')

    return res


def marks(data,score):
    df=pd.DataFrame()
    data['score']=0
    for label in labels:

        data['score']=data.apply(lambda x:x['score']+score.iloc[x['count_%s'%label]]['%sscore' % label],axis=1)
        print(data)
    for day in data['trade_date'].unique():

        df=pd.concat([data[data['trade_date']==day].sort_values(by='trade_date',ascending=False).head(30),df])
    df.to_csv("30ofall.csv")
    return df

def wool(stock,data):
    PRICEB = "close"
    PRICES = "close"
    days=1
    print(stock)
    print(data)
    limit_up = stock[['ts_code','trade_date']].sort_values(by="trade_date").reset_index(drop=True)
    print(limit_up)

    # for trade_date in  data["trade_date"].unique():
    #     z=pro.limit_list(trade_date=trade_date, limit_type='U', fields='ts_code,trade_date,pct_chg')
    #     h=z[z["pct_chg"]>=10]
    #     d=limit_up[limit_up["trade_date"]==trade_date]
    # print(limit_up)

    buy_data = limit_up.merge(data[['ts_code', 'trade_date', PRICEB]], on=['ts_code', 'trade_date'])[
        ['ts_code', 'trade_date', PRICEB]]
    print(buy_data)
    buy_data.columns = ['ts_code', 'buy_date', "buy_price"]
    pre_date = tool.pre_date(data[["trade_date"]], days=days)
    sell_data = data[['ts_code', 'trade_date', PRICES]].merge(pre_date, on='trade_date')
    sell_data.rename(columns={"trade_date": "sell_date", "pre_%s_date" % days: "buy_date", PRICES: 'sell_price'},
                     inplace=True)
    sell_data = sell_data.merge(buy_data, on=['ts_code', 'buy_date'])
    print(sell_data)
    sell_data.to_csv("wool4sell.csv")

    sell_data['pct'] = (sell_data['sell_price'] / sell_data['buy_price'])
    sell_cut = sell_data.groupby(by='sell_date')['pct'].mean()
    sell_cut = pd.DataFrame(sell_cut)
    sell_cut['all_pct'] = sell_cut['pct'].cumprod()
    sell_cut.to_csv('pctwool3.csv')
    print(sell_cut)
    return sell_cut

stock_data=sheep(data)
print(stock_data)
stock_data=marks(stock_data,score)
stock=wool(stock_data,data)
print(stock)