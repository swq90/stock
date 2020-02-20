# 首次出现涨停(前一日非涨停），后一日开盘与昨日收盘间关系，划区间回溯
import calendar as cal
from dateutil.parser import parse
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import stock.util.stockfilter as sfilter
import stock.util.basic as basic
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data

class idea1:

    def __init__(self, start_date=None,end_date=None,limit_type='up',days=2):
        self.start_date=start_date
        self.end_date=end_date
        self.raw_data = gls.first_limit(start_date=self.start_date, end_date=self.end_date, limit_type=limit_type, days=days)
        self.data=pd.DataFrame()

    def sell_model(self,model,pb='open', rate=1.01):
        self.PRICEB=pb
        if isinstance(model,str):
            self.PRICES=model
            return
        else:
            self.PRICES = 'ps_%s_%s'%(model,rate)

        if model == 1:
            # 开盘大于昨收，开盘卖，昨收小于今高，昨收卖，否则收盘卖
            self.raw_data.loc[:,self.PRICES] = self.raw_data.apply(
                lambda x: x['open'] if x['open'] >= x['pre_close'] else x['pre_close'] if x['pre_close'] <= x[
                    'high'] else x['close'], axis=1)
        if model == 2:
            # 当日均价卖
            self.raw_data.loc[:,self.PRICES] = 10 * self.raw_data['amount'] / self.raw_data['vol']
        if model == 3:
            # 开盘大于昨收的倍数，开盘卖
            self.raw_data.loc[:,self.PRICES] = self.raw_data.apply(
                lambda x: x['open'] if x['open'] > (x['pre_close'] * rate) else x['close'], axis=1)
        if model == 4:
            # 最高价大约开盘的倍数，high卖，否则close卖
            self.raw_data.loc[:,self.PRICES] = self.raw_data.apply(
                lambda x: x['open'] if x['open'] >= (1.03 * x['pre_close']) else x['open'] * rate if x['high'] > (
                        x['open'] * rate) else x['close'], axis=1)

    def preprocess(self, CHANGE=['open', 'pre_close'],bins=None, labels=None,
                   cuts=2):
        """
        获取基础价，涨停价，
        计算当日开盘/昨日收盘比，
        """
        if bins is None:
            bins = [-100] + list(range(-10, 11, cuts)) + [100]

        self.raw_data.loc[:,'%s/%s' % (CHANGE[0], CHANGE[1])] = self.raw_data.apply(
            lambda x: 99 if x['open'] == x['up_limit'] else -99 if x['open'] == x['down_limit'] else (x[CHANGE[0]] / x[
                CHANGE[1]] - 1) * 100, axis=1)
        self.data = self.raw_data[(self.raw_data['pre_2_is_roof'] == 0) & (self.raw_data['pre_1_is_roof'] == 1)]
        self.data.loc[:,'Categories'] = pd.cut(self.data['%s/%s' % (CHANGE[0], CHANGE[1])], bins, right=False)

    def segment(self):

        res=pd.DataFrame()
        for cut in self.data['Categories'].unique():

            print(cut,type(cut))
            cut=str(cut)
            df = self.data[self.data['Categories'] == cut]
            if df.empty:
                res.loc[cut,'roi']=None
                continue
            res_seg = self.roi(df)
            if res_seg.empty:
                res.loc[cut, 'roi'] = None
                continue
            save_data(res_seg,'首板部分回溯%s_%s_%s_%s_%s.csv'%(cut,self.PRICEB,self.PRICES,self.start_date,self.end_date))
            res.loc[cut,'roi']=res_seg.iloc[-1,-1]
            res.loc[cut, 'n_mean'] = res_seg['n'].mean()
            res.loc[cut, 'days'] = res_seg.shape[0]
        save_data(res,'首板分类回溯汇总_%s_%s_%s_%s.csv'%(self.PRICEB,self.PRICES,self.start_date,self.end_date))
        return

    def roi(self,df=None):
        if df is None:
            return sheep.wool(self.data,self.raw_data,PRICEB=self.PRICEB,PRICES=self.PRICES)
        return sheep.wool(df,self.raw_data,PRICEB=self.PRICEB,PRICES=self.PRICES)



        # 筛选收盘涨停数据



        if model:
            PRICES = 'price_sell'
            if model == 1:
                raw_data[PRICES] = raw_data.apply(
                    lambda x: x['open'] if x['open'] >= x['pre_close'] else x['pre_close'] if x['pre_close'] <= x[
                        'high'] else x['close'], axis=1)
            if model == 2:
                raw_data[PRICES] = 10 * raw_data['amount'] / raw_data['vol']
            if model == 3:
                raw_data[PRICES] = raw_data.apply(
                    lambda x: x['open'] if x['open'] > (x['pre_close'] * 1.01) else x['close'], axis=1)
            if model == 4:
                raw_data[PRICES] = raw_data.apply(
                    lambda x: x['open'] if x['open'] >= (1.03 * x['pre_close']) else x['open'] * 1.01 if x['high'] > (
                            x['open'] * 1.01) else x['close'], axis=1)

                # temp = basic.basic().pre_data(raw_data[['ts_code', 'trade_date', 'ma']], label=['ma'], new_label=[PRICES])
                # raw_data = raw_data.merge(temp[['ts_code', 'trade_date', PRICES]],on=['ts_code', 'trade_date'])
        res_some = pd.DataFrame()
        for cut in range(-12, 11, CUTS):
            if cut < -10:
                buy_data = data[data['open_pre_close_change'] == -9999]
                print('open down')
            elif cut > 8:
                buy_data = data[data['open_pre_close_change'] == 9999]
                print('open up')

            else:
                buy_data = data[(data['open_pre_close_change'] < cut + 2) & (data['open_pre_close_change'] >= cut)]
                print('%s<=x<%s' % (cut, cut + CUTS))
            if buy_data.empty:
                # res_all.loc['%s~%s' % (cut, cut + CUTS), 'res'] = None
                res_some.loc['%s~%s' % (cut, cut + CUTS), 'res'] = None
                print(buy_data['open_pre_close_change'].max(), buy_data['open_pre_close_change'].min(),
                      buy_data['open_pre_close_change'].mean())
                continue
            # wool_all = sheep.wool(buy_data, raw_data, PRICEB='price_buy', PRICES=PRICES, days=sell_date)
            # if wool_all.iloc[-1, -1]:
            #     wool_all.to_csv('pct%swool_all%s~%s.csv' % (cut, start_date, end_date))
            #     print('all%s' % cut, wool_all.iloc[-1, -1])
            #     res_all.loc['%s~%s' % (cut, cut + CUTS), 'res'] = wool_all.iloc[-1, -1]
            #     res_all.loc['%s~%s' % (cut, cut + CUTS), 'n_mean'] = wool_all['n'].mean()
            #     res_all.loc['%s~%s' % (cut, cut + CUTS), 'days'] = wool_all.shape[0]
            wool_some = sheep.wool(buy_data[buy_data['pre_close'] != buy_data['pre_up_limit']], raw_data,
                                   PRICEB='price_buy', PRICES=PRICES, days=sell_date)
            if wool_some.empty:
                continue
            if wool_some.iloc[-1, -1]:
                wool_some.to_csv('pct%swool_some%s~%s-%s.csv' % (cut, start_date, end_date, sell_date))
                print('some', wool_some.iloc[-1, -1])
                res_some.loc['%s~%s' % (cut, cut + CUTS), 'res'] = wool_some.iloc[-1, -1]
                res_some.loc['%s~%s' % (cut, cut + CUTS), 'n_mean'] = wool_some['n'].mean()
                res_some.loc['%s~%s' % (cut, cut + CUTS), 'days'] = wool_some.shape[0]
            # buy_data.to_csv('pct%slimit_stock_list%s~%s.csv' % (cut, start_date, end_date))

        # res_all.to_csv('cut%s-of-all(%s,%s)%s~%s.csv' % (CUTS, PRICEB, PRICES, start_date, end_date))
        # res_some.to_csv('cut%s-of-first(%s,%s)%s~%s.csv' % (CUTS, PRICEB, PRICES, start_date, end_date))
        return res_some


if __name__ == '__main__':
    # print(dir())
    # start, end = '20180101', '20200219'
    start, end, days = '20190220', '20200220', 2
    t=idea1(start_date=start, end_date=end, limit_type='up', days=days)
    t.preprocess( CHANGE=['open', 'pre_close'])
    mlist=['open','close']+list(range(1,5))
    for model in mlist:
        print(model)
        print(t.data.shape,t.raw_data.shape)

        t.sell_model(model)
        t.segment()


