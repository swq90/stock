# 首次出现涨停(前一日非涨停），后一日开盘与昨日收盘间关系，划区间回溯
import pandas as pd

import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data


class idea1:

    def __init__(self, start_date=None, end_date=None, limit_type='up', days=2):
        self.start_date = start_date
        self.end_date = end_date
        self.raw_data = gls.first_limit(start_date=self.start_date, end_date=self.end_date, limit_type=limit_type,
                                        days=days)
        self.data = pd.DataFrame()
        self.date = pd.DataFrame({'trade_date': self.raw_data['trade_date'].unique()}).sort_values('trade_date')

    def sell_model(self, model, pb='open', rate=1.01):
        self.PRICEB = pb
        if isinstance(model, str):
            self.PRICES = model
            return
        else:
            self.PRICES = 'ps_%s_%s' % (model, rate)

        if model == 1:
            # 开盘大于昨收，开盘卖，昨收小于今高，昨收卖，否则收盘卖
            self.raw_data.loc[:, self.PRICES] = self.raw_data.apply(
                lambda x: x['open'] if x['open'] >= x['pre_close'] else x['pre_close'] if x['pre_close'] <= x[
                    'high'] else x['close'], axis=1)
        # if model == 2:
        #     # 当日均价卖
        #     self.raw_data.loc[:, self.PRICES] = 10 * self.raw_data['amount'] / self.raw_data['vol']
        if model == 2:
            # 开盘大于昨收的倍数，开盘卖
            self.raw_data.loc[:, self.PRICES] = self.raw_data.apply(
                lambda x: x['open'] if x['open'] > (x['pre_close'] * rate) else x['close'], axis=1)
        if model == 3:
            # 最高价大约开盘的倍数，high卖，否则close卖
            self.raw_data.loc[:, self.PRICES] = self.raw_data.apply(
                lambda x: x['open'] if x['open'] >= (1.03 * x['pre_close']) else x['open'] * rate if x['high'] > (
                        x['open'] * rate) else x['close'], axis=1)

    def preprocess(self, CHANGE=['open', 'pre_close'], bins=None, labels=None,
                   cuts=2):
        """
        获取基础价，涨停价，
        计算当日开盘/昨日收盘比，
        """
        if bins is None:
            bins = [-100] + list(range(-10, 11, cuts)) + [100]

        self.raw_data.loc[:, '%s/%s' % (CHANGE[0], CHANGE[1])] = self.raw_data.apply(
            lambda x: 99 if x['open'] == x['up_limit'] else -99 if x['open'] == x['down_limit'] else (x[CHANGE[0]] / x[
                CHANGE[1]] - 1) * 100, axis=1)
        self.data = self.raw_data.loc[
            (self.raw_data['pre_2_is_roof'] == 0) & (self.raw_data['pre_1_is_roof'] == 1)].copy()
        self.data.loc[:, 'Categories'] = pd.cut(self.data['%s/%s' % (CHANGE[0], CHANGE[1])], bins, right=False)

    def segment(self):

        res = pd.DataFrame()
        for cut in self.data['Categories'].unique():
            df = self.data.loc[self.data['Categories'] == cut]
            if df.empty:
                res.loc[str(cut), 'roi'] = None
                continue
            res_seg = self.roi(df)
            if res_seg.empty:
                res.loc[str(cut), 'roi'] = None
                continue
            save_data(res_seg,
                      '首板部分回溯%s_%s_%s_%s_%s.csv' % (cut, self.PRICEB, self.PRICES, self.start_date, self.end_date))
            res.loc[str(cut), 'roi'] = res_seg.iloc[-1, -1]
            res.loc[str(cut), 'n_mean'] = res_seg['n'].mean()
            res.loc[str(cut), 'days'] = res_seg.shape[0]
        save_data(res, '首板分类回溯汇总_%s_%s_%s_%s.csv' % (self.PRICEB, self.PRICES, self.start_date, self.end_date))
        return res

    def roi(self, df=None):
        if df is None:
            return sheep.wool(self.data, self.raw_data, PRICEB=self.PRICEB, PRICES=self.PRICES)
        return sheep.wool(df, self.raw_data, PRICEB=self.PRICEB, PRICES=self.PRICES)

        # 筛选收盘涨停数据


if __name__ == '__main__':
    # print(dir())
    # start, end = '20180101', '20200219'
    # start, end, days = '20190220', '20200220', 2
    start, end, days = '20200210', '20200220', 2

    t = idea1(start_date=start, end_date=end, limit_type='up', days=days)
    t.raw_data['ma']= 10 * t.raw_data['amount'] / t.raw_data['vol']
    print('%s个交易日' % t.raw_data['trade_date'].unique().shape[0])
    t.preprocess(CHANGE=['open', 'pre_close'])
    pct=pd.DataFrame()
    for item in ['open','close','ma','high','low']:
        t.raw_data['%s/pre_close'%item]=t.raw_data[item]/t.raw_data['pre_close']
        t.data['%s/pre_close' % item] = t.data[item] / t.data['pre_close']
        print('all',t.raw_data['%s/pre_close'%item].describe(),type(t.raw_data['%s/pre_close'%item].describe()))
        print('up',t.data['%s/pre_close'%item].describe())
        pct['%s/pre_close_all'%item]=t.raw_data['%s/pre_close'%item].describe()[:3]
        pct['%s/pre_close_up'%item]=t.data['%s/pre_close'%item].describe()[:3]
    save_data(pct,'首板后价格变化.csv'%(start,end))
    save_data(t.data, '首板涨停数据%s-%s.csv'%(start,end))
    t.PRICEB, t.PRICES = 'open', 'close'
    save_data(t.roi(), '首板回溯%s%s%s%s' % (t.PRICEB, t.PRICES, start, end))

    mlist = ['open', 'close','ma'] + list(range(1, 4))
    summary = pd.DataFrame()
    for model in mlist:
        print(model, t.data.shape, t.raw_data.shape)

        t.sell_model(model)
        df = t.segment()
        summary[model] = df['roi']
        save_data(summary, '首板回溯汇总%s%s.csv' % (start, end))
