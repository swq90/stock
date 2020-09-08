# 首次出现涨停(前一日非涨停），后一日开盘与昨日收盘间关系，划区间回溯
import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data,save_to_sql
from stock.util.basic import basic

class idea1:

    def __init__(self, start_date=None, end_date=None, limit_type='up', days=2):
        self.start_date = start_date
        self.end_date = end_date
        self.raw_data = gls.first_limit(start_date=self.start_date, end_date=self.end_date, limit_type=limit_type,
                                        days=days)
        self.data = pd.DataFrame()
        self.date = pd.DataFrame({'trade_date': self.raw_data['trade_date'].unique()}).sort_values('trade_date')

    def sell_model(self, model, pb='open', rate=1.03):
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
            # 最高价大约开盘的x=?倍数，high卖，否则close卖
            self.raw_data.loc[:, self.PRICES] = self.raw_data.apply(
                lambda x: x['open'] if x['open'] >= (1.05 * x['pre_close']) else x['open'] * rate if x['high'] > (
                        x['open'] * rate) else x['close'], axis=1)
        if model == 4:
            # high高于open的rate倍，high卖，否则close卖
            self.raw_data.loc[:, self.PRICES] = self.raw_data.apply(
                lambda x: x['open'] * rate if x['high'] > (
                        x['open'] * rate) else x['close'], axis=1)
    def preprocess(self, CHANGE=['open', 'pre_close'], bins=None, labels=None,
                   cuts=2):
        """
        获取基础价，涨停价，
        计算当日开盘/昨日收盘比，
        """

        if bins is None:
            bins = [-100] + list(range(-10, 11, cuts)) + [100]

        self.raw_data['ma']= 10 * t.raw_data['amount'] / t.raw_data['vol']

        if CHANGE is not None:
            self.raw_data.loc[:, '%s/%s' % (CHANGE[0], CHANGE[1])] = self.raw_data.apply(
                lambda x: 99 if x[CHANGE[0]] == x['up_limit'] else -99 if x[CHANGE[0]] == x['down_limit'] else (x[CHANGE[0]] / x[
                    CHANGE[1]] - 1) * 100, axis=1)
        self.data = self.raw_data.loc[
            (self.raw_data['pre_2_is_roof'] == 0) & (self.raw_data['pre_1_is_roof'] == 1)].copy()

        self.data.loc[:, 'Categories'] = pd.cut(self.data['%s/%s' % (CHANGE[0], CHANGE[1])], bins, right=False)

    def segment(self,days=1):

        res = pd.DataFrame()
        for cut in self.data['Categories'].unique():
            df = self.data.loc[self.data['Categories'] == cut]
            if df.empty:
                res.loc[str(cut), 'roi'] = None
                continue
            res_seg = self.roi(df,days=days)
            if res_seg.empty:
                res.loc[str(cut), 'roi'] = None
                continue
            save_data(res_seg,
                      '首板部分回溯%s_%s_%s_%s_%s2.csv' % (cut, self.PRICEB, self.PRICES, self.start_date, self.end_date))
            res.loc[str(cut), 'roi'] = res_seg.iloc[-1, -1]
            res.loc[str(cut), 'n_mean'] = res_seg['n'].mean()
            res.loc[str(cut), 'days'] = res_seg.shape[0]
        save_data(res, '首板分类回溯汇总_%s_%s_%s_%s.csv' % (self.PRICEB, self.PRICES, self.start_date, self.end_date))
        return res

    def roi(self, df=None,days=1):
        if df is None:
            return sheep.wool2(self.data, self.raw_data, PRICEB=self.PRICEB, PRICES=self.PRICES,days=days)
        return sheep.wool2(df, self.raw_data, PRICEB=self.PRICEB, PRICES=self.PRICES,days=days)

        # 筛选收盘涨停数据
    def select(self,method,days=2):

        if method=='up':
            return self.data
        if method==1:
            #buy:open>=close,sell:pre_close>=open
            return  self.data.loc[(self.data['pre_open']>=self.data['pre_close'])&(self.data['pre_close']>=self.data['open'])]

        if method==2:
            #buy:open>=close,sell:pre_close<open
            return self.data.loc[
                (self.data['pre_open'] >= self.data['pre_close']) & (self.data['pre_close'] <self.data['open'])]

        if method==3:
            #buy:open<close,sell:pre_close>=open
            return self.data.loc[
                (self.data['pre_open'] < self.data['pre_close']) & (self.data['pre_close'] >= self.data['open'])]

        if method==4:
            #buy:open<close,sell:pre_close<open
            return  self.data.loc[(self.data['pre_open']<self.data['pre_close'])&(self.data['pre_close']<self.data['open'])]
        if method=='all':
            return self.raw_data

o=[-4,2]
if __name__ == '__main__':
    # print(dir())
    # start, end = '20180101', '20200219'
    # start, end, days,sell_days = '20190220', '20200224', 2,1
    # start, end, days = '20200120', '20200224', 2
    # start, end, days = '20200210', '20200220', 3
    start, end, days,sell_days = '20200101', '20201231', 3,0
    t = idea1(start_date=start, end_date=end, limit_type='up', days=days)
    print(t.raw_data.shape)
    t.raw_data=t.raw_data[(t.raw_data['pct_chg']>=-11)&(t.raw_data['pct_chg']<=11)]
    print(t.raw_data.shape)
    t.raw_data.dropna(inplace=True)
    print(t.raw_data.shape)

    # # 1.卖出当日股价较前日收盘的变化

    # t.raw_data['ma']= 10 * t.raw_data['amount'] / t.raw_data['vol']
    # t.raw_data['pct:h-l']=(t.raw_data['high']-t.raw_data['low'])
    # t.raw_data['pct:o-c']=(t.raw_data['open']-t.raw_data['close'])
    # t.raw_data['pct:h-o']=(t.raw_data['high']-t.raw_data['open'])
    #
    # print('%s个交易日' % t.raw_data['trade_date'].unique().shape[0])
    # t.data = t.raw_data.loc[
    #     (t.raw_data['pre_%s_is_roof'%days] == 0) & (t.raw_data['pre_%s_is_roof'%(days-1)] == 1)].copy()
    # df=basic().pre_data(t.data,label=['open'],new_label=['pre_open'])
    # t.data=t.data.merge(df[['ts_code','trade_date','pre_open']],on=['ts_code','trade_date'])
    # t.raw_data.dropna(inplace=True)
    # print(t.raw_data.shape)
    # pct=pd.DataFrame()
    # # for con in ['all','up']+list(range(1,5)):
    # for con in ['all','up']:
    #
    #     df = t.select(con, days=days)
    #     pct_some = pd.DataFrame()
    #     for item in ['open','close','ma','high','low','pct:h-l','pct:o-c','pct:h-o']:
    #
    #         df['%s_%s/pre_close'%(con,item)]=df[item]/df['pre_close']
    #         pct_some=pd.concat([pct_some, df['%s_%s/pre_close'%(con,item)]],axis=1)
    #     pct_some = pd.DataFrame(pct_some.describe(include='all')).reset_index()
    #     pct_some['index'] = pct_some['index'].astype('object')
    #     pct=pd.concat([pct,pct_some],axis=1)
    # save_data(pct,'不同条件价格分布%s%s.csv'%(start,end))
    # # 2.不同策略卖出回溯
    # t.preprocess(CHANGE=['open', 'pre_close'])
    # t.PRICEB='open'
    # for sell in ['close','ma','open']:
    #     t.PRICES=sell
    #     print(sell,t.roi().iloc[-1,-1])
    # save_data(t.data, '首板涨停数据%s-%s.csv'%(start,end))
    # t.PRICEB, t.PRICES = 'open', 'close'
    # save_data(t.roi(), '首板回溯%s%s%s%s' % (t.PRICEB, t.PRICES, start, end))
    # #3.
    t.preprocess(CHANGE=['open', 'pre_close'])
    t.PRICEB='open'
    t.PRICES='close'
    limit_count=pd.DataFrame(t.data.groupby('trade_date')['ts_code'].count()).reset_index()
    limit_count.columns=['trade_date','count']

    t.data=t.data.loc[(t.data['open/pre_close']<=o[1])&(t.data['open/pre_close']>=o[0])]
    # t['all_count']=limit_count
    print(t.data.shape)
    t.data.dropna(inplace=True)
    res=t.roi(days=sell_days)
    print(t.data.shape)
    res=res.merge(limit_count,on='trade_date')
    res['proportion']=res['n']/res['count']
    print(res.iloc[-3:,:])
    save_data(res,'%s-%s回溯指标.csv'%(start,end))
    save_to_sql(res,'limit_performance')

    # mlist = [ 'open','close','ma'] + list(range(1, 5))
    # summary = pd.DataFrame()
    # for model in mlist:
    #     print(model, t.data.shape, t.raw_data.shape)
    #     t.sell_model(model)
    #     df = t.segment(days=sell_days)
    #     summary[model] = df['roi']
    # save_data(summary, '首板%s回溯汇总%s%s2.csv' % (sell_days,start, end))
    # l={}
    # res=pd.DataFrame()
    # for rate in arange(1.02,1.06,0.005):
    #     t.sell_model(4,rate=rate)
    #     df=t.roi()
    #     l[rate]=df.iloc[-1,-1]
    #     print(l)
    #     df=t.segment(days=sell_days)
    #     res[rate]=df['roi']
    # save_data(res,'rate回溯汇总%s%s2.csv' % (start, end))
    #
