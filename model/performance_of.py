# 首次出现涨停(前一日非涨停），后一日开盘与昨日收盘间关系，划区间回溯
import calendar as cal
from dateutil.parser import parse
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import stock.util.stockfilter as sfilter
import stock.util.basic as basic
import stock.util.sheep as sheep

engine = create_engine('postgresql://nezha:nezha@10.0.0.5:5432/stock', echo=False)

def get_data(start_date='20200201', end_date='20200215',CHANGE=['open', 'pre_close']):
    # 基础数据
    # 科创板，st数据
    NOTCONTAIN = sfilter.StockFilter().stock_basic(end_date, name="st|ST", market="科创板")
    raw_data = pd.read_sql_query('select * from daily where (trade_date>=%(start)s and trade_date<=%(end)s)',
                                 params={'start': start_date, 'end': end_date}, con=engine)

    stk_limit = pd.read_sql_query('select * from stk_limit where (trade_date>=%(start)s and trade_date<=%(end)s)',
                                  params={'start': start_date, 'end': end_date}, con=engine)
    raw_data.drop_duplicates(inplace=True)
    stk_limit.drop_duplicates(inplace=True)
    print('交易数据%s,包含%s个交易日,涨停数据%s' % (raw_data.shape, raw_data['trade_date'].unique().shape, stk_limit.shape))

    raw_data = raw_data[raw_data["ts_code"].isin(NOTCONTAIN['ts_code']) == False]
    print('科创,st,out', '交易数据%s,包含%s个交易日' % (raw_data.shape, raw_data['trade_date'].unique().shape))
    raw_data = raw_data.merge(stk_limit.loc[:, ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
                              on=['ts_code', 'trade_date'])
    raw_data['%s/%s' % (CHANGE[0], CHANGE[1])] = raw_data.apply(
        lambda x: 9999 if x['open'] == x['up_limit'] else -9999 if x['open'] == x['down_limit'] else (x[CHANGE[0]] / x[
            CHANGE[1]] - 1) * 100,
        axis=1)
    return raw_data
def first_up(raw_data,start_date='20200201', end_date='20200215', PRICEB='open', NEWPRICEB='price_buy', PRICES='close',
             sell_date=1,
             buy_date=2, CUTS=2,CHANGE=['open', 'pre_close'],
              model=None, model3_pct=1.01):


    if buy_date != 1:
        price_buy = basic.basic().pre_data(
            raw_data[['ts_code', 'trade_date', PRICEB, '%s/%s' % (CHANGE[0], CHANGE[1])]],
            label=[PRICEB, '%s/%s' % (CHANGE[0], CHANGE[1])],
            new_label=[NEWPRICEB, 'open_pre_close_change', ],
            pre_days=-buy_date + 1)
        raw_data = raw_data.merge(price_buy[['ts_code', 'trade_date', NEWPRICEB, 'open_pre_close_change']],
                                  on=['ts_code', 'trade_date'])

    # 筛选收盘涨停数据
    data = raw_data.loc[:, ['ts_code', 'trade_date', 'close', 'pre_close', 'up_limit', 'open_pre_close_change']]
    print(data.shape)
    data = data.merge(
        basic.basic().pre_data(data[['ts_code', 'trade_date', 'up_limit']], label=['up_limit'],
                               new_label=['pre_up_limit'])[
            ['ts_code', 'trade_date', 'pre_up_limit']], on=['ts_code', 'trade_date'])
    print(data.shape)
    data.dropna(inplace=True)
    print(data.shape)

    data = data[data['close'] == data['up_limit']]
    # print(data.shape)
    # raw_data=raw_data[raw_data["ts_code"].isin(data['ts_code'].unique()) == True]
    # print('不在涨停df中的股票out',raw_data.shape)
    # print(raw_data['ts_code'].unique().shape)

    # buy_data = raw_data.loc[:, ['ts_code', 'trade_date', PRICES]]
    # z1=sheep.wool(data, raw_data, PRICEB='price_buy')
    # print(z1.iloc[-1, -1])
    # 全市场，首板，涨停回溯
    # z2 = sheep.wool(data, raw_data)
    # print(z2.iloc[-1, -1])
    # # t1=sheep.wool(data[data['pre_close'] != data['pre_up_limit']],
    # #                  raw_data, PRICEB='price_buy')
    # # print(t1.iloc[-1, -1])
    # t2 = sheep.wool(data[data['pre_close'] != data['pre_up_limit']],
    #                 raw_data)
    # print(t2.iloc[-1, -1])

    # data.to_csv('%s~%slimit_stock_list.csv' % (start_date, end_date))

    # res_all = pd.DataFrame()
    if model:
        PRICES = 'price_sell'
        if model == 1:
            raw_data[PRICES] = raw_data.apply(
                lambda x: x['open'] if x['open'] >= x['pre_close'] else x['pre_close'] if x['pre_close'] <= x[
                    'high'] else x['close'],axis=1)
        if model == 2:
            raw_data[PRICES] =  10 * raw_data['amount'] / raw_data['vol']
        if model==3:
            raw_data[PRICES]=raw_data.apply(lambda x:x['open'] if x['open'] >(x['pre_close']*1.01) else x['close'],axis=1)
        if model==4:
            raw_data[PRICES]=raw_data.apply(lambda x:x['open'] if x['open']>=(1.03*x['pre_close']) else x['open']*1.01 if x['high']>(x['open']*1.01) else x['close'] ,axis=1)

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
            # wool_some.to_csv('pct%swool_some%s~%s.csv' % (cut, start_date, end_date))
            print('some', wool_some.iloc[-1, -1])
            res_some.loc['%s~%s' % (cut, cut + CUTS), 'res'] = wool_some.iloc[-1, -1]
            res_some.loc['%s~%s' % (cut, cut + CUTS), 'n_mean'] = wool_some['n'].mean()
            res_some.loc['%s~%s' % (cut, cut + CUTS), 'days'] = wool_some.shape[0]
        # buy_data.to_csv('pct%slimit_stock_list%s~%s.csv' % (cut, start_date, end_date))

    # res_all.to_csv('cut%s-of-all(%s,%s)%s~%s.csv' % (CUTS, PRICEB, PRICES, start_date, end_date))
    # res_some.to_csv('cut%s-of-first(%s,%s)%s~%s.csv' % (CUTS, PRICEB, PRICES, start_date, end_date))
    return res_some


if __name__ == '__main__':
    start,end='20191101','20200218'
    BUY_DATE=2
    raw_data=get_data(start_date=start, end_date=end, CHANGE=['open', 'pre_close'])
    ##分月回测
    res=pd.DataFrame()
    # years = [ 2019, 2020]
    #     for month in range(1, 13):
    # month = range(1, 13)
    # for year in years:
    #         day = cal.monthrange(year, month)
    #         start = str(parse('%s-%s-%s' % (year, month, 1)).date()).replace('-', '')
    #         end = str(parse('%s-%s-%s' % (year, month, day[-1])).date()).replace('-', '')
    #
    #         if start <'20191001':
    #             continue
    #         if start > '2020025':
    #             break
    #         print(start, end)
    #         z=first_up(start_date=start, end_date=end, PRICEB='open', PRICES='close', sell_date=2, buy_date=2,
    #                  CUTS=2, CHANGE=['open', 'pre_close'])
    #         res[start]=z['res']
    # res.to_csv('month-of-first-up-limit-close.csv')

    # first_up(start_date=start, end_date='20200215', PRICEB='open', PRICES='close', sell_date=1,  buy_date=2,
    #          CUTS=2, CHANGE=['open', 'pre_close'])

    # t=first_up(raw_data, PRICEB='open', PRICES='close', sell_date=2, buy_date=2,             CUTS=2)
    # t.to_csv('history-res-%s~%s.csv'% ( start,end ))
    for model in range(5):
        t=first_up(raw_data,PRICEB='open', PRICES='close', sell_date=2, buy_date=2,
                 CUTS=2, model=model)
        t.to_csv('model%s~%s~%s.csv' % (model,start, end))
        res['res%s'%model]=	t['res']
        res['n_mean%s'%model]=t['n_mean']
        res['days%s'%model]=	t['days']
    res.to_csv('model-n-%s~%s.csv' % (start, end))
    # first_up(start_date='20190101', end_date=end, PRICEB='open', PRICES='open', sell_date=2, buy_date=2,
    #          CUTS=2, CHANGE=['open', 'pre_close'])
    # first_up(start_date='20180101', end_date='20181231', PRICEB='open', PRICES='close', sell_date=2,  buy_date=2,
    #          CUTS=2, CHANGE=['open', 'pre_close'])
    # first_up(start_date='20180101', end_date='20181231', PRICEB='open', PRICES='open', sell_date=2, buy_date=2,
    #          CUTS=2, CHANGE=['open', 'pre_close'])
    pass
