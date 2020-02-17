# 首次出现涨停(前一日非涨停），后一日
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import stock.util.stockfilter as sfilter
import stock.util.basic as basic
import stock.util.sheep as sheep

engine = create_engine('postgresql://nezha:nezha@10.0.0.5:5432/stock', echo=False)


def first_up(start_date='20200201', end_date='20200215', PRICEB='open', PRICES='close', sell_date=1, buy_date=2, CUTS=2,
             CHANGE=['open', 'pre_close'], ):

    NOTCONTAIN = sfilter.StockFilter().stock_basic(end_date, name="st|ST", market="科创板")
    raw_data = pd.read_sql_query('select * from daily where (trade_date>=%(start)s and trade_date<=%(end)s)',
                                 params={'start': start_date, 'end': end_date}, con=engine)

    stk_limit = pd.read_sql_query('select * from stk_limit where (trade_date>=%(start)s and trade_date<=%(end)s)',
                                  params={'start': start_date, 'end': end_date}, con=engine)
    raw_data.drop_duplicates(inplace=True)
    stk_limit.drop_duplicates(inplace=True)
    print(raw_data.shape,stk_limit.shape)

    raw_data = raw_data[raw_data["ts_code"].isin(NOTCONTAIN['ts_code']) == False]
    print('科创,st,out', raw_data.shape)
    raw_data = raw_data.merge(stk_limit.loc[:, ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
                              on=['ts_code', 'trade_date'])
    raw_data['open/pre_close'] = raw_data.apply(
        lambda x: 99 if x['open'] == x['up_limit'] else -99 if x['open'] == x['down_limit'] else x['open'] / x[
            'pre_close'],
        axis=1)
    if buy_date != 1:
        price_buy = basic.basic().pre_data(raw_data[['ts_code', 'trade_date', PRICEB, 'open/pre_close']],
                                           label=[PRICEB, 'open/pre_close'], new_label=['price_buy', 'xishu_pct', ],
                                           pre_days=-buy_date + 1)
        raw_data = raw_data.merge(price_buy[['ts_code', 'trade_date', 'price_buy', 'xishu_pct']],
                                  on=['ts_code', 'trade_date'])
        print(raw_data.shape)
        raw_data.dropna(inplace=True)
        print(raw_data.shape)

    # 筛选收盘涨停数据
    data = raw_data.loc[:, ['ts_code', 'trade_date', 'close', 'pre_close', 'up_limit', 'xishu_pct']]
    print(data.shape)
    data = data.merge(
        basic.basic().pre_data(data[['ts_code', 'trade_date', 'up_limit']], label=['up_limit'],
                               new_label=['pre_up_limit'])[
            ['ts_code', 'trade_date', 'pre_up_limit']], on=['ts_code', 'trade_date'])
    print(data.shape)

    data = data[data['close'] == data['up_limit']]
    # print(data.shape)
    # raw_data=raw_data[raw_data["ts_code"].isin(data['ts_code'].unique()) == True]
    # print('不在涨停df中的股票out',raw_data.shape)
    # print(raw_data['ts_code'].unique().shape)

    # buy_data = raw_data.loc[:, ['ts_code', 'trade_date', PRICES]]
    # z1=sheep.wool(data, raw_data, PRICEB='price_buy')
    # print(z1.iloc[-1, -1])

    z2 = sheep.wool(data, raw_data)
    print(z2.iloc[-1, -1])
    # t1=sheep.wool(data[data['pre_close'] != data['pre_up_limit']],
    #                  raw_data, PRICEB='price_buy')
    # print(t1.iloc[-1, -1])
    t2 = sheep.wool(data[data['pre_close'] != data['pre_up_limit']],
                    raw_data)
    print(t2.iloc[-1, -1])
    res_all = pd.DataFrame()
    res_some = pd.DataFrame()
    # data.to_csv('%s-%slimit_stock_list.csv' % (start_date, end_date))
    for cut in range(-10, 12, CUTS):

        if cut < -9:
            buy_data = data[data['xishu_pct'] == -99]
            print('open down')
        elif cut > 10:
            buy_data = data[data['xishu_pct'] == 99]
            print('open up')
        else:
            buy_data = data[(data['xishu_pct'] < (1 + 0.01 * cut)) & (data['xishu_pct'] >= (0.98 + 0.01 * cut))]
            print('%s<=x<%s' % ((0.98 + 0.01 * cut), (1 + 0.01 * cut)))
        if buy_data.empty:
            res_all.loc[cut, 'res'] = None
            res_some.loc[cut, 'res'] = None
            print(cut, buy_data['xishu_pct'].max(), buy_data['xishu_pct'].min(),
                  buy_data['xishu_pct'].mean())

            continue
        wool_all = sheep.wool(buy_data, raw_data, PRICEB='price_buy', days=sell_date)
        if wool_all.iloc[-1, -1]:
            # wool_all.to_csv('%s-%spct%swool_all.csv' % (start_date, end_date, cut))
            print('all%s' % cut, wool_all.iloc[-1, -1])
            res_all.loc[cut, 'res'] = wool_all.iloc[-1, -1]
            res_all.loc[cut, 'n_mean'] = wool_all['n'].mean()
            res_all.loc[cut, 'days'] = wool_all.shape[0]
        wool_some = sheep.wool(buy_data[buy_data['pre_close'] != buy_data['pre_up_limit']], raw_data,
                               PRICEB='price_buy', days=sell_date)
        if wool_some.iloc[-1, -1]:
            # wool_some.to_csv('%s-%spct%swool_some.csv' % (start_date, end_date, cut))
            print('some', wool_some.iloc[-1, -1])
            res_some.loc[cut, 'res'] = wool_some.iloc[-1, -1]
            res_some.loc[cut, 'n_mean'] = wool_some['n'].mean()
            res_some.loc[cut, 'days'] = wool_some.shape[0]
        # buy_data.to_csv('%s-%spct%slimit_stock_list.csv' % (start_date, end_date, cut))

    # res_all.to_csv('%s%scutofall.csv' % (start_date, end_date))
    # res_some.to_csv('%s%scutoffirst.csv' % (start_date, end_date))


first_up(start_date='20190101', end_date='20200215', PRICEB='open', PRICES='close', sell_date=1, buy_date=2,
         CUTS=20, CHANGE=['open', 'close'])
print()