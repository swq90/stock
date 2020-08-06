import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic


def roi2(red_line, raw_data, selling_price, cut_left=-24, cut_right=1, step=2):
    res = []
    section = list(range(cut_left, cut_right, step
                         ))
    # red_line['low_pct']=100*(red_line['low']/red_line['pre_close']-1)
    raw_data = raw_data.loc[raw_data['ts_code'].isin(red_line['ts_code'])]
    for pb in section:
        if red_line.empty:
            continue
        PB = 'PB'
        red_line.loc[:, PB] = red_line['open'] * (1 + 0.01 * pb)
        raw_data.loc[:, PB] = raw_data['open'] * (1 + 0.01 * pb)
        grass = red_line.loc[(red_line[PB] >= red_line['low'])].copy()
        if grass.empty:
            continue
        raw_data.loc[:, PB] = raw_data.apply(lambda x: x['open'] if pb >= 0 else x[PB] if x[PB] >= x['low'] else None,
                                             axis=1)

        # for ps in section:
        #     if ps not in raw_data.columns:
        #         if isinstance(ps,int):
        #             PS = 'PS'
        #             raw_data[PS] = raw_data['pre_close'] * (1 + 0.01 * ps)
        #     else:
        #         PS=ps
        #
        #     # raw_data[PS]=raw_data.apply(lambda x:x[PS] if x[PS]<=x['high'] else x['close'],axis=1)
        #     # 集合竞价中，价格低于open，卖出，否则，价格小于当天最高价卖出，否则收盘价卖出
        #     # raw_data[PS]=raw_data.apply(lambda x:x['open'] if x[PS]<=x['open'] else x['up_limit'] if x[PS]>=x['up_limit'] else x[PS] if x[PS]<=x['high'] else x['close'],axis=1)
        #
        #
        #     meat=sheep.wool2(grass,raw_data,PRICEB=PB,PRICES=PS).dropna()
        #     if meat.empty:
        #         continue
        #     res.append([pb,ps,meat.shape[0],meat.iloc[-1,-1]])
        meat = sheep.avg_yeild(grass, raw_data, PRICEB=PB, PRICES=selling_price).dropna()
        if meat.empty:
            continue
        # grass_count=grass.groupby('trade_date')['trade_date'].count().reset_index()
        print([pb, meat['pct'].mean(), meat['n'].sum()])
        res.append([pb, meat['pct'].mean(), meat['n'].sum()])

    res = pd.DataFrame(res, columns=['pb', 'pct', 'nums', ])
    return res


def func1(data, modes, day):
    '''
    给入股票数据，返回后续表现
    @param data: dataframe，股票代码，交易日期
    @param mode: 表现方式
    @return: dataframe股票后续表现
    '''
    start, end = data['trade_date'].min()
    raw_data = read_data('daily', start_date=start)
    # mode=[close,open,high,mean,low,pct_chg,low_open,high_open]
    for mode in modes:
        if mode in ['open', 'close', 'high', 'low']:


        else:
            # if mode not in ['low_open', 'high_open']:
            pre = basic().pre_data(data, label=[mode], pre_days=day)
            raw_data = raw_data.merge(pre[['ts_code', 'trade_date', 'pre_%s_%s' % (day, mode)]],
                                      on=['ts_code', 'trade_date'])
            print('%s/pre_close' % mode, raw_data['pre'])


data = pd.DataFrame()
