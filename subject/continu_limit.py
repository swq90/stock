# 利用匿名函数,动态传参给filter
import sys
import sys
import pandas as pd
import datetime
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic

# 所有条件以list[lambda1,lambda2,……，func1)

start_date = '20190101'
end_date = '20201231'
performance_table = pd.DataFrame()
OPEN, CLOSE, HIGH, LOW, PCT_CHG, PRE_CLOSE, UP_LIMIT, DOWN_LIMIT = 'open', 'close', 'high', 'low', 'pct_chg', 'pre_close', 'up_limit', 'down_limit'


def red_line_limit(x):
    return x[LOW] == x[UP_LIMIT]


def red_t_limit(x):
    return (x[OPEN] == x[CLOSE]) & (x[OPEN] == x[UP_LIMIT]) & (x[LOW] < x[CLOSE])


def ord_limit(x):
    return (x[OPEN] != x[UP_LIMIT]) & (x[CLOSE] == x[UP_LIMIT])


def up_limit(x):
    return x[CLOSE] == x[UP_LIMIT]


def red_block(x):
    return (x[CLOSE] >= x[OPEN]) & (x[LOW] > x[DOWN_LIMIT]) & (x[HIGH] < x[UP_LIMIT])


def open_up_limit(x):
    return x[OPEN] == x[UP_LIMIT]


def n_n(x):
    return (x[CLOSE] != x[UP_LIMIT]) & (x[CLOSE] != x[DOWN_LIMIT])


def green_line_limit(x):
    return x[HIGH] == x[DOWN_LIMIT]


def green_t_limit(x):
    return (x[OPEN] == x[CLOSE] == x[DOWN_LIMIT]) & (x[HIGH] > x[CLOSE])


def green_block(x):
    return
    (x[CLOSE] < x[OPEN]) & (x[LOW] > x[DOWN_LIMIT]) & (x[HIGH] < x[UP_LIMIT])


def open_down_limit(x):
    return x[OPEN] == x[DOWN_LIMIT]


def down_limit(x):
    return x[CLOSE] == x[DOWN_LIMIT]


def open_green(x):
    return (x[OPEN] < x[PRE_CLOSE]) & (x[OPEN] != x[UP_LIMIT])


def open_red(x):
    return (x[OPEN] >= x[PRE_CLOSE]) & (x[OPEN] != x[UP_LIMIT])


def open_not_limit(x):
    return (x[OPEN] != x[UP_LIMIT]) & (x[OPEN] != x[DOWN_LIMIT])


def next_performance(df, data, ):
    df['ma'] = df['amount'] / df['vol'] * 10
    res = {}
    for ps in ['high', 'low', 'ma', 'open', 'close']:
        df['%s/pre_close' % ps] = 100 * (df[ps] / df['pre_close'] - 1)
        print(df['%s/pre_close' % ps].mean())
        res['%s' % ps] = df['%s/pre_close' % ps].mean()
        # print(ps, df['%s/pre_close' % ps].describe())

    res['close_up_limit'] = df.loc[df['close'] == df['up_limit']].shape[0] / df.shape[0]

    res['close_down_limit'] = df.loc[df['close'] == df['down_limit']].shape[0] / df.shape[0]


    res['open_up_limit'] = df.loc[df['open'] == df['up_limit']].shape[0] / df.shape[0]
    res['open_down_limit'] = df.loc[df['open'] == df['down_limit']].shape[0] / df.shape[0]

    res['high_up_limit'] = df.loc[df['high'] == df['up_limit']].shape[0] / df.shape[0]
    res['low_down_limit'] = df.loc[df['low'] == df['down_limit']].shape[0] / df.shape[0]
    res['line_red'] = df.loc[df['low'] == df['up_limit']].shape[0] / df.shape[0]

    if df.loc[df['high'] == df['up_limit']].shape[0]:
        res['keep_up_limit'] = df.loc[df['close'] == df['up_limit']].shape[0] / \
                                df.loc[df['high'] == df['up_limit']].shape[0]
    res['pobanhou_pct'] = df.loc[(df['high'] == df['up_limit']) & (df['close'] != df['up_limit'])]['pct_chg'].mean()

    return res

def roi2(df, raw_data,  selling_price ,  cut_left=-24,cut_right=1, step=2       ):
    res = []
    section = list(range(cut_left, cut_right, 2))

    raw_data = raw_data.loc[raw_data['ts_code'].isin(df['ts_code'])]
    for pb in section:
        if df.empty:
            continue
        PB = 'PB'
        df.loc[:, PB] = df['open'] * (1 + 0.01 * pb)
        raw_data.loc[:, PB] = raw_data['open'] * (1 + 0.01 * pb)
        grass = df.loc[(df[PB] >= df['low'])].copy()
        if grass.empty:
            continue
        raw_data.loc[:, PB] = raw_data.apply(lambda x: x['open'] if pb >= 0 else x[PB] if x[PB] >= x['low'] else None,
                                             axis=1)


        meat = sheep.avg_yeild(grass, raw_data, PRICEB=PB, PRICES=selling_price).dropna()
        if meat.empty:
            continue

        print([pb, meat['pct'].mean(), meat['n'].sum()])
        res.append([pb, meat['pct'].mean(), meat['n'].sum()])

    res = pd.DataFrame(res, columns=['pb', 'pct', 'nums',])
    return res
def process(df, expressions):
    # print(len(expressions))
    # df.sort_values('trade_date',inplace=True )
    # print(df.iloc[0],type(df.iloc[0]))

    # df=df.loc[expressions[0]]
    # print(df)
    # for i in range(len(expressions)):
    #     if not expressions:continue
    #     ds=df.iloc[i].copy()
    #     print(ds)
    #     # print(ds['a']==ds['b'])
    #     ds[i]=expressions[i](ds)
    #     # if expressions[i](ds):
    #     #     print(ds)
    #     #     continue
    #     # else:
    #     #     return
    #     print(ds)
    ds = df.copy()
    ds.sort_values(['ts_code', 'trade_date'], inplace=True)
    for i in range(len(expressions)):
        if not expressions[i]: continue
        ds[i] = expressions[i](ds)
        ds[i] = ds.groupby('ts_code')[i].shift(len(expressions) - i)

    for i in range(len(expressions)):
        ds = ds.loc[ds[i] == True]
    print(ds.shape)
    # 过滤新股
    list_days = basic().list_days(ds, list_days=30)
    ds = ds.merge(list_days, on=['ts_code', 'trade_date'])


    ds = ds.loc[ds['up_limit'] / ds['pre_close'] >= 1.08]
    print(ds.shape)
    return ds


def reform_performance(res, label_name):
   performance_table[label_name] = pd.Series(res)

def open_cut(df,label,new_label=''):
    if not new_label:
        new_label='%s_cut'%label
        df['%s_pct'%label]=df.apply(lambda x:1000 if x[label]==x[UP_LIMIT] else -1000 if x[label]==x[DOWN_LIMIT] else 100*(x[label]/[PRE_CLOSE]-1),axis=1)
    cuts=[min(df['%s_pct'%label])-1]+list(range(-10,10))+[max(df['%s_pct'%label])+1]
    df[new_label]=pd.cut(df[label],cuts,right=False)
    print(df.groupby(by=new_label).size())



if __name__ == '__main__':
    raw_data = read_data('daily', start_date=start_date, end_date=end_date).merge(
        read_data('stk_limit', start_date=start_date, end_date=end_date)[
            ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
        on=['ts_code', 'trade_date'])
    # pre = [open_up_limit, open_down_limit, ]
    # for c in [df_limit, red_t_limit]:
    #     conditon = [n_n, df_limit, c]
    #     data = process(raw_data, conditon)
    #     res = next_performance(data, raw_data)
    #     reform_performance(res, '-'.join([x.__name__ for x in conditon]))

    # conditon = [n_n,  up_limit,red_line_limit, lambda x: (x[OPEN] / x[PRE_CLOSE] <= 1.06) &(x[OPEN] / x[PRE_CLOSE] > 1.02)& (x[PCT_CHG] >= -6)& (x[PCT_CHG] <=-2)]
    conditon = [n_n,  up_limit,up_limit, lambda x: (x[OPEN] / x[PRE_CLOSE] >=0.96) &(x[OPEN] / x[PRE_CLOSE] < 1) &(x[CLOSE] ==x[UP_LIMIT])]

    data = process(raw_data, conditon)
    open_cut(data.loc[data['close'] == data['up_limit']],'open')

    res = next_performance(data, raw_data)
    reform_performance(res, '-'.join([x.__name__ for x in conditon]))


    save_data(data, '%s%s%sdata2.csv' % (
    '002571', '~'.join([x.__name__ if 'lambda' not in x.__name__ else 'lambda' for x in conditon]), end_date),
              fp_date=True)
    save_data(performance_table, '%s%s%sperformance.csv' % ('002571', '~'.join([x.__name__ if 'lambda' not in x.__name__ else 'lambda' for x in conditon]), end_date),
              fp_date=True)

