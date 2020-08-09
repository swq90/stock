# 利用匿名函数,动态传参给filter

import pandas as pd
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic
from stock import vars

# from stock.subject import continu_list

# 所有条件以list[lambda1,lambda2,……，func1)


OPEN, CLOSE, HIGH, LOW, PCT_CHG, PRE_CLOSE, UP_LIMIT, DOWN_LIMIT = 'open', 'close', 'high', 'low', 'pct_chg', 'pre_close', 'up_limit', 'down_limit'
MA = 'ma'


def NR(x):
    # 不限制
    return True


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


def performance(df, data, LIMIT=False):
    if LIMIT:
        df = df.loc[(df[vars.HIGH] != df[UP_LIMIT]) & (df[vars.LOW] != df[DOWN_LIMIT])]
    if MA not in df.columns:
        df[MA] = df['amount'] / df['vol'] * 10
    res = {}
    for ps in [HIGH, LOW, MA, OPEN, CLOSE]:
        df['%s/pre_close' % ps] = 100 * (df[ps] / df['pre_close'] - 1)
        print(df['%s/pre_close' % ps].mean())
        res['%s' % ps] = df['%s/pre_close' % ps].mean()
        # print(ps, df['%s/pre_close' % ps].describe())

    res['close_up_limit'] = df.loc[df['close'] == df['up_limit']].shape[0] / df.shape[0]

    res['close_down_limit'] = df.loc[df['close'] == df['down_limit']].shape[0] / df.shape[0]

    res['open_up_limit'] = df.loc[df[OPEN] == df['up_limit']].shape[0] / df.shape[0]
    res['open_down_limit'] = df.loc[df[OPEN] == df['down_limit']].shape[0] / df.shape[0]

    res['high_up_limit'] = df.loc[df[HIGH] == df['up_limit']].shape[0] / df.shape[0]
    res['low_down_limit'] = df.loc[df['low'] == df['down_limit']].shape[0] / df.shape[0]
    res['line_red'] = df.loc[df['low'] == df['up_limit']].shape[0] / df.shape[0]

    if df.loc[df[HIGH] == df['up_limit']].shape[0]:
        res['keep_up_limit'] = df.loc[df['close'] == df['up_limit']].shape[0] / \
                               df.loc[df[HIGH] == df['up_limit']].shape[0]
    res['pobanhou_pct'] = df.loc[(df[HIGH] == df['up_limit']) & (df['close'] != df['up_limit'])]['pct_chg'].mean()
    print(res)
    return res


def roi2(df, raw_data, selling_price, cut_left=-24, cut_right=1, step=2):
    res = []
    section = list(range(cut_left, cut_right, 2))

    raw_data = raw_data.loc[raw_data['ts_code'].isin(df['ts_code'])]
    for pb in section:
        if df.empty:
            continue
        PB = 'PB'
        df.loc[:, PB] = df[OPEN] * (1 + 0.01 * pb)
        raw_data.loc[:, PB] = raw_data[OPEN] * (1 + 0.01 * pb)
        grass = df.loc[(df[PB] >= df['low'])].copy()
        if grass.empty:
            continue
        raw_data.loc[:, PB] = raw_data.apply(lambda x: x[OPEN] if pb >= 0 else x[PB] if x[PB] >= x['low'] else None,
                                             axis=1)

        meat = sheep.avg_yeild(grass, raw_data, PRICEB=PB, PRICES=selling_price).dropna()
        if meat.empty:
            continue

        print([pb, meat['pct'].mean(), meat['n'].sum()])
        res.append([pb, meat['pct'].mean(), meat['n'].sum()])

    res = pd.DataFrame(res, columns=['pb', 'pct', 'nums', ])
    return res


def process(df, expressions, st=False, next_day=1, new=False):
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
        ds[i] = ds.groupby('ts_code')[i].shift(len(expressions) - i - 1 + next_day)
    # save_data(ds, '标注信息表2.csv')
    for e in range(len(expressions)):
        if not expressions[e]: continue
        ds = ds.loc[ds[e] == True]
        print(ds.shape)
    # 过滤新股
    if not new:
        list_days = basic().list_days(ds, list_days=30)
        ds = ds.merge(list_days, on=['ts_code', 'trade_date'])
    if not st:
        ds = ds.loc[ds['up_limit'] / ds['pre_close'] >= 1.08]
    print(ds.shape)
    return ds


def reform_performance(res, label_name):
    performance_table[label_name] = pd.Series(res).map(FORMAT)


def open_cut(df, cuts_label='open/pre_close=100*(open/pre_close-1)', limit_l=[HIGH, CLOSE, LOW],
             limit_r=[UP_LIMIT, DOWN_LIMIT]):
    res = pd.DataFrame()
    if cuts_label not in df.columns:

        if cuts_label.split('=')[0] in df.columns:
            cuts_label = cuts_label.split('=')[0]
        else:
            df[cuts_label.split('=')[0]] = df.eval(cuts_label.split('=')[1])
            cuts_label = cuts_label.split('=')[0]
    df['cate'] = pd.cut(
        df.apply(lambda x: 99 if x[OPEN] == x[UP_LIMIT] else -99 if x[OPEN] == x[DOWN_LIMIT] else x[cuts_label],
                 axis=1), [-99] + list(range(-10, 11, 2)) + [100], right=False)

    for con in limit_l:
        for con_r in limit_r:
            if ((con == vars.LOW) & (con_r == vars.UP_LIMIT)) or ((con == vars.HIGH) & (con_r == vars.DOWN_LIMIT)):
                continue
            df['%s=%s' % (con, con_r)] = df.apply(lambda x: 1 if x[con] == x[con_r] else 0, axis=1)
            res = pd.concat([res, df.groupby('cate')['%s=%s' % (con, con_r)].sum()], axis=1)
    res['total'] = df.groupby('cate')[CLOSE].count()
    for col in res.columns:
        res.loc['total', col] = res[col].sum()
    for con in limit_l:
        for con_r in limit_r:
            if ((con == vars.LOW) & (con_r == vars.UP_LIMIT)) or ((con == vars.HIGH) & (con_r == vars.DOWN_LIMIT)):
                continue
            res['%s=%s:total' % (con, con_r)] = (res['%s=%s' % (con, con_r)] / res['total']).map(FORMAT)
    # if not new_label:
    #     new_label='%s_cut'%label
    #     df['%s_pct'%label]=df.apply(lambda x:1000 if x[label]==x[UP_LIMIT] else -1000 if x[label]==x[DOWN_LIMIT] else 100*(x[label]/[PRE_CLOSE]-1),axis=1)
    # cuts=[min(df['%s_pct'%label])-1]+list(range(-10,10))+[max(df['%s_pct'%label])+1]
    # df[new_label]=pd.cut(df[label],cuts,right=False)
    # print(df.groupby(by=new_label).size())
    res.index.astype('object')
    res.dropna(how='all', inplace=True)

    return res


def detection(ts_code, start, end):
    vector = read_data('daily', start_date=start, end_date=end)
    vector = vector.loc[vector['ts_code'] == ts_code].copy().sort_values(vars.TRADE_DATE)
    check_list = {1: [red_line_limit, green_line_limit], 2: [red_t_limit, green_t_limit], 3: [ord_limit],
                  4: [red_block, green_block]}


start_date = '20150101'
end_date = '20211201'
FORMAT = lambda x: '%.4f' % x
# ts_code = '300209'
# conditon = [n_n, up_limit, red_t_limit,
#             lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[OPEN] != x[UP_LIMIT]) & (x[PCT_CHG] >= -5) & (
#                         x[PCT_CHG] <= 0)]
# ts_code='600075'
# condition=[n_n, up_limit, red_t_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1) & (x[HIGH]== x[UP_LIMIT])]
# ts_code='6030693'
# condition=[n_n, up_limit, up_limit, up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1) & (x[CLOSE]== x[UP_LIMIT])& (x[OPEN]!= x[UP_LIMIT])]
# condition=[n_n,ord_limit,ord_limit,ord_limit,ord_limit]
# ts_code='300117'
# condition=[n_n, up_limit,up_limit, red_t_limit]
# ts_code='300464'
# condition=[n_n, up_limit, up_limit,  up_limit,  up_limit,  red_line_limit,red_t_limit]
# ts_code='000698'
# condition=[n_n, up_limit,red_line_limit]
# ts_code='000796'
# condition=[red_line_limit,  red_line_limit,red_line_limit,lambda x: (x[CLOSE]  != x[UP_LIMIT]) ,lambda x:x[CLOSE]==x[DOWN_LIMIT]]
# ts_code = '002614'
# condition = [lambda x: (x[PCT_CHG] > 9) & (x[CLOSE] != x[UP_LIMIT]),
#              lambda x: (x[PCT_CHG] < -9) & (x[CLOSE] != x[DOWN_LIMIT])]
# ts_code='002613'
# condition=[n_n, up_limit,up_limit,up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1.02) & (x[OPEN] != x[UP_LIMIT]) & (x[PCT_CHG] >= -8) & (
#                         x[PCT_CHG] <= 0)& (x[LOW] != x[DOWN_LIMIT]),up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE]< 0.98)]

# ts_code='300301-2'
# condition=[n_n, red_line_limit,red_line_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] <= 1.05) &(x[OPEN] / x[PRE_CLOSE] >= 1)  & (x[PCT_CHG] >= -6) & (
#                         x[PCT_CHG] <= -3)]

# ts_code='000573-2'
# condition=[n_n, ord_limit,red_t_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] <= 1.05) &(x[OPEN] / x[PRE_CLOSE] >= 1)  & (x[PCT_CHG] >= -6) & (
#                         x[PCT_CHG] <=-3)]
# ts_code='600961'
# condition=[n_n, up_limit, up_limit,red_t_limit]
# ts_code='000698'
# condition=[n_n, up_limit,red_line_limit,red_line_limit]
# ts_code='603633,303178'
# condition=[n_n, up_limit,red_line_limit]
# ts_code = '300178'
# condition = [up_limit, red_t_limit,
#              lambda x: (x[OPEN] != x[UP_LIMIT]) & (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[PCT_CHG] >= 4) & (
#                      x[PCT_CHG] <= 7)]
# ts_code='300084'
# condition=[n_n,  red_line_limit,  red_line_limit]
# ts_code='300052'
# condition=[n_n,  ord_limit,  red_t_limit]
# ts_code='603690'
# condition=[n_n,  ord_limit,  red_line_limit]
# ts_code='603020'
# condition=[n_n,  ord_limit,  lambda x:(x[OPEN] ==x[UP_LIMIT] ) & (x[PCT_CHG] >= 6) & (x[CLOSE]!=x[UP_LIMIT])]
# ts_code='300148'
# condition=[n_n,  up_limit,red_line_limit,  lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[CLOSE]==x[UP_LIMIT])]
# ts_code='601615'
# condition=[n_n,  lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[HIGH]==x[UP_LIMIT]) & (x[CLOSE]!=x[UP_LIMIT])]
# ts_code='002960'
# condition=[n_n,  ord_limit,ord_limit]
# ts_code='300842'
# condition=[ord_limit,ord_limit,red_t_limit,lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[OPEN] / x[PRE_CLOSE] >1) & (x[PCT_CHG] >= -7) & (
#                      x[PCT_CHG] <=-4)]
# ts_code='000011'
# condition=[n_n,up_limit,up_limit,red_line_limit,red_t_limit]
# ts_code='600127'
# condition=[n_n,ord_limit,ord_limit]
# ts_code='000026a'
# condition=[up_limit,red_line_limit or red_t_limit]
# ts_code='300556'
# condition=[n_n,up_limit,red_line_limit,lambda x:(x[HIGH]==x[UP_LIMIT])& (x[OPEN] / x[PRE_CLOSE] >1.03) & (x[OPEN] / x[PRE_CLOSE] <1.08)& (x[PCT_CHG] >= 1) & (
#                      x[PCT_CHG] <=8)]
# ts_code='002208'
# condition=[n_n,ord_limit,red_line_limit,lambda x:(x[vars.HIGH]!=x[vars.UP_LIMIT])& (x[OPEN] / x[PRE_CLOSE] >1.05) & (x[PCT_CHG] >= -5) & (
#                      x[PCT_CHG] <-1)]
#
# ts_code='300183'
# condition=[up_limit,red_t_limit,lambda x:(x[vars.OPEN]==x[vars.UP_LIMIT]) & (x[PCT_CHG] >0) & (
#                      x[PCT_CHG] <5)]

# ts_code='002751'
# condition=[n_n,ord_limit,red_line_limit,lambda x:(x[OPEN] / x[PRE_CLOSE] >1) & (x[OPEN] / x[PRE_CLOSE] <1.05)&(x[PCT_CHG] >-5) & (
#                      x[PCT_CHG] <0)]
# ts_code='300779'
# condition=[n_n,ord_limit,red_line_limit,ord_limit]
# ts_code='600697'
# condition=[n_n,red_line_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >=1.02) &(x[OPEN] / x[PRE_CLOSE] <=1.05) &(x[PCT_CHG] >= -1) & (
#                      x[PCT_CHG] <2)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code='002285'
# condition=[ord_limit,red_line_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >=1) &(x[OPEN] / x[PRE_CLOSE] <=1.03) &(x[PCT_CHG] >= 2) & (
#                      x[PCT_CHG] <5)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# condition=[up_limit,NR,lambda x: (x[OPEN] / x[PRE_CLOSE] >=1) &(x[OPEN] / x[PRE_CLOSE] <=1.03) &(x[PCT_CHG] >= 2) & (
#                      x[PCT_CHG] <5)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code=continu_list.ts_code
# condition=continu_list.condition

# condition=[n_n,
#
#            up_limit,red_line_limit]
# ts_code='600267'
# condition=[n_n,red_line_limit,red_line_limit,red_t_limit]

# ts_code='test000026a'
# # condition=[up_limit,red_line_limit,NR]
# condition=[up_limit,lambda x:(x[OPEN]==x[CLOSE])&(x[OPEN]==x[UP_LIMIT]),NR]
# ts_code='26nordt--open'
# condition=[n_n,ord_limit,red_t_limit,lambda x:  (x[OPEN] / x[PRE_CLOSE] >=1) &(x[OPEN] / x[PRE_CLOSE] <=1.03) &(x[PCT_CHG] >= 2) & (
#                      x[PCT_CHG] <5)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]

# ts_code='test'
# condition=[n_n,ord_limit,red_t_limit]
# ts_code='601038'
# condition=[n_n,ord_limit,ord_limit,ord_limit]
# ts_code = '000710'
# condition = [n_n, ord_limit, ord_limit,
#              lambda x: (x[OPEN] <= x[CLOSE]) & (x[HIGH] == x[UP_LIMIT]) & (x[PCT_CHG] >= 1) & (
#                      x[PCT_CHG] < 4)]
# ts_code = '300208'
#
# condition = [n_n, ord_limit, ord_limit,
#              lambda x: (x[OPEN] > x[CLOSE])  & (x[PCT_CHG] >= -4) & (
#                      x[PCT_CHG] < 0)]
# ts_code = '000822A'
#
# condition = [n_n, ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 3) & (
#                      x[PCT_CHG] < 8)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '300307'
# #
# condition = [n_n, ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 0) & (
#                      x[PCT_CHG] < 3)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '300086'
# #
# condition = [ red_line_limit,red_line_limit,
#              lambda x: (x[CLOSE]==x[DOWN_LIMIT])]
# ts_code = '002910'
# condition = [n_n, ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 0) & (
#                      x[PCT_CHG] < 5)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '002163'
# condition = [n_n, ord_limit,ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 3) & (
#                      x[PCT_CHG] < 8)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '002946'
# condition = [n_n, ord_limit,ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 4) & (
#                      x[PCT_CHG] < 9)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '300183'
# condition = [n_n, ord_limit,
#              lambda x: (x[OPEN] >x[CLOSE]) ,lambda x: (x[LOW] ==x[DOWN_LIMIT])&(x[CLOSE]!=x[DOWN_LIMIT])]
# ts_code = '002315'
# condition = [n_n, ord_limit,ord_limit,
#              lambda x: (x[OPEN] > x[CLOSE])  & (x[PCT_CHG] >= -5) & (
#                      x[PCT_CHG] < 0)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '603898-222'
# condition = [red_line_limit,red_t_limit,
#              lambda x: (x[CLOSE] > x[OPEN])  & (x[PCT_CHG] >= 5) & (x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '600187'
# condition = [n_n, ord_limit,ord_limit,ord_limit]


# ts_code = '002009'
# condition = [n_n, ord_limit,red_line_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 1) & (
#                      x[PCT_CHG] < 6)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]

# ts_code = '002614C-O'
# condition = [n_n,
#              lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1.01)  & (x[OPEN] / x[PRE_CLOSE] <= 1.02)  &(x[PCT_CHG] >= 5) & (
#                      x[PCT_CHG] < 6)&(x[HIGH]/x[PRE_CLOSE]<1.07),
#              lambda x:(x[OPEN] / x[PRE_CLOSE] >= 1.03)  & (x[OPEN] / x[PRE_CLOSE] <= 1.04) & (x[PCT_CHG] >= 4) & (
#                      x[PCT_CHG] < 5)&(x[LOW]/x[PRE_CLOSE]>1.02)]

# ts_code = '002151'
# condition = [up_limit,red_line_limit, red_line_limit, red_line_limit, ord_limit]
ts_code = '600351'
condition = [red_line_limit,lambda x: (x[OPEN]<x[CLOSE])& (x[PCT_CHG] >= 0) & (
                     x[PCT_CHG] < 5)
             ]

performance_table = pd.DataFrame()

if __name__ == '__main__':
    print(ts_code)
    raw_data = read_data('daily', start_date=start_date, end_date=end_date).merge(
        read_data('stk_limit', start_date=start_date, end_date=end_date)[
            ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
        on=['ts_code', 'trade_date'])
    print(raw_data.shape[0])

    data = process(raw_data, condition, next_day=1)
    print(data.shape)
    # open_cut(data.loc[data['close'] == data['up_limit']],OPEN)

    res = performance(data, raw_data)
    reform_performance(res, '-'.join([x.__name__ if x != None else '' for x in condition]))
    res = performance(data, raw_data, LIMIT=True)
    reform_performance(res, 'limit' + '-'.join([x.__name__ if x != None else '' for x in condition]))
    save_data(data, '%s%s%sdata.csv' % (
        ts_code,
        '~'.join(['_' if x == None else x.__name__ if 'lambda' not in x.__name__ else 'special' for x in condition]),
        start_date),
              fp_date=True)
    save_data(performance_table, '%s%s%sperformance.csv' % (
        ts_code, '~'.join([x.__name__ if 'lambda' not in x.__name__ else 'lambda' for x in condition]), start_date),
              fp_date=True)
    save_data(open_cut(data), '%s%s%sperformance.csv' % (
        ts_code, '~'.join([x.__name__ if 'lambda' not in x.__name__ else 'lambda' for x in condition]), start_date),
              fp_date=True, mode='a')
    print(data.shape)
