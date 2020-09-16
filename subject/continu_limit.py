# 利用匿名函数,动态传参给filter
import datetime
import math
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

def red(x):
    return x[OPEN]<=x[CLOSE]
def green(x):
    return x[OPEN]>x[CLOSE]
def sth_up_limit(x,label):
    return x[label]==x[UP_LIMIT]
def sth_down_limit(x,label):
    return x[label]==x[DOWN_LIMIT]
# def sth_all(x,pct):
#     if pct>=0:
#         return (x[CLOSE]>=max(0,math.floor(pct)-2))&(x[CLOSE]<)


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


def open_cut(df, cuts_label='open/pre_close=100*(open/pre_close-1)', limit_l=[HIGH, LOW, CLOSE],
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

    for con_r in limit_r:
        for con in limit_l:
            if ((con == vars.LOW) & (con_r == vars.UP_LIMIT)) or ((con == vars.HIGH) & (con_r == vars.DOWN_LIMIT)):
                continue
            df['%s=%s' % (con, con_r)] = df.apply(lambda x: 1 if x[con] == x[con_r] else 0, axis=1)
            res = pd.concat([res, df.groupby('cate')['%s=%s' % (con, con_r)].sum()], axis=1)
    res['total'] = df.groupby('cate')[CLOSE].count()
    for col in res.columns:
        res.loc['total', col] = res[col].sum()
    for con_r in limit_r:
        for con in limit_l:
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


def detection(ts_code, end,start=7 ):
    start_date=(datetime.datetime.strptime(end_date, '%Y%m%d')-datetime.timedelta(start)).strftime('%Y%m%d')
    vector = read_data('daily', start_date=start_date, end_date=end)
    vector = vector.loc[vector['ts_code'] == ts_code].copy().sort_values(vars.TRADE_DATE)
    check_list = [ n_n,[red_line_limit, red_t_limit, ord_limit], [green_line_limit, green_t_limit, green_block]]
    #
    # for i in range(1,start):
    #     for funcs in check_list:
    #     if isinstance(funcs,list):
    #         for func in funcs:
    #             pass
    #     else:




start_date = '20150101'
end_date = '20211201'
FORMAT = lambda x: '%.4f' % x

# ts_code = '603595'
# condition = [n_n, ord_limit, red_line_limit,lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= -9) & (
#         x[PCT_CHG] < -4) & (x[HIGH] != x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '000638'
# condition = [n_n, red_line_limit, ord_limit]

ts_code = '000796'
condition = [n_n, red_t_limit,lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= 0) & (
        x[PCT_CHG] < 4) & (x[HIGH] != x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]


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
