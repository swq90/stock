# 找到处于增长趋势的股票（蓝英，蓝思）
# 前n日，每一日的ma大于它两天前的均价，保证前期n内股票的上涨趋势
# 最近三日的稳定器，要求最近每一日均价占三日内均价的0.985~1.015之间，
#最后易日的均价不大于前一日均价的！%，不大于前两日均价的2%，
# 怎么保证最近三日的open-close块保持基本一致
from stock.sql.data import read_data, save_data
from stock.util import vars
from stock.subject import continu_limit as cl

FORMAT = lambda x: '%.2f' % x
start_date = '20200101'
end_date = '20201231'


def Ma(data, N):
    for label in [vars.AMOUNT, vars.VOL]:
        Z = data.groupby(vars.TS_CODE)[label].rolling(N).sum().reset_index(level=0).rename(
            columns={label: '%s%s' % (label, N)})
        data = data.merge(Z[['%s%s' % (label, N)]], left_index=True, right_index=True, how='inner')
    data['%s%s' % (vars.MA, N)] = (10 * data['%s%s' % (vars.AMOUNT, N)] / data['%s%s' % (vars.VOL, N)]).map(
        FORMAT).astype('float')
    return data


def increase(N, M):
    data = read_data('daily', start_date=start_date, end_date=end_date).merge(
        read_data('stk_limit', start_date=start_date, end_date=end_date)[
            ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
        on=['ts_code', 'trade_date'])
    data.sort_values(vars.TRADE_DATE, inplace=True)
    data[vars.MA] = (10 * data[vars.AMOUNT] / data[vars.VOL]).map(FORMAT).astype('float')

    # # data['ma5']=(10*(data.groupby(vars.TS_CODE)[vars.AMOUNT].rolling(5,min_periods=5).sum())/(data.groupby(vars.TS_CODE)[vars.VOL].rolling(5,min_periods=5).sum())).map(FORMAT).astype('float')
    data = Ma(data, N)
    data = Ma(data, M)

    Z = data.groupby(vars.TS_CODE)[vars.HIGH].rolling(10).max().reset_index(level=0).rename(
        columns={vars.HIGH: '%s10' % vars.HIGH})
    data = data.merge(Z[['%s10' % vars.HIGH]], left_index=True, right_index=True, how='inner')
    Z = data.groupby(vars.TS_CODE)[vars.LOW].rolling(60).min().reset_index(level=0).rename(
        columns={vars.LOW: '%s60' % vars.LOW})
    data = data.merge(Z[['%s60' % vars.LOW]], left_index=True, right_index=True, how='inner')
    Z = data.groupby(vars.TS_CODE)[vars.HIGH].rolling(3).max().reset_index(level=0).rename(
        columns={vars.HIGH: '%s3' % vars.HIGH})
    data = data.merge(Z[['%s3' % vars.HIGH]], left_index=True, right_index=True, how='inner')
    data['next_1_ma%s' % M] = data.groupby(vars.TS_CODE)['%s%s' % (vars.MA, M)].shift(-1)
    data['next_2_ma%s' % M] = data.groupby(vars.TS_CODE)['%s%s' % (vars.MA, M)].shift(-2)
    data['pre_ma2'] = data.groupby(vars.TS_CODE)[vars.MA].shift(2)
    # 我的方法，每日涨幅限制，三日收盘与五日均价关系
    # res=cl.process(data,[lambda x:(abs(x[vars.PCT_CHG])<1)&(x[vars.CLOSE]>x['ma5']),lambda x:abs(x[vars.PCT_CHG])<1,lambda x:(abs(x[vars.PCT_CHG]<1)&(abs(x[vars.CLOSE]/x['ma5']-1)<0.01))],st=True,next_day=0)
    # 10dhigh>60dlow*120%，最近一日low<ma5,high-ma5>ma5-low，
    # 三天内，每日均价在三日均价的0.99-1.01（0.985-1.015）
    # 最后一天十日最高价大于60日最低价*1.2，最低价小于5日均价，最高价-五日均价大于五日均价-最低价。（补充十日最高价大于三日最高价，却过滤掉部分好数据
    res = cl.process(data, [lambda x: x[vars.MA] > x['pre_ma2'], lambda x: x[vars.MA] > x['pre_ma2'],
                            lambda x: x[vars.MA] > x['pre_ma2'], lambda x: x[vars.MA] > x['pre_ma2'],
                            lambda x: x[vars.MA] > x['pre_ma2'], lambda x: x[vars.MA] > x['pre_ma2'],

                            cl.NR, cl.NR,
                            lambda x: abs(x[vars.MA] / x['next_2_ma%s' % M] - 1) < 0.015,
                            lambda x: abs(x[vars.MA] / x['next_1_ma%s' % M] - 1) < 0.015,
                            lambda x: (abs(x[vars.MA] / x['ma%s' % M] - 1) < 0.015) & (
                                    x['high10'] > (1.2 * x['low60'])) & (x[vars.LOW] < x['%s%s' % (vars.MA, N)]) & (
                                              (x[vars.HIGH] - x['%s%s' % (vars.MA, N)]) > (
                                              x['%s%s' % (vars.MA, N)] - x[vars.LOW]))], st=True, next_day=0)

    save_data(res, '横盘%sprema2.csv' % start_date)
    print(res.describe())
    # performance = pd.DataFrame()
    # performance['pct'] = pd.Series(cl.performance(res, data)).map(FORMAT)
    # save_data(performance, '横盘%s表现.csv' % start_date)
    # # data['vol5']=data.groupby(vars.TS_CODE)[vars.VOL].rolling(5).sum()
    # save_data(cl.open_cut(res), '横盘%s表现.csv' % start_date, mode='a')
    return


increase(5, 3)
