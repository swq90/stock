# 连续出现n日low>ma5的数据回溯效果
import os
import datetime
import numpy as np
import pandas as pd
import tushare as ts
import matplotlib.pyplot as plt
import stock.util.basic as basic
import stock.util.sheep as sheep


# 获得基础数据

# XISHU=0.996
# PERIOD=5
# TIMES=5
# CHGPCT=[1,2]
def idea7(XISHU=0.998, UPPER=False, PERIOD=5, TIMES=5, CHGPCT=[1, 2]):
    FORMAT = lambda x: '%.4f' % x
    label = ['low_ma5']
    path = 'D:\\workgit\\stock\\util\\stockdata\\'
    # pct=list(range(-11,11))

    today = datetime.datetime.today().date()
    tool = basic.basic()
    pro = ts.pro_api()
    while (not os.path.isfile(path + str(today) + '\data.csv')) or (
            not os.path.isfile(path + str(today) + '\daily-basic.csv')) or (
            not os.path.isfile(path + str(today) + '\stock-label.csv')):
        today = today - datetime.timedelta(1)
    # 基础数据，市值信息，

    data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})[
        ['ts_code', 'trade_date', 'close', 'pct_chg', 'low', 'ma5']]
    # data=data[data['trade_date']>='20191201']
    data['lowma5'] = data.apply(lambda x: 1 if x['low'] > (XISHU * x['ma5']) else 0, axis=1)
    if UPPER:
        data['lowma5'] = data.apply(lambda x: 1 if (x['lowma5'] == 1) & (x['low'] <= (XISHU + UPPER) * x['ma5']) else 0,
                                    axis=1)

    stock_baisc = pd.read_csv(path + str(today) + '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
        ['ts_code', 'trade_date', 'turnover_rate', 'total_mv']]
    history = pd.read_csv(path + str(today) + '\history_name.csv', index_col=0, dtype={'trade_date': object})
    history['name'] = 'st'

    path = path + str(today) + 'idea7\\'
    if not os.path.isdir(path):
        os.makedirs(path)
        print(path)
    today = today.strftime('%Y%m%d')
    df = data.copy()

    df.sort_values(by='trade_date', inplace=True)
    # df.to_csv(path+'2019datalowma5.csv')
    c_times = df.groupby('ts_code')['lowma5'].rolling(PERIOD).sum()
    c_times.index = c_times.index.droplevel()
    c_times = pd.DataFrame(c_times)
    c_times.rename(columns={'lowma5': 'count'}, inplace=True)
    # count_times=count_times[count_times['%s_%s_uptimes'%(label,period)]>=up_period]
    df = df.join(c_times)

    df = df.merge(history, on=['ts_code', 'trade_date'], how='left')
    print(df.shape)
    df = df[df['name'].isna()]
    df.drop(columns='name', inplace=True)
    print(df.shape)
    df.dropna(inplace=True)
    #
    # df.to_csv(path + 'data.csv')
    if CHGPCT:
        df = df[(df['count'] >= TIMES) & (df['pct_chg'] >= CHGPCT[0]) & (df['pct_chg'] <= CHGPCT[1])][
            ['ts_code', 'trade_date']]
        if df.empty:
            return pd.DataFrame()
        else:
            df.to_csv(path + 'low%sma%stimes%s(%s-%s).csv' % (XISHU, PERIOD, TIMES, CHGPCT[0], CHGPCT[1]))

    wool = sheep.wool(df, data)
    if not wool.empty:
        wool.to_csv(
            path + 'low%s-%sma%stimes%s(%s-%s)huisuxiaoguo.csv' % (XISHU, XISHU+UPPER, PERIOD, TIMES, CHGPCT[0], CHGPCT[1]))
    df[df['trade_date'] == today][['ts_code']].to_csv(
        path + 'low%sma%stimes%s(%s-%s)%s.txt' % (XISHU, PERIOD, TIMES, CHGPCT[0], CHGPCT[1], today),index=False)
    return wool

idea7()
# 对比详情
# print(datetime.datetime.now())
# res = pd.DataFrame()
# start=0.99
#
# end=1.06
# interval=0.001
#
# for i in np.arange(start, end,interval,float):
#     print(i)
#     t = idea7(XISHU=i)
#     if t.empty:
#         continue
#     else:
#         res.loc[i, 'res'] = t.iloc[-1, -1]
#         print(t.iloc[-1,-1])
#         res.loc[i, 'avg_deals'] = t['n'].mean()
#         res.loc[i, 'dates'] = t.shape[0]
#         print(res)
# if res.empty:
#     print(res,'无数据')
# else:
#     res[['res']].plot()
#     plt.title('%s-%s2,jiange%s_upper%s'%(start,end,interval,interval))
#     plt.savefig(datetime.datetime.today().date().strftime('%Y%m%d')+'-%s-%s,jiange%s_upper%s.png'%(start,end,interval,interval))
#     plt.show()
#     res.to_csv('%s-%s,jiange%s_upper%s.csv'%(start,end,interval,interval))
#     print(datetime.datetime.now())
