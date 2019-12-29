import time
import pandas as pd
import tushare as ts
# import util.basic as basic


PRICE_COLS = ['open', 'close', 'high', 'low', 'pre_close','vol']
FORMAT=lambda x: '%.4f' % x
pro = ts.pro_api()

# if adj is not None:
#     fcts = api.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)[['trade_date', 'adj_factor']]
#     if fcts.shape[0] == 0:
#         return None
#     data = data.set_index('trade_date', drop=False).merge(fcts.set_index('trade_date'), left_index=True,
#                                                           right_index=True, how='left')
#     if 'min' in freq:
#         data = data.sort_values('trade_time', ascending=False)
#     data['adj_factor'] = data['adj_factor'].fillna(method='bfill')
#     for col in PRICE_COLS:
#         if adj == 'hfq':
#             data[col] = data[col] * data['adj_factor']
#         if adj == 'qfq':
#             data[col] = data[col] * data['adj_factor'] / float(fcts['adj_factor'][0])
#         data[col] = data[col].map(FORMAT)
#         data[col] = data[col].astype(float)
#     if adjfactor is False:
#         data = data.drop('adj_factor', axis=1)
# 前复权即就是保持现有价位不变，将以前的价格缩减，将除权前的K线向下平移，使图形吻合，保持股价走势的连续性。
#
# 　　例如，某只股票当前价格10元，在这之前曾经每10股送10股，前者复权后的价格仍是10元。
#
# 　　前复权就是在K线图上以除权后的价格为基准来测算除权前股票的市场成本价。就是把除权前的价格按现在的价格换算过来。复权后现在价格不变，以前的价格减少。
#
# 　　前复权：复权后价格＝(复权前价格-现金红利)÷(1＋流通股份变动比例)
#
# 　　前复权的优点
#
# 　　前复权是以目前价为基准复权，其意义是让你一目了然的看到成本分布情况，如相对最高、最低价，成本密集区域，以及目前股价所处的位置是高还是低。均线系统也更顺畅，利于分析。
# 前复权：复权后价格＝[(复权前价格 - 现金红利)＋配(新)
# 股价格×流通股份变动比例]÷(1＋流通股份变动比例)
#
# 复权说明
#
# 类型	算法	参数标识
# 不复权	无	空或None
# 前复权	当日收盘价 × 当日复权因子 / 最新复权因子	qfq
# 后复权	当日收盘价 × 当日复权因子	hfq
def fuqan(data,adj='qfq'):
    res=pd.DataFrame()
    start_date=data['trade_date'].min()
    end_date=data['trade_date'].max()
    i = 1
    for ts_code in data['ts_code'].unique():
        fcts = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        i+=1
        print(i)
        # if i%500==0:
        #    time.sleep(60)

        # print(fcts.shape[0],fcts.shape)
        if fcts.empty or fcts.shape[0] == 0 or len(fcts['adj_factor'].unique())==1:
            res=res.append(data[data['ts_code']==ts_code],ignore_index=True)

            continue
        # print(fcts.info())
        df = data[data['ts_code']==ts_code].merge(fcts, on=['ts_code','trade_date'], how='left')

        df['adj_factor'] = df['adj_factor'].fillna(method='bfill')
        for col in PRICE_COLS:
            if adj == 'hfq':
                if col=='vol':
                    print('hfq没写要'
                          ''
                          '补充')
                else:
                    df[col] = df[col] * df['adj_factor']
            if adj == 'qfq':
                if col=='vol':
                    df[col] = df[col] * float(fcts['adj_factor'][0]) / df['adj_factor']
                else:
                    df[col] = df[col] * df['adj_factor'] / float(fcts['adj_factor'][0])
            # df[col] = df[col].map(FORMAT)
            # df[col] = df[col].astype(float)

        res=res.append(df.drop('adj_factor',axis=1),ignore_index=True)
        # print(res.index)


    return res








# 后复权就是在K线图上以除权前的价格为基准来测算除权后股票的市场成本价。就是把除权后的价格按以前的价格换算过来。简单的说，就是保持先前的价格不变，而将以后的价格增加。
#
# 　　复权后以前的价格不变，现在的价格增加，所以为了利于分析一般推荐前复权。
#
# 　　后复权：复权后价格＝复权前价格×(1＋流通股份变动比例)-配(新)股价格×流通股份变动比例＋现金红利
#
# 　　后复权的优点
#
# 　　后复权可让你清楚该股上市以来累计涨幅，如当时买入，参与全部配送、分红，一直持有到目前的价位。
# def hfq():
#     pass
#
# df=pro.daily(ts_code='002208.sz',start_date='20190510',end_date='20190515')
#
# df2=fuqan(df)
#
# print(df2)