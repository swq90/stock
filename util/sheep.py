import pandas as pd
import tushare as ts
import stock.util.basic as basic

ts.set_token('006b49622d70edc237ab01340dc210db15d9580c59b40d028e34e015')
pro = ts.pro_api()
tool = basic.basic()

#
# # CONTAIN = stockfilter.StockFilter().stock_basic('20191127', total_mv=[0,5000000000])
# # print(CONTAIN.shape)
# ma = [1, 5]
# period = 5
# up_cal = 240
# temp = 0
# pre = 5
# # labels = ['low_ma5', 'low']
#
labels = ['low_ma5', 'low', 'ma1', 'ma5']


def grass(data, period=5, up_cal=240, items=0):
    # up 满足上涨条件的数据
    score = pd.DataFrame()
    df = sheep(data)

    up = tool.up_info(data, days=period, up_range=0.5, pct=0)
    # print('up', up.shape)

    if up_cal & (not items):
        up = up[up["trade_date"].isin(tool.tradeCal(cal=up_cal))]
    elif items:
        up = up.sort_values('trade_date', ascending=False).head(items)
    # print('up', up.shape)

    # for label in labels:
    #     df = data[['ts_code', 'trade_date', label]]
    #     print('df', df.shape)
    #
    #     # df.loc[:,['count']] = 0
    #     for i in range(1, pre + 1):
    #         df = tool.pre_label(df, label=label, days=i)
    #
    #         if label == 'low_ma5':
    #             if i == 1:
    #                 # .copy(),避免引用warn
    #                 df['count_%s' % label] = df[label].copy()
    #                 print(df)
    #             df['count_%s' % label] = df.apply(
    #                     lambda x: x['count_%s' % label] + x['pre_%s_%s' % (i, label)], axis=1)
    #             # df['count_%s'%label]=df['count_%s'%label].astype('int')
    #
    #             # else:
    #             #     if i == 1:
    #             #         df['count_%s' % label] = df.apply(
    #             #             lambda x: 1 + x['count_%s' % label] if x[label] >= x['pre_%s_%s' % (i, label)] else x[
    #             #                 'count_%s' % label], axis=1)
    #             #
    #             #     else:
    #             #         df['count_%s' % label] = df.apply(
    #             #             lambda x: 1 + x['count_%s' % label] if x['pre_%s_%s' % ((i - 1), label)] >= x[
    #             #                 'pre_%s_%s' % (i, label)] else x[
    #             #                 'count_%s' % label], axis=1)
    #             print(df)
    #
    #         else:
    #             if i == 1:
    #                 df['count_%s' % label] = df.apply(
    #                     lambda x: 1 if x[label] >= x['pre_%s_%s' % (i, label)] else 0, axis=1)
    #
    #             else:
    #                 df['count_%s' % label] = df.apply(
    #                     lambda x: 1 + x['count_%s' % label] if x['pre_%s_%s' % ((i - 1), label)] >= x[
    #                         'pre_%s_%s' % (i, label)] else x[
    #                         'count_%s' % label], axis=1)

    up_pre = up[['ts_code', 'pre_n_date']]
    up_pre.rename(columns={'pre_n_date': 'trade_date'}, inplace=True)
    up_pre = up_pre.merge(df, on=['ts_code', 'trade_date'])
    df = df[df["trade_date"] >= up_pre['trade_date'].min()]

    # up_pre_score=up_pre.groupby(by=)

    # df.to_csv(filename + 'all%sindays.csv' % label)
    # print('df', df.shape)
    # print('uppre', up_pre.shape)
    # up_pre.to_csv(filename + 'up_pre%s.csv' % label)
    for label in labels:
        up_pct = pd.DataFrame(up_pre.groupby(by='count_%s' % label).size(), columns=['count_%s' % label])
        up_pct['pct'] = up_pct['count_%s' % label] / up_pct['count_%s' % label].sum()
        df_pct = pd.DataFrame(df.groupby(by='count_%s' % label).size(), columns=['count_%s' % label])
        df_pct['pct'] = df_pct['count_%s' % label] / df_pct['count_%s' % label].sum()
        #
        # print('up:%s>pre_%s' % (label, label), up_pct)
        # print('all:%s>pre_%s' % (label, label), df_pct)
        up_pct.rename(columns={'pct': 'up_pct'}, inplace=True)
        df_pct.rename(columns={'pct': 'all_pct'}, inplace=True)
        g = up_pct[['up_pct']].merge(df_pct[['all_pct']], left_index=True, right_index=True)
        # g['div'] = g['up_pct'] / g['all_pct']
        g['div'] = g['up_pct'] / g['all_pct'] - 1
        #
        # g['%sscore' % label] = g.apply(lambda x: 10 * x['div'], axis=1) / g['div'].sum()
        g['%sscore' % label] = (g['div'].max() // 0.5 + 5) * g['div'] / g['div'].max()

        # print(g)
        if score.empty:
            score = g[['%sscore' % label]]
        else:
            score = score.merge(g[['%sscore' % label]], left_index=True, right_index=True)
    return score


def sheep(data, pre=5, labels=labels):
    res = pd.DataFrame()
    for label in labels:
        df = data[['ts_code', 'trade_date', label]].copy()
        # df['count_%s' % label] = 0
        if label == 'low_ma5':
            df.sort_values(by='trade_date', inplace=True)
            z = df.groupby('ts_code')[label].rolling(pre).sum()
            z.index = z.index.droplevel()
            z = pd.DataFrame(z)
            z.rename(columns={label: 'count_%s' % label}, inplace=True)

            df = df.join(z)
        else:
            df = tool.up_times(df, period=5, label=label)
            # df=df = tool.pre_label(df, label=label, days=pre)
            # for i in range(1, pre + 1):
            #     df = tool.pre_label(df, label=label, days=i)
            #     if i == 1:
            #         df['count_%s' % label] = df.apply(
            #             lambda x: 1 if x[label] >= x['pre_%s_%s' % (i, label)] else 0, axis=1)
            #
            #     else:
            #         df['count_%s' % label] = df.apply(
            #             lambda x: 1 + x['count_%s' % label] if x['pre_%s_%s' % ((i - 1), label)] >= x[
            #                 'pre_%s_%s' % (i, label)] else x[
            #                 'count_%s' % label], axis=1)
            # print('df', df.shape)

        df = df.dropna()

        if res.empty:
            res = df[['ts_code', 'trade_date', 'count_%s' % label]]
        else:
            res = res.merge(df[['ts_code', 'trade_date', 'count_%s' % label]], on=['ts_code', 'trade_date'])
        res.loc[:, 'count_%s' % label] = res['count_%s' % label].astype('int')
    res = res.dropna()

    return res


def marks(data, score, labels=labels):
    df = pd.DataFrame()
    data['score'] = 0
    for label in labels:
        data['score'] = data.apply(lambda x: x['score'] + score.loc[x['count_%s' % label]]['%sscore' % label], axis=1)
    # for day in data['trade_date'].unique():
    #     df = pd.concat([data[data['trade_date'] == day].sort_values(by='trade_date', ascending=False).head(50), df])
    # #     df = pd.concat([data[data['trade_date'] == day].sort_values(by='score', ascending=False).head(30), df])
    # #
    # # data.to_csv(FILENAME + "all_marks.csv")
    # # df = df[df['score'] >= 10]
    # df.to_csv(filename + "50ofall_marks.csv")
    # data.to_csv(filename + "all_marks.csv")

    return data


def wool(stock, data, days=1, PRICEB="close", PRICES="close", pct=11):
    limit_up = stock[['ts_code', 'trade_date']].sort_values(by="trade_date").reset_index(drop=True)
    # for trade_date in  data["trade_date"].unique():
    #     z=pro.limit_list(trade_date=trade_date, limit_type='U', fields='ts_code,trade_date,pct_chg')
    #     h=z[z["pct_chg"]>=10]
    #     d=limit_up[limit_up["trade_date"]==trade_date]
    # print(limit_up)

    buy_data = limit_up.merge(data[['ts_code', 'trade_date', PRICEB]], on=['ts_code', 'trade_date'])[
        ['ts_code', 'trade_date', PRICEB]]

    buy_data.columns = ['ts_code', 'buy_date', "buy_price"]
    pre_date = tool.pre_date(data[["trade_date"]], days=days)
    sell_data = data[['ts_code', 'trade_date', PRICES]].merge(pre_date, on='trade_date')
    sell_data.rename(columns={"trade_date": "sell_date", "pre_%s_date" % days: "buy_date", PRICES: 'sell_price'},
                     inplace=True)
    sell_data = sell_data.merge(buy_data, on=['ts_code', 'buy_date'])
    # sell_data.to_csv(str(datetime.datetime.today()).replace(':','').replace(' ','')[:20]+'selldata.csv')
    print('每日平均交易量', sell_data.groupby(by='buy_date')['ts_code'].size().mean())

    sell_data['pct'] = (sell_data['sell_price'] / sell_data['buy_price'])
    sell_cut = pd.DataFrame()
    # sell_data.to_csv('pctwool1.csv')

    sell_cut['pct'] = sell_data.groupby(by='sell_date')['pct'].mean()
    sell_cut['n'] = sell_data.groupby(by='sell_date')['sell_date'].size()

    # sell_cut = pd.DataFrame(sell_cut)
    sell_cut['all_pct'] = sell_cut['pct'].cumprod()
    # sell_cut.to_csv('pctwool2.csv')
    # print(sell_cut)

    return sell_cut


def wool2(stock_list, data, days=1, PRICEB="close", PRICES="close"):
    limit_up = stock_list[['ts_code', 'trade_date']].sort_values(by="trade_date").reset_index(drop=True)
    # for trade_date in  data["trade_date"].unique():
    #     z=pro.limit_list(trade_date=trade_date, limit_type='U', fields='ts_code,trade_date,pct_chg')
    #     h=z[z["pct_chg"]>=10]
    #     d=limit_up[limit_up["trade_date"]==trade_date]
    # print(limit_up)

    df = limit_up.merge(data[['ts_code', 'trade_date', PRICEB]], on=['ts_code', 'trade_date'])[
        ['ts_code', 'trade_date', PRICEB]]
    df.rename(columns={PRICEB: 'buy_price'}, inplace=True)
    df = df.merge(
        tool.pre_data(data[['ts_code', 'trade_date', PRICEB]], label=[PRICEB], days=-1, new_label=['sell_price']))

    print('每日平均买入量', df.groupby(by='trade_date')['ts_code'].size().mean())

    df['pct'] = (df['sell_price'] / df['buy_price'])
    sell_cut = pd.DataFrame()
    sell_cut['pct'] = df.groupby(by='trade_date')['pct'].mean()
    sell_cut['n'] = df.groupby(by='trade_date')['trade_date'].size()

    # sell_cut = pd.DataFrame(sell_cut)
    sell_cut['all_pct'] = sell_cut['pct'].cumprod()

    return sell_cut
# # score=pd.DataFrame()
#
# path = os.getcwd() + '\\data\\'
# if not os.path.isdir(path):
#     os.mkdir('data')
#     print(path)
# filename = path + str(datetime.datetime.today())[:10] + '-'
# score = pd.DataFrame()
# data = pd.DataFrame()
# daily_basic = pd.DataFrame()
# stock_label = pd.DataFrame()
# stock_score = pd.DataFrame()
# stock_marks = pd.DataFrame()
#
# if os.path.isfile(filename + 'data.csv'):
#     data = pd.read_csv(filename + 'data.csv', index_col=0, dtype={'trade_date': object})
#     # data['trade_date']=data['trade_date'].astype('str')
# else:
#     data = tool.trade_daily(cal=up_cal + temp).reset_index(drop=True)
#     data = data.merge(tool.get_all_ma(data, ma=ma, dis_pct=False), on=['ts_code', 'trade_date'])
#     # 要修改Lget_all_ma 返回只保留ma，code，date，其他删除
#     data['low_ma5'] = data.apply(lambda x: 1 if x['low'] > x['ma5'] else 0, axis=1)
#     data.to_csv(filename + 'data.csv')
#
# print("基础数据")
#
# if os.path.isfile(filename + 'score.csv'):
#     score = pd.read_csv(filename + 'score.csv', index_col=0, dtype=np.float64)
# else:
#     score = grass(data)
#     score.to_csv(filename + 'score.csv')
# #     调用方法获得得分表
# print('对应分数')
#
# if os.path.isfile(filename + 'stock-label.csv'):
#     stock_label = pd.read_csv(filename + 'stock-label.csv', index_col=0, dtype={'trade_date': object})
# else:
#     stock_label = sheep(data)
#     stock_label.to_csv(filename + 'stock-label.csv')
# print('各项满足情况')
#
# if os.path.isfile(filename + 'stock_marks.csv'):
#     stock_marks = pd.read_csv(filename + 'stock_marks.csv', index_col=0, dtype={'trade_date': object})
# else:
#     stock_marks = marks(stock_label, score)
#     stock_marks.to_csv(filename + 'stock_marks.csv')
# print('marks', stock_marks.shape)
#
# if os.path.isfile(filename + 'daily-basic.csv'):
#     daily_basic = pd.read_csv(filename + 'daily-basic.csv', index_col=0, dtype={'trade_date': object})
# else:
#     for i in data['trade_date'].unique():
#         daily_basic = pd.concat([pro.daily_basic(trade_date=i), daily_basic], ignore_index=True)
#     for key in ["total_share", "float_share", "free_share", "total_mv", "circ_mv"]:
#         daily_basic[key] = daily_basic[key] / 10000
#     daily_basic.to_csv(filename + 'daily-basic.csv')
#
# run_time = datetime.datetime.today()
# FILENAME = path + str(run_time).replace(":", "-").replace(' ', '-')[:19] + '-'
#
# # if os.path.isfile(filename+'stock-score.csv'):
# #     stock_score = pd.read_csv(filename + 'stock-score.csv', index_col=0)
# # else:
# #     stock_score = marks(stock_label,score)
# #     stock_score.to_csv(filename + 'stock-score.csv')
#
# stock_marks = stock_marks[stock_marks['score'] >= 10]
# print('marks1', stock_marks.shape)
# stock_need = data[(data['close'] >= (0.97 * data['pre_close'])) & (data['close'] <= (1.03 * data['pre_close'])) & (
#         abs(data['open'] - data['close']) <= (0.04 * data['pre_close']))]
# # stock_need=stock_need[stock_need['close']<(1.1*stock_need['pre_close'])][['ts_code','trade_date']]
# # print(stock_need.info())
# # print(stock_marks.info())
# stock_marks = stock_marks.merge(stock_need[['ts_code', 'trade_date']], on=['ts_code', 'trade_date'])
# print('marks2', stock_marks.shape)
#
# # data_m = data[((data["low"] == data["high"])) == False]
# # stock_marks.merge(data_m[['ts_code','trade_date']],on=['ts_code', 'trade_date']).to_csv(filename+'50ofall.csv')
# # 保存当天前五十
# print('marks3', stock_marks.shape)
# # mv_bins=[]
# mv_bins = list(range(0, 101, 20)) + [150, 200, 400, 30000]
# if mv_bins:
#     print(mv_bins)
#     for i in range(len(mv_bins) - 1):
#         CONTAIN = daily_basic[(daily_basic['total_mv'] >= mv_bins[i]) & (daily_basic['total_mv'] < mv_bins[i + 1])]
#         print((mv_bins[i], mv_bins[i + 1]))
#         stock_data1 = stock_marks.merge(CONTAIN, on=['ts_code', 'trade_date'])
#         df = pd.DataFrame()
#         for day in stock_data1['trade_date'].unique():
#             # df=pd.concat([data[data['trade_date']==day].sort_values(by='trade_date',ascending=False).head(30),df])
#             df = pd.concat(
#                 [stock_data1[stock_data1['trade_date'] == day].sort_values(by='score', ascending=False).head(30), df])
#             # stock.to_csv(FILENAME + str((mv_bins[i], mv_bins[i + 1])) + "30ofall_marks.csv")
#         df.to_csv(FILENAME + str(mv_bins[i])+ "---30ofall_marks.csv")
#
#         stock = wool(df, data)
#         stock.to_csv(FILENAME + str((mv_bins[i], mv_bins[i + 1])) + "pct_wool.csv")
#         print(stock)
# else:
#     df = pd.DataFrame()
#     for day in stock_marks['trade_date'].unique():
#         # df=pd.concat([data[data['trade_date']==day].sort_values(by='trade_date',ascending=False).head(30),df])
#         df = pd.concat(
#             [stock_marks[stock_marks['trade_date'] == day].sort_values(by='score', ascending=False).head(30), df])
#     df.to_csv('sort_data_score.csv')
#     stock = wool(df, data)
#     stock.to_csv(FILENAME + "pct_wool.csv")
#     print(stock)
#
# # 过滤
#
#
# # data.to_csv(FILENAME + "all_marks.csv")
# # df = df[df['score'] >= 10]
#
#
# # stock_del=data[(abs(data['open']-data['close'])<=(0.05*data['pre_close']))&(data['close']>=(0.97*data['preclose']))]
#
# # stock_data=sheep(data)
# # print('stockdata',stock_data.shape)
#
# # # stock_need=stock_need[]
# # print(stock_need)
# # print(stock_need.shape)

# stock_data=stock_data.merge(stock_need[['ts_code','trade_date']],on=['ts_code','trade_date'],how='inner')
# print('filter',stock_data.shape)


# CONTAIN = stockfilter.StockFilter().stock_basic('20191127', total_mv=[5000000000,20000000000])
# stock_data=stock_data[stock_data['ts_code'].isin(CONTAIN)]
# print(stock_data)
# stock_data=marks(stock_data,score)
# stock=wool(stock_data,data)
# print(stock)
