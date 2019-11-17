import os
import numpy as np
import pandas as pd
import time
import datetime
import tushare as ts
import matplotlib.pyplot as plt
import model.filter as filter

# ts.set_token('73bebe31744e1d24de4e95aa59828b2cf9d5e07a40adbbc77a02a53e')
pro = ts.pro_api()
TODAY = str(datetime.datetime.today().date())[:10].replace("-", "")
NOTCONTAIN = filter.StockFilter().stock_basic(TODAY, name="st|ST")
PATH = os.getcwd()

PRICE_COLS = ['open', 'close', 'high', 'low', 'pre_close']
FORMAT = lambda x: '%.4f' % x
CAL = []


class basic:

    def tradeCal(self, start_date="", end_date="", cal=0, period=0):
        """
        交易日历相关操作

        :param start_date: str
        :param end_date: str
        :param cal: int
        :param period: int
        :return: list or str or int
                如果给出cal，则返回以开始日期或结束日期在cal个交易日内，所有交易日的列表cal是交易日的和
                如果给出period，则返回对应开始或结束日期的交易日，period=结束-开始
                都没给出，则返回对应开始结束日的交易日天数
        """
        global CAL
        today = datetime.datetime.today().date()
        today = str(today)[0:10]

        start_date = '' if start_date is None else start_date
        end_date = today if end_date == '' or end_date is None else end_date

        # 去掉-,不然日期取值计算错误
        start_date = start_date.replace('-', '')
        end_date = end_date.replace('-', '')
        # if not CAL or start_date<CAL[0] or end_date>CAL[-1]:
        # start_date 为空总是小于，总是走入,暂时不管/、

        if not CAL or end_date > CAL[-1]:
            today = datetime.datetime.today().date()
            today = str(today)[0:10]

            start_date = '' if start_date is None else start_date
            end_date = today if end_date == '' or end_date is None else end_date

            # 去掉-,不然日期取值计算错误
            start_date = start_date.replace('-', '')
            end_date = end_date.replace('-', '')
            CAL = pd.concat(
                [pro.trade_cal(start_date=start_date, end_date=end_date, is_open=1)["cal_date"], pd.Series(CAL)],
                sort=True, axis=0).drop_duplicates().sort_values().tolist()

        if cal:
            if start_date:
                # CAL=CAL[:cal]
                loc = CAL.index(start_date)
                return CAL[loc:loc + cal]
            elif end_date in CAL:
                loc = CAL.index(str(end_date))
                return CAL[loc - cal + 1:loc + 1]
            else:
                return CAL[-cal:]

        # 不包括给出的start 或end 当天
        if period:
            if start_date:
                loc = CAL.index(start_date)
                return (CAL[loc], CAL[loc + period])
            elif end_date:
                loc = CAL.index(end_date)
                return (CAL[loc - period], CAL[loc])
        # 如果period为空返回周期
        else:
            # 没想好
            return

    def trade_daily(self, ts_code="", trade_date="", start_date="", end_date="", cal=5, period=0):
        """
        查询基础交易信息
        :param ts_code: str
        :param trade_date: str
        :param start_date: str
        :param end_date: str
        :param cal: int,交易日
        :param period: int, 交易周期，包含非交易日
        :return: dataframe
            返回股票日交易信息
            如果给出ts_code,返回对应股票指定日期数据，否则返回所有股票数据
            如果给出ts_code,返回对应股票指定日期数据，否则返回所有股票数据

        """
        res = pd.DataFrame()

        if cal or period:
            cal_list = self.tradeCal(start_date=start_date, end_date=end_date, cal=cal, period=period)
            # print("callist",cal_list)
            # query_times = 0
            for date in cal_list:
                res = pd.concat([pro.daily(trade_date=date), res])
                # query_times += 1
                # if query_times % 190 == 0:
                #     time.sleep(60)
        else:
            res = pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)

        return res

    # def ma(self, data, ma=5):
    #
    #     res = pd.DataFrame()
    #
    #     date_list = data[["trade_date"]].drop_duplicates().sort_values(by="trade_date").reset_index(drop=True)
    #     stock_list = data["ts_code"].unique()
    #     if isinstance(ma, int):
    #         for date in date_list["trade_date"]:
    #             # 如果缺少前期数据则查询补充
    #             date_ctn = self.tradeCal(end_date=date, cal=ma)
    #             date_need = [new for new in date_ctn if
    #                          new not in data["trade_date"].unique().tolist()]
    #             if date_need:
    #                 for day in date_need:
    #                     data = pd.concat([data, pro.daily(trade_date=day)], sort=False)
    #             df = data[(data["trade_date"].isin(date_ctn)) & (data["ts_code"].isin(stock_list))].groupby("ts_code")[
    #                 "amount", "vol"].sum().reset_index()
    #             df["trade_date"] = pd.DataFrame([date] * len(df))
    #             res = pd.concat([df, res])
    #         res["ma" + str(ma)] = res["amount"] * 10 / res["vol"]
    #         return res[["ts_code", "trade_date", "ma" + str(ma)]]
    #     else:
    #         ma.sort(reverse=True)
    #         for i in ma:
    #             if res.empty:
    #                 res = self.ma(data, ma=i)
    #             else:
    #                 res = res.merge(self.ma(data, ma=i), on=["ts_code", "trade_date"], how="outer")
    #             print("ma", i, "计算完毕")
    #             print(datetime.datetime.now())
    #     return res

    def MA(self, DF, N):

        return pd.Series.rolling(DF, N, min_periods=N).sum()

    def ma(self, data, ma=[]):
        if ma is not None and len(ma) > 0:
            for i in ma:
                if isinstance(i, int):
                    data["ma_%s_" % i] = (10 * self.MA(data["amount"], i) / self.MA(data["vol"], i)).map(FORMAT).shift(
                        -(i - 1))
                    data["ma_%s_" % i] = data["ma_%s_" % i].astype(float)
        return data

    def pre_date(self, date_list, days=1):
        res = pd.DataFrame()
        date_list = date_list.iloc[:, [0]].drop_duplicates()
        date_list.columns = ["trade_date"]

        for i in date_list["trade_date"]:
            day = [self.tradeCal(end_date=i, period=days)]

            res = pd.concat([res, pd.DataFrame(day)])
        res.columns = ["pre_%s_date" % days, "trade_date"]

        return res

    def list_days(self, data, days):
        """
        上市天数
        :param data:
        :param days:
        :return:
        """
        stock_basic = pro.stock_basic()[['ts_code', 'list_date']]
        if not isinstance(data, pd.DataFrame):
            return
        data = data.merge(stock_basic, on="ts_code")
        # data["days"] = data.apply(lambda x: (datetime.date(int(x["trade_date"][:4]), int(x["trade_date"][4:6]),
        #                                                    int(x["trade_date"][6:])) - datetime.date(
        #     int(x["list_date"][:4]), int(x["list_date"][4:6]), int(x["list_date"][6:]))).days, axis=1)
        # days为上市日期到交易日期之间的交易日天数
        data["days"] = data.apply(lambda x: self.tradeCal(start_date=x["list_date"], end_date=x["trade_date"], axis=1))
        return data[["ts_code", "trade_date", "days"]]

    def avg_up_info(self):
        pass

    def label(self, data, formula, up_pct=0.5):
        pass

    def get_all_ma(self, data, period=1, ma=[1], is_abs=False):
        # if not data:
        #     cal = period + max(ma)
        #     data = basic().daily(cal=cal)
        res = pd.DataFrame()
        # print(data.info())
        for i in data["ts_code"].unique():
            dm = data[data["ts_code"] == i][["ts_code", "trade_date", "amount", "vol"]]
            dm = basic().ma(dm, ma=ma)
            res = pd.concat([dm, res])

        for i in ma[1:]:
            if isinstance(i, int):
                res["ma1of_%s_" % i] = res.apply(lambda x: 100 * (x["ma1"] / x["ma%i" % i] - 1), axis=1)
                if is_abs:
                    # print("abs")
                    res["ma1of_%s_" % i] = res["ma1of_%s_" % i].apply(lambda x: abs(x))

        # res = res[res["trade_date"].isin(self.tradeCal(cal=period))]
        return res

    def get_all_slice(self, data, period=1):
        res = pd.DataFrame()

        # print(data.info())
        for i in data["ts_code"].unique():
            row = 0
            dm = data[data["ts_code"] == i][["ts_code", "trade_date", "amount", "vol"]].reset_index()

            while row < len(dm):
                # df=dm.iloc[]
                pass

    def dis_key(self, data, ma=[], bins=[], sec=1, between=100, is_abs=False, keys=[]):
        """

        :param data:
        :param ma: 用key替换
        :param bins: 切分依据，同pandas.cut bins
        :param sec：未给出bins，切分间距
        :param between: 未给出bins，切分上下限
        :param is_abs: True 则bins 为正区间，False为全部区间
        :param keys:给出要求分布的所有列名
        :return:
        """
        if not bins:
            if is_abs:
                bins = np.arange(0, between, sec)
            else:
                bins = np.arange(-between, between, sec)
        print(bins)
        tongji = pd.DataFrame()
        for key in keys:
            tongji[key] = pd.cut(data[key], bins=bins)
        tongji = tongji.apply(pd.value_counts)
        tongji = tongji.apply(lambda x: 100 * x / len(data))
        print(tongji)
        return tongji

    def up_info(self, data, days=5, up_range=0.5):
        """
        多日涨停股票，日期重叠要合并日期区间后得到涨停大于等于days的股票
        :param data:
        :param days: int 连续涨停天数
        :param up_range: float,days涨停下限
        :return: datarame:data       [ts_code trade_date    high pre_n_date  pre_n_close  up_n_pct]
        """

        data2 = data[["ts_code", "trade_date", "close"]]
        data2.columns = ["ts_code", "pre_%s_date" % days, "pre_%s_close" % days]

        pre_date = self.pre_date(data[["trade_date"]], days=days)
        data = data.merge(pre_date, on="trade_date")
        data = data.merge(data2, on=["ts_code", "pre_%s_date" % days])
        data["up_pct"] = data["high"] / data["pre_%s_close" % days] - 1
        data = data[data["up_pct"] >= up_range]

        data.to_csv("aaa.csv")

        del_rept_date = pd.DataFrame()
        for code in data["ts_code"].unique():

            df = data[data["ts_code"] == code][
                ["ts_code", "trade_date", "pre_%s_date" % days, "pre_%s_close" % days]].sort_values(by="trade_date",
                                                                                                ascending=False).reset_index(
                drop=True)
            print("df", df)
            i, j = 1, 0

            while len(df) > i:

                if df.at[i, "trade_date"] >= df.at[j, "pre_%s_date" % days]:
                    df.at[j, "pre_%s_date" % days] = df.at[i, "pre_%s_date" % days]
                    df.at[j, "pre_%s_close" % days] = df.at[i, "pre_%s_close" % days]

                    df.at[i, "ts_code"] = ""
                else:
                    j = i
                i += 1
            del_rept_date = pd.concat([df[df["ts_code"] != ""], del_rept_date])
        del_rept_date.columns = ["ts_code", "trade_date", "pre_n_date", "pre_n_close"]
        data = data[['ts_code', 'trade_date', 'high']].merge(del_rept_date, on=["ts_code", "trade_date"])

        data["up_n_pct"] = data["high"] / data["pre_n_close"] - 1
        print("up_data has done,it has %s items"%len(data))
        return data

    def to_save(self, data, filename, type=["csv"], path=PATH):
        """

        :param data: dataframe，需要保存的数据
        :param filename: str，文件名
        :param type: list，需要保存的文件类型
        :param path: str，默认为当前路径下增加data文件夹
        :return:
        """
        for t in type:
            # data.to_csv(str(path) + "\data\\" + str(datetime.datetime.today())[:10] + filename + "." + t)
            full_filename=path + "\data\\" + str(datetime.datetime.today())[:16] + filename + "." + t
            data.to_csv(full_filename)
            print("has saved file : "+full_filename)


pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)
days = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90]
# ma=[1]+list(range(0,11,5))[1:]
ma = [1]
period = 5
up_cal = 18
# sec=list(range(1,6,1))
between = 20
IS_ABS = False
pic_all_ma = 0
pic_pct = 0
pic_range = 0
t = basic()
query_days = max(ma) + up_cal

test = t.trade_daily(cal=query_days)
print(test.shape)
# 过滤st市值等
# test = test[test["ts_code"].isin(NOTCONTAIN) == False]

# print(t.tradeCal(cal=up_cal)[:-period])
up = test[test["trade_date"].isin(t.tradeCal(cal=up_cal))][['ts_code', 'trade_date',  'high', 'close', 'pre_close']
]
up = t.up_info(up, days=period, up_range=0.5)

print("up", list(up))
# print("all",test.shape,list(test))

test_ma = t.get_all_ma(test, ma=ma, is_abs=IS_ABS)
# print(test_ma)

# print(datetime.datetime.now())
test_ma = test_ma[test_ma["trade_date"].isin(t.tradeCal(cal=up_cal))]
print("test", test_ma.shape)

test_ma.to_csv("testma.csv")
up.to_csv("up.csv")

# t.to_save(test_ma,"allma")
# t.to_save(up_ma,"upma")

# 求前n天


code = 0
# up前一天所有数据
# up_ma_drop = pd.DataFrame()
# for code in range(len(up)):
#     # print(up.loc[[code], ["trade_date"]].values[0])
#     # up_ma_drop = pd.concat(test_ma[(test_ma["ts_code"] == up.loc[[code], ["ts_code"]].values[0]) & (
#     #             test_ma["trade_date"] <= up.loc[[code], ["trade_date"]].values[0]) & (
#     #                                            test_ma["trade_date"] >= up.loc[[code], ["prendate"]].values[0])])
#     up_ma_drop = up_ma_drop.append(test_ma1[(test_ma["ts_code"] == up.at[code, "ts_code"]) & (
#                 test_ma1["trade_date"] <= up.at[code, "trade_date"]) & (
#                                                test_ma1["trade_date"] >= up.at[code, "prendate"])])
# print(up_ma_drop.shape)
# drop_up = test_ma1[test_ma1["ts_code"].isin(up["ts_code"])]
# print(drop_up.shape)
# drop_up = drop_up.append(up_ma_drop)
# print(drop_up.shape)
# up_ma1 = drop_up.drop_duplicates(["ts_code", "trade_date"], keep=False)
# print(up_ma1.shape)
# print(test_ma1.shape)


for day in days:
    pre_ndays = t.pre_date(test_ma[["trade_date"]], days=day)
    test_ma1 = test_ma.merge(pre_ndays, on="trade_date")
    print(list(test_ma1))
    df = test_ma1[["ts_code", "trade_date", "ma1"]]
    df.columns = ["ts_code", "pre_%s_date" % day, "pre_ma1"]
    # print(df,df.info)
    print(list(df))
    test_ma1 = pd.merge(test_ma1, df, on=["ts_code", "pre_%s_date" % day])
    test_ma1["pct"] = test_ma1.apply(lambda x: abs(100 * (x["ma1"] / x["pre_ma1"] - 1)), axis=1)
    print("testma1", test_ma1)
    pre_ndays = t.pre_date(up[["prendate"]], days=day)
    pre_ndays.columns = ["upprendays", "prendate"]
    up1 = up.merge(pre_ndays, on="prendate")
    print(up1)
    up_ma1 = pd.DataFrame()
    for i in range(len(up1)):
        # print(up.at[i, "ts_code"],up.at[code, "prendate"],up.at[code, "upprendays"])
        up_ma1 = up_ma1.append(test_ma1[(test_ma1["ts_code"] == up1.at[i, "ts_code"]) & (
                test_ma1["trade_date"] <= up1.at[code, "prendate"]) & (
                                                test_ma1["trade_date"] >= up1.at[code, "upprendays"])],
                               ignore_index=True)
        # print("aaaa",up_ma1)
    print(test_ma1)
    print(up_ma1)
    test_ma1 = t.dis_key(test_ma1, ma=ma, between=between, is_abs=IS_ABS, key="pct")
    up_ma1 = t.dis_key(up_ma1, ma=ma, between=between, is_abs=IS_ABS, key="pct")
    test_ma1["pct"].plot(label="all")
    up_ma1["pct"].plot(label="limit_up", title=str(day) + "fluctuate", grid=True)
    # plt.legend(labels=[ 'all-ma','up'])
    plt.legend(loc="best")
    plt.show()

# 不算周期，只算前第n天的数据
noperiod = 1

if noperiod:
    for day in days:
        pre_ndays = t.pre_date(test_ma[["trade_date"]], days=day)
        test_ma1 = test_ma.merge(pre_ndays, on="trade_date")

        df = test_ma1[["ts_code", "trade_date", "ma1"]]
        df.columns = ["ts_code", "pre_%s_date" % day, "pre_ma1"]
        # print(df,df.info)
        test_ma1 = pd.merge(test_ma1, df, on=["ts_code", "pre_%s_date" % day])
        test_ma1["pct"] = test_ma1.apply(lambda x: abs(100 * (x["ma1"] / x["pre_ma1"] - 1)), axis=1)
        print("ma1", test_ma1)
        pre_ndays = t.pre_date(up[["prendate"]], days=day)
        pre_ndays.columns = ["upprendays", "prendate"]
        up1 = up[["ts_code", "prendate"]]
        up1 = up1.merge(pre_ndays, on="prendate")
        print(list(up1))
        up1 = up1.rename(columns={"prendate": "trade_date", "upprendays": "pre_%s_date" % day})
        print(list(up1))
        up_ma1 = up1.merge(test_ma1, on=["ts_code", "trade_date", "pre_%s_date" % day])

        test_ma1 = t.dis_key(test_ma1, ma=ma, between=between, is_abs=IS_ABS, key="pct")
        up_ma1 = t.dis_key(up_ma1, ma=ma, between=between, is_abs=IS_ABS, key="pct")
        test_ma1["pct"].plot(label="all")
        up_ma1["pct"].plot(label="limit_up", title="pre_no." + str(day) + "fluctuate", grid=True)
        # plt.legend(labels=[ 'all-ma','up'])
        plt.legend(loc="best")
        plt.show()

# up_ma = up[["ts_code", "prendate"]].merge(test_ma, left_on=["ts_code", "prendate"], right_on=["ts_code", "trade_date"])
# print(up_ma)
# test_ma.to_csv("all_ma.csv")
print("up_ma1", up_ma1)

#
#
#
#
#
#
# up_ma1 = up[["ts_code", "prendate"]].merge(test_ma1, left_on=["ts_code", "prendate"],
#                                            right_on=["ts_code", "trade_date"])
# print(up_ma1)


# for i in sec:
#     test_dis = t.dis_key(test_ma, ma=ma,between=between,sec=i,is_abs=IS_ABS)
# #     up_dis = t.dis_key(up_ma, ma=ma,between=between,sec=i,is_abs=IS_ABS)
# #     print("sec",i)
# #     print(test_dis)
# #
# #     print("dis",test_dis.iloc[int(len(test_dis)/2)])
# #     print(up_dis)
#     # print(test_dis.ind)
# if pic_all_ma:
#     test_dis.iloc[int(len(test_dis) / 2)].plot(label='all_%s_' % i, grid=True)
#     up_dis.iloc[int(len(up_dis) / 2)].plot(label='up_%s_' % i, grid=True)
# test_dis.to_csv("test-dis_%s_.csv"%i)
# up_dis.to_csv("up-dis_%s_.csv"%i)
#
# test_dis = t.dis_key(test_ma, ma=ma, between=between, is_abs=IS_ABS)
# up_dis = t.dis_key(up_ma, ma=ma, between=between, is_abs=IS_ABS)

# t.to_save(test_dis,"mafenbu")
# t.to_save(up_dis,"upfenbu")
# print(up_dis)
#
# if pic_pct:
#     for i in list(test_dis):
#         test_dis[i].plot()
#         up_dis[i].plot(title=i, grid=True)
#         plt.legend(labels=['all-ma', 'up'])
#         # plt.legend(loc="best")
#         plt.show()
#         # plt.savefig-
#
# if pic_range:
#     plt.plot(list(test_dis), test_dis.iloc[int(len(test_dis) / 2)], "s-", label="all-ma_%s_" % i, )
#     plt.plot(list(up_dis), up_dis.iloc[int(len(up_dis) / 2)], "o-", label="up-ma_%s_" % i)
# # plt.xticks(list(test_dis)[::2])
# # plt.plot(list(test_dis),test_dis.iloc[int(len(test_dis)/2)],"s-", label="all-ma_%s_"%i,)
# # plt.plot(list(up_dis),up_dis.iloc[int(len(up_dis)/2)],"o-", label="up-ma_%s_"%i)
#

print(datetime.datetime.now())
# t = basic().tradeCal(start_date="20191105")
# t = basic().tradeCal(start_date="20191031")
