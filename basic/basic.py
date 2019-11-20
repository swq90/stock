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
NOTCONTAIN = filter.StockFilter().stock_basic(TODAY, name="st|ST", market="科创板")
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
                    data["ma%s" % i] = (10 * self.MA(data["amount"], i) / self.MA(data["vol"], i)).map(FORMAT).shift(
                        -(i - 1))
                    data["ma%s" % i] = data["ma%s" % i].astype(float)

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

    def list_days(self, data, list_days):
        """
        上市天数
        :param data:
        :param list_days:int 上市天数需要大于list_days
        :return:data[["ts_code", "trade_date", "list_days"]],list_days 当前交易日该股票已经上市天数
        """
        stock_basic = pro.stock_basic()[['ts_code', 'list_date']]
        if not isinstance(data, pd.DataFrame):
            print("must be dataframe")
            return
        data = data.merge(stock_basic, on="ts_code")
        # data["days"] = data.apply(lambda x: (datetime.date(int(x["trade_date"][:4]), int(x["trade_date"][4:6]),
        #                                                    int(x["trade_date"][6:])) - datetime.date(
        #     int(x["list_date"][:4]), int(x["list_date"][4:6]), int(x["list_date"][6:]))).days, axis=1)
        # days为上市日期到交易日期之间的交易日天数
        data["list_days"] = data.apply(
            lambda x: self.tradeCal(start_date=x["list_date"], end_date=x["trade_date"], axis=1))
        return data[["ts_code", "trade_date", "list_days"]]

    def avg_up_info(self):
        pass

    def label(self, data, formula, up_pct=0.5):
        pass

    def get_all_ma(self, data, ma=[1], is_abs=False, dis_pct=True):
        """

        :param data: dataframe 股票数据
        :param ma: int 列表，需要计算的n日均价
        :param is_abs: 相对ma1的涨幅是否取绝对值
        :param dis_pct: 是否求man相对ma1 的浮动   %
        :return: dataframe
        """
        t = datetime.datetime.now()

        res = pd.DataFrame()
        # print(data.info())
        count=0
        for i in data["ts_code"].unique():
            t1=datetime.datetime.now()
            dm = data[data["ts_code"] == i][["ts_code", "trade_date", "amount", "vol"]]
            dm = self.ma(dm, ma=ma)
            res = pd.concat([dm, res])
            count+=1
            print("%s ma 计算完成"%i,datetime.datetime.now()-t1,count)
        print(datetime.datetime.now()-t)
        if dis_pct:
            for i in ma[1:]:
                if isinstance(i, int):
                    res["ma1of%s" % i] = res.apply(lambda x: 100 * (x["ma1"] / x["ma%s" % i] - 1), axis=1)
                    if is_abs:
                        # print("abs")
                        res["ma1of%s" % i] = res["ma1of%s" % i].apply(lambda x: abs(x))
        print(datetime.datetime.now()-t)

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
        return tongji.sort_index()

    def up_info(self, data, days=5, up_range=0.5):
        """
        多日涨停股票，日期重叠要合并日期区间后得到涨停大于等于days的股票
        :param data:
        :param days: int 连续涨停天数
        :param up_range: float,days涨停下限
        :return: datarame:data       [ts_code trade_date    high pre_n_date  pre_n_close  up_n_pct]
        """
        t1 = datetime.datetime.now()


        data2 = data[["ts_code", "trade_date", "close"]]
        data2.columns = ["ts_code", "pre_%s_date" % days, "pre_%s_close" % days]

        pre_date = self.pre_date(data[["trade_date"]], days=days)
        data = data.merge(pre_date, on="trade_date")
        data = data.merge(data2, on=["ts_code", "pre_%s_date" % days])
        data["up_pct"] = data["high"] / data["pre_%s_close" % days] - 1
        data = data[data["up_pct"] >= up_range]
        # data.to_csv("kkk.csv")
        df = self.revise(data, days=days, rekeys="pre_%s_close", inplace=True)
        #
        #
        # del_rept_date = pd.DataFrame()
        # for code in data["ts_code"].unique():
        #
        #     df = data[data["ts_code"] == code][
        #         ["ts_code", "trade_date", "pre_%s_date" % days, "pre_%s_close" % days]].sort_values(by="trade_date",
        #                                                                                         ascending=False).reset_index(
        #         drop=True)
        #     #
        #     # i, j = 1, 0
        #     #
        #     # while len(df) > i:
        #     #     i += 1
        #     df= self.revise(df,days=days)
        #     del_rept_date = pd.concat([df[df["ts_code"] != ""], del_rept_date])
        # del_rept_date.columns = ["ts_code", "trade_date", "pre_n_date", "pre_n_close"]
        data = data[['ts_code', 'trade_date', 'high']].merge(df, on=["ts_code", "trade_date"])

        data["up_n_pct"] = data["high"] / data["pre_n_close"] - 1
        print("up_data has done,it has %s items" % len(data))
        # data.to_csv("jjk.csv")
        print("up 计算完成", datetime.datetime.now() - t1)

        return data

    def revise(self, data, days=5, rekeys="pre_%s_close", inplace=True,reverse=True):
        res = pd.DataFrame()
# all_pre = t.revise(all_pre, days=day, rekeys="ma%spct", inplace=False)
        for code in data["ts_code"].unique():
            df = data[data["ts_code"] == code][
                ["ts_code", "trade_date", "pre_%s_date" % days, rekeys % days]].sort_values(by="trade_date",
                                                                                            ascending=False).reset_index(
                drop=True)

            i, j = 1, 0

            while len(df) > i:
                if reverse:
                    if df.at[i, "trade_date"] >= df.at[j, "pre_%s_date" % days]:
                        df.at[j, "pre_%s_date" % days] = df.at[i, "pre_%s_date" % days]
                        # df.at[j, "pre_%s_close" % days] = df.at[i, "pre_%s_close" % days]
                        if inplace:
                            df.at[j, rekeys % days] = df.at[i, rekeys % days]
                        df.at[i, "ts_code"] = ""
                    else:
                        j = i

                else:
                    if df.at[i, "trade_date"] > df.at[j, "pre_%s_date" % days]:
                        df.at[i, "ts_code"] = ""
                    else:
                        j=i
                i+=1
            res = pd.concat([df[df["ts_code"] != ""], res])
        if inplace:
            res.columns = ["ts_code", "trade_date", "pre_n_date", "pre_n_close"]
        return res

    def to_save(self, data, filename, type=["csv"], path=PATH):
        """

        :param data: dataframe，需要保存的数据
        :param filename: str，文件名
        :param type: list，需要保存的文件类型
        :param path: str，默认为当前路径下增加data文件夹
        :return:
        """
        for t in type:
            full_name = data.to_csv(str(path) + "\\data\\" + str(datetime.datetime.today())[:10] + filename + "." + t)

            data.to_csv(full_name)
            print("has saved file : ")


pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)
ma=[1, 5, 10, 20, 30,60]
# ma=[1, 5, 10, 20, 30,60,90,120,150,180]
days = ma[:5]

period = 5
up_cal = 240
# sec=list(range(1,6,1))
sec = 1
between = 50
IS_ABS = False
pic_all_ma = 0
pic_pre = 0
pic_pct=1
t = basic()
query_days = max(max(ma), max(days)) + up_cal
all_stock = t.trade_daily(cal=query_days)
# 过滤st市值等
all_stock = all_stock[all_stock["ts_code"].isin(NOTCONTAIN) == False]

up = all_stock[all_stock["trade_date"].isin(t.tradeCal(cal=up_cal))][
    ['ts_code', 'trade_date', 'high', 'close', 'pre_close']]
up = t.up_info(up, days=period, up_range=0.5)
# 计算所有股票n日均价，dis_pct为True计算ma1相对n日均价的涨幅
all_ma = t.get_all_ma(all_stock, ma=ma, is_abs=IS_ABS, dis_pct=True)
all_ma = all_ma[all_ma["trade_date"].isin(t.tradeCal(cal=up_cal))]
# t.to_save(all_ma,"allma")
# 计算满足上涨条件的

up_ma = pd.DataFrame()
for row in range(len(up)):
    df = all_ma[(all_ma["ts_code"] == up.at[row, "ts_code"]) & (all_ma["trade_date"] <= up.at[row, "pre_n_date"])]
    col_name=df.columns.tolist()
    col_name.insert(2,"up_pre_days")
    df["up_pre_days"]=
    df=df.reindex(col_name)
    up_ma = up_ma.append(df)
# 计算满足上涨的up 均价
all_ma.to_csv("all_ma.csv")
up_ma.to_csv("up_ma.csv")

if pic_all_ma:
    all_ma_dis = t.dis_key(all_ma, between=between, sec=sec, keys=list(all_ma)[-len(ma) + 1:])
    up_ma_dis = t.dis_key(up_ma, between=between, sec=sec, keys=list(up_ma)[-len(ma) + 1:])
    all_ma_dis.to_csv("all_ma_dis.csv")
    up_ma_dis.to_csv("up_ma_dis.csv")


    print("准备画图：pct(ma1/man)")
    for col_name in list(all_ma_dis):
        all_ma_dis[col_name].plot(label="all")
        up_ma_dis[col_name].plot(label="up", title=str(datetime.datetime.today())[:10] + col_name, grid=True)
        plt.legend(loc="best")
        plt.savefig(os.getcwd() + "\\" + str(datetime.datetime.today()).replace(":", "").replace(" ", "")[
                                         :20] + col_name + "pct.png")
        plt.show()
    print("准备画图：不同区间pct(ma1/man)")
    # d.drop(d[d.apply(lambda x: sum(x), axis=1) < 3].index, inplace=True)
    # all_ma_dis.drop(all_ma_dis[all_ma_dis.apply(lambda x:sum(x),axis=1)<=0].index,inplace=True)
    # up_ma_dis.drop(up_ma_dis[up_ma_dis.apply(lambda x: sum(x), axis=1) <=0].index, inplace=True)



    for row in range(len(all_ma_dis)):
        # print("v",all_ma_dis.index.values[row])
        # print("str",str(all_ma_dis.index.values[row]))
        all_ma_dis.iloc[row].plot(label="all")
        up_ma_dis.iloc[row].plot(label="up", title=str(datetime.datetime.today())[:10]+"pct" + str(all_ma_dis.index.values[row]), grid=True)
        plt.savefig(os.getcwd() + "\\" + str(datetime.datetime.today()).replace(":", "").replace(" ", "")[:20] + str(
            row) + "range_pct.png")
        plt.legend(loc="best")
        plt.show()
    all_ma_dis.to_csv("allmadis.csv")
    up_ma_dis.to_csv("upmadis.csv")


if pic_pre:
    for day in days:
        pre_n_days = t.pre_date(all_ma[["trade_date"]], days=day)
        pre_ma = all_ma[["ts_code", "trade_date", "ma%s" % day]]
        pre_ma.columns = ["ts_code", "pre_%s_date" % day, "pre_ma%s" % day]
        all_pre = all_ma[["ts_code", "trade_date", "ma%s" % day]].merge(pre_n_days, on="trade_date")
        all_pre = all_pre.merge(pre_ma, on=["ts_code", "pre_%s_date" % day])
        all_pre["ma%spct" % day] = (all_pre["pre_ma%s" % day] / all_pre["ma%s" % day] - 1) * 100
        up_pre = up_ma[["ts_code", "trade_date"]].merge(all_pre, on=["ts_code", "trade_date"])
        all_pre.to_csv("all1.csv")
        up_pre.to_csv("up1.csv")

        if day!=1:
            all_pre = t.revise(all_pre, days=day, rekeys="ma%spct", inplace=False,reverse=False)
            print("up_pre.revise")
            up_pre = t.revise(up_pre, days=day, rekeys="ma%spct", inplace=False,reverse=False)
            all_pre.to_csv("all2.csv")

        all_pre_dis = t.dis_key(all_pre, between=between, sec=sec, keys=list(all_pre)[-1:])
        up_pre_dis = t.dis_key(up_pre, between=between, sec=sec, keys=list(up_pre)[-1:])
        all_pre_dis.to_csv("all%spct.csv"%day)
        up_pre_dis.to_csv("up%spct.csv"%day)
        print(all_pre_dis)
        print(up_pre_dis)
        # all_pre_dis.drop(all_pre_dis[all_pre_dis.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)
        # up_pre_dis.drop(up_pre_dis[up_pre_dis.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)
        df= pd.merge(all_pre_dis,up_pre_dis,left_index=True,right=True,how="outer")
        print(df)
        df.columns=["all","up"]
        df.drop(df[df.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)

        df["all"].plot(label="all")
        df["up"].plot(label="up", title="pre%spct" % day, grid=True)

        # all_pre_dis["ma%spct" % day].plot(label="all")
        # up_pre_dis["ma%spct" % day].plot(label="up", title="pre%spct" % day, grid=True)
        plt.legend(loc="best")
        plt.savefig(os.getcwd() + "\\" + str(datetime.datetime.today()).replace(":", "").replace(" ", "")[
                                         :20] + "period%spre_pct.png" % day)
        plt.show()

        # for row in len(up):
        #     df=all_ma[(all_ma["ts_code"]==up.at[row,"ts_code"])&(all_ma["trade_date"]<=up.at[row,"pre_n_date"])]
        #     df=t.revise(df, days=day,rekeys="pre_ma%s",inplace=False)
        #     up_pre=up_pre.append(df)
        all_pre_dis.to_csv("all%spct.csv"%day)
        up_pre_dis.to_csv("up%spct.csv"%day)

if pic_pct:
    for day in days:
        pre_n_days = t.pre_date(all_ma[["trade_date"]], days=day)
        pre_ma = all_ma[["ts_code", "trade_date", "ma%s" % day]]
        pre_ma.columns = ["ts_code", "pre_%s_date" % day, "pre_ma%s" % day]
        all_pre = all_ma[["ts_code", "trade_date", "ma%s" % day]].merge(pre_n_days, on="trade_date")
        all_pre = all_pre.merge(pre_ma, on=["ts_code", "pre_%s_date" % day])
        all_pre["ma%spct" % day] = (all_pre["pre_ma%s" % day] / all_pre["ma%s" % day] - 1) * 100
        up_pre = up_ma[["ts_code", "trade_date"]].merge(all_pre, on=["ts_code", "trade_date"])
        all_pre.to_csv("all1.csv")
        up_pre.to_csv("up1.csv")

        if day != 1:
            all_pre = t.revise(all_pre, days=day, rekeys="ma%spct", inplace=False, reverse=False)
            print("up_pre.revise")
            up_pre = t.revise(up_pre, days=day, rekeys="ma%spct", inplace=False, reverse=False)
            all_pre.to_csv("all2.csv")

        all_pre_dis = t.dis_key(all_pre, between=between, sec=sec, keys=list(all_pre)[-1:])
        up_pre_dis = t.dis_key(up_pre, between=between, sec=sec, keys=list(up_pre)[-1:])
        all_pre_dis.to_csv("all%spct.csv" % day)
        up_pre_dis.to_csv("up%spct.csv" % day)
        print(all_pre_dis)
        print(up_pre_dis)
        # all_pre_dis.drop(all_pre_dis[all_pre_dis.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)
        # up_pre_dis.drop(up_pre_dis[up_pre_dis.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)
        df = pd.merge(all_pre_dis, up_pre_dis, left_index=True, right=True, how="outer")
        print(df)
        df.columns = ["all", "up"]
        df.drop(df[df.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)

        df["all"].plot(label="all")
        df["up"].plot(label="up", title="pre%spct" % day, grid=True)

        # all_pre_dis["ma%spct" % day].plot(label="all")
        # up_pre_dis["ma%spct" % day].plot(label="up", title="pre%spct" % day, grid=True)
        plt.legend(loc="best")
        plt.savefig(os.getcwd() + "\\" + str(datetime.datetime.today()).replace(":", "").replace(" ", "")[
                                         :20] + "period%spre_pct.png" % day)
        plt.show()

        # for row in len(up):
        #     df=all_ma[(all_ma["ts_code"]==up.at[row,"ts_code"])&(all_ma["trade_date"]<=up.at[row,"pre_n_date"])]
        #     df=t.revise(df, days=day,rekeys="pre_ma%s",inplace=False)
        #     up_pre=up_pre.append(df)
        all_pre_dis.to_csv("all%spct.csv" % day)
        up_pre_dis.to_csv("up%spct.csv" % day)
# 涨停股票非涨停期间股票变动
# up_ma_nup=up_ma.append(up_drop).drop_duplicates(["ts_code", "trade_date"], keep=False)


# 1计算所有股票和up股票非涨停期内均价变化pct分布
# 画图，all 和up非涨停期涨幅落在指定区间的波动曲线 画图
# 画图，两组数据 在不同的man pct= ma1/man-1 的波动线

# # if pic_all_ma:
# #     all_stock_dis.iloc[int(len(all_stock_dis) / 2)].plot(label='all_%s_' % i, grid=True)
# #     up_dis.iloc[int(len(up_dis) / 2)].plot(label='up_%s_' % i, grid=True)
#
#
# # 2计算所有股票和up非涨停期内股票，每一天的前一天均价变化分布，计算每一天的前n天分布
#     # 每一天和前n天比pct分布

#
# # 3 计算非涨停期每n天和前n天，两个时间段内均价波动
#


# # #
# # #     print("dis",all_stock_dis.iloc[int(len(all_stock_dis)/2)])
# # #     print(up_dis)
# #     # print(all_stock_dis.ind)
#
# # all_stock_dis = t.dis_key(all_ma, ma=ma, between=between, is_abs=IS_ABS)
# # up_dis = t.dis_key(up_ma, ma=ma, between=between, is_abs=IS_ABS)
#
# # t.to_save(all_stock_dis,"mafenbu")
# # t.to_save(up_dis,"upfenbu")
# # print(up_dis)
# #
# # if pic_pct:
# #     for i in list(all_stock_dis):
# #         all_stock_dis[i].plot()
# #         up_dis[i].plot(title=i, grid=True)
# #         plt.legend(labels=['all-ma', 'up'])
# #         # plt.legend(loc="best")
# #         plt.show()
# #         # plt.savefig-
# #
# # if pic_range:
# #     plt.plot(list(all_stock_dis), all_stock_dis.iloc[int(len(all_stock_dis) / 2)], "s-", label="all-ma_%s_" % i, )
# #     plt.plot(list(up_dis), up_dis.iloc[int(len(up_dis) / 2)], "o-", label="up-ma_%s_" % i)
# # # plt.xticks(list(all_stock_dis)[::2])
# # # plt.plot(list(all_stock_dis),all_stock_dis.iloc[int(len(all_stock_dis)/2)],"s-", label="all-ma_%s_"%i,)
# # # plt.plot(list(up_dis),up_dis.iloc[int(len(up_dis)/2)],"o-", label="up-ma_%s_"%i)
