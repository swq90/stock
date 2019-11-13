import os
import numpy as np
import pandas as pd
import time
import datetime
import tushare as ts
import matplotlib.pyplot as plt

# ts.set_token('73bebe31744e1d24de4e95aa59828b2cf9d5e07a40adbbc77a02a53e')
pro = ts.pro_api()

PATH=os.getcwd()

PRICE_COLS = ['open', 'close', 'high', 'low', 'pre_close']
FORMAT = lambda x: '%.4f' % x
CAL=[]

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

        if not CAL or end_date>CAL[-1]:
            today = datetime.datetime.today().date()
            today = str(today)[0:10]

            start_date = '' if start_date is None else start_date
            end_date = today if end_date == '' or end_date is None else end_date

            # 去掉-,不然日期取值计算错误
            start_date = start_date.replace('-', '')
            end_date = end_date.replace('-', '')
            CAL =pd.concat([pro.trade_cal(start_date=start_date, end_date=end_date, is_open=1)["cal_date"],pd.Series(CAL)],sort=True,axis=0).drop_duplicates().sort_values().tolist()


        # print(CAL)
        if cal:
            if start_date:
                # CAL=CAL[:cal]
                loc=CAL.index(start_date)
                return CAL[loc:loc+cal]
            elif end_date:
                loc=CAL.index(str(end_date))
                return CAL[loc-cal+1:loc+1]

        # 不包括给出的start 或end 当天
        if period:
            if start_date:
                loc=CAL.index(start_date)
                return (CAL[loc],CAL[loc+period])
            elif end_date:
                loc=CAL.index(end_date)
                return (CAL[loc-period], CAL[loc])
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
            print("callist",cal_list)
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
        res.columns = ["pre%sdate"%days, "trade_date"]
        # print(res)
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

    def get_all_ma(self, data, period=1, ma=[1]):
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
                res["ma1of%s" % i] = res.apply(lambda x: 100 * (x["ma1"] / x["ma%i" % i] - 1), axis=1)

        # print(res)
        # res = res[res["trade_date"].isin(self.tradeCal(cal=period))]
        return res

    def dis_ma(self, data, ma=[5]):
        bins = np.arange(-150, 150, 5)
        # # ma=[1,5,10,20,60,90,120,150,180]
        # ma = [1, 5, 10]
        # df = self.get_all_ma(data=data,period=5, ma=ma)
        tongji = pd.DataFrame()
        for i in ma[1:]:
            if isinstance(i, int):
                tongji["pct1of%s" % i] = pd.cut(data["ma1of%s" % i], bins=bins)

        tongji = tongji.apply(pd.value_counts)
        tongji = tongji.apply(lambda x: 100 * x / x.sum())
        print(tongji)
        return tongji

    def up_info(self, data, days=5, up_range=0.5):
        # data["avg"]=10*data["amount"]/data["vol"]

        print(data)
        data2 = data[["ts_code", "trade_date", "close"]]
        data2.columns = ["ts_code", "pre%sdate" % days, "pre%sclose" % days]
         # data2.rename(columns={"trade_date":"pre_date"},inplace=True)

        pre_date = self.pre_date(data[["trade_date"]], days=days)
        print(pre_date)
        data = data.merge(pre_date, on="trade_date")
        # print(data)
        data = data.merge(data2, on=["ts_code", "pre%sdate" % days])
        data["up_pct"] = data["high"] / data["pre%sclose" % days] - 1
        data = data[data["up_pct"] >= up_range]
        # print(data)
        del_rept_date = pd.DataFrame()
        for code in data["ts_code"].unique():

            df = data[data["ts_code"] == code][
                ["ts_code", "trade_date", "pre%sdate" % days, "pre%sclose" % days]].sort_values(by="trade_date",
                                                                                                ascending=False).reset_index(
                drop=True)

            i, j = 1, 0

            while len(df) > i:
                if df.iloc[[i], [1]].values >= df.iloc[[j], [2]].values:
                    # print(df.iloc[[i], [1]].values,df.iloc[[j], [2]].values)
                    df.iloc[[j], [2]] = df.iloc[[i], [2]].values
                    df.iloc[[j], [3]] = df.iloc[[i], [3]].values
                    df.iloc[[i], [0]] = [""]
                else:
                    j += 1
                i += 1
            del_rept_date = pd.concat([df[df["ts_code"] != ""], del_rept_date])
        del_rept_date.columns = ["ts_code", "trade_date", "prendate", "prenclose"]
        # data.drop(["pre_date"], axis=1, inplace=True)
        data = data.merge(del_rept_date, on=["ts_code", "trade_date"])
        data = data[['ts_code', 'trade_date', 'high', 'pre5date', 'pre5close', 'up_pct', 'prendate', 'prenclose']]
        data["up_n_pct"] = data["high"] / data["prenclose"] - 1
        # print(data)
        # print(list(data))
        # data = data.merge(del_rept_date, on=["ts_code", "trade_date"])
        return data

    def to_save(self,data,filename,type=["csv"],path=PATH):
        pass


pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)

ma=[1]+list(range(0,181,5))[1:]
period = 5
up_cal = 180
t = basic()
query_days = max(ma) + up_cal
print(ma,query_days)


test = t.trade_daily(cal=query_days)
print("all",test)
# print(t.tradeCal(cal=up_cal)[:-period])
up = test[test["trade_date"].isin(t.tradeCal(cal=up_cal)[period:])]
up = t.up_info(up, days=period, up_range=0.5)
print("up", up)


test_ma = t.get_all_ma(test, ma=ma)
print(test_ma)
up_ma = up[["ts_code", "prendate"]].merge(test_ma, left_on=["ts_code", "prendate"], right_on=["ts_code", "trade_date"])
test_ma = test_ma[test_ma["trade_date"].isin(t.tradeCal(cal=up_cal))]
# test_ma.to_csv("all_ma.csv")
print("up_ma", up_ma)
test_dis = t.dis_ma(test_ma, ma=ma)

up_dis = t.dis_ma(up_ma, ma=ma)
for i in list(test_dis):
    test_dis[i].plot(color="g")
    up_dis[i].plot(color="r",title=i,grid=True)
    plt.legend(labels=['all-ma', 'up'])
    plt.show()
    # plt.savefig


# t = basic().tradeCal(start_date="20191105")
# t = basic().tradeCal(start_date="20191031")

