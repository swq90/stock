import os
import numpy as np
import pandas as pd
import datetime
import tushare as ts
import stock.util.stockfilter

# ts.set_token()

pro = ts.pro_api()

TODAY = str(datetime.datetime.today().date())[:10].replace("-", "")
# NOTCONTAIN = util.stockfilter.StockFilter().stock_basic(TODAY, name="st|ST|退", market="科创板")
NOTCONTAIN = stock.util.stockfilter.StockFilter().stock_basic(TODAY, market="科创板")

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
            # today = datetime.datetime.today().date()
            # today = str(today)[0:10]
            #
            # start_date = '' if start_date is None else start_date
            # end_date = today if end_date == '' or end_date is None else end_date

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
                if loc + period < len(CAL):
                    return (start_date, CAL[loc + period])
                else:
                    return (start_date, '')
            elif end_date:
                loc = CAL.index(end_date)
                return (CAL[loc - period], CAL[loc])
        # 如果period为空返回周期
        else:
            # 返回回中间交易日天数
            return CAL.index(start_date) - CAL.index(start_date) + 1

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

        else:
            res = pro.daily(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)

        return res[res["ts_code"].isin(NOTCONTAIN['ts_code']) == False]
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
    #             df = data[(data["trade_date"].isin(date_ctn)) & (data["ts_code"].isin(stock_list))].by("ts_code")[
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

        return data[list(set(list(data)).difference(('amount', 'vol')))]

    def pre_date(self, date_list, days=1):
        global CAL
        res = pd.DataFrame()
        date_list = date_list.iloc[:, [0]].drop_duplicates()
        date_list.columns = ["trade_date"]
        date_list = date_list.sort_values(by='trade_date')
        if not CAL:
            CAL = pro.trade_cal(end_date=date_list.iloc[-1]['trade_date'], is_open=1)["cal_date"].sort_values().tolist()
        if days > 0:
            for i in date_list["trade_date"]:
                day = [self.tradeCal(end_date=i, period=days)]

                res = pd.concat([res, pd.DataFrame(day)])
            res.columns = ["pre_%s_date" % days, "trade_date"]
        else:
            for i in date_list["trade_date"]:
                day = [self.tradeCal(start_date=i, period=-days)]

                res = pd.concat([res, pd.DataFrame(day)])
            res.columns = ["trade_date", "pre_%s_date" % days]
        return res

    def pre_label(self, data, label, days=1):

        # pre_date = self.pre_date(data[["trade_date"]], days=days)
        # data = data.merge(pre_date, on="trade_date")
        # data = data.merge(data2, on=["ts_code", "pre_%s_date" % days])

        pre_day = self.pre_date(date_list=data[['trade_date']], days=days)

        data_pre = data[['ts_code', 'trade_date', label]]

        data_pre.rename(columns={'trade_date': 'pre_%s_date' % days, label: 'pre_%s_' % days + label}, inplace=True)

        data = data.merge(pre_day, on='trade_date')
        data = data.merge(data_pre, on=['ts_code', 'pre_%s_date' % days], how='left')
        return data

    def pre_label2(self, data, labels, days=1):

        # pre_date = self.pre_date(data[["trade_date"]], days=days)
        # data = data.merge(pre_date, on="trade_date")
        # data = data.merge(data2, on=["ts_code", "pre_%s_date" % days])

        pre_day = self.pre_date(date_list=data[['trade_date']], days=days)

        data_pre = data[['ts_code', 'trade_date', labels]]

        data_pre.rename(columns={'trade_date': 'pre_%s_date' % days, labels: 'pre_%s_' % days + labels}, inplace=True)

        data = data.merge(pre_day, on='trade_date')
        data = data.merge(data_pre, on=['ts_code', 'pre_%s_date' % days], how='left')
        return data

    def list_days(self, data, list_days=20):
        """
        上市天数
        :param data:
        :param list_days:int 上市天数需要大于list_days
        :return:data[["ts_code", "trade_date", "list_days"]],list_days 当前交易日该股票已经上市天数
        """
        data=data[['ts_code','trade_date']]
        if not isinstance(data, pd.DataFrame):
            print("must be dataframe")
            return
        stock_basic=pd.DataFrame()
        for status in list('LDP'):
            df=pro.stock_basic(list_status=status,fields='ts_code,list_date,list_status')
            stock_basic = pd.concat([df,stock_basic],ignore_index=True)
            print(stock_basic.shape)
        # [['ts_code', 'list_date','list_status']]
        print(stock_basic['list_status'].unique())
        data = data.merge(stock_basic, on="ts_code")
        # data["days"] = data.apply(lambda x: (datetime.date(int(x["trade_date"][:4]), int(x["trade_date"][4:6]),
        #                                                    int(x["trade_date"][6:])) - datetime.date(
        #     int(x["list_date"][:4]), int(x["list_date"][4:6]), int(x["list_date"][6:]))).days, axis=1)
        # days为上市日期到交易日期之间的交易日天数
        # data["list_days"] = data.apply(
        #     lambda x: self.days(start_date=x['list_date'], end_date=x['trade_date'], axis=1))
        data["list_days"] = data.apply(
            lambda x: self.days(x['list_date'],end_date=x['trade_date']), axis=1)

        return data[data["list_days"] >= list_days][["ts_code", "trade_date"]]
    def days(self,start_date,end_date=''):
        end_date=datetime.datetime.strptime(end_date, '%Y%m%d') if end_date else datetime.datetime.today()
        start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
        # print(start_date,end_date,(end_date-start_date).days)
        return (end_date-start_date).days


    def avg_up_info(self, data, days):
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
        count = 0
        for i in data["ts_code"].unique():
            t1 = datetime.datetime.now()
            dm = data[data["ts_code"] == i][["ts_code", "trade_date", "amount", "vol"]]
            dm = self.ma(dm, ma=ma)
            # print('ma',list(dm))

            res = pd.concat([dm, res], ignore_index=False)
            count += 1
            # print("%s ma 计算完成" % i, datetime.datetime.now() - t1, count)

        if dis_pct:
            for i in ma[1:]:
                if isinstance(i, int):
                    res["ma1of%s" % i] = res.apply(lambda x: 100 * (x["ma1"] / x["ma%s" % i] - 1), axis=1)
                    if is_abs:
                        # print("abs")
                        res["ma1of%s" % i] = res["ma1of%s" % i].apply(lambda x: abs(x))
        print(datetime.datetime.now() - t)

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

    def pre_data(self, data, label=['trade_date', 'close'], new_label=[], pre_days=1):

        if not new_label:
            new_label = ['pre_%s_%s' % (pre_days, x) for x in label]
        if len(label) != len(new_label):
            print('列名输入有误')
        data = data[set(['ts_code', 'trade_date'] + label)]
        res = pd.DataFrame()
        for ts_code in data['ts_code'].unique():
            df = data[data['ts_code'] == ts_code].sort_values(by='trade_date', ascending=False)
            for i in range(len(label)):
                df[new_label[i]] = df[label[i]].shift(-pre_days)
            res = pd.concat([df, res], ignore_index=True)

        return res

    def \
            up_2(self, data, days=5, up_range=0.5, up_range_top='', pct=1, revise=1, limit=0):
        """
        多日涨停股票，日期重叠要合并日期区间后得到涨停大于等于days的股票
        :param data:
        :param days: int 连续涨停天数
        :param up_range: float,days涨停下限
        :param up_range_top:
        :return: datarame:data       [ts_code trade_date    high pre_n_date  pre_n_close  up_n_pct]
        """
        t1 = datetime.datetime.now()

        data2 = data[["ts_code", "trade_date", "close"]]
        data2.columns = ["ts_code", "pre_%s_date" % days, "pre_%s_close" % days]

        pre_date = self.pre_date(data[["trade_date"]], days=days)
        data = data.merge(pre_date, on="trade_date")
        data = data.merge(data2, on=["ts_code", "pre_%s_date" % days])
        if limit:
            data["up_pct"] = data["close"] / data["pre_%s_close" % days] - 1
        else:
            data["up_pct"] = data["high"] / data["pre_%s_close" % days] - 1
        # data.to_csv("woolpct.csv")
        if up_range:
            data = data[data["up_pct"] >= up_range]
        if up_range_top != '':
            data = data[data["up_pct"] <= up_range_top]

        # data.to_csv("woolup.csv")
        if revise:
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
        if pct:
            label = "pre_n_close" if revise else "pre_%s_close" % days
            data["up_n_pct"] = data["high"] / data[label] - 1
        print("up_data has done,it has %s items" % len(data))
        # data.to_csv("jjk.csv")
        # print("up 计算完成", datetime.datetime.now() - t1)

        return data

    def \
            up_info(self, data, days=5, up_range=0.5, up_range_top='', pct=1, revise=1, limit=0):
        """
        多日涨停股票，日期重叠要合并日期区间后得到涨停大于等于days的股票
        :param data:
        :param days: int 连续涨停天数
        :param up_range: float,days涨停下限
        :param up_range_top:
        :return: datarame:data       [ts_code trade_date    high pre_n_date  pre_n_close  up_n_pct]
        """
        t1 = datetime.datetime.now()

        data2 = data[["ts_code", "trade_date", "close"]]
        data2.columns = ["ts_code", "pre_%s_date" % days, "pre_%s_close" % days]

        pre_date = self.pre_date(data[["trade_date"]], days=days)
        data = data.merge(pre_date, on="trade_date")
        data = data.merge(data2, on=["ts_code", "pre_%s_date" % days])
        if limit:
            data["up_pct"] = data["close"] / data["pre_%s_close" % days] - 1
        else:
            data["up_pct"] = data["high"] / data["pre_%s_close" % days] - 1
        # data.to_csv("woolpct.csv")
        if up_range:
            data = data[data["up_pct"] >= up_range]
        if up_range_top != '':
            data = data[data["up_pct"] <= up_range_top]

        # data.to_csv("woolup.csv")
        if revise:
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
        if pct:
            label = "pre_n_close" if revise else "pre_%s_close" % days
            data["up_n_pct"] = data["high"] / data[label] - 1
        print("up_data has done,it has %s items" % len(data))
        # data.to_csv("jjk.csv")
        # print("up 计算完成", datetime.datetime.now() - t1)

        return data

    def limit_up_info(self, data):
        df = pd.DataFrame()
        for trade_date in data['trade_date'].unique():
            t = pro.stk_limit(trade_date=trade_date)
            df = pd.concat([t[['ts_code', 'trade_date', 'up_limit', 'down_limit']], df])
        df = data.merge(df, on=['ts_code', 'trade_date'])
        df = df[df["close"] == df['up_limit']]
        # df=df[df["close"]==df['down_limit']]

        df.to_csv("limit_up.csv")
        return df[['ts_code', 'trade_date']]

    def revise(self, data, days=5, rekeys="pre_%s%s_close", inplace=True, reverse=True):
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
                        j = i
                i += 1
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

    def history_name(self, keyword='ST|退', start_date='', end_date=''):

        '''
        股票曾用名中，包含指定关键字的股票信息
        :param keyword:需要查找的关键字，或|，
        :param start_date: 查询的开始时间
        :param end_date: 查询的结束日期
        :return: 要顾虑的历史曾用名数据
        '''
        # 判断是有缓存文件存在
        res = pd.DataFrame()
        data = pro.namechange()
        data = data[data['name'].str.contains(keyword) == True]
        end_date = end_date if end_date else datetime.datetime.today().strftime('%Y%m%d')
        data['end_date'] = data['end_date'].fillna(end_date)
        data=data[data['end_date']>=start_date]
        data.to_csv('data33.txt')
        for i in range(data.shape[0]):
            df = pd.DataFrame()
            source = data.iloc[i]
            if source['start_date']<start_date:
                source['start_date']=start_date
            df['trade_date'] = pd.date_range(start=source['start_date'], end=source['end_date'])
            df['trade_date'] = df['trade_date'].astype(str).apply(lambda x: x.replace('-', ''))
            df['ts_code'] = source['ts_code']
            df['name'] = source['name']
            # df=df.loc[(df['trade_date']>=start_date)]
            res = pd.concat([df, res], ignore_index=True)
        return res.drop(columns='name').drop_duplicates()

    def up_times(self, data, period=10, up_times=1, label='ma1', low=0):
        def func(df, low=low):
            return len(df[df >=low])

        data = data[['ts_code', 'trade_date'] + [label]].sort_values(by='trade_date')

        data['%s_up' % label] = data.groupby('ts_code')[label].diff()

        count_times = data.groupby('ts_code')['%s_up' % label].rolling(period, min_periods=up_times).apply(func,
                                                                                                            raw=True)
        count_times.index = count_times.index.droplevel()
        count_times=pd.DataFrame(count_times)
        count_times.rename(columns={'%s_up' % label: 'count_%s' % label}, inplace=True)
        # count_times=count_times[count_times['%s_%s_uptimes'%(label,period)]>=up_times]
        data = data.join(count_times)

        return data[['ts_code','trade_date', 'count_%s' % label]]
