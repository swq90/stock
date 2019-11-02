# 换手率大于1%,股票开头不是688【科创版】,不是st，最后一天换手率大于1%
# 12天内，最近2天没涨停，limit_up in (1,2,3),最后一天涨幅大于5%，小于第一天30%
import datetime

import pandas as pd
import tushare as ts


# avg_n : n日均价
# up_n : n 日上涨
# limit_up_n : n 日涨停

pro = ts.pro_api()

# 提供period，返回period的开始和结束日期(默认结束日期是最近一个交易日
# 提供cal，则返回cal天的交易日日历，
def get_date(start_date="", end_date="", period=0, cal=0):
    today = datetime.datetime.today().date()
    today = str(today)[0:10]
    start_date = '' if start_date is None else start_date
    end_date = today if end_date == '' or end_date is None else end_date
    # ts_code = ts_code.strip().upper() if asset != 'C' else ts_code.strip().lower()
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    # period>0,给定开始日期或者结束日期，返回期间日历的list
    date_list = pro.trade_cal(start_date=start_date, end_date=end_date, is_open=1)["cal_date"]
    if cal:
        if start_date:
            return date_list[:cal].tolist()
        else:
            return date_list[-cal:].tolist()
    if period:
        print(date_list)
        print(type(date_list))
        if start_date:
            date_list = date_list[:period].tolist()
            # return date_list[:period].tolist()
            return date_list[0], date_list[-1]
        else:
            date_list = date_list[-period:].tolist()
            return date_list[0], date_list[-1]
            # return date_list[-period:].tolist()
    # 如果period为空返回周期
    else:
        return date_list.shape[0]
    # if period:
    #
    #     if start_date:
    #         end_date =pro.trade_cal(start_date=start_date,is_open=1)["cal_date"]
    #         return start_date,end_date[period-1]
    #
    #
    #     if end_date:
    #         start_date = pro.trade_cal(end_date=end_date,is_open=1)["cal_date"][-period:]
    #         return start_date.values[0],end_date
    #
    # else:
    #     return pro.trade_cal(start_date=start_date,end_date=end_date,is_open=1).shape[0]


def stock_basic(trade_date="", contain=False, **basic):
    # basic = {'name':'股票名',
    #          'area': '所在地域',
    #          'industry': '所属行业',
    #          'market': '市场类型（主板/中小板/创业板/科创板）',
    #          'exchange': '交易所代码',
    #          'curr_type': '交易货币',
    #          'list_status': '上市状态： L上市 D退市 P暂停上市',
    #          'list_date': '上市日期',
    #          'delist_date': '退市日期',
    #          'is_hs': '是否沪深港通标的，N否 H沪股通 S深股通'}

    stock_basic = pro.stock_basic()
    # print(stock_basic.shape)

    daily_basic = pro.daily_basic(trade_date="20191030")
    # print(daily_basic.shape)

    for key, value in basic.items():
        if key in list(stock_basic):
            stock_basic = stock_basic[stock_basic[key].str.contains(value) == contain]
            # print(stock_basic.shape)
            basic[key] = ""
        elif key in list(daily_basic):
            daily_basic = daily_basic[(daily_basic[key] > value[0]) & (daily_basic[key] < value[1])]
            basic[key] = ""
            # print(daily_basic.shape)

    stock = pd.merge(stock_basic, daily_basic, on="ts_code")["ts_code"]
    # print(stock.shape)
    return stock


# limit_up_times=[涨停次数的下限，上限，周期（3代表period的前三天，-3代表后三天，空代表整个period]
# up_times=[上涨次数的下限，上限，周期（3代表period的前三天，-3代表后三天，空代表整个period]
# pct_chg=

def up_limit_info(start_date="", end_date="", period=1, *, up_limit_times=[]):
    dailys = pd.DataFrame()
    limits = pd.DataFrame()
    date_list = get_date(start_date, end_date, cal=period)
    # print(date_list)
    # print(dailys.shape)
    for date in date_list:
        daily = pro.daily(trade_date=date)
        limit = pro.stk_limit(trade_date=date)
        dailys = pd.concat([daily, dailys], axis=0)
        limits = pd.concat([limit, limits], axis=0)
        # print(dailys.shape)
        # print(limits.shape)
    limit_list = pd.merge(dailys, limits, on=["ts_code", "trade_date"])[["ts_code", "trade_date", "close", "up_limit"]]
    limit_list = limit_list[limit_list["close"] == limit_list["up_limit"]]
    limit_list = limit_list.groupby("ts_code").size().sort_values(ascending=False).reset_index()

    limit_list.columns = ["ts_code", "up_limit_times"]
    limit_list = limit_list[
        (limit_list["up_limit_times"] >= up_limit_times[0]) & (limit_list["up_limit_times"] <= up_limit_times[1])]

    return limit_list


# 均价
    # avg_up_times 均价上涨次数
    # avg_chg_pct 均价上涨幅度,avg/pre_avg-1
# 横盘
#     当天为x，x-m~x-n之间存在high最高的那天为y，
#     x日的close大于总均价的pct，close/all_avg>pct
#     x日的均价小于y日均价的pct
#     y+2~x 之间，每日均价在当期总均价的3%浮动
#     y-s~y-t之间，存在最低的low，使得high(y)>pct*low,
#                               且期间high(s~t)<high(y)

# stable1=[x0:当前日期x0天找到最大值对应y日,x1:y日后x1天至当日,x2：前面时段内每日均价占时段内总均价浮动范围],
def avg_up_info(start_date="", end_date="", period=1, *, avg_chg_pct=0, avg_up_times=0, stable1=[], stable2=[], end_close_pct=0,
                up_range=[], end_y_avg_pct=0):
    res = pd.DataFrame()
    date_list = get_date(start_date=start_date, end_date=end_date, cal=period)
    date_list.append("")
    date_pre = get_date(start_date=start_date, end_date=end_date, cal=period + 1)
    # print(date_list)
    # print(date_pre)
    # date 每一个日和上一个交易日对应
    date = pd.DataFrame({"pre_date": date_pre, "trade_date": date_list})
    date.drop(date.tail(1).index, inplace=True)
    print(date)


    # daily_info 查询period+1日（为了得到第一天的上一日均价）的所有daily信息
    daily_info = pd.DataFrame()
    # 可以改成多线程
    for i in date_pre:
        daily_info = pd.concat([pro.daily(trade_date=i), daily_info])
        # print(daily_info.shape)
    print(list(daily_info))


    daily =daily_info.eval("avg=10*amount/vol")
    print(daily)

    pre_daily = daily[["ts_code", "trade_date", "avg"]]
    pre_daily.columns = ["ts_code", "pre_date", "pre_avg"]

    print(pre_daily)
    # print(daily)
    daily = daily.merge(date, on="trade_date")
    daily = daily.merge(pre_daily, left_on=["ts_code", "pre_date"], right_on=["ts_code", "pre_date"])
    print(daily)

    daily = daily.eval("avg_chg_pct=(avg-pre_avg)/pre_avg")
    daily = daily[['ts_code', 'trade_date', "high", "low", "close", 'vol', 'amount', 'avg', 'pre_avg', 'avg_chg_pct']]
    # 满足均价上涨次数
    if avg_up_times:
        df = daily[daily["avg_chg_pct"] > avg_chg_pct].groupby(by="ts_code").size().sort_values(ascending=False).reset_index()
        # print(df)
        df.columns = list(('ts_code', 'avg_up_times'))
        if type(avg_up_times) == int:

            df = df[(df["avg_up_times"] >= avg_up_times)]
        elif type(avg_up_times) == list:
            df = df[(df["avg_up_times"] >= avg_up_times[0]) & (df["avg_up_times"] <= avg_up_times[1])]

        if res.empty:
            res = df
        else:
            res = res.merge(df, on="ts_code")
        return res

    # print(list(daily))
    # daily_max = daily
    # 如果找某段日期最大值
    if stable1:
        df = pd.DataFrame()
        up_days = date["trade_date"][:stable1[0]]
        # print(up_days)
        daily_max = daily[daily["trade_date"].isin(up_days)]
        # daily_max=daily_max[["ts_code", "avg"]]
        # daily_max=daily_max.groupby(by="ts_code",as_index=False).max()
        # daily_max=daily_max.merge(daily[["ts_code","trade_date","avg"]],on=["ts_code","avg"])
        # print(daily_max.shape)

        daily_max = daily_max[["ts_code", "high"]]
        # print(daily_max.shape)

        daily_max = daily_max.groupby(by="ts_code", as_index=False).max()
        # print(daily_max.shape)

        # 可能会有多天出现相同的high
        daily_max = daily_max.merge(daily[["ts_code", "trade_date", "high", "avg"]], on=["ts_code", "high"])
        # print("fdsf",daily_max[daily_max["ts_code"]=="002705.SZ"])

        # 前段时间每支股票最大值出现对应的日期
        daily_max.sort_values(["trade_date"], ascending=False, inplace=True)

        for i in up_days:
            code = daily_max[daily_max["trade_date"] == i]["ts_code"]
            # print(date_list[date_list.index(i)+stable1[1]:-1])
            df = pd.concat([daily[(daily["ts_code"].isin(code)) & (
                daily["trade_date"].isin(date_list[date_list.index(i) + stable1[1]:-1]))], df])

        # print(list(df))
        df_date = df.groupby("ts_code")["trade_date"].count().reset_index()
        # print("tianshu",df_date[df_date["ts_code"]=="002705.SZ"])

        # print(df_date.shape)
        # 最大值日+2到当体整体均价浮动
        df_all_avg = df[['ts_code', 'vol', 'amount']].groupby("ts_code")["vol", "amount"].sum().reset_index()
        df_all_avg.eval("all_avg=amount/vol", inplace=True)
        df_all_avg = df_all_avg[['ts_code', "all_avg"]]
        df = df.merge(df_all_avg, on="ts_code")
        # print(df["trade_date"].unique())
        # print(df)
        # print("214",df[df["ts_code"]=="002705.SZ"])

        if end_close_pct:
            df.eval("close_avg=close/10/all_avg", inplace=True)
            df_close = df[df["trade_date"] == end_date]
            # print(df_close)
            df_close = df_close[df_close["close_avg"] >= end_close_pct]
            # print("end_close_pct",df_close[df_close["ts_code"] == "002705.SZ"])
            # print(df)
            df = df[df["ts_code"].isin(df_close["ts_code"])]
            # print("fdsf", df[df["ts_code"] == "002705.SZ"])
            #
            print("满足close的df")
            # print(list(df_close))

        if end_y_avg_pct:
            df_close = df_close[["ts_code", "avg"]]
            # print(df_close)
            df_close.columns = ["ts_code", "close_avg"]
            # print(df_close)
            daily_max = daily_max.merge(df_close, on="ts_code")
            daily_max.eval("close_of_avg=close_avg/avg", inplace=True)
            # print("238", daily_max[daily_max["ts_code"] == "002705.SZ"])

            daily_max = daily_max[daily_max["close_of_avg"] <= end_y_avg_pct]
            # print("238", daily_max[daily_max["ts_code"] == "002705.SZ"])

            # print("close", df)
            # print("max",daily_max)
        df.eval("pct_avg=avg/all_avg", inplace=True)
        df = df[(df["pct_avg"] <= 1 + stable1[2]) & (df["pct_avg"] >= 1 - stable1[2])]
        df_dates = df.groupby("ts_code")["trade_date"].size().reset_index()

        # print("天数",df_dates[df_dates["ts_code"]=="002705.SZ"])
        # print("fdsf",df[df["ts_code"]=="002705.SZ"][["ts_code","trade_date","avg","pct_avg","all_avg"]])

        # 满足最大值后两日至当日之间浮动在stable[2]范围内的股票
        df_date = df_date.merge(df_dates, on=["ts_code", "trade_date"])
        # print(df_date.shape, df_date["ts_code"].unique().shape)

        daily_max = daily_max[daily_max["ts_code"].isin(df_date["ts_code"])]

        # print("fdsf",daily_max[daily_max["ts_code"]=="002705.SZ"])

        # print("1", daily_max.shape, daily_max["ts_code"].unique().shape)
        if stable2:
            date_pre2 = get_date(start_date=start_date, end_date=end_date, cal=period + max(stable2[:2]))
            # print(date_pre2)
            date_pre = list(set(date_pre) ^ set(date_pre2))
            # print(date_pre)
            for i in date_pre:
                daily_info = pd.concat([pro.daily(trade_date=i), daily_info])
                # print(daily_info.shape)
            daily_min = pd.DataFrame()
            for i in daily_max["trade_date"].unique():
                # print(i)
                code = daily_max[daily_max["trade_date"] == i]["ts_code"]
                # print(code.shape)
                # 包括y-m,y-n这两天在内
                # print(date_pre2[date_pre2.index(i)-stable2[1]:date_pre2.index(i)-stable2[0]+1])
                daily_min = pd.concat([daily_info[(daily_info["ts_code"].isin(code) == True) & (
                    daily_info["trade_date"].isin(
                        date_pre2[date_pre2.index(i) - stable2[1]:date_pre2.index(i) - stable2[0] + 1]))], daily_min])
                # print(daily_min["ts_code"].unique().shape)
            # print(list(daily_min))
            # daily_min.eval("avg=amount/vol",inplace=True)
            daily_min = daily_min[["ts_code", "low", "high"]]
            daily_min.columns = ["ts_code", "low", "pre_high"]
            daily_min_high = daily_min[["ts_code", "pre_high"]].groupby(by="ts_code", as_index=False).max()

            daily_min_low = daily_min[["ts_code", "low"]].groupby(by="ts_code", as_index=False).min()
            # print("sf", daily_min[daily_min["ts_code"] == "002705.SZ"])
            daily_min = pd.merge(daily_min_high, daily_min_low, on="ts_code")

            # daily_min.columns=["ts_code","low"]
            # print(daily_min.shape)
            daily_min = daily_min.merge(daily_max, on="ts_code")
            daily_min_ = daily_min[daily_min["pre_high"] < daily_min["high"]]
            # print("2", daily_min)
            daily_min.eval("max_chg=high/low", inplace=True)
            # print("sf", daily_min[daily_min["ts_code"] == "002705.SZ"])

            daily_min = daily_min[daily_min["max_chg"] >= stable2[2]]
            # print("sf", daily_min[daily_min["ts_code"] == "002705.SZ"])

            # print(daily_min)
            return daily_min



# t = get_date(start_date="20191001",cal=5)
# s = pro.daily(ts_code="002543.SZ", start_date="20191001", end_date="20191010")
# s = s.eval('avg=amount/vol', inplace=False)
# pd.set_option('display.max_columns', None)
# print(s)
# print(list(s))
#

# t = up_limit_info(period=3,up_limit_times=[2,float('inf')])
# print(t)
# search_time=datetime.datetime.strftime(str(datetime.datetime.today().date())+"17:00","%Y-%M-%D%H:%M")
# print(search_time)
today = datetime.datetime.today().date()
print(today)
today = str(today)[0:10]
# name = stock_basic(name="st|ST", market="科创板")
#
#
# t = avg_up_info(end_date="20191031", period=12, stable1=[-3, 2, 0.03], stable2=[1, 20, 1.15], end_close_pct=0.99,
#                 end_y_avg_pct=1.01)
# t = t[t["ts_code"].isin(name)]
# # # print(t["ts_code"].unique().shape)
# t.to_csv(today + "2stable.txt")
# t=avg_up_info(period=12,avg_up_times=9)
# print(t)
# t = t[t["ts_code"].isin(name)]
# t.to_csv(today+"2up9-12.txt",sep='\t')


m=ts.pro_bar(ts_code="600223.SH",start_date="20190828",ma=[1,])

print(m)


#