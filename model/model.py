# 当天为n，n日的high和n-5日的close满足high/close-1>=0.4
# 满足条件的n还满足n-60天内有交易
# 返回满足条件的股票代码和对应日期

# 对出来的数据做分类，对大数据量多的样本做分析
#
# # 查询日期，获取数据，数据处理 ma，计算涨停信息，涨停pct分布,涨停次数分布


import datetime
import time
import pandas as pd
import tushare as ts

pro = ts.pro_api()


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


def get_data():
    return


def avg_up_info(start_date="", end_date="", ma=5, period=5, cal=8, *, up_pct=0.5, start_p="close", end_p="high",
                list_days=60):
    res = pd.DataFrame()
    date_list = get_date(start_date=start_date, end_date=end_date, cal=cal)
    temp = [""] * period
    date_list.extend(temp)
    date_pre = get_date(start_date=start_date, end_date=end_date, cal=cal + period)

    date = pd.DataFrame({"pre_date": date_pre, "trade_date": date_list})
    date.drop(date.tail(period).index, inplace=True)
    print(date)
    daily_info = pd.DataFrame()
    count = 0
    for i in date_pre:
        daily_info = pd.concat([pro.daily(trade_date=i), daily_info])
        count += 1
        if count % 190 == 0:
            time.sleep(60)

    daily_info=daily_info.merge(date,on="trade_date")

    # 过滤上市日期不符合的股票
    # data['交易时间'] = pd.to_datetime(data['交易时间'])
    # print(daily)
    stock_basic = pro.stock_basic()[['ts_code', 'list_date']]
    # daily["trade_date_new"]=pd.to_datetime(daily["trade_date"])
    # # daily= daily[["trade_date"]]

    # daily.eval("s=trade_date_new+1")

    daily_info = daily_info.merge(stock_basic, on="ts_code")
    # daily["day"]= daily["trade_date"].apply(
    #     lambda x: (datetime.date(int(x[:4]),int(x[4:6]),int(x[6:]))
    #                                    - datetime.timedelta(days=35)) )
    daily_info["days"] = daily_info.apply(
        lambda row: (datetime.date(int(row["trade_date"][:4]), int(row["trade_date"][4:6]), int(row["trade_date"][6:]))
                     - datetime.date(int(row["list_date"][:4]), int(row["list_date"][4:6]),
                                     int(row["list_date"][6:]))).days, axis=1)


    daily = daily_info[daily_info["days"] > list_days]
    print(daily_info.shape)
    ma_data = pd.DataFrame()
    # for code in daily["ts_code"].unique():
    #      m = daily[daily["ts_code"]==code][["trade_date","amount","vol"]]
    for d in date["trade_date"]:
        # print(date_pre[date_pre.index(d)-ma+1:date_pre.index(d)+1])
        m = daily_info[daily_info["trade_date"].isin(date_pre[date_pre.index(d) - ma + 1:date_pre.index(d) + 1])]
        # print("m",m[m["ts_code"]=='002638.SZ' ])
        m = m.groupby("ts_code")["vol", "amount"].sum().reset_index()
        t = [d] * m.shape[0]
        m["trade_date"] = pd.DataFrame(t)
        m["ma"] = m["amount"] * 10 / m["vol"]

        ma_data = pd.concat([m[["ts_code", "trade_date", "ma"]], ma_data])
    daily_info = daily_info.merge(ma_data, on=["ts_code", "trade_date"])
    daily_info["avg"] = daily_info["amount"] * 10 / daily_info["vol"]
    print(daily_info.shape)

    date_list = get_date(start_date=start_date, end_date=end_date, cal=cal)
    temp = [""] * 1
    date_list.extend(temp)
    date_pre = get_date(start_date=start_date, end_date=end_date, cal=cal + 1)

    date = pd.DataFrame({"pre_1date": date_pre, "trade_date": date_list})
    date.drop(date.tail(1).index, inplace=True)
    daily_info = daily_info.merge(date, on="trade_date")

    pre_data = daily_info[["ts_code", "trade_date", "avg", "ma"]]
    pre_data.columns = ["ts_code", "pre_1date", "pre_avg", "pre_ma"]
    daily_info = daily_info.merge(pre_data, on=["ts_code", "pre_1date"])

    daily_info=daily_info.drop(["list_date","days","pre_1date"], axis=1)



    return  daily_info


def avg_up_time( all_data, cal=5):
    # daily_info["avg"]=daily_info["amount"]*10/daily_info["vol"]
    all_data["up_avg"] = all_data.apply(lambda x: 1 if x["avg"] - x["pre_avg"] > 0 else 0, axis=1)
    all_data["up_ma"] = all_data.apply(lambda x: 1 if x["ma"] - x["pre_ma"] > 0 else 0, axis=1)
    all_data["low>ma"]=all_data.apply(lambda x:1 if x["low"]>x["ma"] else 0,axis=1)
    daily = pd.DataFrame()
    for date in all_data["pre_date"].unique():
        df= all_data[all_data["pre_date"]==date][["ts_code","trade_date"]]
        date_list = get_date(end_date=date, cal=cal)
        data = all_data[
            (all_data["ts_code"].isin(df["ts_code"])) & all_data["trade_date"].isin(
                date_list)]
        data = data.groupby("ts_code")["up_avg", "up_ma","low>ma"].sum().reset_index()
        df= df.merge(data,on="ts_code")

        daily=pd.concat([df,daily])
    print(daily)


    return daily


pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)
all_data = avg_up_info(cal=380, up_pct=0.5)

# all_data = all_data[all_data["ts_code"].isin(up_data["ts_code"].unique())]
#
up_data=avg_up_time(all_data)
up_data.to_csv("所有数据五天分布")
#
# print(up_data.shape)
# print(all_data.shape)
for item in list(up_data)[-3:]:
    # print(item,up_data.groupby(item)["ts_code"].size())
    d=pd.DataFrame(up_data.groupby(item)["ts_code"].size())

    d.columns=[item]

    d["概率"]=d[item]/d[item].sum()
    print(d)
# up_data.to_csv("五天上涨超50.csv")
# up_data[["ts_code"]].to_csv("导入五天上涨50.txt")
# all_data.to_csv("all_data.txt")

#
# # 查询日期，获取数据，数据处理 ma，计算涨停信息，涨停pct分布,涨停次数分布
#
# t = pro.daily(ts_code="603022.SH",start_date="20190825")
# # print(t)
# ma(t)