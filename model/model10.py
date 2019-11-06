# 当天为n，n日的high和n-5日的close满足high/close-1>=0.4
# 满足条件的n还满足n-60天内有交易
# 返回满足条件的股票代码和对应日期

# 对出来的数据做分类，对大数据量多的样本做分析
#


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

    # daily_info["avg"]=daily_info["amount"]*10/daily_info["vol"]
    daily = daily_info[['ts_code', 'trade_date', 'high']]
    # print(daily)
    daily = daily.merge(date, on="trade_date")
    # print(list(daily),daily.shape)
    # print(daily)
    daily_pre = daily_info[['ts_code', 'trade_date', 'close']]
    daily_pre.columns = ['ts_code', 'pre_date', 'close']

    daily = daily.merge(daily_pre, left_on=["ts_code", "pre_date"], right_on=['ts_code', 'pre_date'])


    formula = "pct=" + str(end_p) + "/" + str(start_p) + "-1"
    daily[formula] = daily[end_p] / daily[start_p] - 1
    daily = daily[daily[formula] >= up_pct]


    print("aaaaa",daily)
    del_rept_date=pd.DataFrame()
    for code in daily["ts_code"].unique():

        df = daily[daily["ts_code"]==code][["ts_code","trade_date","pre_date"]].sort_values(by="trade_date",ascending=False).reset_index(drop=True)
        i,j= 1,0
        print(df)
        while len(df)> i:
            if df.iloc[[i],[1]].values >= df.iloc[[j],[2]].values:
                df.iloc[[j], [2]] = df.iloc[[i], [2]].values
                df.iloc[[i],[0]]=[""]
            else:
                j += 1
            i+=1
        print(df)
        del_rept_date=pd.concat([df[df["ts_code"]!=""],del_rept_date])
    print(del_rept_date)
    daily.drop(["pre_date"],axis=1,inplace=True)
    daily=daily.merge(del_rept_date,on=["ts_code","trade_date"])
    print(daily)






    # 过滤上市日期不符合的股票
    # data['交易时间'] = pd.to_datetime(data['交易时间'])
    # print(daily)
    stock_basic = pro.stock_basic()[['ts_code', 'list_date']]
    # daily["trade_date_new"]=pd.to_datetime(daily["trade_date"])
    # # daily= daily[["trade_date"]]

    # daily.eval("s=trade_date_new+1")

    daily = daily.merge(stock_basic, on="ts_code")
    # daily["day"]= daily["trade_date"].apply(
    #     lambda x: (datetime.date(int(x[:4]),int(x[4:6]),int(x[6:]))
    #                                    - datetime.timedelta(days=35)) )
    daily["days"] = daily.apply(
        lambda row: (datetime.date(int(row["trade_date"][:4]), int(row["trade_date"][4:6]), int(row["trade_date"][6:]))
                     - datetime.date(int(row["list_date"][:4]), int(row["list_date"][4:6]),
                                     int(row["list_date"][6:]))).days, axis=1)
    # print(t)
    print(daily.shape)

    daily = daily[daily["days"] > list_days]
    print(daily.shape,"listday>60")

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

    date_list = get_date(start_date=start_date, end_date=end_date, cal=cal)
    temp = [""] * 1
    date_list.extend(temp)
    date_pre = get_date(start_date=start_date, end_date=end_date, cal=cal + 1)

    date = pd.DataFrame({"pre_1date": date_pre, "trade_date": date_list})
    print(date,"pre1day")
    date.drop(date.tail(1).index, inplace=True)
    daily_info = daily_info.merge(date, on="trade_date")
    # daily_pre = daily_info.merge(date, on="trade_date")

    pre_data = daily_info[["ts_code", "trade_date", "avg", "ma"]]
    pre_data.columns = ["ts_code", "pre_1date", "pre_avg", "pre_ma"]
    daily_info = daily_info.merge(pre_data, on=["ts_code", "pre_1date"])
    daily_info.drop(["pre_1date"], axis=1,inplace=True)
    daily.drop(["list_date","days"],axis=1,inplace=True)

    return daily, daily_info


def avg_up_time(up_data, all_data, cal=5):
    print("up",up_data)
    # daily_info["avg"]=daily_info["amount"]*10/daily_info["vol"]
    all_data["up_avg"] = all_data.apply(lambda x: 1 if x["avg"] - x["pre_avg"] > 0 else 0, axis=1)
    all_data["up_ma"] = all_data.apply(lambda x: 1 if x["ma"] - x["pre_ma"] > 0 else 0, axis=1)
    all_data["low>ma"]=all_data.apply(lambda x:1 if x["low"]>x["ma"] else 0,axis=1)

    print(all_data)
    daily = pd.DataFrame()
    for date in up_data["pre_date"].unique():
        df= up_data[up_data["pre_date"]==date]["ts_code","trade_date"]
        date_list = get_date(end_date=date, cal=cal)
        data = all_data[
            (all_data["ts_code"].isin(df["ts_code"])) & all_data["trade_date"].isin(
                date_list)]
        data = data.groupby("ts_code")["up_avg", "up_ma","low>ma"].sum().reset_index()

        df= df.merge(data,on="ts_code")
        daily=pd.concat([df,daily])


    return daily


pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)
up_data, all_data = avg_up_info(cal=15, up_pct=0.5)

all_data = all_data[all_data["ts_code"].isin(up_data["ts_code"].unique())]

up_data=avg_up_time(up_data, all_data)



for item in list(up_data)[-3:]:
    # print(item,up_data.groupby(item)["ts_code"].size())
    d = pd.DataFrame(up_data.groupby(item)["ts_code"].size())

    d.columns = [item]

    d["概率"] = d[item] / d[item].sum()
    print(d)



# up_data.to_csv("五天上涨超50.csv")
# up_data[["ts_code"]].to_csv("导入五天上涨50.txt")
# all_data.to_csv("all_data.txt")
