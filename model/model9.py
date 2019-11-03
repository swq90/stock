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


def ma_info(start_date="", end_date="", ma=5, *, stable_days=10, stable_times_pct=0.95, up_days=5,up_times=1,
            stable_pct=[1.005, 0.995]):
    # res = pd.DataFrame()
    cal = stable_days + up_days
    date_list = get_date(start_date=start_date, end_date=end_date, cal=cal)
    # temp = [""] * ma
    # date_list.extend(temp)
    date_pre = get_date(start_date=start_date, end_date=end_date, cal=cal + ma)

    # daily
    daily = pd.DataFrame()
    count = 0
    for i in date_pre:
        daily = pd.concat(
            [pro.daily(trade_date=i), daily])
        count += 1
        if count % 190 == 0:
            time.sleep(60)
    print(list(daily))
    print(daily.info())

    # def get_ma(df):
    #     df=df.groupby("ts_code")["vol","amount"]
    #     print(df)
    #
    # get_ma(daily)
    ma_data = pd.DataFrame()
    # for code in daily["ts_code"].unique():
    #      m = daily[daily["ts_code"]==code][["trade_date","amount","vol"]]
    for d in date_list:

        # print(date_pre[date_pre.index(d)-ma+1:date_pre.index(d)+1])
        m = daily[daily["trade_date"].isin(date_pre[date_pre.index(d) - ma + 1:date_pre.index(d)+1])]
        # print("m",m[m["ts_code"]=='002638.SZ' ])
        m = m.groupby("ts_code")["vol", "amount"].sum().reset_index()
        t = [d] * m.shape[0]
        m["trade_date"] = pd.DataFrame(t)
        m["ma"] = m["amount"] * 10 / m["vol"]


        ma_data = pd.concat([m[["ts_code", "trade_date", "ma"]], ma_data])
    # print(daily[daily["ts_code"] == "002417.SZ"])
    daily = daily.merge(ma_data, on=["ts_code", "trade_date"])
    print(daily)

    daily.dropna()

    # print(daily.shape)
    pd.set_option('display.max_columns', None)
    return daily

std={"rg":[1,-1.5],"ccp":[-3,0,-1]}

def func(x):

    # print("aaaaaaaa",x)

    return


def mark(df,**kwargs):
    for k,v in kwargs.items():
        if k == "rg":
            print(df)
            # df["rg"] = df.aaaapply(func);
            # df["rg"] = df.apply(func);
            # df["rg"] = df.apply(lambda x: func(x))
            # df["rg"] = df.apply(lambda x: v[0] if x["close"] - x["open"] >= 0 else v[1], axis=1)
            # print(df)
        if k == "ccg":
            df["ccg"]=df.apply(lambda x:v[0] if x["pct_chg"]>=5 else v[1])
            df[df["ccg"]] = df.apply(lambda x:func(x))

            pass


    # return df

t = ma_info()
s = mark(t,rg=[1,-1.5])

