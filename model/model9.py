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


def ma_info(start_date="", end_date="", ma=5, *, stable_days=20, stable_times_pct=0.95, up_days=5,up_times=1,
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
    daily = daily.merge(ma_data, on=["ts_code", "trade_date"])
    df_on=daily[(daily["trade_date"].isin(date_list[-up_days:]))&(daily["low"]>daily["ma"])].groupby("ts_code")["trade_date"].size().reset_index()

    daily=daily[daily["ts_code"].isin(df_on["ts_code"])]
    daily["end_ma_pct"]=(daily["close"]/daily["ma"]-1)*100

    pd.set_option('display.max_columns', None)
    pd.set_option('max_colwidth', 30)

    pd.set_option('display.width', None)

    return daily,date_list

# 最近5天红线1分，绿线-1.5，rg
# low大于ma5得3分，u_low>ma
# 收盘涨幅大于5%-1分，低于-3%-3分，up_ccg
# 同时回溯最近20天，
# high低于均线-2分，low高于ma5不计分，其他得1分(踩均线)，
# 收盘低于-5%-3分，低于-3%-1分，大于3%-1分，大于5%-2分。ccg
def func(x):
    if x > 5: return -3
    if x > 3: return -1
    if x < -5:return -3
    if x < -3:return -1
    else:return 0

def func2(x):
    if x > 5: return -6
    if x > 3: return -2
    if x < -5:return -6
    if x < -3:return -2
    else:return 0



s1=["rg","up_ccg","u_low>max",]
s2=["high_max_low","20ccg"]
def mark(df,s):

    for k in s:
        if k == "rg":
            # print(df)
            # df["rg"] = df.aaaapply(func);
            # df["rg"] = df.apply(func);
            # df["rg"] = df.apply(lambda x: func(x))
            # df["rg"] = df.apply(lambda x: v[0] if x["close"] - x["open"] >= 0 else v[1], axis=1)
            # print(df)
            df["rg"] = df.apply(lambda x: 1 if x["close"] - x["open"] >= 0 else 0, axis=1)

        if k == "up_ccg":

            df["up_ccg"]=df["pct_chg"].apply(func)
        if k=="u_low>max":
            df["u_low>max"]=df.apply(lambda x:6 if x["low"]>=x["ma"] else 0,axis=1)
        if k == "high_max_low":
            df["high_max_low"]=df.apply(lambda x: -6 if x["high"]<x["ma"] else -6
            if x["low"]>x["ma"] else 1,axis=1)
        if k=="20ccg":
            df["20ccg"]=df["pct_chg"].apply(func2)
    # print(df[df["ts_code"]=="002417.SZ"])
    print(df[df["ts_code"]=="603022.SH"])
    # 603022,600223
    m= df.groupby("ts_code")[s].sum().reset_index()
    print("m",m)
    # for


    return m



    # return df

t,date_list = ma_info()

res1=t[t["trade_date"].isin(date_list[-5:])]
res2=t[t["trade_date"].isin(date_list[:-5])]
score1 = mark(res1,s1)
score2= mark(res2,s2)

score=pd.merge(score1,score2,on="ts_code")
# print("gexiang",score)
score["score"]=score[["rg","up_ccg","u_low>max","high_max_low","20ccg"]].apply(lambda x:x.sum(),axis=1)
score =score.sort_values(by="score",ascending=False).reset_index(drop=True)


print(score.head(10))

score.head(200).to_csv("stable.txt")



# print(s.groupby("ts_code")["trade_date"].size().sort_values(ascending=False).reset_index())

