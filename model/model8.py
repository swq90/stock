# 当天为n，n日的high和n-5日的close满足high/close-1>=0.4
# 满足条件的n还满足n-60天内有交易
# 返回满足条件的股票代码和对应日期
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


def avg_up_info(start_date="", end_date="", period=5, cal=8, *, up_pct=0.4, start_p="close", end_p="high",
                list_days=60):
    res = pd.DataFrame()
    date_list = get_date(start_date=start_date, end_date=end_date, cal=cal)
    temp = [""] * period
    date_list.extend(temp)
    date_pre = get_date(start_date=start_date, end_date=end_date, cal=cal + period)

    # date_pre=date_pre[:-5]
    # print(date_list)
    # print(date_pre)
    date = pd.DataFrame({"pre_date": date_pre, "trade_date": date_list})
    date.drop(date.tail(5).index, inplace=True)
    # print(date)

    # daily_info
    daily_info = pd.DataFrame()
    count = 0
    for i in date_pre:
        daily_info = pd.concat([pro.daily(trade_date=i)[['ts_code', 'trade_date', 'high', 'close']], daily_info])
        count += 1
        if count % 190 == 0:
            time.sleep(60)
    # print(daily_info)

    print(time.time(),"download")
    daily = daily_info[['ts_code', 'trade_date', 'high']]
    # print(daily)
    daily = daily.merge(date, on="trade_date")
    # print(list(daily),daily.shape)
    # print(daily)
    daily_pre = daily_info[['ts_code', 'trade_date', 'close']]
    daily_pre.columns = ['ts_code', 'pre_date', 'close']

    daily = daily.merge(daily_pre, left_on=["ts_code", "pre_date"], right_on=['ts_code', 'pre_date'])

    # print(list(daily),daily.shape)
    # print(daily)
    formula = "pct=" + str(end_p) + "/" + str(start_p) + "-1"
    daily.eval(formula, inplace=True)
    daily = daily[daily["pct"] >= up_pct]
    print(time.time(),"pct")

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
    # print(daily)
    print(daily.shape)

    # print(stock_basic)
    # print(daily.info())
    return daily

start=time.time()
t = avg_up_info(cal=365)
t.to_csv("limit_up40%.txt", sep='\t')
end=time.time()
print("running time:",end-start)