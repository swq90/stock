import numpy as np
import pandas as pd
import time
import datetime
import data.get_data as gd


# p1-p2交易日内，周期为p3至少p4个均价交易日上涨，最后一天 浮动是pre_close 的p5p6之间


# 需求1，12天，10天up，最后易日均价 是第一日均价涨幅在5——15%
# 需求2，12天，9天up,名字不包含st排序，第一up次数，第二涨幅,流通市值<10亿或者市值<200亿


def f2(start_date='20190925', end_date="20191018", period=12, times=9, pct_low=0, pct_high=0.2):
    stocks = []
    # 获取股票代码list
    # if not end_date:
    #     end_date = "20191017"

    # print(end_date)
    # end_date.strftime('%Y%m%d')
    # print(end_date)
    # ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
    # 查询end_date日价格是否满足pct得到list
    stock_name = gd.get_stock_basic()
    stock_name.to_csv("name.csv")
    print(stock_name.shape)
    stock_name = stock_name[stock_name["name"].str.contains("ST") == False]
    print(stock_name.shape)
    stock_name.to_csv("name.csv")

    stock_mv = gd.get_daily_basic(trade_date=end_date, fields='ts_code,total_mv,circ_mv')
    stock_mv.to_csv("mv.csv")
    print(stock_mv.shape)
    stock_mv = stock_mv[(stock_mv["total_mv"]<2000000) | (stock_mv["circ_mv"] < 100000)]
    print(stock_mv.shape)

    stock1 = gd.get_daily(trade_date="20190926")
    stock1 = stock1.eval("avg1=amount/vol", inplace=False)
    stock1 = stock1[["ts_code", "avg1"]]
    print(stock1.shape)

    stock2 = gd.get_daily(trade_date=end_date)
    stock2 = stock2.eval("avg2=amount/vol", inplace=False)
    stock2 = stock2[["ts_code", "avg2"]]
    print(stock2.shape)
    stock1 = stock1.merge(stock2, how="inner", on="ts_code")
    stock1 = stock1.eval("pct=(avg2/avg1-1)", inplace=False)
    # 过滤出涨幅满足条件的
    stock_list = stock1[(stock1["pct"] < 0.2) & (stock1["pct"] > 0.05)]
    print(stock_list.shape)
    # 过滤掉市值不符的
    stock_list = stock_list[stock_list["ts_code"].isin(stock_mv["ts_code"])]
    print(stock_list.shape)
    # 过滤出有st的
    stock_list = stock_list[stock_list["ts_code"].isin(stock_name["ts_code"])][["ts_code", "pct"]]
    print(stock_list.shape)

    # date_list = gd.get_trade_date(start_date=start_date,end_date=end_date,is_open='1')['cal_date']
    # if date_list.count() < period:
    #     return "period is wrong"
    # for day in date_list:
    #     print(day)
    i = 0
    for stock in stock_list['ts_code']:
        df = gd.get_daily(ts_code=stock, start_date=start_date, end_date=end_date)
        # t = df[df['change']>0]['ts_code'].count()
        df = df.eval('avg=amount/vol', inplace=False)
        df = df[["ts_code", "trade_date", "avg"]]
        df = df.sort_values(["trade_date"], ascending=False)

        ts = df["avg"]
        ts.name = "pre_avg"
        ts = ts.drop([0]).reset_index()
        ts = ts.drop(["index"], axis=1)
        df = pd.concat([df, ts], axis=1)

        t = df[df["avg"] - df["pre_avg"] > 0]["ts_code"].count()
        if t >= times:
            stocks.append([stock, t])

        i += 1
        if not i % 180:
            print("has run", i, 'round')

            time.sleep(60)

    # pd.DataFrame(stocks).to_csv(end_date +"5-20%"+ str(period)+"dates" + str(times)+"up"+".csv")
    res = pd.DataFrame(stocks, columns=["ts_code", "up_times"])
    # res.columns.values[0] = "ts_code"
    # res.columns.values[1] = "up_times"
    # res.columns = ["ts_code","up_times"]
    res = res.merge(stock_list, how="inner", on="ts_code")
    res = res.sort_values(["up_times", "pct"], ascending=[False,False])

    # res.to_csv("sort"+end_date+"up"+str(times)+".csv")
    res.set_option("precision", 4)
    print(res)


f2()
