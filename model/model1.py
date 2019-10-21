# 某股票需要满足：在p1时间段内，超过p2%的交易日*均价上涨，且close>open,满足(high-close)/(close-open)>p3
# 1.所有股票代码 。2.
# 10天内8次上涨，下引线比例大于0.3

import pandas as pd
import time
import datetime
import data.get_data as gd

def f1(start_date = '20190928',end_date="20191021", period=10, times=8, pct=0.3):
    stocks = []
    # 获取股票代码list
    # if not end_date:
    #     end_date = "20191017"

    # print(end_date)
    # end_date.strstrftime('%Y%m%d')
    # print(end_date)
    #['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
    # 查询end_date日价格是否满足pct得到list

    stock_name = gd.get_stock_basic()
    stock_name = stock_name[stock_name["name"].str.contains("ST") == False]
    stock_list = gd.get_daily(trade_date=end_date)
    # 红引线
    stock_list = stock_list[stock_list["open"]<stock_list["close"]]
    # stock_list = stock_list[stock_list["close"]>stock_list["pre_close"]]
    # 当天close>pre_close
    # stock_list = stock_list[stock_list["change"]>0]

    # print(stock_list.shape)
    # print(stock_list.head())
    # stock_list = stock_list.eval('pct = (high-close)/(close-open)',inplace=False)
    stock_list = stock_list.eval('pct=(open-low)/(high-low)',inplace=False)
    stock_list = stock_list[stock_list["pct"] >= pct]
    print(stock_list.shape)
    stock_basic=gd.get_daily_basic(trade_date=end_date)
    stock_basic.to_csv("bbb.csv")
    stock_basic = stock_basic[(stock_basic["turnover_rate_f"]>2)&(stock_basic["turnover_rate"]>1)]
    stock_basic = stock_basic[(stock_basic["total_mv"] < 2000000) | (stock_basic["circ_mv"] < 200000)]
    stock_list = stock_list.merge(stock_basic,how="inner",on="ts_code")
    stock_list = stock_list[stock_list["ts_code"].isin(stock_name["ts_code"])][["ts_code", "pct"]]
    # stock_list.to_csv("f1.csv")

    print(stock_basic.shape)
    print(stock_list.shape)


    # date_list = gd.get_trade_date(start_date=start_date,end_date=end_date,is_open='1')['cal_date']
    # if date_list.count() < period:
    #     return "period is wrong"
    # for day in date_list:
    #     print(day)

    i = 0
    for stock in stock_list['ts_code']:
        df = gd.get_daily(ts_code=stock,start_date=start_date,end_date=end_date)
        # t = df[df['change']>0]['ts_code'].count()
        df = df.eval('avg=amount/vol',inplace=False)
        df = df[["ts_code","trade_date","avg"]]
        df = df.sort_values(["trade_date"], ascending=False)

        ts = df["avg"]
        ts.name = "pre_avg"
        ts = ts.drop([0]).reset_index()
        ts = ts.drop(["index"], axis=1)
        df = pd.concat([df, ts], axis=1)


        t = df[df["avg"]-df["pre_avg"]>0]["ts_code"].count()

        if t >= times:
            stocks.append(stock)
        i+=1
        if not i%180:
            print("has run", i, 'round')
            time.sleep(60)

    pd.DataFrame(stocks).to_csv(end_date+'p'+str(period)+"up"+str(pct)+".csv")
    return stocks


    # days = 0
    #
    #
    # while times:
    #     end_date = datetime.datetime.today() - datetime.timedelta(days=days)
    #     date = end_date.strftime('%Y%m%d')
    #     days += 1
    #
    #     # ('trade_cal', start_date=date, end_date=date, fields='is_open').values[0][0]
    #         # times -= 1
    #
    #
    #
    # for i in stock_list['ts_code']:
    #     pass



    # if stock_list:
    #     return "nothing"


    # 迭代list
    # 单只股票period内


# p1-p2交易日内，至少p3个交易日上涨，最后一天 浮动大于前一天
def f2(start_date = '20190925',end_date="20191018", period=12, times=10, pct_low=0,pct_high=0.2):
    stocks = []
    # 获取股票代码list
    # if not end_date:
    #     end_date = "20191017"

    # print(end_date)
    # end_date.strstrftime('%Y%m%d')
    # print(end_date)
    # ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
    # 查询end_date日价格是否满足pct得到list
    stock_list = gd.get_daily(trade_date=end_date)
    # 红线
    stock_list = stock_list[stock_list["open"] < stock_list["close"]]
    # stock_list = stock_list[stock_list["close"]>stock_list["pre_close"]]
    # stock_list = stock_list[stock_list["change"] > 0]

    print(stock_list.shape)
    # print(stock_list.head())
    stock_list = stock_list.eval('rate = (high-low)/pre_close', inplace=False)

    stock_list = stock_list[(stock_list["rate"] < pct_high)&(stock_list["rate"]>pct_low)][["ts_code"]]

    print(stock_list.shape)

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
        print(t)
        if t >= times:
            stocks.append(stock)
        i += 1
        if not i % 180:
            print("has run", i, 'round')
            time.sleep(60)

    pd.DataFrame(stocks).to_csv("h-l"+end_date + 'p' + str(period) + ".csv")
    return stocks

def f3():
   pass

f1()