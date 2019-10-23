# p=12,0<up<=2,sort by up_range,end_date close<limit_up
import datetime
import time

import pandas as pd
import tushare as ts

pro = ts.pro_api()

# 给定首尾时间，返回周期数
# 给定周期，开始时间，返回结束日期
# 给定周期，结束时间，返回开始日期
def get_date(start_date="",end_date="",period="",cal=0):
    today= datetime.datetime.today().date()
    today = str(today)[0:10]
    start_date = '' if start_date is None else start_date
    end_date = today if end_date == '' or end_date is None else end_date
    # ts_code = ts_code.strip().upper() if asset != 'C' else ts_code.strip().lower()
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    # 返回cal日期的list
    if cal:
        date_list = pro.trade_cal(start_date=start_date,end_date=end_date,is_open=1)["cal_date"]
        if start_date:
            return date_list[:cal].tolist()

        return date_list[-cal:].tolist()
    if period:

        if start_date:
            end_date =pro.trade_cal(start_date=start_date,is_open=1)["cal_date"]
            return start_date,end_date[period-1]


        if end_date:
            start_date = pro.trade_cal(end_date=end_date,is_open=1)["cal_date"][-period:]
            return start_date.values[0],end_date

    else:
        return pro.trade_cal(start_date=start_date,end_date=end_date,is_open=1).shape[0]


def get_avg_up(ts_code,start_date,end_date,avg_up_times):
    # 以后加上日期判定,然后把调用时的recent+1还原

    pct_up = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)[["ts_code","trade_date","amount","vol"]]
    if pct_up.empty:
        return
    # print(pct_up)
    pct_up = pct_up.eval('avg=amount/vol', inplace=False)
    pct_up = pct_up[["ts_code", "trade_date", "avg"]]
    pct_up = pct_up.sort_values(["trade_date"], ascending=False)

    ts = pct_up["avg"]
    ts.name = "pre_avg"
    ts = ts.drop([0]).reset_index(drop=True)
    # ts = ts.drop(["index"], axis=1)
    pct_up = pd.concat([pct_up, ts], axis=1)
    t = pct_up[pct_up["avg"] - pct_up["pre_avg"] > 0]["ts_code"].count()

    if t >= avg_up_times:
        # print("avg",[ts_code, t])
        return [ts_code, t]
    return

# 返回df形式的涨停股票
def get_limit_up(trade_date=""):
    daily = pro.daily(trade_date=trade_date)[["ts_code", "trade_date", "close"]]
    limit = pro.stk_limit(trade_date=trade_date)[["ts_code", "trade_date", "up_limit"]]
    # print(daily.shape, daily.shape)
    daily = daily.merge(limit, on="ts_code")
    daily = daily[daily["close"] == daily["up_limit"]]
    # print(daily.shape)
    return daily

def get_pct_up(type=5,start_date="",end_date="",period=10,pct=0.01):

    pass


def f4(start_date="",end_date="",period=5,nud=1,*range_up,recent,avg_up_times,up_times,range_low):
    start_date,end_date=get_date(period=period)
    file_name=end_date+str(period)+"range"+str(range_low)+"nud"+str(nud)+"r"+str(recent)+"cup"+str(avg_up_times)

    # df
    df = pd.DataFrame()
    # 名字中不包含st的股票
    stock_name = pro.stock_basic()
    stock_name = stock_name[stock_name["name"].str.contains("ST") == False]
    # period-nud中所有存在涨停的股票

    date_list_y = get_date(start_date=start_date,cal=period-nud)

    for trade_date in date_list_y:
        daily_up=get_limit_up(trade_date=trade_date)
        df = pd.concat([daily_up,df],axis=0)
        # print(df.shape)

    df = df.groupby("ts_code").size().sort_values(ascending=False).reset_index()
    print(df.shape)
    df.columns = list(('ts_code', 'times'))
    # 满足涨停次数股票
    df = df[(df["times"]>0) & (df["times"]<=up_times)]
    print("df-up<=times",df.shape)
    # nud没有涨停
    nud_list=pd.DataFrame()
    date_list_n = get_date(cal=nud)

    for trade_date in date_list_n:
        daily_up_n=get_limit_up(trade_date=trade_date)
        nud_list= pd.concat([daily_up_n,nud_list],axis=0)
        print(nud_list.shape)
    nud_list= nud_list["ts_code"].unique()


    # 过滤nud没涨停的股票
    df = df[df["ts_code"].isin(nud_list)==False]
    print(df.shape)

    # 过滤名字
    df = df[df["ts_code"].isin(stock_name["ts_code"])]
    print("df-st", df.shape)




    # 首尾两日的涨幅
    df_start = pro.daily(trade_date=start_date)[["ts_code","close"]]
    df_end = pro.daily(trade_date=end_date)[["ts_code","close"]]
    df_end.columns = list(('ts_code', "end_close"))
    df_end= df_end.merge(df_start,on="ts_code")
    df_pct = df_end.eval("pct=end_close/close-1",inplace=False)
    df = df.merge(df_pct,on="ts_code")
    df = df[df["pct"]>=range_low]

    df["pct"] = df["pct"].map(lambda x: ('%.2f') % x)
    df.drop(["end_close","close"],axis=1,inplace=True)
    df = df.reset_index(drop=True)
    print("df-pct", df.shape, list(df))

    stocks=[]
    i = 0
    # 周期内均价上涨，首尾日期
    rs, re = get_date(period=recent+1)

    # 逐条查询满足均价上涨的股票
    for stock in df['ts_code']:
        data = get_avg_up(ts_code=stock,start_date=rs,end_date=re,avg_up_times=avg_up_times)
        print(data)
        if data:
            stocks.append(data)
        i += 1
        if not i % 180:
            print("has run", i, 'round')
            time.sleep(60)
    stock_avg_up = pd.DataFrame(stocks,columns=["ts_code","avg_up_times"])

    df = df.merge(stock_avg_up,on="ts_code")
    df = df.sort_values(["pct"], ascending=False)
    print(df)




    df.to_csv(file_name+".csv")


def f5(period ,avg_up_times,mv=0):
    rs, re = get_date(period=period + 1)
    print(rs,re)
    # mv= [cir_mv_low，cir_mv_low，total_mv_low,total_mv_up]

    stocks =[]

    stock_list = pro.daily(trade_date=re)
    print(stock_list.shape)
    stock_name = pro.stock_basic()
    stock_name = stock_name[stock_name["name"].str.contains("ST") == False]
    print(stock_name.shape)
    stock_list = stock_list[stock_list["ts_code"].isin(stock_name["ts_code"])]
    print(stock_list.shape)

    if mv:
        stock_mv = pro.daily_basic(trade_date=re, fields='ts_code,total_mv,circ_mv')
        stock_mv = stock_mv[(stock_mv["total_mv"] <= mv/10000)]
        stock_list = stock_list[stock_list["ts_code"].isin(stock_mv["ts_code"])]
    print(stock_list.shape)
    i = 0
    for stock in stock_list["ts_code"]:
        # print(stock)
        data = get_avg_up(ts_code=stock,start_date=rs,end_date=re,avg_up_times=avg_up_times)
        # print("f5",data)
        if data:
            stocks.append(data)

        i += 1
        if not i % 180:
            print("has run", i, 'round',len(stocks))
            time.sleep(60)
    stocks = pd.DataFrame(stocks,columns=["ts_code","avg_up_times"])
    print(stocks.shape)
    stocks.sort_values(["avg_up_times"], ascending=False).reset_index(drop=True)
    stocks.to_csv("period"+str(period)+"avg_up_times"+str(avg_up_times)+".csv")

    # stocks.to_csv("period"+str(period)+"avg_up_times"+str(avg_up_times)+"mv"+str(mv/100000000)+"b"+".csv")
# p=12,0<up<=2,sort by up_range,end_date close<limit_up
# f4(start_date="20190930",end_date="20191022",period=12,up_times=2)
# 上面条件下，range_low>0.05,nud最近三天没有涨停，recent=5满足至少2天均价上涨
# f4(period=12,nud=3,up_times=2,range_low=0.05,recent=5,avg_up_times=2)

# 12天内上涨次数>=9
f5(period =12,avg_up_times=9)

# 10天内满足6次上涨，且市值小于30亿 # 最后一天均价大于10日均价的1%,最后一天换手率大于2%
# f5(period=10,avg_up_times=6,mv=3000000000)


# x,y = get_date(start_date="20191001",period=7)
# print(x)
# print(y)

# data = pro.daily(ts_code='000029.SZ',start_date="20191001")
# print(data)