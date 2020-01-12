# p=12,0<up<=2,sort by up_range,end_date close<limit_up
# 10天内满足7次上涨，且市值小于30亿 # 最后一天均价大于10日均价的1%且小于10%,换手率1.5%
# t = f5(period=10, avg_up_times=7, total_mv=3000000000, turnover_rate=1.5, up_range=[0.01, 0.1])

# 1.所有股票筛选100*(ma1/ma10-1)在[1,10]之间
# 2.筛选换手率
# 3.去除st，退市股票
# 4.10天上涨大于等于7次的股票


import os
import datetime
import time

import pandas as pd
import tushare as ts

import util.basic as basic

pro = ts.pro_api()
tool = basic.basic()

path = 'D:\\workgit\\stock\\util\\stockdata\\'
today = datetime.datetime.today().date()
up_n_pct=[1,10]
periods = 10
times = 7
others={'total_mv':[-10,30],
'turnover_rate':[1.5,float('inf')]}
while (not os.path.isfile(path + str(today) + '\data.csv')) or (
not os.path.isfile(path + str(today) + '\daily-basic.csv')):
    today = today - datetime.timedelta(1)
path=path + str(today)
data = pd.read_csv(path  + '\data.csv', index_col=0,
                   dtype={'trade_date': object})[['ts_code', 'trade_date', 'ma1','ma10']]
daily_basic = pd.read_csv(path  + '\daily-basic.csv', index_col=0,
                          dtype={'trade_date': object})

up_times=tool.up_times(data,period=periods,label='ma1',up_times=times)
print(up_times.shape)


print(daily_basic.shape)
print(data.shape)
data['ma1/ma10pct']=100*(data['ma1']/data['ma10']-1)
print(data.shape)
data=data[(data['ma1/ma10pct']>=up_n_pct[0])&(data['ma1/ma10pct']<=up_n_pct[1])]
print(data.shape)
# daily_baisc 用于筛选市值，换手率，

for k,v in others.items():
    daily_basic=daily_basic[(daily_basic[k]>=v[0])&(daily_basic[k]<=v[1])]
    print(daily_basic.shape)



up_times.to_csv('up_time.csv')
print(up_times.shape)

history = tool.history_name(start_date=data['trade_date'].min())
history['name'] = 'st'
data = data.merge(history, on=['ts_code', 'trade_date'], how='left')
print(data.shape)
data = data[data['name'].isna()]
data.drop(columns=['name'],inplace=True)

print(daily_basic.shape)
print(data.shape)
df=data.loc[:, ('ts_code', 'trade_date', 'ma1')]
print(df.shape)
res = pd.DataFrame()
# df = data.loc[:, ('ts_code', 'trade_date', 'ma1')]
df.loc[:,'prema1'] = df.sort_values(by='trade_date').groupby(by='ts_code')['ma1'].shift(periods=1)
df.loc[:, 'up'] = df.apply(lambda x: 1 if x['ma1'] > x['prema1'] else 0, axis=1)
i=0
for code in df['ts_code'].unique():
    t=df[df['ts_code'] == code]
    t.loc[:,'uptimes']=t.loc[:,'up'].rolling(periods, min_periods=times).sum().shift(-periods + 1)
    res = pd.concat([res, t])
    i +=1
    if i%100==0:
        print(i,t)

# df['uptimes']=df['up'].rolling( periods, min_periods=periods).sum().shift(-periods+1)

res['ma1/ma10']=res['ma1']/res['ma10']-1
res.dropna(inplace=True)
res=res[(res['ma1/ma10']>=0.01)&(res['ma1/ma10']<=0.1)]

# 满足市值
# 股价波动大的时候市值变化频繁会过滤掉数据
daily_basic=daily_basic[daily_basic['total_mv']<=total_mv]
print(daily_basic.shape)

# 满足换手率
daily_basic=daily_basic[daily_basic['turnover_rate']>=turnover_rate]
print(daily_basic.shape)
res=res.merge(daily_basic[['ts_code','trade_date']],on=['ts_code','trade_date'])
res=res[res['uptimes']>=times]

res.to_csv('abcde3.csv')









# for code in data['ts_code'].unique():
#     df=data[data['ts_code']==code].sort_values(by='trade_date',ascending=False).copy()
#     df['pre_ma']=df['ma1'].shift(periods=-1)
# data['pre_ma']=pd.DataFrame.rolling(pe)


# # 10天内满足6次上涨，且市值小于30亿 # 最后一天均价大于10日均价的1%且小于10%,换手率1.5%
# t=f5(period=10,avg_up_times=7,total_mv=3000000000,turnover_rate=1.5,up_range=[0.01,0.1])


# 给定首尾时间，返回周期数
# 给定周期，开始时间，返回结束日期
# 给定周期，结束时间，返回开始日期
def get_date(start_date="", end_date="", period="", cal=0):
    today = datetime.datetime.today().date()
    today = str(today)[0:10]
    start_date = '' if start_date is None else start_date
    end_date = today if end_date == '' or end_date is None else end_date
    # ts_code = ts_code.strip().upper() if asset != 'C' else ts_code.strip().lower()
    start_date = start_date.replace('-', '')
    end_date = end_date.replace('-', '')
    # 返回cal日期的list
    if cal:
        date_list = pro.trade_cal(start_date=start_date, end_date=end_date, is_open=1)["cal_date"]
        if start_date:
            return date_list[:cal].tolist()

        return date_list[-cal:].tolist()
    if period:

        if start_date:
            end_date = pro.trade_cal(start_date=start_date, is_open=1)["cal_date"]
            return start_date, end_date[period - 1]

        if end_date:
            start_date = pro.trade_cal(end_date=end_date, is_open=1)["cal_date"][-period:]
            return start_date.values[0], end_date

    else:
        return pro.trade_cal(start_date=start_date, end_date=end_date, is_open=1).shape[0]


def get_avg_up(ts_code, start_date="", end_date="", period=1, avg_up_times=0, up_range=[]):
    # 以后加上日期判定,然后把调用时的recent+1还原
    res = []
    if start_date or end_date:
        pct = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)[
            ["ts_code", "trade_date", "amount", "vol"]]
        # print(pct)
        if pct.empty:
            return
        if avg_up_times:
            pct_up_times = pct.eval('avg=amount/vol', inplace=False)
            pct_up_times = pct_up_times[["ts_code", "trade_date", "avg"]]
            # pct_up_times = pct_up_times.sort_values(["trade_date"], ascending=False)

            ts = pct_up_times["avg"]
            ts.name = "pre_avg"
            ts = ts.drop([0]).reset_index(drop=True)
            # ts = ts.drop(["index"], axis=1)
            pct_up_times = pd.concat([pct_up_times, ts], axis=1)
            t = pct_up_times[pct_up_times["avg"] - pct_up_times["pre_avg"] > 0]["ts_code"].count()
            # print(pct_up_times)
            if t >= avg_up_times:
                if not res:
                    # print("avg",[ts_code, t])
                    res.append(ts_code)
                res.append(t)
            else:
                return

        if up_range:
            # start_date, end_date = get_date(period=period)
            # pct = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)[
            #     ["ts_code", "trade_date", "amount", "vol"]]
            # if pct.empty:
            #     return
            # print(pct)
            pct.drop(pct.tail(1).index, inplace=True)
            # print(pct)
            t_amout = pct["amount"].sum()
            t_vol = pct["vol"].sum()
            t = pct[pct["trade_date"] == end_date]

            pct_range = t['amount'] / t["vol"] / (t_amout / t_vol)
            pct_range = pct_range[0] - 1
            # print(up_range[0],up_range[1])
            if (pct_range >= up_range[0]) & (pct_range <= up_range[1]):
                if not res:
                    res.append(ts_code)
                res.append(pct_range)
            else:
                return
        # print(res)
        # if avg_up_pct:
        #     start_date, end_date = get_date(period=period)
        #     pct_end= pct[pct["trade_date"]==end_date]
        #     pct_start=pct[pct["trade_date"]==start_date]
        #     pct_end[]

    return res


# 返回df形式的涨停股票
def get_limit_up(trade_date=""):
    daily = pro.daily(trade_date=trade_date)[["ts_code", "trade_date", "close"]]
    limit = pro.stk_limit(trade_date=trade_date)[["ts_code", "trade_date", "up_limit"]]
    # print(daily.shape, daily.shape)
    daily = daily.merge(limit, on="ts_code")
    daily = daily[daily["close"] == daily["up_limit"]]
    # print(daily.shape)
    return daily


def f4(start_date="", end_date="", period=5, nud=1, range=[], recent=0, avg_up_times=0, up_times=0):
    start_date, end_date = get_date(period=period)
    file_name = end_date + "period" + str(period) + "range" + str(range) + "nud" + str(nud)

    # df
    df = pd.DataFrame()
    # 名字中不包含st的股票
    stock_name = pro.stock_basic()
    stock_name = stock_name[stock_name["name"].str.contains("ST") == False]
    stock_name = stock_name[stock_name["market"].str.contains("科创板") == False]
    # print(stock_name.shape)

    # print(stock_name["market"].unique())
    # 前period-nud中所有存在涨停的股票
    date_list_y = get_date(start_date=start_date, cal=period - nud)
    for trade_date in date_list_y:
        daily_up = get_limit_up(trade_date=trade_date)
        df = pd.concat([daily_up, df], axis=0)
        # print(df.shape)

    df = df.groupby("ts_code").size().sort_values(ascending=False).reset_index()
    # print(df.shape)
    df.columns = list(('ts_code', 'times'))
    # 满足涨停次数股票
    df = df[(df["times"] > 0) & (df["times"] <= up_times)]
    print("df-up<=times", df.shape)
    # nud没有涨停
    nud_list = pd.DataFrame()
    date_list_n = get_date(cal=nud)

    for trade_date in date_list_n:
        daily_up_n = get_limit_up(trade_date=trade_date)
        nud_list = pd.concat([daily_up_n, nud_list], axis=0)

    nud_list = nud_list["ts_code"].unique()
    print(nud_list.shape)

    # 过滤nud没涨停的股票
    df = df[df["ts_code"].isin(nud_list) == False]
    print(df.shape)

    # 过滤名字
    df = df[df["ts_code"].isin(stock_name["ts_code"])]
    print("df-name", df.shape)

    # 首尾两日的涨幅
    df_start = pro.daily(trade_date=start_date)[["ts_code", "close"]]
    df_end = pro.daily(trade_date=end_date)[["ts_code", "close"]]
    df_end.columns = list(('ts_code', "end_close"))
    df_end = df_end.merge(df_start, on="ts_code")
    df_pct = df_end.eval("pct=end_close/close-1", inplace=False)
    df = df.merge(df_pct, on="ts_code")
    df = df[df["pct"] >= range[0]]
    df = df[df["pct"] <= range[1]]

    df["pct"] = df["pct"].map(lambda x: ('%.2f') % x)
    print(df)
    df.drop(["end_close", "close"], axis=1, inplace=True)
    print(df)
    df = df.reset_index(drop=True)
    print("df-pct", df.shape, list(df))

    stocks = []
    i = 0

    # 周期内均价上涨，首尾日期
    rs, re = get_date(period=recent + 1)
    # 逐条查询满足均价上涨的股票
    if avg_up_times:
        for stock in df['ts_code']:
            data = get_avg_up(ts_code=stock, start_date=rs, end_date=re, avg_up_times=avg_up_times)
            if data:
                stocks.append(data)
            i += 1
            if not i % 180:
                print("has run", i, 'round')
                time.sleep(60)
        stock_avg_up = pd.DataFrame(stocks, columns=["ts_code", "avg_up_times"])

        df = df.merge(stock_avg_up, on="ts_code")
    df = df.sort_values(["pct"], ascending=False)
    return df
    print(df)

    df.to_csv(file_name + ".csv")


def f5(start_date="", end_date="", period=1, avg_up_times=0, total_mv=None, turnover_rate=None, up_range=[]):
    rs, re = get_date(period=period)
    # df_start = pro.daily(trade_date=rs)[["ts_code", "close"]]
    df_end = pro.daily_basic(trade_date=re)
    # df_start.columns = list(('ts_code', "start_close"))
    # df_end = df_end.merge(df_start, on="ts_code")
    # df_end = df_end.eval("pct=close/start_close-1", inplace=False)
    # df_end =df_end[(df_end["pct"]>=up_range[0])&(df_end["pct"]<=up_range[1])]
    # print(df_end)
    stocks = []

    stock_name = pro.stock_basic()
    stock_name = stock_name[stock_name["name"].str.contains("ST") == False]
    # print(stock_name.shape)
    stock_name = stock_name[stock_name["market"].str.contains("科创板") == False]
    # print(stock_name.shape)

    # print(stock_name["market"].unique())
    # 过滤名字
    stock_list = df_end[df_end["ts_code"].isin(stock_name["ts_code"])]
    print(stock_list.shape)
    # print(list(stock_list))
    # print(turnover_rate,mv,avg_up_pct)
    if turnover_rate:
        stock_list = stock_list[stock_list["turnover_rate"] >= turnover_rate]
        print("turnover_rate", stock_list.shape)
        # print(list(stock_list))

    if total_mv:
        stock_list = stock_list[stock_list["total_mv"] < (total_mv / 10000)]
        print("mv", stock_list.shape)

    i = 0
    ts, te = get_date(period=period + 1)
    print(ts, te)
    for stock in stock_list["ts_code"]:
        # print(stock)
        data = get_avg_up(ts_code=stock, start_date=ts, end_date=te, period=period, avg_up_times=avg_up_times,
                          up_range=up_range)
        # print("f5",data)
        if data:
            # print(data)
            stocks.append(data)

        i += 1

    if up_range:
        stocks = pd.DataFrame(stocks, columns=["ts_code", "avg_up_times", "up_range"])
    else:
        stocks = pd.DataFrame(stocks, columns=["ts_code", "avg_up_times"])

    print(stocks.shape)
    stocks.sort_values(["avg_up_times"], ascending=False).reset_index(drop=True)
    # stocks.to_csv(te+"period"+str(avg_up_times)+str(period)+"mv30"+"range"+str(up_range)+".txt",sep='\t')
    # stocks.to_csv("period"+str(period)+"avg_up_times"+str(avg_up_times)+".txt",sep='\t')
    return stocks

    # stocks.to_csv("peri
    # od"+str(period)+"avg_up_times"+str(avg_up_times)+"mv"+str(mv/100000000)+"b"+".csv")


# p=12,0<up<=2,sort by up_range,end_date close<limit_up
# f4(start_date="20190930",end_date="20191022",period=12,up_times=2)
# 上面条件下，range_low>0.05,nud最近三天没有涨停，recent=5满足至少2天均价上涨
# f4(period=12,nud=3,up_times=2,range_low=0.05,recent=5,avg_up_times=2)

# 12天内上涨次数>=9

# 10天内满足7次上涨，且市值小于30亿 # 最后一天均价大于10日均价的1%,最后一天换手率大于2%
# t=f5(period=10,avg_up_times=6,mv=3000000000,turnover_rate=0.02,avg_up_pct=0.01)

# x,y = get_date(start_date="20191001",period=7)
# print(x)
# print(y)

# data = pro.daily(ts_code='000029.SZ',start_date="20191001")
# print(data)
# t = get_avg_up(period=5,ts_code="002045.SZ",avg_up_pct=0.01,avg_up_times=1)


today = datetime.datetime.today().date()
today = str(today).replace('-', '')

# 换手率大于1%,股票开头不是688【科创版】,不是st，最后一天换手率大于1%
# 12天内，最近 2天没涨停，limit_up in (1,2,3),最后一天涨幅大于5%，小于第一天30%
# t = f4(period=12,nud=2,up_times=3,range=[0.05,0.3])
# t.to_csv(today+"limitUp1-3.txt",sep='\t')
# # 10天内满足6次上涨，且市值小于30亿 # 最后一天均价大于10日均价的1%且小于10%,换手率1.5%
t = f5(period=10, avg_up_times=7, total_mv=3000000000, turnover_rate=1.5, up_range=[0.01, 0.1])
t.to_csv(today + "10dayUp1-5%.txt", sep='\t')
# 12天内上涨次数>=9
# t= f5(period =12,avg_up_times=9)
# t.to_csv(today+"up9-12.txt",sep='\t')
# # print(t)
