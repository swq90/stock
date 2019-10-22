# p=12,0<up<=2,sort by up_range,end_date close<limit_up
import datetime

import pandas as pd
import tushare as ts

pro = ts.pro_api()

def f4(start_date,end_date,period,up_times):
    # df 为period中有涨停的股票
    df = pd.DataFrame()

    stock_name = pro.stock_basic()
    stock_name = stock_name[stock_name["name"].str.contains("ST") == False]

    days,count = 1,period-1
    while count:
        date = datetime.datetime.today() - datetime.timedelta(days=days)
        date = date.strftime('%Y%m%d')

        days += 1
        if pro.query('trade_cal', start_date=date, end_date=date, fields='is_open').values[0][0]:
            count -= 1
        else:
            continue
        # print(date)
        daily = pro.daily(trade_date=date)[["ts_code","close"]]
        limit = pro.stk_limit(trade_date=date)[["ts_code","up_limit"]]
        # print(daily.shape, daily.shape)
        daily = daily.merge(limit, on="ts_code")
        daily = daily[daily["close"] == daily["up_limit"]]
        # print(daily.shape)
        df = pd.concat([daily,df],axis=0)
        # print(df.shape)
    df =df.groupby("ts_code").size().sort_values(ascending=False).reset_index()
    df.columns = list(('ts_code', 'times'))
    df = df[(df["times"]>0) & (df["times"]<=up_times)]
    print("df-up<=times",df.shape)
    df = df[df["ts_code"].isin(stock_name["ts_code"])]
    print("df-st", df.shape)


    # 过滤 ：end_date 涨停
    df_basic = pro.daily(trade_date=end_date)
    df_limit = pro.stk_limit(trade_date=end_date)
    stock_list=df_basic.merge(df_limit,on=["ts_code","trade_date"])
    stock_list=stock_list[stock_list["close"]!=stock_list["up_limit"]]
    stock_list = stock_list[["ts_code","close"]]
    # print(list(stock_list),stock_list.shape)
    stock_list.columns = list(('ts_code', "end_close"))
    # print(list(stock_list),stock_list.shape)
    # print("sl",stock_list.shape)
    df = df.merge(stock_list,on="ts_code")
    print("df",df.shape,list(df))

    # 得到涨幅
    df_start = pro.daily(trade_date=start_date)[["ts_code","close"]]
    df = df.merge(df_start,on="ts_code")

    df = df.eval("pct=end_close/close-1",inplace=False)
    df = df.sort_values(["pct"], ascending=False)
    df["pct"] = df["pct"].map(lambda x: ('%.2f') % x)

    df = df.reset_index(drop=True)
    df.drop(["end_close","close"],axis=1,inplace=True)
    df.to_csv(start_date+"~"+end_date+"Nup"+"~p"+str(period)+"up"+str(up_times)+".csv")
    print(df)




f4(start_date="20190930",end_date="20191022",period=12,up_times=2)