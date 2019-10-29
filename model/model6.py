#换手率大于1%,股票开头不是688【科创版】,不是st，最后一天换手率大于1%
# 12天内，最近2天没涨停，limit_up in (1,2,3),最后一天涨幅大于5%，小于第一天30%
import datetime
import pandas as pd
import tushare as ts
# import model.stockfilter as sf


# avg_n : n日均价
# up_n : n 日上涨
# limit_up_n : n 日涨停

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
            return date_list[0],date_list[-1]
        else:
            date_list=date_list[-period:].tolist()
            return date_list[0],date_list[-1]
            # return date_list[-period:].tolist()
    # 如果period为空返回周期
    else:
        return date_list.shape[0]
    # if period:
    #
    #     if start_date:
    #         end_date =pro.trade_cal(start_date=start_date,is_open=1)["cal_date"]
    #         return start_date,end_date[period-1]
    #
    #
    #     if end_date:
    #         start_date = pro.trade_cal(end_date=end_date,is_open=1)["cal_date"][-period:]
    #         return start_date.values[0],end_date
    #
    # else:
    #     return pro.trade_cal(start_date=start_date,end_date=end_date,is_open=1).shape[0]


# limit_up_times=[涨停次数的下限，上限，周期（3代表period的前三天，-3代表后三天，空代表整个period]
# up_times=[上涨次数的下限，上限，周期（3代表period的前三天，-3代表后三天，空代表整个period]
# pct_chg=

def up_limit_info(start_date="",end_date="",period=1,*,up_limit_times=[]):


    dailys = pd.DataFrame()
    limits = pd.DataFrame()
    date_list = get_date(start_date,end_date,cal=period)
    # print(date_list)
    # print(dailys.shape)
    for date in date_list:
        daily = pro.daily(trade_date=date)
        limit = pro.stk_limit(trade_date=date)
        dailys = pd.concat([daily,dailys],axis=0)
        limits = pd.concat([limit,limits],axis=0)
        # print(dailys.shape)
        # print(limits.shape)
    limit_list = pd.merge(dailys,limits,on=["ts_code","trade_date"])[["ts_code","trade_date","close","up_limit"]]
    limit_list = limit_list[limit_list["close"]==limit_list["up_limit"]]
    limit_list = limit_list.groupby("ts_code").size().sort_values(ascending=False).reset_index()

    limit_list.columns=["ts_code","up_limit_times"]
    limit_list = limit_list[(limit_list["up_limit_times"]>=up_limit_times[0]) &(limit_list["up_limit_times"]<=up_limit_times[1])]

    return limit_list



def avg_up_info(start_date="",end_date="",period=1,*,avg_up_times=0,up_range=[]):




def get_avg_up(ts_code,start_date="",end_date="",period=1,avg_up_times=0,up_range=[]):
    # 以后加上日期判定,然后把调用时的recent+1还原
    res =[]
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
            else:return

        if up_range:
            # start_date, end_date = get_date(period=period)
            # pct = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)[
            #     ["ts_code", "trade_date", "amount", "vol"]]
            # if pct.empty:
            #     return
            # print(pct)
            pct.drop(pct.tail(1).index,inplace=True)
            # print(pct)
            t_amout = pct["amount"].sum()
            t_vol = pct["vol"].sum()
            t = pct[pct["trade_date"]==end_date]

            pct_range=t['amount']/t["vol"]/(t_amout/t_vol)
            pct_range =pct_range[0]-1
            # print(up_range[0],up_range[1])
            if (pct_range>=up_range[0]) & (pct_range <= up_range[1]):
                if not res:
                    res.append(ts_code)
                res.append(pct_range)
            else: return
        # print(res)
        # if avg_up_pct:
        #     start_date, end_date = get_date(period=period)
        #     pct_end= pct[pct["trade_date"]==end_date]
        #     pct_start=pct[pct["trade_date"]==start_date]
        #     pct_end[]

    return res


# t = get_date(start_date="20191001",cal=5)
# s = pro.daily(ts_code="002543.SZ", start_date="20191001", end_date="20191010")
# s = s.eval('avg=amount/vol', inplace=False)
# pd.set_option('display.max_columns', None)
# print(s)
# print(list(s))
#

t = up_limit_info(period=3,up_limit_times=[2,float('inf')])
print(t)


