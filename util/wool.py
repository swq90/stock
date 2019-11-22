import os
import datetime
import numpy as np
import pandas as pd

import tushare as ts
import util.basic as basic
"""
买入卖出价格：open avg  close
本金:AMOUNT
分配比例：金额等分amount，股数等分，vol
剩余金额：pins

"""
PRICEB="open"
PRICES=""
AMOUNT=1000000
days=1
# PROPORTION="vol"
PROPORTION="amount"
PIN=0.0
# m
pro=ts.pro_api()
tool=basic.basic()
data =tool.trade_daily(cal=100)
# trade_date=data["trade_date"].unique().tolist()
# 收盘涨停
limit_up=tool.up_info(data,days=days,up_range=0.1,pct=0,revise=0,limit=1).sort_values(by="trade_date")
# [['ts_code','trade_date','up_n_pct']]
print(list(limit_up))
d=pd.DataFrame()
for trade_date in  data["trade_date"]:
    z=pro.limit_list(trade_date=trade_date, limit_type='U', fields='ts_code,trade_date,pct_chg')
    d=limit_up.merge(z,on=["ts_code","trade_date"],how="outer")
print(limit_up)


# pre_date = tool.pre_date(data[["trade_date"]], days=days)
# sold_data=data.merge(pre_date,on="trade_date")
# sold_data.rename({"trade_date":"sold_date","pre_%s_date"%days:"limit_date"})
