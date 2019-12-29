import os
import datetime
import numpy as np
import math
import pandas as pd

import tushare as ts
import util.basic as basic
from util import sheep
"""
买入卖出价格：open avg  close
本金:AMOUNT
分配比例：金额等分amount，股数等分，vol
剩余金额：pins
指标简介：
昨日涨停今天的表现情况，但不包括一字涨停板
指标图示：
暂无指标图示
指标代码：
未来函数检测
T1:=(C/REF(C,1)-1)*100>=9.96;
T2:=NOT(H=L);
T3:=NAMELIKE('S') OR NAMELIKE('*');
T4:=(C/REF(C,1)-1)*100>=4.96 AND T3;
XG:(REF(T1,1) OR REF(T4,1)) AND T2
"""
PRICEB="close"
PRICES="close"
AMOUNT=1000000
days=1
# PROPORTION="vol"
PROPORTION="amount"
PIN=0.0

pro=ts.pro_api()
tool=basic.basic()
data =tool.trade_daily(cal=240)
print(data.shape)
data=data[((data["low"]==data["high"]))==False]
print(data.shape)

data.to_csv("wool2data.csv")
# trade_date=data["trade_date"].unique().tolist()
# 收盘涨停
limit_up=tool.limit_up_info(data).sort_values(by="trade_date").reset_index(drop=True)
print(limit_up)
limit_up.to_csv("wool2limitup.csv")

# for trade_date in  data["trade_date"].unique():
#     z=pro.limit_list(trade_date=trade_date, limit_type='U', fields='ts_code,trade_date,pct_chg')
#     h=z[z["pct_chg"]>=10]
#     d=limit_up[limit_up["trade_date"]==trade_date]
# print(limit_up)

buy_data=limit_up.merge(data[['ts_code','trade_date',PRICEB]],on=['ts_code','trade_date'])[['ts_code','trade_date',PRICEB]]
print(buy_data)
buy_data.columns=['ts_code','buy_date',"buy_price"]
pre_date = tool.pre_date(data[["trade_date"]], days=days)
sell_data=data[['ts_code','trade_date',PRICES]].merge(pre_date,on='trade_date')
sell_data.rename(columns={"trade_date":"sell_date","pre_%s_date"%days:"buy_date",PRICES:'sell_price'},inplace=True)
sell_data=sell_data.merge(buy_data,on=['ts_code','buy_date'])
print(sell_data)
sell_data.to_csv("wool3sell.csv")

sell_data['pct']=(sell_data['sell_price']/sell_data['buy_price'])
sell_cut=sell_data.groupby(by='sell_date')['pct'].mean()
sell_cut=pd.DataFrame(sell_cut)
sell_cut['all_pct']=sell_cut['pct'].cumprod()
sell_cut.to_csv('pctwool3.csv')
print(sell_cut)