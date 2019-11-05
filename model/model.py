import datetime
import time
import pandas as pd
import tushare as ts
import basic

pro = ts.pro_api()
def ma(df,ma=5,key="close"):
    ma_data = pd.DataFrame()
    # date_list=df[""].max()
    date_list=pd.DataFrame(df["trade_date"].unique(),dtype="str")
    date_list.columns=["date"]
    date_list=date_list.sort_values("date",ascending=False)
    print(date_list)
    for i in date_list["date"][:-5]:
        print(i)
        m = df[df["trade_date"].isin(str(date_list[date_list.index(i)-ma+1:date_list.index(i)+1])]
        print(m)
# m = daily[daily["trade_date"].isin(date_pre[date_pre.index(d) - ma + 1:date_pre.index(d) + 1])]


def avg_up_info(start_date="", end_date="", period=5, cal=8, *, up_pct=0.4, start_p="close", end_p="high",
                list_days=60):

    pass

# 查询日期，获取数据，数据处理 ma，计算涨停信息，涨停pct分布,涨停次数分布

t = pro.daily(ts_code="603022.SH",start_date="20190825")
# print(t)
ma(t)