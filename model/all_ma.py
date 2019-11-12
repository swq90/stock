import datetime
import time
import numpy as np
import pandas as pd
import tushare as ts
from basic.basic import basic

pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)


def get_all_ma(data=[], period=1, ma=[]):
    if not data:
        cal = period + max(ma)
        data = basic().daily_basic(cal=cal)
    res = pd.DataFrame()
    print(data.info())
    for i in data["ts_code"].unique():
        dm = data[data["ts_code"] == i][["ts_code", "trade_date", "amount", "vol"]]
        dm = basic().ma(dm, ma=ma)
        res = pd.concat([dm, res])
    print("a", res)

    for i in ma[1:]:
        if isinstance(i, int):
            res["ma1of%s" % i] = res.apply(lambda x: 100 * (x["ma1"] / x["ma%i" % i] - 1), axis=1)

    print(res)
    res = res[res["trade_date"].isin(basic().tradeCal(cal=period))]
    return res


def dis_ma(data):
    bins = np.arange(-100, 110, 5)
    # ma=[1,5,10,20,60,90,120,150,180]
    ma = [1, 5, 10]
    df = get_all_ma(period=5, ma=ma)
    tongji = pd.DataFrame()
    for i in ma[1:]:
        if isinstance(i, int):
            tongji["pct1of%s" % i] = pd.cut(df["ma1of%s" % i], bins=bins)
            # tongji=df[df["pc

    tongji = tongji.apply(pd.value_counts)
    tongji = tongji.apply(lambda x: 100 * x / x.sum())

    df.to_csv("allma.csv")
    tongji.to_csv("tongjima.csv")
