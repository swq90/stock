import util.basic as basic

t=basic.basic()
all=t.trade_daily(cal=240)
pre_n=t.pre_date(all[["trade_date"]])
all=all.merge(pre_n,on="trade_date")
df=all[["ts_code","trade_date","low"]].rename({"trade_date":"p1","low":"pre_low"})
# all["up_avg"] = all.apply(lambda x: 1 if x["avg"] - x["pre_avg"] > 0 else 0, axis=1)
# all["up_ma"] = all.apply(lambda x: 1 if x["ma"] - x["pre_ma"] > 0 else 0, axis=1)
all["low>pre"] = all.apply(lambda x: 1 if x["low"] > x["pre_low"] else 0, axis=1)


