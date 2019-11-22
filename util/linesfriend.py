import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import util.basic as basic



pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)
ma = [1, 5]
# ma=[1, 5, 10, 20, 30,60,90,120,150,180]
# days = ma[:3]

period = 5
up_cal = 240
# sec=list(range(1,6,1))
sec = 1
between = 10
IS_ABS = False
pic_all_ma = 0
pic_pre = 0
pic_pct = 0
t = basic.basic()
query_days = max(ma) + up_cal
all_stock = t.trade_daily(cal=query_days)


all_ma = t.get_all_ma(all_stock, ma=ma, is_abs=IS_ABS, dis_pct=True)
print(all_ma.shape)
all_ma = all_ma[all_ma["trade_date"].isin(t.tradeCal(cal=up_cal))]
print(all_ma.shape)
all_ma.reset_index(drop=True, inplace=True)


up = all_stock[all_stock["trade_date"].isin(t.tradeCal(cal=up_cal))][
    ['ts_code', 'trade_date', "low", 'high', 'close', 'pre_close']]
up = t.up_info(up, days=period, up_range=0.5)
# 计算所有股票n日均价，dis_pct为True计算ma1相对n日均价的涨幅

# t.to_save(all_ma,"allma")
# 计算满足上涨条件的

up_ma = pd.DataFrame()
for row in range(len(up)):
    df = all_ma[
        (all_ma["ts_code"] == up.at[row, "ts_code"]) & (all_ma["trade_date"] <= up.at[row, "pre_n_date"])].sort_values(
        by="trade_date", ascending=False)
    df.reset_index(drop=True, inplace=True)

    up_ma = up_ma.append(df)
# 计算满足上涨的up 均价
# all_ma.to_csv("all_ma.csv")
up_ma.to_csv("up_ma.csv")

up_ma.dropna(inplace=True)
up_ma.to_csv("up_ma2.csv")





# up_ma=up_ma.set_index("n")
df2 = t.dis_key(all_ma, between=between, sec=sec, keys=["ma1of5"])
# for i in range(5):
#     df = t.dis_key(up_ma.loc[i, :], between=between, sec=sec, keys=["ma1of5"])
#     print(i, df)
#     df.to_csv("%sma5.csv"%i)
#     for col_name in list(df):
#         df[col_name].plot(label="up%s" % (i + 1), title="x:,y:pct" + col_name, grid=True)
#
#         df2["ma1of5"].plot(label="all", title="x:,y:pctall", grid=True)
#         plt.legend(loc="best")
#
#         plt.show()
df=pd.DataFrame()
for i in range(5):
    print(up_ma.loc[i, :])
    # df=pd.merge(t.dis_key(up_ma.loc[i, :], between=between, sec=sec, keys=["ma1of5"]),df,left_index=True)
    print(t.dis_key(up_ma.loc[i, :], between=between, sec=sec, keys=["ma1of5"]))
df.to_csv("%ver2ma5.csv"%i)
# for col_name in list(df):
#     df[col_name].plot(label=col_name, title="x:,y:pct" + col_name, grid=True)
#
#     df2["ma1of5"].plot(label="all", title="x:,y:pctall", grid=True)
for row in range(int(0.5*between),int(1.5*between)):
    df.iloc[row].plot(label="up")
    plt.plot([-5,5],[df2.at[row],df2.at[row]],label="all")
    plt.legend(loc="best")
    # plt.show()


if pic_all_ma:
    all_ma_dis = t.dis_key(all_ma, between=between, sec=sec, keys=list(all_ma)[-len(ma) + 1:])
    up_ma_dis = t.dis_key(up_ma, between=between, sec=sec, keys=list(up_ma)[-len(ma) + 1:])
    all_ma_dis.to_csv("all_ma_dis.csv")
    up_ma_dis.to_csv("up_ma_dis.csv")

    print("准备画图：pct(ma1/man)")
    for col_name in list(all_ma_dis):
        all_ma_dis[col_name].plot(label="all")
        up_ma_dis[col_name].plot(label="up", title=str(datetime.datetime.today())[:10] + col_name, grid=True)
        plt.legend(loc="best")
        plt.savefig(os.getcwd() + "\\" + str(datetime.datetime.today()).replace(":", "").replace(" ", "")[
                                         :20] + col_name + "pct.png")
        plt.show()
    print("准备画图：不同区间pct(ma1/man)")
    # d.drop(d[d.apply(lambda x: sum(x), axis=1) < 3].index, inplace=True)
    # all_ma_dis.drop(all_ma_dis[all_ma_dis.apply(lambda x:sum(x),axis=1)<=0].index,inplace=True)
    # up_ma_dis.drop(up_ma_dis[up_ma_dis.apply(lambda x: sum(x), axis=1) <=0].index, inplace=True)

    for row in range(len(all_ma_dis)):
        # print("v",all_ma_dis.index.values[row])
        # print("str",str(all_ma_dis.index.values[row]))
        all_ma_dis.iloc[row].plot(label="all")
        up_ma_dis.iloc[row].plot(label="up",
                                 title=str(datetime.datetime.today())[:10] + "pct" + str(all_ma_dis.index.values[row]),
                                 grid=True)
        plt.savefig(os.getcwd() + "\\" + str(datetime.datetime.today()).replace(":", "").replace(" ", "")[:20] + str(
            row) + "range_pct.png")
        plt.legend(loc="best")
        plt.show()
    all_ma_dis.to_csv("allmadis.csv")
    up_ma_dis.to_csv("upmadis.csv")

if pic_pre:
    for day in days:
        pre_n_days = t.pre_date(all_ma[["trade_date"]], days=day)
        pre_ma = all_ma[["ts_code", "trade_date", "ma%s" % day]]
        pre_ma.columns = ["ts_code", "pre_%s_date" % day, "pre_ma%s" % day]
        all_pre = all_ma[["ts_code", "trade_date", "ma%s" % day]].merge(pre_n_days, on="trade_date")
        all_pre = all_pre.merge(pre_ma, on=["ts_code", "pre_%s_date" % day])
        all_pre["ma%spct" % day] = (all_pre["pre_ma%s" % day] / all_pre["ma%s" % day] - 1) * 100
        up_pre = up_ma[["ts_code", "trade_date"]].merge(all_pre, on=["ts_code", "trade_date"])
        all_pre.to_csv("all1.csv")
        up_pre.to_csv("up1.csv")

        if day != 1:
            all_pre = t.revise(all_pre, days=day, rekeys="ma%spct", inplace=False, reverse=False)
            print("up_pre.revise")
            up_pre = t.revise(up_pre, days=day, rekeys="ma%spct", inplace=False, reverse=False)
            all_pre.to_csv("all2.csv")

        all_pre_dis = t.dis_key(all_pre, between=between, sec=sec, keys=list(all_pre)[-1:])
        up_pre_dis = t.dis_key(up_pre, between=between, sec=sec, keys=list(up_pre)[-1:])
        all_pre_dis.to_csv("all%spct.csv" % day)
        up_pre_dis.to_csv("up%spct.csv" % day)
        print(all_pre_dis)
        print(up_pre_dis)
        # all_pre_dis.drop(all_pre_dis[all_pre_dis.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)
        # up_pre_dis.drop(up_pre_dis[up_pre_dis.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)
        df = pd.merge(all_pre_dis, up_pre_dis, left_index=True, right=True, how="outer")
        print(df)
        df.columns = ["all", "up"]
        df.drop(df[df.apply(lambda x: sum(x), axis=1) <= 0].index, inplace=True)

        df["all"].plot(label="all")
        df["up"].plot(label="up", title="pre%spct" % day, grid=True)

        # all_pre_dis["ma%spct" % day].plot(label="all")
        # up_pre_dis["ma%spct" % day].plot(label="up", title="pre%spct" % day, grid=True)
        plt.legend(loc="best")
        plt.savefig(os.getcwd() + "\\" + str(datetime.datetime.today()).replace(":", "").replace(" ", "")[
                                         :20] + "period%spre_pct.png" % day)
        plt.show()

        # for row in len(up):
        #     df=all_ma[(all_ma["ts_code"]==up.at[row,"ts_code"])&(all_ma["trade_date"]<=up.at[row,"pre_n_date"])]
        #     df=t.revise(df, days=day,rekeys="pre_ma%s",inplace=False)
        #     up_pre=up_pre.append(df)
        all_pre_dis.to_csv("all%spct.csv" % day)
        up_pre_dis.to_csv("up%spct.csv" % day)

pre_n_days = t.pre_date(all_stock[["trade_date"]], days=1)
pre = all_stock[["ts_code", "trade_date", "low"]]
pre.columns = ["ts_code", "pre_1_date", "pre_low"]
all_pre = all_stock[["ts_code", "trade_date", "low"]].merge(pre_n_days, on="trade_date")
all_pre = all_pre.merge(pre, on=["ts_code", "pre_1_date"])
all_pre["low>pre"] = all_pre.apply(lambda x: 1 if x["low"] > x["pre_low"] else 0, axis=1)
print(all_pre["low>pre"].sum() / len(all_pre))
up = up_ma[["ts_code", "trade_date"]].merge(all_pre, on=["ts_code", "trade_date"])
print(up["low>pre"].sum() / len(up))

# up_ma_nup=up_ma.
#
#
#
# (up_drop).drop_duplicates(["ts_code", "trade_date"], keep=False)


# 1计算所有股票和up股票非涨停期内均价变化pct分布
# 画图，all 和up非涨停期涨幅落在指定区间的波动曲线 画图
# 画图，两组数据 在不同的man pct= ma1/man-1 的波动线

# # if pic_all_ma:
# #     all_stock_dis.iloc[int(len(all_stock_dis) / 2)].plot(label='all_%s_' % i, grid=True)
# #     up_dis.iloc[int(len(up_dis) / 2)].plot(label='up_%s_' % i, grid=True)
#
#
# # 2计算所有股票和up非涨停期内股票，每一天的前一天均价变化分布，计算每一天的前n天分布
#     # 每一天和前n天比pct分布

#
# # 3 计算非涨停期每n天和前n天，两个时间段内均价波动
#


# # #
# # #     print("dis",all_stock_dis.iloc[int(len(all_stock_dis)/2)])
# # #     print(up_dis)
# #     # print(all_stock_dis.ind)
#
# # all_stock_dis = t.dis_key(all_ma, ma=ma, between=between, is_abs=IS_ABS)
# # up_dis = t.dis_key(up_ma, ma=ma, between=between, is_abs=IS_ABS)
#
# # t.to_save(all_stock_dis,"mafenbu")
# # t.to_save(up_dis,"upfenbu")
# # print(up_dis)
# #
# # if pic_pct:
# #     for i in list(all_stock_dis):
# #         all_stock_dis[i].plot()
# #         up_dis[i].plot(title=i, grid=True)
# #         plt.legend(labels=['all-ma', 'up'])
# #         # plt.legend(loc="best")
# #         plt.show()
# #         # plt.savefig-
# #
# # if pic_range:
# #     plt.plot(list(all_stock_dis), all_stock_dis.iloc[int(len(all_stock_dis) / 2)], "s-", label="all-ma_%s_" % i, )
# #     plt.plot(list(up_dis), up_dis.iloc[int(len(up_dis) / 2)], "o-", label="up-ma_%s_" % i)
# # # plt.xticks(list(all_stock_dis)[::2])
# # # plt.plot(list(all_stock_dis),all_stock_dis.iloc[int(len(all_stock_dis)/2)],"s-", label="all-ma_%s_"%i,)
# # # plt.plot(list(up_dis),up_dis.iloc[int(len(up_dis)/2)],"o-", label="up-ma_%s_"%i)
