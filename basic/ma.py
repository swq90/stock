import pandas as pd
import datetime

pd.set_option('display.max_columns', None)
def ma(data,freq=1):
    """
    获取均价
    :param data: dataframe 数据源
    :param freq: 均价频次
    :return:
    """
    ma_data = []
    print(data)
    print(data.info())
    print(list(data))
    date_list= data[["trade_date"]]
    print(type(date_list))
    data.sort_values(by="trade_date",inplace=True)
    print(data)

    data["trade"] = data.apply(lambda row: datetime.date(int(row["trade_date"][:4]), int(row["trade_date"][4:6]), int(row["trade_date"][6:])),axis=1)
    data.sort_values(by="trade_date",inplace=True)


    # for d in date_list:
    #     print(d)
    #     # print(date_pre[date_pre.index(d)-ma+1:date_pre.index(d)+1])
    #     m = daily[daily["trade_date"].isin(date_pre[date_pre.index(d) - ma + 1:date_pre.index(d)+1])]
    #     # print("m",m[m["ts_code"]=='002638.SZ' ])
    #     m = m.groupby("ts_code")["vol", "amount"].sum().reset_index()
    #     t = [d] * m.shape[0]
    #     m["trade_date"] = pd.DataFrame(t)
    #     m["ma"] = m["amount"] * 10 / m["vol"]
    #     # print(m)
    #     ma_data = pd.concat([m[["ts_code", "trade_date", "ma"]], ma_data])


# 读数据的时候把str时间，读作整数
t = pd.read_csv("D:\workgit\stock\model\stable20_up5.csv")
s = ma(t,freq=5)