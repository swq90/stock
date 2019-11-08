import numpy as np
import pandas as pd
import time
import datetime
import tushare as ts

# ts.set_token('73bebe31744e1d24de4e95aa59828b2cf9d5e07a40adbbc77a02a53e')
pro = ts.pro_api()


class basic:
    def tradeCal(self, start_date="", end_date="", cal=0, period=0):
        """
        交易日历相关操作

        :param start_date: str
        :param end_date: str
        :param cal: int
        :param period: int
        :return: list or str or int
                如果给出cal，则返回以开始日期或结束日期在cal个交易日内，所有交易日的列表cal是交易日的和
                如果给出period，则返回对应开始或结束日期的交易日，period=结束-开始
                都没给出，则返回对应开始结束日的交易日天数
        """
        today = datetime.datetime.today().date()
        today = str(today)[0:10]

        start_date = '' if start_date is None else start_date
        end_date = today if end_date == '' or end_date is None else end_date

        # 去掉-,不然日期取值计算错误
        start_date = start_date.replace('-', '')
        end_date = end_date.replace('-', '')

        date_list = pro.trade_cal(start_date=start_date, end_date=end_date, is_open=1)["cal_date"]
        if cal:
            if start_date:
                return date_list[:cal].tolist()
            else:
                return date_list[-cal:].tolist()
        if period:

            if start_date:
                date_list = date_list[:period].tolist()
                # return date_list[:period].tolist()
                return [date_list[0], date_list[-1]]
            else:
                date_list = date_list[-period-1:].tolist()
                return date_list[0], date_list[-1]
                # return date_list[-period:].tolist()
        # 如果period为空返回周期
        else:
            return date_list.shape[0]

    def daily_basic(self, ts_code="", trade_date="", start_date="", end_date="", cal=0, period=0):
        res = pd.DataFrame()
        if cal or period:
            cal_list = self.tradeCal(start_date=start_date, end_date=end_date, cal=cal, period=period)
            query_times = 0
            for date in cal_list:
                res = pd.concat([pro.daily(trade_date=date), res])
                query_times += 1
                if query_times % 190 == 0:
                    time.sleep(60)
        else:
            res=pro.daily(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)

        return res

    def ma(self,data,ma=5):

        res = pd.DataFrame()

        date_list=data[["trade_date"]].drop_duplicates().sort_values(by="trade_date").reset_index(drop=True)
        stock_list= data["ts_code"].unique()
        if isinstance(ma,int):
            for date in date_list["trade_date"]:
                # 如果缺少前期数据则查询补充
                date_ctn=self.tradeCal(end_date=date, cal=ma)
                date_need = [new for new in date_ctn if
                             new not in data["trade_date"].unique().tolist()]
                if date_need:
                    for day in date_need:

                        data = pd.concat([data, pro.daily(trade_date=day)],sort=False)
                df = data[(data["trade_date"].isin(date_ctn))&(data["ts_code"].isin(stock_list))].groupby("ts_code")["amount","vol"].sum().reset_index()
                df["trade_date"]=pd.DataFrame([date]*len(df))
                res = pd.concat([df,res])
            res["ma"+str(ma)]=res["amount"]*10/res["vol"]
            return res[["ts_code","trade_date","ma"+str(ma)]]
        else:
            for i in ma:
                if res.empty:
                    res = self.ma(data,ma=i)
                else:
                    res = res.merge(self.ma(data,ma=i),on=["ts_code","trade_date"],how="outer")
                print("ma",i,"计算完毕")
                print(datetime.datetime.now())
        return res

    def pre_date(self,date_list,days=1):
        res=pd.DataFrame()
        date_list=date_list.iloc[:,[0]].drop_duplicates()
        date_list.columns=["trade_date"]

        for i in date_list["trade_date"]:
            day =[self.tradeCal(end_date=i,period=days)]
            res = pd.concat([res,pd.DataFrame(day)])
        res.columns=["pre"+str(days)+"date","trade_date"]
        return res

    def label(self,data,formula,):
        pass






# why  type(ma)=="int" is false

