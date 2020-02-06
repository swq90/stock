import datetime
import time
import pandas as pd
import tushare as ts

ts.set_token('006b49622d70edc237ab01340dc210db15d9580c59b40d028e34e015')
pro = ts.pro_api()
TODAY = str(datetime.datetime.today().date()).replace("-", "")
# NOTCONTAIN = stockfilter.StockFilter().stock_basic(TODAY, name="st|ST", market="科创板")

# 满足给如一组基本信息，过滤
# 给出一组指标，根据数据取值区间筛选，


class StockFilter:

    # 默认过滤掉传入关键字，如果要

    def stock_basic(self, trade_date=None, contain=True, **basic):

        # basic = {'name':'股票名',
        #          'area': '所在地域',
        #          'industry': '所属行业',
        #          'market': '市场类型（主板/中小板/创业板/科创板）',
        #          'exchange': '交易所代码',
        #          'curr_type': '交易货币',
        #          'list_status': '上市状态： L上市 D退市 P暂停上市',
        #          'list_date': '上市日期',
        #          'delist_date': '退市日期',
        #          'is_hs': '是否沪深港通标的，N否 H沪股通 S深股通'}

        stock_basic = pro.stock_basic()
        trade_date=trade_date if trade_date else TODAY
        daily_basic = pro.daily_basic(trade_date=trade_date)

        res = pd.DataFrame()
        for key, value in basic.items():
            if key in list(stock_basic):
                df = stock_basic[stock_basic[key].str.contains(value) == contain]
                print(df.shape)
                basic[key] = ""
            elif key in list(daily_basic):
                if key in ["total_share", "float_share", "free_share", "total_mv", "circ_mv"]:
                    daily_basic[key] = daily_basic[key] * 10000
                df = daily_basic[(daily_basic[key] > value[0]) & (daily_basic[key] < value[1])]
                basic[key] = ""
                # print(daily_basic.shape)
            print("过滤股票条件%s" % key)
            res = res.append(df[["ts_code"]])
        res = res.drop_duplicates()
        print("共过滤掉数据", res.shape[0])

        # return res["ts_code"]
        return res


    # 每日指标
    # def stock_daily(self,**content):
    #     content={"ts_code":"TS股票代码",
    #              "trade_date":"交易日期",
    #              "close":"当日收盘价",
    #              "turnover_rate":"换手率（ % ）",
    #              "turnover_rate_f":"换手率（自由流通股）",
    #              "volume_ratio": "量比",
    #              "pe": "市盈率（总市值 / 净利润）",
    #              "pe_ttm": "市盈率（TTM）",
    #              "pb": "市净率（总市值 / 净资产）",
    #              "ps": "市销率",
    #              "ps_ttm": "市销率（TTM）",
    #              "total_share": "总股本 （万股）",
    #              "float_share": "流通股本 （万股）",
    #              "free_share": "自由流通股本 （万）",
    #              "total_mv": "总市值 （万元）",
    #              "circ_mv": "流通市值（万元）",
    #              "": "",
    #              "": "",
    #              "": "",
    #              "": "",
    #              }

# #
# if __name__=="__main__":
#     o = StockFilter()
#     trade_date=datetime.datetime.today().date()
#     trade_date=str(trade_date)[:10].replace("-","")
#     t = o.stock_basic(trade_date,name="st|ST|药",market="科创板",industry='生物制药|医药商业|医疗保健|中成药|化学制药')
#     print(t,t.shape)
#     pd.read_csv('')
# # print(z)
# pd.set_option('display.max_columns', None)
# pd.set_option('max_colwidth', 1000)
# pd.set_option('display.width', None)
# pd.set_option('display.max_rows', None)
#
# daily_basic = pro.daily_basic(trade_date=TODAY)
# for key in ["total_share", "float_share", "free_share", "total_mv", "circ_mv"]:
#     daily_basic[key] = daily_basic[key] / 10000
# print(daily_basic.sort_values(by='total_mv'))
# # yi=pow(10,8)
# # print(yi)
# # bins=list(range(0,101,20))+list(range(150,201,50))+list(range(5000,40000,5000))
# bins=list(range(0,101,20))+[150,200,400,30000]
# # bins=list(range(0,30000,5000))
# z=pd.cut(daily_basic['total_mv'],bins=bins)
# z=pd.DataFrame(z)
#
# z=z.apply(pd.value_counts)
# z.to_csv("cut.csv")
# print(z.head(20))

#
# stock_basic = pro.stock_basic()
# # stock_basic.to_csv('all.csv')
# # print(stock_basic['industry'].unique())
# tool=StockFilter()
# res=tool.stock_basic(industry='生物制药|医药商业|医疗保健|中成药|化学制药')
# res=res.merge(stock_basic,on='ts_code')
# res=res[res['name'].str.contains('ST') == False]
# res.to_csv('yiyao.csv')
# print(res.shape)
# print(res['industry'].unique())
#
# res=tool.stock_basic(industry='互联网')
# res=res.merge(stock_basic,on='ts_code')
# res = res[res['name'].str.contains('ST') == False]
# res.to_csv('internet.csv')
# print(res.shape)
# print()