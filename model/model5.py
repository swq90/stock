import datetime
import time
import pandas as pd
import tushare as ts

pro = ts.pro_api()
pro = ts.pro_api()

# 满足给如一组基本信息，过滤
# 给出一组指标，根据数据取值区间筛选，

class StockFilter:
    # 默认过滤掉传入关键字，如果要
    def stock_basic(self, contain=False, **basic):
        print(basic)
        # basic = {'name':股票名，
        #          'area': '所在地域',
        #          'industry': '所属行业',
        #          'market': '市场类型（主板/中小板/创业板/科创板）',
        #          'exchange': '交易所代码',
        #          'curr_type': '交易货币',
        #          'list_status': '上市状态： L上市 D退市 P暂停上市',
        #          'list_date': '上市日期',
        #          'delist_date': '退市日期',
        #          'is_hs': '是否沪深港通标的，N否 H沪股通 S深股通'}
        stock_name = pro.stock_basic()
        for key, value in basic.items():
            print(key,value)
            stock_name = stock_name[stock_name[key].str.contains(value) == contain]

        return stock_name["ts_code"]

    # 每日指标
    def stock_daily(self,**content):
        content={"ts_code":"TS股票代码",
                 "trade_date":"交易日期",
                 "close":"当日收盘价",
                 "turnover_rate":"换手率（ % ）",
                 "turnover_rate_f":"换手率（自由流通股）",
                 "volume_ratio": "量比",
                 "pe": "市盈率（总市值 / 净利润）",
                 "pe_ttm": "市盈率（TTM）",
                 "pb": "市净率（总市值 / 净资产）",
                 "ps": "市销率",
                 "ps_ttm": "市销率（TTM）",
                 "total_share": "总股本 （万股）",
                 "float_share": "流通股本 （万股）",
                 "free_share": "自由流通股本 （万）",
                 "total_mv": "总市值 （万元）",
                 "circ_mv": "流通市值（万元）",
                 "": "",
                 "": "",
                 "": "",
                 "": "",
                 }


o = StockFilter()
mm=o.stock_basic(market="科创板",contain=True)
print(mm)