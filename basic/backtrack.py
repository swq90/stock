import numpy as np
import pandas as pd

import tushare as ts
pro = ts.pro_api()

def earn(stock,bill,trade_date="",pct=1):
    """
    股票回溯
    :param stock: datafram,给定股票代码,涨停日期
    :param bill: float,资金
    :param pct: list,分配比例
    :return:
    """


