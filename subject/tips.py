import pandas as pd
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic
from stock import vars
# start,end='20190801',''
class Stock:
    def __init__(self,start,end):
        self.start=start
        self.end=end
        self.raw_data = read_data('daily', start_date=start, end_date=end)

    def stock_increse(self,period):
        data=self.raw_data.copy()
        data
