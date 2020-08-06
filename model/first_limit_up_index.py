from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import stock.util.stockfilter as sfilter
import stock.util.basic as basic
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls

start,end='20190101','20191231'
first_up=gls.first_limit()
print()