import pandas as pd
import tushare as ts
from tushare.util import upass

from stock.sql.data import read_data,save_data


token = upass.get_token()
tool = ts.pro.client.DataApi(token)

dividend=tool.dividend()
print()