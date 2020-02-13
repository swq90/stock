from sqlalchemy import create_engine
import pandas as pd
import datetime
import tushare as ts
from tushare.util import upass


token = upass.get_token()
tool = ts.pro.client.DataApi(token, timeout=60)
engine = create_engine('postgresql://10.0.0.4:5432/stock', echo=False)


start_date = '20180101'
end_date = '20191231'
tables = ['daily', 'stk_limit', 'limit_list']

trade_cal = tool.query('trade_cal', start_date=start_date, end_date=end_date, is_open='1')
count_dict = dict.fromkeys(tables, 0)
for date in trade_cal['cal_date']:
    for table in tables:
        data = tool.query(table, trade_date=date)
        if data.empty:
            continue
        data.to_sql(table, con=engine, if_exists='append', index=False)
        count_dict[table] += data.shape[0]

