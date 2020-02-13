from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import datetime
import tushare as ts
from tushare.util import upass

token=upass.get_token()
tool=ts.pro.client.DataApi(token,timeout=60)
engine=create_engine('postgresql://nezha:nezha@10.0.0.4:5432/stock',echo=False)

saved_date=pd.read_sql_query('select distinct trade_date from daily',con=engine)
saved_date=saved_date.sort_values('trade_date',ascending=False)

t1=datetime.datetime.now()
start_date='20191221'
end_date='20200212'
tables=['daily','stk_limit','limit_list']

trade_cal=tool.query('trade_cal',start_date=start_date,end_date=end_date,is_open='1')
count_dict=dict.fromkeys(tables,0)
for date in trade_cal['cal_date']:
    if date in saved_date['trade_date'].values:
        continue
    print(date)
    for table in tables:
        data=tool.query(table,trade_date=date)
        if data.empty:
            continue
        data.to_sql(table,con=engine,if_exists='append',index=False)
        count_dict[table]+=data.shape[0]

t2=datetime.datetime.now()
print(t2-t1)