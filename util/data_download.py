from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import datetime
import tushare as ts
from tushare.util import upass

token=upass.get_token()
tool=ts.pro.client.DataApi(token,timeout=60)
engine=create_engine('postgresql://nezha:nezha@10.0.0.5:5432/stock',echo=False)

# saved_date=pd.read_sql_query('select distinct trade_date from daily',con=engine)
# print(type(saved_date))
# saved_date=saved_date.sort_values('trade_date',ascending=False)

t1=datetime.datetime.now()
start_date='20200101'
end_date='20200219'
# trade_date=''
tables=['daily','stk_limit','limit_list']
saved_date=pd.DataFrame()
saved_date['daily']=pd.read_sql_query('select distinct trade_date from daily',con=engine)['trade_date']
saved_date['stk_limit']=pd.read_sql_query('select distinct trade_date from stk_limit',con=engine)['trade_date']
saved_date['limit_list']=pd.read_sql_query('select distinct trade_date from limit_list',con=engine)['trade_date']

trade_cal=tool.query('trade_cal',start_date=start_date,end_date=end_date,is_open='1')
count_dict=dict.fromkeys(tables,0)
for date in trade_cal['cal_date']:

    for table in tables:
        if date in saved_date[table].values:
            continue
        data=tool.query(table,trade_date=date)
        if data.empty:
            print('%s%s'%(date,table),'not download')
            continue
        data.to_sql(table,con=engine,if_exists='append',index=False)
        print(date,table)
        count_dict[table]+=data.shape[0]

t2=datetime.datetime.now()
print(t2-t1)