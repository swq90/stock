import datetime
from sqlalchemy import create_engine
import pandas as pd
import stock.util.stockfilter as sfilter
import stock.util.sheep as sheep

engine = create_engine('postgresql://nezha:nezha@10.0.0.5:5432/stock', echo=False)

def get_data(start_date,end_date):
    NOTCONTAIN = sfilter.StockFilter().stock_basic(end_date, name="st|ST", market="科创板")
    t1=datetime.datetime.now()
    raw_data = pd.read_sql_query('select * from daily where (trade_date>=%(start)s and trade_date<=%(end)s)',
                                 params={'start': start_date, 'end': end_date}, con=engine)

    stk_limit = pd.read_sql_query('select * from stk_limit where (trade_date>=%(start)s and trade_date<=%(end)s)',
                                  params={'start': start_date, 'end': end_date}, con=engine)
    print(datetime.datetime.now()-t1)
    raw_data.drop_duplicates(inplace=True)
    stk_limit.drop_duplicates(inplace=True)
    print('交易数据%s,包含%s个交易日,涨停数据%s' % (raw_data.shape, raw_data['trade_date'].unique().shape, stk_limit.shape))
    raw_data = raw_data[raw_data["ts_code"].isin(NOTCONTAIN['ts_code']) == False]
    df=raw_data.merge(stk_limit,on=['ts_code','trade_date'])


    return df
def verify(up,data):
    t1 = datetime.datetime.now()
    s1=sheep.wool(up,data)
    t2= datetime.datetime.now()
    s2=sheep.wool2(up,data)
    t3 = datetime.datetime.now()

    s1.to_csv('s1.csv')
    s2.to_csv('s2.csv')

    print()
t=get_data('20190220','20190430')
s=t[t['close']==t['up_limit']]
pd.DataFrame(s.groupby('trade_date')['ts_code'].count()).to_csv('count.csv')
verify(s,t)
