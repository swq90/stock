import os
import sys
import datetime
import calendar as cal
from dateutil.parser import parse
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from functools import partial
import stock.util.stockfilter as sfilter
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
from tushare.util import upass

token = upass.get_token()
tool = ts.pro.client.DataApi(token)
engine = create_engine('postgresql://nezha:nezha@10.0.0.5:5432/stock', echo=False)


def download_data(start_date=None, end_date=None, trade_date=None,days=7, tables=['daily', 'stk_limit', 'limit_list']):
    if trade_date is None:
        if not end_date:
            end_date = str(datetime.datetime.today().date()).replace('-', '')
        if not start_date:
            start_date = str(datetime.datetime.today().date() - datetime.timedelta(days)).replace('-', '')
        trade_cal = tool.query('trade_cal', start_date=start_date, end_date=end_date, is_open='1')
    else:
        trade_cal = tool.query('trade_cal', start_date=trade_date, end_date=trade_date, is_open='1')['cal_date']
    if trade_cal.empty:
        return

    saved_date = pd.DataFrame()
    for table in tables:
        sql = 'select distinct trade_date from %s' % table
        saved_date[table] = pd.read_sql_query(sql, con=engine)['trade_date']

    count_dict = dict.fromkeys(tables, 0)
    for date in trade_cal['cal_date']:
        for table in tables:
            if date in saved_date[table].values:
                continue
            data = tool.query(table, trade_date=date)
            if data.empty:
                print('%s%s' % (date, table), 'not download')
                continue
            data.to_sql(table, con=engine, if_exists='append', index=False)
            # print(date, table)
            count_dict[table] += data.shape[0]


def read_data(table_name='daily', start_date='20200201', end_date=None, filter=True, **notcontain):
    # 基础数据
    # 科创板，st数据
    if not end_date:
        end_date = str(datetime.datetime.today().date()).replace('-', '')
    sql = "select * from %s where (trade_date>='%s' and trade_date<='%s')" % (table_name, start_date, end_date)
    data = pd.read_sql_query(sql, con=engine)
    data.drop_duplicates(inplace=True)
    if filter and ('ts_code' in data.columns):
        NOTCONTAIN = sfilter.StockFilter().stock_basic(end_date, name="st|ST", market="科创板")
        data = data[data["ts_code"].isin(NOTCONTAIN['ts_code']) == False]
    return data


def save_data(data, filename, fp=None, fp_date=False):

    if not fp:
        fp = os.path.join(os.path.dirname(os.getcwd()), 'data', os.path.basename(sys.argv[0]).split('.py')[0])

    if isinstance(fp_date, str):
        fp = os.path.join(fp, fp_date)
    elif fp_date:
        fp = os.path.join(fp, str(datetime.datetime.today().date()))
    if not os.path.exists(fp):
        os.makedirs(fp)
    filename=os.path.join(fp, filename)
    data.to_csv(filename)
def stock_basic():

    sql='select count(*) from stock_basic'
    df=pd.DataFrame()
    if pd.read_sql_query(sql,con=engine).iloc[-1,-1]==0:
        for status in list('LDP'):
            df = pd.concat([tool.query('stock_basic',list_status=status),df],ignore_index=True)
            print(df.shape)
        df.to_sql('stock_basic', con=engine, if_exists='replace' ,index=False)
            # , fields='ts_code,list_date,list_status')
        # stock_basic = pd.concat([df, stock_basic], ignore_index=True)

def adj_share():
    pass
if __name__ == '__main__':
    download_data()
    if datetime.datetime.today().weekday()%3==0:
        stock_basic()
    # print()