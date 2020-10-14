# 基础常用数据自动备份，读取常用数据，过滤板块，亏损股票，自定义文件保存，
import os
import sys
import datetime
import time
import calendar as cal
from dateutil.parser import parse
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from functools import partial
from sqlalchemy import create_engine
import pandas as pd
import tushare as ts
from tushare.util import upass

token = upass.get_token()
tool = ts.pro.client.DataApi(token)
engine = create_engine('postgresql://nezha:nezha@10.0.0.4:5432/stock', echo=False)


def days(start_date, end_date=''):
    end_date = datetime.datetime.strptime(end_date, '%Y%m%d') if end_date else datetime.datetime.today()
    start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
    # print(start_date,end_date,(end_date-start_date).days)
    return (end_date - start_date).days


def download_data(start_date=None, end_date=None, trade_date=None, days=4, tables=['daily', 'stk_limit', 'limit_list']):
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
        # print(date)
        for table in tables:
            if date in saved_date[table].values:
                continue
            data = tool.query(table, trade_date=date)
            if data.empty:
                print('%s%s' % (date, table), 'not download！！！！！！！！！！')
                continue
            data.to_sql(table, con=engine, if_exists='append', index=False)
            count_dict[table] += data.shape[0]
            print('%s%s' % (date, table), ' download')


def read_data(table_name='daily', start_date='20200201', end_date=None, filter=True, **notcontain):
    # 基础数据
    # 科创板，st数据
    if table_name in ['stock_basic', 'namechange']:
        sql = "select * from %s" % table_name
        return pd.read_sql_query(sql, con=engine)
    if not end_date:
        end_date = str(datetime.datetime.today().date()).replace('-', '')
    sql = "select * from %s where (trade_date>='%s' and trade_date<='%s')" % (table_name, start_date, end_date)
    data = pd.read_sql_query(sql, con=engine)
    data.drop_duplicates(inplace=True)
    if filter and ('ts_code' in data.columns):
        NOTCONTAIN = sfilter(end_date, name="st|ST|退", market="科创板")
        data = data[data["ts_code"].isin(NOTCONTAIN['ts_code']) == False]
    return data


def sfilter(self, trade_date=None, contain=True, **basic):
    # basic = {'name':'股票名',
    #          'area': '所在地域',
    #          'industry': '所属行业',
    #          'market': '市场类型（主板/中小板/创业板/科创板）',
    #          'exchange': '交易所代码',
    #          'curr_type': '交易货币',
    #          'list_status': '上市状态： L上市 D退市 P暂停上市',
    #          'list_date': '上市日期',
    #          'delist_date': '退市日期',
    #          'is_hs': '是否沪深港通标的，N否 H沪股通 S深股通'}
    sql = 'select * from stock_basic'

    stock_basic = pd.read_sql('stock_basic', con=engine)

    res = pd.DataFrame()
    for key, value in basic.items():
        if key in stock_basic.columns:
            df = stock_basic[stock_basic[key].str.contains(value) == contain]
            # print(df.shape)
            basic[key] = ""
        # elif key in list(daily_basic):
        #     if key in ["total_share", "float_share", "free_share", "total_mv", "circ_mv"]:
        #         daily_basic[key] = daily_basic[key] * 10000
        #     df = daily_basic[(daily_basic[key] > value[0]) & (daily_basic[key] < value[1])]
        #     basic[key] = ""
        # print(daily_basic.shape)
        # print("过滤股票条件%s" % key)
        res = res.append(df[["ts_code"]])
        # print(res.shape)
    res = res.drop_duplicates()
    # print("共过滤掉数据", res.shape[0])

    # return res["ts_code"]
    return res


def save_data(data, filename, fp=None, fp_date=False, mode='w', header=True,encoding=None):
    if not fp:
        fp = os.path.join(os.path.dirname(os.getcwd()), 'data', os.path.basename(sys.argv[0]).split('.py')[0])

    if isinstance(fp_date, str):
        fp = os.path.join(fp, fp_date)
    elif fp_date:
        fp = os.path.join(fp, str(datetime.datetime.today().date()))
    if not os.path.exists(fp):
        os.makedirs(fp)
    filename = os.path.join(fp, filename)
    data.to_csv(filename, mode=mode, header=header,encoding=encoding)


def stock_basic():
    df = pd.DataFrame()
    for status in list('LDP'):
        df = pd.concat([tool.query('stock_basic', list_status=status,
                                   fields='ts_code,symbol,name,area,industry,market,list_date,list_status,delist_date'),
                        df], ignore_index=True)
        # print(df.shape)
    df.to_sql('stock_basic', con=engine, if_exists='replace', index=False)
    print('stock_basic has done')


# def cal(**kwargs):
#     tool.query()
def cal(start=None, end=None, days=3):
    if not end:
        end = str(datetime.datetime.today().date()).replace('-', '')
    cal_list = tool.query('trade_cal', start_date=start, end_date=end, is_open='1')
    if start:
        return cal_list['cal_date']

    if end:
        return cal_list['cal_date'].iloc[-days]


def adj_share():
    pass


def fina_mainbz(start_date='20181231'):
    stock_basic = pd.read_sql('stock_basic', con=engine)
    data=pd.DataFrame()
    for ts_code in stock_basic[stock_basic['list_status']=='L']['ts_code']:
        data=pd.concat([tool.query('fina_mainbz',ts_code=ts_code,start_date=start_date,type='P'),data],ignore_index=True)
        time.sleep(1)
    data.to_sql('fina_mainbz', con=engine,if_exists='replace', index=False)


def new_list():
    start = '20200501'
    ll = read_data('stock_basic')
    # ll = ll.loc[ll['list_date'] >= '20200101']
    df = read_data('daily', start_date=start).merge(ll[['ts_code', 'list_date']],
                                                    left_on=['ts_code', 'trade_date'],
                                                    right_on=['ts_code', 'list_date'])
    print(df.shape, df[df['high'] == df['low']].shape, df[df['high'] == df['close']].shape,
          df[(df['high'] == df['close']) & (df['high'] > df['low'])].shape)
    print(df['trade_date'].min())
    df1 = read_data('limit_list', start_date=start).merge(ll[['ts_code', 'list_date']],
                                                          on='ts_code')
    df2 = read_data('stk_limit', start_date=start).merge(ll[['ts_code', 'list_date']],
                                                         on='ts_code')
    print()


def new_stock(data, list_days=100, new=False):
    s_basic = pd.read_sql('stock_basic', con=engine)
    data = data.merge(s_basic[['ts_code', 'list_date']], on=['ts_code'])
    data['list_days'] = data.apply(lambda x: days(x['list_date'], x['trade_date']), axis=1)
    data = data.loc[(data['list_days'] < list_days) == new]
    return data


def save_to_sql(data, tablename, fp=None, fp_date=False, mode='w', header=True):
    # if tablename not in (pd.read_sql_query("select * from pg_tables", con=engine))['tablename']:
    #     # pd.read_sql_query("select * from pg_tables", con=engine)
    #     # 创建表
    #     print(111)
    data.to_sql(tablename, con=engine, if_exists='replace')

    # else:
    #     data.to_sql(tablename, con=engine, if_exists='append')
        # sql = 'select distinct trade_date from %s if %s is' % tablename
    # saved_date = pd.read_sql_query(sql, con=engine)['trade_date']
    #
    # data.to_sql(tablename[tablename['trade_date'].isin(saved_date) == False], con=engine, if_exists='append',
    #             index=False)
    # print(tablename, ' download')

if __name__ == '__main__':
    # download_data()
    # if datetime.datetime.today().weekday() % 2 == 0:
    #     stock_basic()


    fina_mainbz()


    # new_stock(read_data('daily'))
    # new_list()


        # stock_basic()

        # # print(cal(end='20200301'),type(cal(end='20200301')))
