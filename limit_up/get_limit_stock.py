import pandas as pd
from functools import partial
from stock.sql.data import read_data, save_data,cal
import stock.util.basic as basic


def limit_stock(start_date=None, end_date=None, limit_type='up'):
    """
    基础价格&涨跌停价格
    """
    if start_date is None:
        start_date=cal(end=end_date)

    data = read_data(table_name='daily', start_date=start_date, end_date=end_date)
    stk_limit = read_data(table_name='stk_limit', start_date=start_date, end_date=end_date)
    data = data.merge(stk_limit, on=['ts_code', 'trade_date'])
    if limit_type == 'both':
        return data
    return data[data['close'] == data['%s_limit' % limit_type]]


def first_limit(data=None, start_date=None, end_date=None, limit_type='up', days=1):
    """
    首板涨停，pre_1_is_roof==0,is_roof==1
    """
    if data is None:
        data = limit_stock(start_date, end_date, limit_type='both')
    if limit_type == 'up':
        data['is_roof'] = data.apply(lambda x: 99 if x['low'] == x['%s_limit' % limit_type] else 1 if x['close'] == x[
            '%s_limit' % limit_type] else 0, axis=1)
    # elif limit_type=='DOWN':
    #     data['is_floor']=data.apply(lambda x:-99 if x['high']==x['%s_limit'%limit_type] else -1 if x['close']==x['%s_limit'%limit_type] else 0,axis=1)
    # elif limit_type == 'BOTH':
    #     data['limit'] = data.apply(
    #         lambda x: 99 if x['low'] == x['up_limit'] else 1 if x['close'] == x['up_limit'] else -1 if x['close']==x['down_limit'] else -99 if x['high']==x['down_limit'] else 0, axis=1)
    for i in range(1, days + 1):
        df = basic.basic().pre_data(data, label=['is_roof'], pre_days=i)
        data = data.merge(df[['ts_code', 'trade_date', 'pre_%s_is_roof' % i]], on=['ts_code', 'trade_date'])
    return data.dropna()


def dis_first_limit_second(data=None, start_date=None, end_date=None, days=2):
    """
    首板后连板的概率
    """
    if data is None:
        data = first_limit(start_date=start_date, end_date=end_date, limit_type='up', days=days)
    count_all = data[data['is_roof'] >= 1].groupby('trade_date')['ts_code'].count()
    count_all_not_one = data[data['is_roof'] == 1].groupby('trade_date')['ts_code'].count()
    count_first = data[(data['is_roof'] == 1) & (data['pre_1_is_roof'] == 0)].groupby('trade_date')['ts_code'].count()
    count_n_y_y = \
        data[(data['is_roof'] >= 1) & (data['pre_2_is_roof'] == 0) & (data['pre_1_is_roof'] == 1)].groupby(
            'trade_date')[
            'ts_code'].count()
    count_n_y_a = data[(data['pre_2_is_roof'] == 0) & (data['pre_1_is_roof'] >= 1)].groupby('trade_date')[
        'ts_code'].count()
    df = pd.DataFrame({'count_all': count_all, 'count_all_not_one': count_all_not_one, 'count_first': count_first,
                       'count_n_y_y': count_n_y_y, 'count_n_y_a': count_n_y_a})
    # save_data(data[['ts_code','trade_date','is_roof','pre_1_is_roof','pre_2_is_roof']],'连板股票%s.csv'%start)

    df['p_nyy'] = df['count_n_y_y'] / df['count_n_y_a']
    df.fillna(0, inplace=True)
    return df

def m_up_in_n_day():
    # N天M板
    N,M=7,3
    limit_up=read_data('stk_limit')
    daily=read_data('daily')
    data=daily.merge(limit_up,on=['ts_code','trade_date'])
    print(limit_up.shape,daily.shape,data.shape)
    data['UP']=data.apply(lambda x :1 if x['close']==x['up_limit'] else 0,axis=1)
    print(data.shape,data.dropna().shape)

    data.sort_values(['ts_code','trade_date'],inplace=True)
    data.reset_index(drop=True,inplace=True)

    data['times']=data.groupby('ts_code')['UP'].rolling(7).sum().reset_index(drop=True)

    # df=data.groupby('ts_code')['UP'].rolling(7).sum().reset_index()

print()

if __name__ == '__main__':
    start = '20180101'
    data = first_limit(start_date=start, days=2)
    save_data(data.drop(columns=['change', 'pct_chg', 'vol', 'amount', 'up_limit', 'down_limit']),
              '连板股票%s.csv' % start)
    # df=dis_first_limit_second(data=data,start_date=start,days=2)
    df = dis_first_limit_second(start_date=start, days=2)
    save_data(df, '连板概率%s.csv' % start)
    print('have_done')

