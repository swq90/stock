import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data,save_data
from stock.util.basic import basic

days=2
def first_limit(data=None, start_date=None, end_date=None, limit_type='up', days=1):
    """
    首板涨停，pre_1_is_roof==0,is_roof==1
    """
    if data is None:
        data = gls.limit_stock(start_date, end_date, limit_type='both')
    if limit_type == 'up':
        data['is_roof'] = data.apply(lambda x: 99 if x['close'] == x['%s_limit' % limit_type]  else -99 if x['close']==x['down_limit'] else x['pct_chg'], axis=1)
    # elif limit_type=='DOWN':
    #     data['is_floor']=data.apply(lambda x:-99 if x['high']==x['%s_limit'%limit_type] else -1 if x['close']==x['%s_limit'%limit_type] else 0,axis=1)
    # elif limit_type == 'BOTH':
    #     data['limit'] = data.apply(
    #         lambda x: 99 if x['low'] == x['up_limit'] else 1 if x['close'] == x['up_limit'] else -1 if x['close']==x['down_limit'] else -99 if x['high']==x['down_limit'] else 0, axis=1)
    for i in range(1, days + 1):

        df = basic().pre_data(data, label=['is_roof'], pre_days=i)
        data = data.merge(df[['ts_code', 'trade_date', 'pre_%s_is_roof' % i]], on=['ts_code', 'trade_date'])
    return data.dropna()

# 首板次日跌停
# data=first_limit(start_date=start_date,end_date=end_date,days=days)
# data=data.loc[(data['pre_%s_is_roof'%(days-1)]>=80)&(data['pre_%s_is_roof'%days]<80)].copy()
# break_data=data.loc[data['is_roof']==-99].copy()
# print(data.shape,break_data.shape)
# if not break_data.empty:
#     df=read_data('daily',start_date=start_date,end_date=end_date)
#     res1=sheep.wool2(break_data,df,PRICEB='open',PRICES='close')
#     res1 = sheep.wool2(break_data, df, PRICEB='open',
#                        PRICES='open')



# 一字板次日表现
def line_stock():
    raw_data=read_data('daily',start_date=start_date,end_date=end_date)
    data=raw_data.loc[raw_data['low']==raw_data['high']]
    print(data.shape)

    list_days=basic().list_days(data)
    print(list_days.shape)
    data=data.merge(list_days,on=['ts_code','trade_date'])
    res1=sheep.wool2(data.loc[data['pct_chg']>0],raw_data,)
    res2=sheep.wool2(data.loc[data['pct_chg']<0],raw_data)
    print(res1.describe())
    print(res2.describe())
    print()



def re_limit_up(start_date,end_date):
    rawdata=read_data('daily',start_date=start_date,end_date=end_date)
    # list_days=basic().list_days(rawdata)
    limit=read_data('stk_limit',start_date=start_date,end_date=end_date)
    data=rawdata.merge(limit[['ts_code','trade_date','up_limit']],on=['ts_code','trade_date'])
    print(data.shape)
    data['red_line']=data.apply(lambda x:1 if x['low']==x['up_limit'] else 0,axis=1)
    # data['reback_limit']=data.apply(lambda x:1 if (x['open']==x['close'])&(x['open']==x['up_limit'])&(x['low']<x['close']) else 0,axis=1)
    print(data.shape,)
    pre=basic().pre_data(data,label=['red_line'])
    data=data.merge(pre[['ts_code','trade_date','pre_1_red_line']],on=['ts_code','trade_date'])
    print(data.shape)

    data=data.loc[(data['up_limit']==data['close'])&(data['open']==data['close'])&(data['low']<data['up_limit'])&(data['pre_1_red_line']==1)]
    print(data.shape)
    data=data.merge(basic().list_days(data,list_days=15))
    print(data.shape)
    wool=sheep.wool(data,rawdata)
    return wool
def re_limit_up2(start_date,end_date):
    rawdata=read_data('daily',start_date=start_date,end_date=end_date)
    # list_days=basic().list_days(rawdata)
    limit=read_data('stk_limit',start_date=start_date,end_date=end_date)
    data=rawdata.merge(limit[['ts_code','trade_date','up_limit']],on=['ts_code','trade_date'])
    print(data.shape)
    data['red_line']=data.apply(lambda x:1 if x['low']==x['up_limit'] else 0,axis=1)
    data['reback_limit']=data.apply(lambda x:1 if (x['open']==x['close'])&(x['open']==x['up_limit'])&(x['low']<x['close']) else 0,axis=1)
    print(data.shape)
    pre=basic().pre_data(data,label=['red_line'],pre_days=2)
    data=data.merge(pre[['ts_code','trade_date','pre_2_red_line']],on=['ts_code','trade_date'])
    pre=basic().pre_data(data,label=['reback_limit'],pre_days=1)
    data=data.merge(pre[['ts_code','trade_date','pre_1_reback_limit']],on=['ts_code','trade_date'])
    print(data.shape)
    data=data.loc[data['pre_2_red_line']==1]
    print(data.shape)
    data=data.loc[data['pre_1_reback_limit'] == 1]
    print(data.shape)

    #
    # data=data.loc[(data['up_limit']==data['close'])&(data['open']==data['close'])&(data['low']<data['up_limit'])]
    # print(data.shape)
    data=data.merge(basic().list_days(data,list_days=15))
    print(data.shape)
    print(data.describe())
    # wool=sheep.wool(data,rawdata)
    return data

# line_stock()
# 开盘涨停中间破板后回封
start_date='20%s0101'
end_date='20%s1231'
for year in range(18,20):
    print(start_date%year,'----',end_date%year)
    res=re_limit_up2(start_date%year,end_date%year)
    save_data(res,'破板后回封次日表现2%s-%s.csv'%(start_date%year,end_date%year))
    # print(res.describe())


print()