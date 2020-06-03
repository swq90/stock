import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data,save_data
from stock.util.basic import basic
start_date='20190101'
end_date='20201231'
# days=2
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
def re_limit_up3(start_date,end_date):
    rawdata=read_data('daily',start_date=start_date,end_date=end_date)
    # list_days=basic().list_days(rawdata)
    limit=read_data('stk_limit',start_date=start_date,end_date=end_date)
    data=rawdata.merge(limit[['ts_code','trade_date','up_limit']],on=['ts_code','trade_date'])
    print(data.shape)
    data['red_line']=data.apply(lambda x:1 if x['low']==x['up_limit'] else 0,axis=1)
    data['reback_limit']=data.apply(lambda x:1 if (x['open']==x['close'])&(x['open']==x['up_limit'])&(x['low']<x['close']) else 0,axis=1)
    data['limit']=data.apply(lambda x:1 if x['close']==x['up_limit'] else 0,axis=1)

    print(data.shape)
    pre=basic().pre_data(data,label=['red_line'],pre_days=3)
    data=data.merge(pre[['ts_code','trade_date','pre_3_red_line']],on=['ts_code','trade_date'])
    pre=basic().pre_data(data,label=['reback_limit'],pre_days=2)
    data=data.merge(pre[['ts_code','trade_date','pre_2_reback_limit']],on=['ts_code','trade_date'])
    pre=basic().pre_data(data,label=['limit'],pre_days=1)
    data=data.merge(pre[['ts_code','trade_date','pre_1_limit']],on=['ts_code','trade_date'])
    print(data.shape)
    data=data.loc[data['pre_3_red_line']==1]
    print(data.shape)
    data=data.loc[data['pre_2_reback_limit'] == 1]
    print(data.shape)
    data=data.loc[data['pre_1_limit'] == 1]
    print(data.shape)

    #
    # data=data.loc[(data['up_limit']==data['close'])&(data['open']==data['close'])&(data['low']<data['up_limit'])]
    # print(data.shape)
    data=data.merge(basic().list_days(data,list_days=15))
    print(data.shape)
    print(data.describe())
    # wool=sheep.wool(data,rawdata)
    return data



#云内动力，pre2涨停，pre1，均价低于前日收盘，
def yunei(start_date,end_date):
    rawdata = read_data('daily', start_date=start_date, end_date=end_date)
    limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
    data = rawdata.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])
    print(data.shape)

    data['limit'] = data.apply(lambda x: 1 if x['close'] == x['up_limit'] else 0, axis=1)
    data['down']=data.apply(lambda x: 1 if x['close'] == x['down_limit'] else 0, axis=1)
    pre = basic().pre_data(data, label=['limit'], pre_days=2)
    data = data.merge(pre[['ts_code', 'trade_date', 'pre_2_limit']], on=['ts_code', 'trade_date'])

    pre=basic().pre_data(data,label=['pct_chg'],pre_days=1)
    data = data.merge(pre[['ts_code', 'trade_date', 'pre_1_pct_chg']], on=['ts_code', 'trade_date'])

    print(data.shape)

    data = data.loc[data['pre_2_limit'] == 1]
    print(data.shape)
    data = data.loc[data['pre_1_pct_chg'] <-5]
    print(data.shape)

    #
    # data=data.loc[(data['up_limit']==data['close'])&(data['open']==data['close'])&(data['low']<data['up_limit'])]
    # print(data.shape)
    data = data.merge(basic().list_days(data, list_days=15))
    print(data.shape)
    print(data.describe())
    # wool=sheep.wool(data,rawdata)
    return data

def dis(data,model):
    start_date=data['trade_date'].min()
    end_date=data['trade_date'].max()
    res=pd.DataFrame()

    rawdata = read_data('daily', start_date=start_date, end_date=end_date)


    return
def aomei(start_date,end_date):
    #跌停破板后如何表现
    rawdata = read_data('daily', start_date=start_date, end_date=end_date)
    limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
    data = rawdata.merge(limit[['ts_code', 'trade_date', 'down_limit']], on=['ts_code', 'trade_date'])
    print(data.shape)

    data['down'] = data.apply(lambda x: 1 if ((x['low'] == x['down_limit']) &(x['close']!=x['low'])) else 0, axis=1)
    pre = basic().pre_data(data, label=['down'], pre_days=1)
    data = data.merge(pre[['ts_code', 'trade_date', 'pre_1_down']], on=['ts_code', 'trade_date'])

    print(data.shape)

    data = data.loc[data['pre_1_down'] == 1]
    print(data['pct_chg'].describe())

    #
    # data=data.loc[(data['up_limit']==data['close'])&(data['open']==data['close'])&(data['low']<data['up_limit'])]
    # print(data.shape)
    data = data.merge(basic().list_days(data, list_days=15))
    print(data.shape)
    print(data.describe())
    # wool=sheep.wool(data,rawdata)
    return data


def guolikeji(start_date,end_date):
    # -5非涨停，-4~-2一字板，-1[-4,-6],上市25天以上
    rawdata = read_data('daily', start_date=start_date, end_date=end_date)
    limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
    data = rawdata.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])
    print(data.shape)

    data['limit'] = data.apply(lambda x: 1 if x['low'] == x['up_limit'] else 0, axis=1)
    data['limit2']=data.apply(lambda x: 1 if x['close'] == x['up_limit'] else 0, axis=1)
    guoli=data.copy()


    for day in range(4,1,-1):
        pre = basic().pre_data(data, label=['limit'], pre_days=day)
        guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_%s_limit'%day]], on=['ts_code', 'trade_date'])
        guoli=guoli.loc[guoli['pre_%s_limit'%day]==1]
        data=data.loc[data['ts_code'].isin(guoli['ts_code'])]
        print(day,guoli.shape)

    print(guoli.shape,guoli.iloc[:,-5:].describe())
    # pre = basic().pre_data(data, label=['pct_chg'], pre_days=1)
    # guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_1_pct_chg']], on=['ts_code', 'trade_date'])
    # guoli=guoli.loc[(guoli['pre_1_pct_chg']>=-7)&(guoli['pre_1_pct_chg']<=-3)]
    # data = data.loc[data['ts_code'].isin(guoli['ts_code'])]
    # print(guoli.shape,guoli.iloc[:,-5:].describe())
    pre = basic().pre_data(data, label=['limit2'], pre_days=5)
    guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_5_limit2']], on=['ts_code', 'trade_date'])
    guoli=guoli.loc[guoli ['pre_5_limit2']==0]
    pre = basic().pre_data(data, label=['pct_chg'], pre_days=1)
    guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_1_pct_chg']], on=['ts_code', 'trade_date'])
    save_data(guoli,'guoli.csv')
    guoli=guoli.loc[(guoli['pre_1_pct_chg']>=-7)&(guoli['pre_1_pct_chg']<=-3)]
    data = data.loc[data['ts_code'].isin(guoli['ts_code'])]
    print(guoli.shape,guoli.iloc[:,-5:].describe())


    # data = data.loc[data['ts_code'].isin(guoli['ts_code'])]



    # 过滤新股
    list_days=basic().list_days(guoli,list_days=25)
    guoli=guoli.merge(list_days,on=['ts_code','trade_date'])

    print(guoli.shape[0])
    guoli['ma']=guoli['amount']/guoli['vol']*10
    save_data(guoli,'guoli相似股票数据.csv')
    for ps in ['high','low','ma']:
        guoli['%s/pre_close'%ps]=100*(guoli[ps]/guoli['pre_close']-1)
        print(guoli['%s/pre_close'%ps].mean())
    print(data.shape)


    print()


def mengjie(start_date,end_date):
    # 连续五天一字板涨停，最近一天涨幅在[2,4],后日表现
    rawdata = read_data('daily', start_date=start_date, end_date=end_date)
    limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
    data = rawdata.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])
    print(data.shape)

    data['limit'] = data.apply(lambda x: 1 if x['low'] == x['up_limit'] else 0, axis=1)
    guoli=data.copy()
    for day in range(2,7):
        pre = basic().pre_data(data, label=['limit'], pre_days=day)
        guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_%s_limit'%day]], on=['ts_code', 'trade_date'])
        guoli=guoli.loc[guoli['pre_%s_limit'%day]==1]
        data=data.loc[data['ts_code'].isin(guoli['ts_code'])]
        print(day,guoli.shape)
    pre = basic().pre_data(data, label=['pct_chg'], pre_days=1)
    guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_1_pct_chg']], on=['ts_code', 'trade_date'])
    save_data(guoli,'mengjie.csv')
    guoli=guoli.loc[(guoli['pre_1_pct_chg']>=1)&(guoli['pre_1_pct_chg']<=5)]

    # 过滤新股
    list_days=basic().list_days(guoli,list_days=25)
    guoli=guoli.merge(list_days,on=['ts_code','trade_date'])

    print(guoli.shape[0])
    guoli['ma']=guoli['amount']/guoli['vol']*10
    save_data(guoli,'mengjie相似股票数据.csv')
    for ps in ['high','low','ma']:
        guoli['%s/pre_close'%ps]=100*(guoli[ps]/guoli['pre_close']-1)
        print(guoli['%s/pre_close'%ps].mean())
    print(data.shape)


    print()

def jiai(start_date,end_date):
    # 非涨跌停，两个一字板，一字板回封，收盘-2~-6low==down_limit
    rawdata = read_data('daily', start_date=start_date, end_date=end_date)
    limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
    data = rawdata.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])
    print(data.shape)

    data['limit'] = data.apply(lambda x: 1 if x['low'] == x['up_limit'] else 0, axis=1)
    data['ulrl']=data.apply(lambda x:1  if ((x['up_limit']==x['close'])&(x['open']==x['close'])&(x['low']<x['close'])) else 0,axis=1)
    data['nn'] = data.apply(lambda x: 1 if ((x['close'] != x['up_limit']) & (x['close'] != x['down_limit'])) else 0,
                               axis=1)

    guoli=data.copy()
    for day in range(3,5):
        pre = basic().pre_data(data, label=['limit'], pre_days=day)
        guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_%s_limit'%day]], on=['ts_code', 'trade_date'])
        guoli=guoli.loc[guoli['pre_%s_limit'%day]==1]
        data=data.loc[data['ts_code'].isin(guoli['ts_code'])]
        print(day,guoli.shape)
    pre = basic().pre_data(data, label=['pct_chg','low','down_limit'], pre_days=1)
    guoli = guoli.merge(pre[['ts_code', 'trade_date','pre_1_low','pre_1_down_limit']], on=['ts_code', 'trade_date'])
    guoli=guoli.loc[(guoli['pre_1_low']==guoli['pre_1_down_limit'])]
    data = data.loc[data['ts_code'].isin(guoli['ts_code'])]

    pre = basic().pre_data(data, label=['nn'], pre_days=5)
    guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_5_nn']], on=['ts_code', 'trade_date'])
    guoli=guoli.loc[(guoli['pre_5_nn']==1)]
    data = data.loc[data['ts_code'].isin(guoli['ts_code'])]

    pre = basic().pre_data(data, label=['ulrl'], pre_days=2)
    guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_2_ulrl']], on=['ts_code', 'trade_date'])

    guoli = guoli.loc[guoli['pre_2_ulrl' ] == 1]
    data = data.loc[data['ts_code'].isin(guoli['ts_code'])]
    # 过滤新股
    save_data(guoli,'jiai.csv')
    list_days=basic().list_days(guoli,list_days=25)
    guoli=guoli.merge(list_days,on=['ts_code','trade_date'])

    print(guoli.shape[0])
    guoli['ma']=guoli['amount']/guoli['vol']*10
    save_data(guoli,'jiai相似股票数据.csv')
    for ps in ['high','low','ma']:
        guoli['%s/pre_close'%ps]=100*(guoli[ps]/guoli['pre_close']-1)
        print(guoli['%s/pre_close'%ps].mean())
    print(data.shape)
    print()

def guangbai(start_date,end_date):
    # 两个普通涨停，一个一字板,后日表现
    rawdata = read_data('daily', start_date=start_date, end_date=end_date)
    limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
    data = rawdata.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])
    print(data.shape)
    # data['cd'] = data.apply(lambda x: 1 if (x['close'] == x['down_limit'])&(x['open']>0.98*x['pre_close']) else 0, axis=1)
    data['open_less_limit']= data.apply(lambda x: 1 if (x['open']!=x['up_limit'])&(x['close']==x['up_limit'])&(x['open'] >= (x['pre_close']*1.05)) else 0, axis=1)
    data['line_limit'] = data.apply(lambda x: 1 if x['low'] == x['up_limit'] else 0, axis=1)
    data['or_limit']=data.apply(lambda x: 1 if( x['open'] != x['up_limit'])&(x['close'] == x['up_limit']) else 0, axis=1)
    data['nn'] = data.apply(lambda x: 1 if (x['high'] < x['up_limit']) & (x['close'] > x['down_limit']) else 0, axis=1)


    guoli=data.copy()
    for day in range(len(days)):
        if not day:
            continue
        pre = basic().pre_data(data, label=[days[day]], pre_days=day+1)
        guoli = guoli.merge(pre[['ts_code', 'trade_date', 'pre_%s_%s'%(day+1,days[day])]], on=['ts_code', 'trade_date'])
        guoli=guoli.loc[guoli['pre_%s_%s'%(day+1,days[day])]==1]
        data=data.loc[data['ts_code'].isin(guoli['ts_code'])]
        print(day,guoli.shape)

    save_data(guoli,'guangbo.csv')

    # 过滤新股
    list_days=basic().list_days(guoli,list_days=30)
    guoli=guoli.merge(list_days,on=['ts_code','trade_date'])

    print(guoli.shape[0])
    guoli['ma']=guoli['amount']/guoli['vol']*10
    save_data(guoli,'guangbo相似股票数据%s.csv'%end_date)
    for ps in ['high','low','ma','open','close']:
        guoli['%s/pre_close'%ps]=100*(guoli[ps]/guoli['pre_close']-1)
        print(ps,guoli['%s/pre_close'%ps].mean())
    print('close_limit_up:',guoli.loc[guoli['close']==guoli['up_limit']].shape[0]/guoli.shape[0])
    print('open_limitup:',guoli.loc[guoli['open']==guoli['up_limit']].shape[0]/guoli.shape[0])
    print('line_red:',guoli.loc[(guoli['open']==guoli['up_limit'])&(guoli['close']==guoli['up_limit'])&(guoli['low']==guoli['up_limit'])].shape[0]/guoli.shape[0])



    print(guoli.shape)


    print()
#
# # line_stock()
# # 开盘涨停中间破板后回封
# start_date='20%s0101'
# end_date='20%s1231'
# for year in range(19,21):
#     print(start_date%year,'----',end_date%year)
#     # res=re_limit_up3(start_date%year,end_date%year)
#     # res=yunei(start_date%year,end_date%year)
#         # save_data(res,'破板后回封次日表现云内%s-%s.csv'%(start_date%year,end_date%year))
#     res=aomei(start_date % year, end_date % year)
#     save_data(res,'破板后回封次日表现奥美%s-%s.csv'%(start_date%year,end_date%year))
#     print(res.describe())

# guolikeji(start_date,end_date)
# mengjie(start_date,end_date)
days=[None,'open_less_limit','line_limit','or_limit','nn']
# days=['cd','or_limit'
# ,'nn']
guangbai(start_date,end_date)
print()