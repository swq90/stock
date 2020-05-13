import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data,save_data
from stock.util.basic import basic

days=2

# 连续两天一字板，或一字板-一字板回封
def red_line(data):
    # rawdata = read_data('daily', start_date=start_date, end_date=end_date)
    # limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
    # data = rawdata.merge(limit[['ts_code', 'trade_date', 'up_limit']], on=['ts_code', 'trade_date'])
    data['red_line']=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['low']==x['close'])) else 0,axis=1)
    data['reback_limit']=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['open']==x['close'])) else 0,axis=1)
    pre = basic().pre_data(data, label=['red_line'], pre_days=1)
    data = data.merge(pre[['ts_code', 'trade_date', 'pre_1_red_line']], on=['ts_code', 'trade_date'])
    data=data.loc[(data['pre_1_red_line'] == 1)&(data['reback_limit'] == 1)]
    print(data.shape)
    list_days=basic().list_days(data)
    print(list_days.shape)
    data=data.merge(list_days,on=['ts_code','trade_date'])
    print(data.shape)
    return data[['ts_code','trade_date']]
# 一字板-一字板，一字板-一字板回封
# 如果买入当天涨停则不处理
def roi(data,rawdata):
    summary=pd.DataFrame()
    print(data.shape[0])
    all_data = rawdata.copy()
    for pctb in range(-10,11,2):
        if pctb==-10:
            all_data['price_buy']=all_data['down_limit']
        elif pctb==10:
            all_data['price_buy']=all_data['up_limit']
        else:

            all_data['price_buy']=all_data['pre_close']*(1+0.01*pctb)

        for pcts in range(-10,11,2):
            if pcts == -10:
                all_data['price_sell'] = all_data['down_limit']
            elif pcts == 10:
                all_data['price_sell'] = all_data['up_limit']
            else:
                all_data['price_sell'] = all_data['pre_close']*(1+0.01*pctb)
            all_data=all_data.loc[(all_data['price_buy']>=all_data['low'])&(all_data['price_sell']<=all_data['high'])]

            res=sheep.wool2(data,all_data,PRICEB='price_buy',PRICES='price_sell')
            detail=pd.DataFrame({'buy':{pctb},'sell':{pcts},'day':{res.shape[0]},'n':{res['n'].sum()},'pct_avg':{res['pct'].mean()},'pct_all':{res.iloc[-1,-1]}})
            summary=pd.concat([summary,detail],ignore_index=True)
            print()

start_date='20%s0401'
end_date='20%s1231'
for year in range(20,21):
    rawdata = read_data('daily', start_date=start_date%year, end_date=end_date%year)
    limit = read_data('stk_limit', start_date=start_date%year, end_date=end_date%year)
    rawdata = rawdata.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])

    print(start_date%year,'----',end_date%year)
    line_stock=red_line(rawdata)
    res=roi(limit,rawdata)

    # res=yunei(start_date%year,end_date%year)

#
print()

