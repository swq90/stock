import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data,read_data
from stock.util.basic import basic

start_date='20180101'
end_date='20181231'
PRICEB='close'
def fun1(limit_type='up'):
    res = pd.DataFrame()
    #
    data=read_data('daily',start_date=start_date,end_date=end_date)
    limit=read_data('stk_limit',start_date=start_date,end_date=end_date)
    data=data.merge(limit,on=['ts_code','trade_date'])
    data['is_roof'] = data.apply(lambda x: 99 if x['close'] == x['up_limit' ] else 1 if x['close'] == x[
        'down_limit'] else x['pct_chg'], axis=1)
    for rate in [-99]+list(range(-10,10))+[99]:
        print(rate)
        df=data.loc[(data['is_roof']>=rate)&(data['is_roof']<(rate+1))].copy()
        if df.empty:
            continue
        # df['pct']=(df['close']/df['open']-1)*100
        # res.loc[rate,'pct']=df['pct'].mean()
        wool=sheep.wool2(df[['ts_code','trade_date']],data,PRICEB=PRICEB,days=1)
        res.loc[rate,'mean']=wool.iloc[:,-3].mean()
        res.loc[rate, 'n'] = wool.iloc[-1, -2]
        res.loc[rate, 'all_pct'] = wool.iloc[-1, -1]

    save_data(res,'pct_chg_cut_res%s-%s.csv'%(start_date,end_date))
def cut_stock():
    pass



def func2():
    data=read_data('stock_basic')


fun1()
print()