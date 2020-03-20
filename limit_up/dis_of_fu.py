import pandas as pd
from stock.sql.data import save_data
import stock.limit_up.performance_of_first_limit_up as fu
from stock.util.basic import basic
# 首板后次日买入，后日卖出，后日价格统计指标汇总
start,end,days='20191201','20200101',3
o=fu.idea1(start_date=start,end_date=end,days=days)
print(o.raw_data.shape)
o.raw_data=o.raw_data[(o.raw_data['pct_chg']>=-11)&(o.raw_data['pct_chg']<=11)]
print(o.raw_data.shape)
o.raw_data.dropna(inplace=True)
print(o.raw_data.shape)
# 1.卖出当日股价较前日收盘的变化

o.raw_data['ma']= 10 * o.raw_data['amount'] / o.raw_data['vol']
o.raw_data['pct:h-l']=(o.raw_data['high']-o.raw_data['low'])
o.raw_data['pct:o-c']=(o.raw_data['open']-o.raw_data['close'])
o.raw_data['pct:h-o']=(o.raw_data['high']-o.raw_data['open'])

print('%s个交易日' % o.raw_data['trade_date'].unique().shape[0])
o.data = o.raw_data.loc[
    (o.raw_data['pre_%s_is_roof'%days] == 0) & (o.raw_data['pre_%s_is_roof'%(days-1)] == 1)].copy()
df=basic().pre_data(o.data,label=['open'],new_label=['pre_open'])
o.data=o.data.merge(df[['ts_code','trade_date','pre_open']],on=['ts_code','trade_date'])
o.raw_data.dropna(inplace=True)
print(o.raw_data.shape)
pct=pd.DataFrame()
# for con in ['all','up']+list(range(1,5)):
for con in ['all','up']:

    df = o.select(con, days=days)
    pct_some = pd.DataFrame()
    for item in ['open','close','ma','high','low','pct:h-l','pct:o-c','pct:h-o']:

        df['%s_%s/pre_close'%(con,item)]=df[item]/df['pre_close']
        pct_some=pd.concat([pct_some, df['%s_%s/pre_close'%(con,item)]],axis=1)
    pct_some = pd.DataFrame(pct_some.describe(include='all')).reset_index()
    pct_some['index'] = pct_some['index'].astype('object')
    pct=pd.concat([pct,pct_some],axis=1)
save_data(pct,'不同条件价格分布%s%s.csv'%(start,end))