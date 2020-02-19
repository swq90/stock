import pandas as pd
from stock.sql.data import read_data
import stock.util.basic as basic
start='20190201'
data=read_data(table_name='daily', start_date=start)
stk_limit=read_data(table_name='stk_limit',start_date=start)
data=data.merge(stk_limit,on=['ts_code','trade_date'])
data['is_roof']=data.apply(lambda x:99 if x['low']==x['up_limit'] else 1 if x['close']==x['up_limit'] else 0,axis=1)

for i in range(1,3):
    df=basic.basic().pre_data(data,label=['is_roof'],pre_days=i)
    data=data.merge(df[['ts_code','trade_date','pre_%s_is_roof'%i]],on=['ts_code','trade_date'])

count_all=data[data['is_roof']>=1].groupby('trade_date')['ts_code'].count()
count_all_not_one=data[data['is_roof']==1].groupby('trade_date')['ts_code'].count()
count_first=data[(data['is_roof']==1)&(data['pre_1_is_roof']==0)].groupby('trade_date')['ts_code'].count()
count_n_y_y=data[(data['is_roof']>=1)&(data['pre_2_is_roof']==0)&(data['pre_1_is_roof']==1)].groupby('trade_date')['ts_code'].count()
count_n_y_a=data[(data['pre_2_is_roof']==0)&(data['pre_1_is_roof']==1)].groupby('trade_date')['ts_code'].count()
df=pd.DataFrame({'count_all':count_all,'count_all_not_one':count_all_not_one,'count_first':count_first,'count_n_y_y':count_n_y_y,'count_n_y_a':count_n_y_a})
df['p_nyy']=df['count_n_y_y']/df['count_n_y_a']
print()