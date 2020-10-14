import pandas as pd
from stock.sql.data import read_data, save_data
from stock.util import vars

start_date,end_date='20200625','20200730'
data = read_data('daily', start_date=start_date, end_date=end_date).merge(
    read_data('stk_limit', start_date=start_date, end_date=end_date)[
        ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
    on=['ts_code    ', 'trade_date'])
df_high=data[data[vars.HIGH] == data[vars.UP_LIMIT]]
df_break=data[(data[vars.HIGH] == data[vars.UP_LIMIT]) & (data[vars.CLOSE] != data[vars.UP_LIMIT])]
df1=df_high.groupby('trade_date')['ts_code'].size()
df2=df_break.groupby('trade_date')['ts_code'].size()
df=pd.DataFrame([df1,df2])
print(df)
save_data(df,'break_limit.py.csv')