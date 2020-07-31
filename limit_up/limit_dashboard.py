# ——T涨停数量，破板率

import pandas as pd
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data,save_to_sql
from stock.util.basic import basic
from stock import vars
from stock.subject.continu_limit import process
def red_line_limit(x):
    return x[vars.LOW] == x[vars.UP_LIMIT]
def zhaban(x):
    return(x[vars.HIGH] == x[vars.UP_LIMIT]) & (x[vars.CLOSE] < x[vars.UP_LIMIT])

def red_t_limit(x):
    return (x[vars.OPEN] == x[vars.CLOSE]) & (x[vars.OPEN] == x[vars.UP_LIMIT]) & (x[vars.LOW] < x[vars.CLOSE])
def up_limit(x):
    return x[vars.CLOSE] == x[vars.UP_LIMIT]

start='20180101'
end='20210101'
raw_data = read_data('daily', start_date=start, end_date=end).merge(
    read_data('stk_limit', start_date=start, end_date=end)[
        ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
    on=['ts_code', 'trade_date'])
print(raw_data.shape)
list_days = basic().list_days(raw_data, list_days=30)
raw_data = raw_data.merge(list_days, on=['ts_code', 'trade_date'])
print(raw_data.shape)

def statistic(data,condition):
    if condition==process:
        res=process(raw_data,[up_limit,up_limit],next_day=0)
    else:
        res=data.loc[condition(data)]
    print(res.shape)
    return res
def pin(data,conditons,classify=vars.TRADE_DATE):
    res=pd.DataFrame()
    for con in conditions:
        res[con.__name__]=statistic(data,con).groupby(classify)[vars.TS_CODE].count()
    print(res.describe())
    return res




conditions=[red_line_limit,red_t_limit,up_limit,zhaban,process]
save_to_sql(pin(raw_data,conditions),'limit_count')