import pandas as pd
import datetime

from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic
import stock.subject.continu_limit as cl

# days = 2




today = str(datetime.datetime.today().date())
# selling = 'mid=(high+low)/2'
selling = 'open'
# selling = 'close'

# 开盘非涨停，涨停，红盘，绿盘，跌停

open_res = [cl.NR,cl.open_not_limit,cl.open_red, cl.open_green,cl.open_up_limit,cl.open_down_limit]

con = [cl.up_limit,cl.up_limit,cl.n_n]


pre_1 = []
start_date = '20%s0101'
end_date = '20%s1231'

for year in range(18, 20):
    trace = pd.DataFrame()
    raw_data = read_data('daily', start_date=start_date % year, end_date=end_date % (year)).iloc[:, :-2]
    limit = read_data('stk_limit', start_date=start_date % year, end_date=end_date % year)
    raw_data = raw_data.merge(limit[['ts_code', 'trade_date', 'up_limit', 'down_limit']],
                              on=['ts_code', 'trade_date'])
    print(raw_data.shape[0])
    if selling not in raw_data.columns:
        raw_data.eval(selling, inplace=True)
    list_days = basic().list_days(raw_data, list_days=25)
    print(list_days.shape)
    raw_data = raw_data.merge(list_days, on=['ts_code', 'trade_date'])
    print(start_date % year, '----', end_date % year, 'include %s items' % raw_data.shape[0])
    for o in open_res:
        ds=cl.process(raw_data,[o]+con)
        save_data(ds, '%s%sraw_data.csv' % (year, ','.join([o]+con)), fp_date=True)
        res = cl.roi2(ds, raw_data, selling_price=selling.split('=')[0], cut_left=-24, cut_right=4, step=2)
        if res.empty:
            print('当前条件下无数据')
            continue
        save_data(res,'trace_back_to%s%s-%s%s.csv' % (year, ','.join([o]+con), selling.split('=')[0], today),
                  fp_date=True)
        res.rename(columns = {"n_days": "n_days_%s%s"% (year, ','.join([o]+con)),"num": "num_%s%s"% (year, ','.join([o]+con)),"pct": "pct_%s%s"% (year, ','.join([o]+con))},inplace=True)

        if trace.empty:
            trace=res.copy()
        else:
            trace=trace.merge(res,how='outer',on='pb')
    save_data(trace, 'trace_back_to%s%s%s.csv' % (year, selling.split('=')[0], today),fp_date=True)

