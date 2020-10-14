from stock.sql.data import read_data
from stock.util import vars
from stock.subject import continu_limit as cl
start_date = '20170101'
end_date = '20201231'
FORMAT = lambda x: '%.3f' % x
raw_data=read_data('daily', start_date=start_date, end_date=end_date).merge(
        read_data('stk_limit', start_date=start_date, end_date=end_date)[
            ['ts_code', 'trade_date', 'up_limit', 'down_limit']],
        on=['ts_code', 'trade_date'])
def poban(data,report=False):
    data=cl.process(data, [lambda x: (x[vars.HIGH] == x[vars.UP_LIMIT]) & (x[vars.CLOSE] != x[vars.UP_LIMIT])])
#
def down_repo(data,report=False):
