import pandas as pd
import datetime
import tushare as ts
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import save_data,read_data
from stock.util.basic import basic
# 新股破板时间一般在多久破板
# 破板后多久huo
pro=ts.pro_api()
class idea2:

    def __init__(self, start_date=None, end_date=None, days=1):
        self.start_date = start_date
        self.end_date = end_date
        self.raw_data = read_data('daily',start_date=self.start_date,end_date=self.end_date)
        self.stk_limt=read_data('stk_limit',start_date=self.start_date,end_date=self.end_date)
        self.basic=read_data('stock_basic')
        self.basic=self.basic.loc[self.basic['list_date']>=start_date]
        self.raw_data=self.raw_data.loc[self.raw_data['ts_code'].isin(self.basic['ts_code'])]

    def days(self,start_date,end_date=''):
        end_date=datetime.datetime.strptime(end_date, '%Y%m%d') if end_date else datetime.datetime.today()
        start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
        # print(start_date,end_date,(end_date-start_date).days)
        return (end_date-start_date).days
    def list_days(self,data=pd.DataFrame(),days=0):
        '''
        上市天数
        :param data:
        :param list_days:int  上市天数需要大于list_days
        :return:data[['ts_code', 'trade_date', 'list_days']],list_days 当前交易日该股票已经上市天数
        '''
        if data.empty:
            data=self.raw_data.copy()

        # self.data=self.data.merge(self.basic[['ts_code','list_date']],on='ts_code')
        # data['days'] = data.apply(lambda x: (datetime.date(int(x['trade_date'][:4]), int(x['trade_date'][4:6]),
        #                                                    int(x['trade_date'][6:])) - datetime.date(
        #     int(x['list_date'][:4]), int(x['list_date'][4:6]), int(x['list_date'][6:]))).days, axis=1)
        # days为上市日期到交易日期之间的交易日天数
        data=data.merge(self.basic[['ts_code','list_date']],on='ts_code')
        data['list_days'] = data.apply(
            lambda x: self.days(x['list_date'], end_date=x['trade_date']), axis=1)
        # print(self.days(self.data.loc[:,'list_date'],self.data.loc[:,'trade_date']))
        if days:
            data=data.loc[data['list_days']<=days]
        return data
    def break_limit(self,N=100):
        data = self.raw_data.copy()
        print(data.shape)
        data = self.list_days(data)
        print(data.shape)
        data=data.loc[data['list_days']<=N]
        data=data.merge(self.stk_limt[['ts_code','trade_date','up_limit']],on=['ts_code','trade_date'])
        print(data.shape)
        data=data.loc[data['low']<data['up_limit']]
        data_first_day=data.loc[data['low']>=(data['pre_close']*1.2-0.01)]
        print(data.shape)
        data=data.append(data_first_day).append(data_first_day).drop_duplicates(keep=False)
        print(data.shape)
        break_data=data.groupby('ts_code')['trade_date'].min().to_frame().reset_index()
        print(break_data.shape)
        return break_data
    def next_performance(self,data,PRICEBUY='close',PRICESELL='close',days=1):
        res=pd.DataFrame()

        for d in range(1, days):
            wool=sheep.wool2(data,self.raw_data,PRICEB=PRICEBUY,PRICES=PRICESELL,days=d)
            res['%spct' % d] = wool['pct']
            res['%sall_pct' % d] = wool['all_pct']
        res['n']=wool['n']
        return res.iloc[:-days,:]
# t=pro.concept('')
# o=idea2(start_date='20190101',end_date='20190201')
# o.list_days(days=60)
# o.data=o.data[o.data['list_date']>='20190101']
# o.data=o.data.merge(o.stl_limt,on=['ts_code','trade_date'])
# o.data['is_up_limit']=o.data.apply(lambda x:1 if x['close']>=x['up_limit'] else 0)
# o.data['up_limit_days']=o.data.groupby('ts_code')[]
# o.red_line=o.data.loc[o.data['low']>=o.data['up_limit']]
# ctnu_days=o.red_line.groupby('ts_code')['list_days'].max()
# o.data_can_buy=o.data[(o.data['low']<o.data['up_limit'])|(o.data['close']<o.data['up_limit'])]
# buy_date=o.data_can_buy.groupby('ts_code')['list_days'].min()
start,end='20180101','20181231'
o=idea2(start_date=start,end_date=end)
t=o.break_limit(N=50)
next=o.next_performance(t,days=5)
print(next.describe())
save_data(next,'新股破板后表现%s-%s.csv'%(start,end))

print()
