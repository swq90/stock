import pandas as pd
import datetime

from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic

# from
# days = 2
today = str(datetime.datetime.today().date())
# selling = 'mid=(high+low)/2'
selling = 'open'

# 1:切分10类

# open_res = ['NOU', 'OU', 'OR', 'OG', 'OD']  # 开盘非涨停，涨停，红盘，绿盘，跌停
# pre_1 = ['URL', 'ULL']
# con=['ULL','NN']
# open_res = [ 'OR']
# pre_1 = ['ULL']
# con=['ULL','NN']

  # 开盘非涨停，涨停，红盘，绿盘，跌停
# pre_1 = []
# con = ['ULL', 'ULL', 'ULL', 'ULL', 'ULL', 'ULL', 'ULL', 'ULL', 'NN']
# open_res = ['CD']  # 002920
# pre_1 = ['CU']
# con = ['NN']
# open_res = ['ULL']  # 002187
# open_res = ['NOU', 'OU', 'OR', 'OG', 'OD']
# pre_1 = ['CU','ULL','URL']
# con = ['CU','NN']
con = [ 'UL', 'UL','NN']
open_res = ['open_pct']  # 002187
pre_1 = ['ULL']

# 连续两天一字板，或一字板-一字板回封


def process(data, condions):
    '''

    @param data: dataframe,原始数据
    @param condions:list，
    @return:
    '''
    # 一字板，破板回封，一字板or破板回封，非涨跌停，开盘非涨停，开盘涨停，开盘红，开盘绿，开盘跌停
    # limit=read_data('stk_limit', start_date=data[['trade_date']].min(), end_date=data[['trade_date']].max())
    # data = data.merge(limit[['ts_code', 'trade_date', 'up_limit', 'down_limit']], on=['ts_code', 'trade_date'])
    for conditon in condions:
        # if '=' in conditon:
        #     s=conditon.split('=')
        #     data[conditon]=data.apply(lambda x:1 if(((x[s[0]]/x['pre_close']-1)*100)>=float(s[1].split(',')[0]))&(((x[s[0]]/x['pre_close']-1)*100)<=float(s[1].split(',')[1])) else 0,axis=1]
        if conditon == 'ULL':
            data[conditon] = data.apply(
                lambda x: 1 if ((x['up_limit'] == x['close']) & (x['low'] == x['close'])) else 0,
                axis=1)
        elif conditon == 'URL':
            data[conditon] = data.apply(lambda x: 1 if (
                    (x['up_limit'] == x['close']) & (x['open'] == x['close']) & (x['low'] < x['close'])) else 0,
                                        axis=1)
        elif conditon == 'UL':
            data[conditon] = data.apply(lambda x: 1 if
                    x['up_limit'] == x['close']  else 0,
                                        axis=1)
        elif conditon == 'ULRL':
            data[conditon] = data.apply(
                lambda x: 1 if ((x['up_limit'] == x['close']) & (x['open'] == x['close'])) else 0, axis=1)
        elif conditon == 'NN':
            data[conditon] = data.apply(
                lambda x: 1 if ((x['close'] != x['up_limit']) & (x['close'] != x['down_limit'])) else 0, axis=1)
        elif conditon == 'NOU':
            data[conditon] = data.apply(lambda x: 1 if ((x['open'] != x['up_limit'])) else 0, axis=1)
        elif conditon == 'OU':
            data[conditon] = data.apply(lambda x: 1 if ((x['open'] == x['up_limit'])) else 0, axis=1)
        elif conditon == 'OR':
            data[conditon] = data.apply(
                lambda x: 1 if ((x['open'] >= x['pre_close']) & (x['open'] < x['up_limit'])) else 0, axis=1)
        elif conditon == 'OG':
            data[conditon] = data.apply(
                lambda x: 1 if ((x['open'] < x['pre_close']) & (x['open'] > x['down_limit'])) else 0, axis=1)
        elif conditon == 'OD':
            data[conditon] = data.apply(lambda x: 1 if ((x['open'] == x['down_limit'])) else 0, axis=1)
        elif conditon == 'CU':#close涨停，open非涨停
            data[conditon] = data.apply(lambda x: 1 if ((x['close'] == x['up_limit'])&(x['open'] != x['up_limit'])) else 0, axis=1)
        elif conditon == 'CD':#close跌停，open非跌停
            data[conditon] = data.apply(lambda x: 1 if ((x['close'] == x['down_limit'])&(x['open'] != x['down_limit'])) else 0, axis=1)
        elif conditon == 'open_pct':
            data[conditon+'-8'] = data.apply(
            lambda x: 1 if ((x['open'] > x['down_limit']) & (100*(x['open'] /x['pre_close']-1)<=-8)) else 0, axis=1)
            data[conditon+'10'] = data.apply(
            lambda x: 1 if ((x['open'] < x['up_limit']) & (100*(x['open'] /x['pre_close']-1)>8)) else 0, axis=1)
            for p in range(-6,10,2):# close跌停，open非跌停
                data[conditon+'%s'%p] = data.apply(
            lambda x: 1 if ((100*(x['open'] /x['pre_close']-1)>(p-2)) & (100*(x['open'] /x['pre_close']-1)<=p)) else 0, axis=1)

    return data

def filter(data, conditions):
    # [0,'ULRL','ULL']
    # ['NOU',0,'LL,RLL','LL'，‘UL','DL',[2,4]]:开盘不涨停，无，一字板or一字板回封，一字板,
    res = data.copy()
    for day in range(len(conditions)):

        if conditions[day]:
            if 'pre_%s_%s' % (day, conditions[day]) not in res.columns:
                pre = basic().pre_data(data, label=[conditions[day]], pre_days=day)
                res = res.merge(pre[['ts_code', 'trade_date', 'pre_%s_%s' % (day, conditions[day])]],
                                on=['ts_code', 'trade_date'])
            res = res.loc[res['pre_%s_%s' % (day, conditions[day])] == 1]
            data = data.loc[data['ts_code'].isin(res['ts_code'].unique())]
            if res.empty:
                return res
            print(res.shape, data.shape)
    return res


def roi(red_line, raw_data, cut_left=-24, cut_right=24, step=2, selling_price='open'):
    res = []
    section = list(range(cut_left, cut_right, 2))
    # red_line['low_pct']=100*(red_line['low']/red_line['pre_close']-1)
    raw_data = raw_data.loc[raw_data['ts_code'].isin(red_line['ts_code'])]
    for pb in section:
        if red_line.empty:
            continue
        PB = 'PB'
        red_line.loc[:, PB] = red_line['open'] * (1 + 0.01 * pb)
        raw_data.loc[:, PB] = raw_data['open'] * (1 + 0.01 * pb)
        grass = red_line.loc[(red_line[PB] >= red_line['low'])].copy()
        if grass.empty:
            continue
        raw_data.loc[:, PB] = raw_data.apply(lambda x: x['open'] if pb >= 0 else x[PB] if x[PB] >= x['low'] else None,
                                             axis=1)

        # for ps in section:
        #     if ps not in raw_data.columns:
        #         if isinstance(ps,int):
        #             PS = 'PS'
        #             raw_data[PS] = raw_data['pre_close'] * (1 + 0.01 * ps)
        #     else:
        #         PS=ps
        #
        #     # raw_data[PS]=raw_data.apply(lambda x:x[PS] if x[PS]<=x['high'] else x['close'],axis=1)
        #     # 集合竞价中，价格低于open，卖出，否则，价格小于当天最高价卖出，否则收盘价卖出
        #     # raw_data[PS]=raw_data.apply(lambda x:x['open'] if x[PS]<=x['open'] else x['up_limit'] if x[PS]>=x['up_limit'] else x[PS] if x[PS]<=x['high'] else x['close'],axis=1)
        #
        #
        #     meat=sheep.wool2(grass,raw_data,PRICEB=PB,PRICES=PS).dropna()
        #     if meat.empty:
        #         continue
        #     res.append([pb,ps,meat.shape[0],meat.iloc[-1,-1]])
        meat = sheep.wool2(grass, raw_data, PRICEB=PB, PRICES=selling_price).dropna()
        if meat.empty:
            continue
        # grass_count=grass.groupby('trade_date')['trade_date'].count().reset_index()
        res.append([pb, meat.shape[0], meat['n'].sum(), meat.iloc[-1, -1]])

    res = pd.DataFrame(res, columns=['pb', 'n_days', 'num', 'pct'])
    return res


def roi2(red_line, raw_data):
    #
    res = []
    section = ['down_limit'] + list(range(-8, 10, 2)) + ['up_limit']
    red_line['low_pct'] = 100 * (red_line['low'] / red_line['pre_close'] - 1)

    for pb in section:
        if pb not in raw_data.columns:
            if isinstance(pb, int):
                PB = 'PB'
                red_line[PB] = red_line['pre_close'] * (1 + 0.01 * pb)
                raw_data[PB] = raw_data['pre_close'] * (1 + 0.01 * pb)
        else:
            PB = pb
        grass = red_line.loc[red_line[PB] >= red_line['low']]
        if grass.empty:
            continue
        for ps in section:
            if ps not in raw_data.columns:
                if isinstance(ps, int):
                    PS = 'PS'
                    raw_data[PS] = raw_data['pre_close'] * (1 + 0.01 * ps)
            else:
                PS = ps

            # raw_data[PS]=raw_data.apply(lambda x:x[PS] if x[PS]<=x['high'] else x['close'],axis=1)
            # 集合竞价中，价格低于open，卖出，否则，价格小于当天最高价卖出，否则收盘价卖出
            raw_data[PS] = raw_data.apply(
                lambda x: x['open'] if x[PS] <= x['open'] else x[PS] if x[PS] <= x['high'] else x['close'], axis=1)

            meat = sheep.wool2(grass, raw_data, PRICEB=PB, PRICES=PS).dropna()
            if meat.empty:
                continue
            res.append([pb, ps, meat.shape[0], meat.iloc[-1, -1]])
    res = pd.DataFrame(res, columns=['pb', 'ps', 'n_days', 'pct'])
    return res


# o,ulrl,ull,nn
# o,ull,ull,nn
# 'NOU','ULRL','ULL','NN'
# N,'ULL','ULL','NN']
if __name__ == '__main__':
    start_date = '20%s0101'
    end_date = '20%s1231'


    for year in range(19, 20):
        trace=pd.DataFrame()
        raw_data = read_data('daily', start_date=start_date % year, end_date=end_date %( year)).iloc[:, :-2]
        limit = read_data('stk_limit', start_date=start_date % year, end_date=end_date % year)
        raw_data = raw_data.merge(limit[['ts_code', 'trade_date', 'up_limit', 'down_limit']],
                                  on=['ts_code', 'trade_date'])
        if selling not in raw_data.columns:
            raw_data.eval(selling, inplace=True)
        list_days = basic().list_days(raw_data, list_days=25)
        print(list_days.shape)
        raw_data = raw_data.merge(list_days, on=['ts_code', 'trade_date'])
        print(start_date % year, '----', end_date % year, 'include %s items' % raw_data.shape[0])

        raw_data = process(raw_data, set(open_res + pre_1 + con))
        # 开盘非涨停回溯
        if 'open_pct' in open_res:
            open_res=['open_pct'+'%s'%x for x in range(-8,11,2)]
        print(open_res)
        for open in open_res:
            if not pre_1:
                pre_1.append(con.pop(0))

            for pre in pre_1:
                conditions = [open, pre] + con
                red_line = filter(raw_data, conditions)
                print(conditions)
                if red_line.empty:
                    print('no stock')
                    continue

                save_data(red_line, '%s%sraw_data.csv' % (year, ','.join(conditions)),fp_date=True)
                res = roi(red_line, raw_data, selling_price=selling.split('=')[0], cut_left=-24, cut_right=4, step=2)
                if res.empty:
                    print('当前条件下无数据')
                    continue
                save_data(res,'trace_back_to%s%s-%s%s.csv' % (year, ','.join(conditions), selling.split('=')[0], today),
                          fp_date=True)
                res.rename(columns = {"n_days": "n_days_%s%s"% (year, ','.join(conditions)),"num": "num_%s%s"% (year, ','.join(conditions)),"pct": "pct_%s%s"% (year, ','.join(conditions))},inplace=True)

                if trace.empty:
                    trace=res.copy()
                else:
                    trace=trace.merge(res,how='outer',on='pb')
        save_data(trace, 'trace_back_to%s%s%s.csv' % (year, selling.split('=')[0], today),fp_date=True)
        # res=yunei(start_date%year,end_date%year)
