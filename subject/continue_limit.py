import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data,save_data
from stock.util.basic import basic
# from
days=2
sell_price='open'
# 连续两天一字板，或一字板-一字板回封


def process(data,condions):
    '''

    @param data: dataframe,原始数据
    @param condions:list，
    @return:
    '''
    # limit=read_data('stk_limit', start_date=data[['trade_date']].min(), end_date=data[['trade_date']].max())
    # data = data.merge(limit[['ts_code', 'trade_date', 'up_limit', 'down_limit']], on=['ts_code', 'trade_date'])
    for condion in condions:
        if condion=='ULL':
            data[condion]=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['low']==x['close'])) else 0,axis=1)
            
        elif condion=='ULRL':
            data[condion]=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['open']==x['close'])) else 0,axis=1)
        elif condion=='NN':
            data[condion]=data.apply(lambda x: 1 if ((x['close']!=x['up_limit']) &(x['close']!=x['down_limit'])) else 0,axis=1)
        elif condion=='NOU':
            data[condion]=data.apply(lambda x: 1 if ((x['open']!=x['up_limit'])) else 0,axis=1)
        elif condion=='OU':
            data[condion]=data.apply(lambda x: 1 if ((x['open']==x['up_limit'])) else 0,axis=1)
        elif condion=='OR':
            data[condion]=data.apply(lambda x: 1 if ((x['open']>=x['pre_close'])) else 0,axis=1)
        elif condion=='OG':
            data[condion]=data.apply(lambda x: 1 if ((x['open']<x['pre_close'])) else 0,axis=1)
        elif condion=='OD':
            data[condion]=data.apply(lambda x: 1 if ((x['open']==x['down_limit'])) else 0,axis=1)

    return data

def filter(data,conditions):

    # [0,'ULRL','ULL']
    # ['NOU',0,'LL,RLL','LL'，‘UL','DL',[2,4]]:开盘不涨停，无，一字板or一字板回封，一字板,
    res=data.copy()
    for day in range(len(conditions)):

        if conditions[day]:
            pre = basic().pre_data(data, label=[conditions[day]], pre_days=day)
            res = res.merge(pre[['ts_code', 'trade_date', 'pre_%s_%s'%(day,conditions[day])]], on=['ts_code', 'trade_date'])
            res = res.loc[res['pre_%s_%s'%(day,conditions[day])] == 1]
            data=data.loc[data['ts_code'].isin(res['ts_code'].unique())]

            print(res.shape,data.shape)
    return res



def roi(red_line,raw_data,cut=26,sell_price=sell_price):
    res=[]
    section=list(range(-cut,1,2))
    # red_line['low_pct']=100*(red_line['low']/red_line['pre_close']-1)

    for pb in section:
        PB = 'PB'
        red_line[PB]=red_line['open']* (1 + 0.01 * pb)
        raw_data[PB] = raw_data['open'] * (1 + 0.01 * pb)


        grass = red_line.loc[(red_line[PB] >= red_line['low'])].copy()
        if grass.empty:
            continue
        raw_data[PB]=raw_data.apply(lambda x:x['open'] if pb>=0 else x[PB] if x[PB]>=x['low'] else None,axis=1)

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
        meat=sheep.wool2(grass,raw_data,PRICEB=PB,PRICES='open').dropna()
        if meat.empty:
            continue
        # grass_count=grass.groupby('trade_date')['trade_date'].count().reset_index()
        res.append([pb,meat.shape[0],meat['n'].sum(),meat.iloc[-1,-1]])

    res=pd.DataFrame(res,columns=['pb','n_days','num','pct'])
    return res



def roi2(red_line,raw_data):
    #
    res=[]
    section=['down_limit']+list(range(-8,10,2))+['up_limit']
    red_line['low_pct']=100*(red_line['low']/red_line['pre_close']-1)

    for pb in section:
        if pb not in raw_data.columns:
            if isinstance(pb,int):
                PB = 'PB'
                red_line[PB]=red_line['pre_close']* (1 + 0.01 * pb)
                raw_data[PB] = raw_data['pre_close'] * (1 + 0.01 * pb)
        else:
            PB=pb
        grass = red_line.loc[red_line[PB] >= red_line['low']]
        if grass.empty:
            continue
        for ps in section:
            if ps not in raw_data.columns:
                if isinstance(ps,int):
                    PS = 'PS'
                    raw_data[PS] = raw_data['pre_close'] * (1 + 0.01 * ps)
            else:
                PS=ps

            # raw_data[PS]=raw_data.apply(lambda x:x[PS] if x[PS]<=x['high'] else x['close'],axis=1)
            # 集合竞价中，价格低于open，卖出，否则，价格小于当天最高价卖出，否则收盘价卖出
            raw_data[PS]=raw_data.apply(lambda x:x['open'] if x[PS]<=x['open'] else x[PS] if x[PS]<=x['high'] else x['close'],axis=1)


            meat=sheep.wool2(grass,raw_data,PRICEB=PB,PRICES=PS).dropna()
            if meat.empty:
                continue
            res.append([pb,ps,meat.shape[0],meat.iloc[-1,-1]])
    res=pd.DataFrame(res,columns=['pb','ps','n_days','pct'])
    return res


#o,ulrl,ull,nn
#o,ull,ull,nn
#'NOU','ULRL','ULL','NN'
# N,'ULL','ULL','NN']
if __name__=='__main__':
    start_date='20%s0101'
    end_date='20%s1231'
    open_res = ['NOU', 'OU', 'OR', 'OG', 'OD']  # 开盘非涨停，涨停，红盘，绿盘，跌停
    pre_1=['ULRL','ULL']

    for year in range(18,21):
        raw_data = read_data('daily', start_date=start_date%year, end_date=end_date%year).iloc[:,:-2]
        limit = read_data('stk_limit', start_date=start_date%year, end_date=end_date%year)
        raw_data = raw_data.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])
        list_days = basic().list_days(raw_data,list_days=25)
        print(list_days.shape)
        raw_data = raw_data.merge(list_days, on=['ts_code', 'trade_date'])
        print(start_date%year,'----',end_date%year,'include %s items'%raw_data.shape[0])

        raw_data=process(raw_data,open_res+pre_1+['NN'])
        # 开盘非涨停回溯
        for open in open_res:
            for pre in pre_1:
                condition=[open,pre,'ULL','NN']
                red_line=filter(raw_data,condition)
                if red_line.empty:
                    continue
                save_data(red_line,'%s%s数据.csv'%(year,','.join(condition)))
                res=roi(red_line,raw_data)
                save_data(res,'%s%s回溯.csv'%(year,','.join(condition)))
        # res=yunei(start_date%year,end_date%year)