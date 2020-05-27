import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data,save_data
from stock.util.basic import basic
# from
days=2

# 连续两天一字板，或一字板-一字板回封


def process(data,condions):
    '''

    @param data: dataframe,原始数据
    @param condions:list，
    @return:


    '''

    key={'ULL':None,'ULRL':None}
    # limit=read_data('stk_limit', start_date=data[['trade_date']].min(), end_date=data[['trade_date']].max())
    # data = data.merge(limit[['ts_code', 'trade_date', 'up_limit', 'down_limit']], on=['ts_code', 'trade_date'])
    for condion in condions:
        if condion=='ULL':
            data[condion]=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['low']==x['close'])) else 0,axis=1)
            
        elif condion=='ULRL':
            data[condion]=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['open']==x['close'])) else 0,axis=1)
        elif condion=='NN':
            data[condion]=data.apply(lambda x: 1 if ((x['close']!=x['up_limit']) &(x['close']!=x['down_limit'])) else 0,axis=1)
    return data

def filter(data,**conditions,):

    # [0,'ULRL','ULL']
    # [0,'LL,RLL','LL'，‘UL','DL',[2,4]]:无，一字板or一字板回封，一字板,
    res=data.copy()
    for day in range(len(conditions)):
        if conditions[day]:
            pre = basic().pre_data(data, label=[conditions[day]], pre_days=day)
            res = res.merge(pre[['ts_code', 'trade_date', 'pre_%s_%s'%(day,conditions[day])]], on=['ts_code', 'trade_date'])
            res = res.loc[res['pre_%s_%s'%(day,conditions[day])] == 1]
            data=data.loc[data['ts_code'].isin(res['ts_code'].unique())]

            print(res.shape,data.shape)
    return res



def roi(red_line,raw_data):
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
        grass = red_line.loc[red_line[PB] >= red_line['low']].copy()
        grass[PB]=grass.apply(lambda x:x['open'] if x[PB]<x['open'] else x[PB],axis=1)
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
            for pct_b in range(len(section)-1)
                buy_data=grass.loc[grass['open']<=grass['pre_close']*pct_b]

            meat=sheep.wool2(grass,raw_data,PRICEB=PB,PRICES=PS).dropna()
            if meat.empty:
                continue
            res.append([pb,ps,meat.shape[0],meat.iloc[-1,-1]])
    res=pd.DataFrame(res,columns=['pb','ps','n_days','pct'])
    return res

def roi2(red_line,raw_data):
    #原版
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


if __name__=='__main__':
    start_date='20%s0101'
    end_date='20%s0231'
    for year in range(18,21):
        raw_data = read_data('daily', start_date=start_date%year, end_date=end_date%year).iloc[:,:-2]

        limit = read_data('stk_limit', start_date=start_date%year, end_date=end_date%year)
        raw_data = raw_data.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])
        list_days = basic().list_days(raw_data,list_days=25)
        print(list_days.shape)
        raw_data = raw_data.merge(list_days, on=['ts_code', 'trade_date'])
        print(start_date%year,'----',end_date%year,'include %s items'%raw_data.shape[0])

        raw_data=process(raw_data,[ 'ULRL','ULL','NN'])
        red_line=filter(raw_data,[0,'ULRL','ULL','NN'],)

        save_data(red_line,'new%s连板nd2.csv'%year)
        res=roi(red_line,raw_data)
        save_data(res,'%s连板回溯nd2.csv'%year)
        # res=yunei(start_date%year,end_date%year)