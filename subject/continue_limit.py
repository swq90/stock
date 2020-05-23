import pandas as pd
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data,save_data
from stock.util.basic import basic
# from
days=2

# 连续两天一字板，或一字板-一字板回封
def red_line(data):
    # raw_data = read_data('daily', start_date=start_date, end_date=end_date)
    # limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
    # data = raw_data.merge(limit[['ts_code', 'trade_date', 'up_limit']], on=['ts_code', 'trade_date'])
    data['red_line']=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['low']==x['close'])) else 0,axis=1)
    data['reback_limit']=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['open']==x['close'])) else 0,axis=1)
    pre = basic().pre_data(data, label=['red_line'], pre_days=1)
    data = data.merge(pre[['ts_code', 'trade_date', 'pre_1_red_line']], on=['ts_code', 'trade_date'])
    data=data.loc[(data['pre_1_red_line'] == 1)&(data['reback_limit'] == 1)]
    print(data.shape)
    list_days=basic().list_days(data)
    print(list_days.shape)
    data=data.iloc[:,:2].merge(list_days,on=['ts_code','trade_date'])
    print(data.shape)
    return data
# 一字板-一字板，一字板-一字板回封
# 如果买入当天涨停则不处理
def roi(data,raw_data):
    summary=pd.DataFrame()
    print(data.shape[0])
    all_data = raw_data.copy()
    for pctb in range(-10,11,2):

        if pctb==-10:
            all_data['price_buy']=all_data['down_limit']
        elif pctb==10:
            all_data['price_buy']=all_data['up_limit']
        else:

            all_data['price_buy']=all_data['pre_close']*(1+0.01*pctb)

        for pcts in range(-10,11,2):
            if pcts == -10:
                all_data['price_sell'] = all_data['down_limit']
            elif pcts == 10:
                all_data['price_sell'] = all_data['up_limit']
            else:
                all_data['price_sell'] = all_data['pre_close']*(1+0.01*pctb)
            all_data=all_data.loc[(all_data['price_buy']>=all_data['low'])&(all_data['price_sell']<=all_data['high'])]

            res=sheep.wool2(data,all_data,PRICEB='price_buy',PRICES='price_sell')
            detail=pd.DataFrame({'buy':{pctb},'sell':{pcts},'day':{res.shape[0]},'n':{res['n'].sum()},'pct_avg':{res['pct'].mean()},'pct_all':{res.iloc[-1,-1]}})
            summary=pd.concat([summary,detail],ignore_index=True)
            print()






print()

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
            data['ULL']=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['low']==x['close'])) else 0,axis=1)
            
        elif condion=='ULRL':
            data['ULRL']=data.apply(lambda x: 1 if ((x['up_limit']==x['close'])&(x['open']==x['close'])) else 0,axis=1)
    return data

def filter(data,conditions):

    # [0,'ULRL','ULL']
    # [0,'LL,RLL','LL'，‘UL','DL',[2,4]]:无，一字板or一字板回封，一字板,
    res=data.copy()
    for day in range(len(conditions)):
        if conditions[day]:
            pre = basic().pre_data(data, label=[conditions[day]], pre_days=day)
            res = res.merge(pre[['ts_code', 'trade_date', 'pre_%s_%s'%(day,conditions[day])]], on=['ts_code', 'trade_date'])
            res = res.loc[res['pre_%s_%s'%(day,conditions[day])] == 1]
            print(res.shape,data.shape)
    return res

# def buy_data(data,PB):
#     if PB in data.columns:
#         return data
#     return
#
# def sell_data(data,PS):
#     if PS in data.columns:
#         return data
#     return
#
#
# def sales_model(model,):
#     M=['cut']#买卖模式，cut代表买入切分
#     # 对应每个回测方法有不同的pb，ps

def roi(red_line,raw_data,mode='PB'):
    res=[]
    section=['down_limit']+list(range(-8,10,2))+['up_limit']
    red_line['low_pct']=100*(red_line['low']/red_line['pre_close']-1)

    for pb in section:
        if pb not in raw_data.columns:
            if type(pb)=='int':
                PB = 'PB'
                red_line[PB]=red_line['pre_close']* (1 + 0.01 * pb)
                raw_data[PB] = raw_data['pre_close'] * (1 + 0.01 * pb)
            else:
                PB=pb
        for ps in section:
            if ps not in raw_data.columns:
                if type(ps) == 'int':
                    PS = 'PS'
                    raw_data[PS] = raw_data['pre_close'] * (1 + 0.01 * ps)
                else:
                    PS=ps

            raw_data[PS]=raw_data.apply(lambda x:x[ps] if x[ps]<x['high'] else None)

            meat=sheep.wool2(red_line.loc[red_line[PB]>=red_line['low']],raw_data,PRICEB=PB,PRICES=PS)

            res.append([pb,ps,meat.shape[0],meat.iloc[-1,-1]])
        res=pd.DataFrame(res,columns=['pb','ps','n_days','pct'])
    return res



if __name__=='__main__':
    start_date='20%s0101'
    end_date='20%s1231'
    for year in range(19,21):
        raw_data = read_data('daily', start_date=start_date%year, end_date=end_date%year).iloc[:,:-2]
        limit = read_data('stk_limit', start_date=start_date%year, end_date=end_date%year)
        raw_data = raw_data.merge(limit[['ts_code', 'trade_date', 'up_limit','down_limit']], on=['ts_code', 'trade_date'])

        print(start_date%year,'----',end_date%year,'include %s items'%raw_data.shape[0])
        list_days = basic().list_days(raw_data,list_days=25)
        print(list_days.shape)
        raw_data = raw_data.merge(list_days, on=['ts_code', 'trade_date'])
        raw_data=process(raw_data,[ 'ULRL','ULL'])
        red_line=filter(raw_data,[0,'ULRL','ULL'])
        save_data(red_line,'new%s连板.csv'%year)
        res=roi(red_line,raw_data)
        save_data(res,'%s连板回溯.csv'%year)
        # res=yunei(start_date%year,end_date%year)