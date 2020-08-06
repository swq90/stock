# 找到好的五百条股票，给出特征，
# import tensorflow as tf
# from tensorflow import keras
# from keras import layers, Sequential, optimizers, losses
import numpy as np
import numpy as np
import pandas as pd
from stock.util.basic import basic

from stock.sql.data import save_data,read_data
import stock.util.sheep as sheep
from stock import vars
START='20200201'
END='20201231'

sample_code='002614.SZ'
sample_start='20200619'
sample_end='20200806'
# UP_PCT=0.5
# UP_DAYS=5
cols=[vars.PCT_CHG]#特征
def cos_sim(vector_a, vector_b):
    """
    计算两个向量之间的余弦相似度
    :param vector_a: 向量 a
    :param vector_b: 向量 b
    :return: sim
    """
    vector_a = np.mat(vector_a)
    vector_b = np.mat(vector_b)
    num = float(vector_a * vector_b.T)
    denom = np.linalg.norm(vector_a) * np.linalg.norm(vector_b)
    cos = num / denom
    sim = 0.5 + 0.5 * cos
    return sim
def cos_cycel(data,N,vector1):
    res=[]
    data.sort_values(vars.TRADE_DATE,inplace=True)
    for l in range(data.shape[0]-N):
        vector2=data.iloc[l:l+N,:]
        res.append([vector2.iloc[0][vars.TS_CODE],vector2.iloc[0][vars.TRADE_DATE],vector2.iloc[-1][vars.TRADE_DATE],cos_sim(vector1[cols].values.reshape(1,-1),vector2[cols].values.reshape(1,-1))])
    return pd.DataFrame(res,columns=['ts_code','start','end','sim'])
def process_data():
    raw_dataset =read_data('daily',start_date=START,end_date=END)
    # 上市日期条件，测试时先不加
    # list_days=basic().list_days(raw_dataset)
    # raw_dataset=raw_dataset.merge(list_days,on=['ts_code','trade_date'])
    df=basic().pre_data(raw_dataset, label=['close'], pre_days=UP_DAYS)
    data=raw_dataset.merge(df[['ts_code','trade_date','pre_%s_close'%UP_DAYS]],on=['ts_code','trade_date'])
    data['pct']=data['close']/data['pre_%s_close'%UP_DAYS]-1
    data=data.loc[data['pct']>UP_PCT][['ts_code','trade_date','pct']]
    print(data.shape)
    dataset=pd.DataFrame()

    def rt_data(up_data):
        for i in range(up_data.shape[0]):
            raw=raw_dataset.loc[raw_dataset['ts_code']==up_data.iloc[i,0]].sort_values('trade_date')
            df=raw.merge(up_data.iloc[i],on=['ts_code','trade_date']).drop_duplicates()
            data=raw.loc[max(0,)]
        data=pd.DataFrame()
        for i in range(up_data.shape[0]):
            df=raw_dataset.loc[raw_dataset['ts_code']==up_data.iloc[i,0]].copy().sort_values('trade_date')
            # location=
            data=pd.concat([data,df.iloc[:,:]])
        return data

    data=rt_data(data)


def sample(ts_code,start=START,end=END):
    #找到有友食品前几日涨停板数据相似的数据
    vector=read_data('daily',start_date=start,end_date=end)
    vector=vector.loc[vector['ts_code']==ts_code].copy()
    # if vector_a.shape[0]<5:
    #     return
    # pre_data=basic().pre_data(vector_a,label=['close'],pre_days=UP_DAYS)
    # vector_a=vector_a.merge(pre_data[['ts_code','trade_date','pre_%s_close'%UP_DAYS]],on=['ts_code','trade_date']).sort_values('trade_date').reset_index(drop=True)
    return vector

def sim_sample(start=START,end=END):
    df=read_data('daily',start_date=start,end_date=end)
    return df



def process(df,days):
    # dataset=pd.DataFrame()
    if df.shape[0]==days:
        # 八个原始特征，未作处理
        # df=np.array(df.sort_values('trade_date').drop(columns=['ts_code','trade_date','pre_close'])).reshape(1,-1)
        # 6原始特征，去线性依赖
        df=np.array(df.sort_values('trade_date').drop(columns=['ts_code','trade_date','pre_close','pct_chg','change'])).reshape(1,-1)
        # 4 特征
        # df=np.array(df.sort_values('trade_date').drop(columns=['ts_code','trade_date','pre_close','pct_chg','change','amount',
        #                                                        'vol'])).reshape(1,-1)

        # df=np.append(df,np.array(data.iloc[i,-1]))
    else :
        return
    return df

if __name__=='__main__':
    res=pd.DataFrame()
    # a=cos_sim([100,100,200],[100,100,100])
    # b=cos_sim([100,2],[100,1])
    #

    # s1=sample('603697.SH',start='20200506',end='20200512')
    # s2=sim_sample(start='20200513',end='20200519')
    # sample_code='002187.SZ'
    s1=sample(sample_code,start=sample_start,end=sample_end)
    data=sim_sample(start=START,end=END)
    N_days=s1.shape[0]
    for ts_code in data['ts_code'].unique():
        df=data.loc[data['ts_code'] == ts_code].copy()
        res=pd.concat([res,cos_cycel(df,N_days,s1)],ignore_index=True)


    res.sort_values('sim',inplace=True,ascending=False)
    save_data(res,'%s%s特征.csv'%(sample_code,'-'.join(cols)))

    # roi=sheep.wool2(sim_data.head(10),read_data('daily',start_date='20200515'))
    print()