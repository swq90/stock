# 找到好的五百条股票，给出特征，
# import tensorflow as tf
# from tensorflow import keras
# from keras import layers, Sequential, optimizers, losses
import numpy as np
import numpy as np
import pandas as pd
from stock.util.basic import basic

from stock.sql.data import save_data,read_data

START='20180101'
END='20180131'
UP_PCT=0.5
UP_DAYS=5

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

raw_dataset =read_data('daily',start_date=START,end_date=END)
# 上市日期条件，测试时先不加
# list_days=basic().list_days(raw_dataset)
# raw_dataset=raw_dataset.merge(list_days,on=['ts_code','trade_date'])
df=basic().pre_data(raw_dataset, label=['close'], pre_days=UP_DAYS)
data=raw_dataset.merge(df[['ts_code','trade_date','pre_%s_close'%UP_DAYS]],on=['ts_code','trade_date'])
data['pct']=data['close']/data['pre_%s_close'%UP_DAYS]-1
data=data.loc[data['pct']>UP_PCT][['ts_code','trade_date','pct']]
print(data.shape)
datatest=pd.DataFrame()

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