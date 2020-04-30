import os
import pandas as pd
import time
import datetime
import tushare as ts
from numpy import arange
import stock.util.sheep as sheep
import matplotlib.pyplot as plt

from stock.sql.data import save_data,read_data
from stock.util.basic import basic
pro=ts.pro_api()
TEST=pro.fina_mainbz(ts_code='600538.SH')
start_date='20200301'
end_date='20200331'
# for market in ['msci','csi','sse','szse','cicc','sw','oth']:
#     index=pro.index_basic(market=market)
#     if index.loc[index['name'].str.contains('口罩')].empty:
#         continue
#     else:
#         print()
limit=read_data('limit_list',start_date=start_date,end_date=end_date)

def concept(limit):
    concept=pro.concept()
    count=1

    concept_detail=pd.DataFrame()
    if os.path.isfile('concept_detail.csv'):
        concept_detail=pd.read_csv('concept_detail.csv')
    else:
        for id in concept['code'].unique():
            concept_detail=pd.concat([concept_detail,pro.concept_detail(id=id)])

            count+=1
            if count%190==0:
                time.sleep(60)
                print(concept_detail.shape)
        concept_detail.to_csv('concept_detail.csv',index=False)

    limit=limit.merge(concept_detail,on=['ts_code'])
    summary=pd.DataFrame(limit.groupby(['trade_date','concept_name'])['concept_name'].count())
    print(summary.info())
    summary.columns=['count']
    print(summary.info())
    summary.reset_index(inplace=True)
    print(summary.info())
    save_data(summary,'概念股.csv')
    summary.sort_values(['trade_date','count'],ascending=False,inplace=True)
    print(summary['concept_name'].unique().shape)

# detail=pd.DataFrame(concept_detail())
# d=concept.loc[concept['name'].str.contains('口')==True]
def fina_mainbz(data):
    # 主营业务构成
    df=pd.DataFrame()
    count=1
    for ts_code in data['ts_code'].unique():
        fina=pro.fina_mainbz(ts_code=ts_code)
        fina=fina.loc[fina['end_date']==fina.loc[0,'end_date']]
        fina['sales_pct']=fina['bz_sales']/fina['bz_sales'].sum()
        fina['profit_pct']=fina['bz_profit']/fina['bz_profit'].sum()
        fina['cost_pct']=fina['bz_cost']/fina['bz_cost'].sum()
        count+=1
        if count%58==0:time.sleep(60)

        df=pd.concat([fina,df],ignore_index=True)

    return df

data = read_data('daily', start_date=start_date, end_date=end_date)
stk_limit = read_data('stk_limit', start_date=start_date, end_date=end_date)
data = data.merge(stk_limit, on=['ts_code', 'trade_date'])
limit = data.loc[data['close'] == data['up_limit']]
print(limit['ts_code'].unique().shape)
fina_mainbz(limit)
print()