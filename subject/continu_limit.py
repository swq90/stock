# 利用匿名函数,动态传参给filter

import pandas as pd
import datetime
from numpy import arange
import stock.util.sheep as sheep
import stock.limit_up.get_limit_stock as gls
from stock.sql.data import read_data, save_data
from stock.util.basic import basic
# 所有条件以list传进来，str表示要求，由前往后分别为卖出，买入，pre1，pre2，……pren日的状态
# 若所给状态含有’=‘eval计算新指标，之后若是包含’/‘则是多重选择，命名关键字参数，传入
# 所有状态给出字典，若由自定义的，（判断元素中是否包含=，有就利用
df=pd.DataFrame({'a':[1,2,4,5,7],'b':[1,2,4,6,7],'c':[1,2,3,5,7],'trade_date':[2,1,4,3,5]})
print(df)
# data=pd.DataFrame()
def process(df,expressions):
    df.sort_values('trade_date',inplace=True )
    print(df.iloc[0],type(df.iloc[0]))

    # df=df.loc[expressions[0]]
    # print(df)
    for i in range(len(expressions)):
        data=df.iloc[i].copy()
        print(data.loc[expressions[i],:])



process(df,[lambda x:x['a']==x['b'],lambda x: (x['b']<=x['c'])&(x['a']!=x['c'])])