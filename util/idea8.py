
# 连续出现n日low>ma5的数据回溯效果
import os
import datetime
import pandas as pd
import tushare as ts
import util.basic as basic
import util.sheep as sheep


FORMAT = lambda x: '%.4f' % x
LABEL = ['lowma5', 'low', 'ma1', 'ma5']
PATH = 'D:\\workgit\\stock\\util\\stockdata\\'
# pct=list(range(-11,11))


today = datetime.datetime.today().date()
tool=basic.basic()
pro=ts.pro_api()
# 获得基础数据

XISHU=0.998
# PERIOD=5
# TIMES=5
# OTHERS={'pct_chg':[1,2]}
OTHERS={}

while (not os.path.isfile(PATH + str(today) + '\data.csv')) or (
        not os.path.isfile(PATH + str(today) + '\daily-basic.csv')) or (
        not os.path.isfile(PATH + str(today) + '\stock-label.csv'))or (
        not os.path.isfile(PATH + str(today) + '\history_name.csv')):
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，
PATH=PATH+str(today)
data = pd.read_csv(PATH+ '\data.csv', index_col=0, dtype={'trade_date': object})[['ts_code','trade_date','close','pct_chg','low','ma1','ma5']]
# data=data[data['trade_date']>='20191201']
stock_baisc = pd.read_csv(PATH+ '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'turnover_rate','total_mv']]
history = pd.read_csv(PATH + '\history_name.csv', index_col=0, dtype={'trade_date': object})
history['name'] = 'st'
PATH=PATH+'idea8\\'
if not os.path.isdir(PATH):
    os.makedirs(PATH)
    print(PATH)


# print(data.shape)
# data = data[data['name'].isna()]
# data.drop(columns='name',inplace=True)
# print(data.shape)
# data.dropna(inplace=True)
print(data.shape)
# PERIOD=5
# TIMES=5
# OTHERS={'pct_chg':[1,2]}
def dis(data,LABEL=[],PERIOD=5,TIMES=5,**OTHERS):


    data.sort_values(by='trade_date',inplace=True)
    # data.to_csv(PATH+'2019datalowma5.csv')
    for l in LABEL:
        print(l)
        if 'lowma5'==l:
            data['lowma5']=data.apply(lambda x:1 if x['low']>(XISHU*x['ma5']) else 0 ,axis=1)

            c_times = data.groupby('ts_code')[l].rolling(PERIOD).sum()
            c_times.index = c_times.index.droplevel()
            c_times = pd.DataFrame(c_times)
            c_times.rename(columns={'lowma5': 'count_%s'%l}, inplace=True)
            # count_times=count_times[count_times['%s_%s_uptimes'%(LABEL,period)]>=up_period]
            data = data.join(c_times)


        else:
            c_data=tool.up_times(data,period=PERIOD,up_times=TIMES,label=l)
            data=data.merge(c_data,on=['ts_code','trade_date'],how='left')
        print(data.shape)
        df=data[data['count_%s'%l]>=TIMES]
        print(df.shape)
        df = df.merge(history, on=['ts_code', 'trade_date'], how='left')
        df = df[df['name'].isna()]
        df.drop(columns='name', inplace=True)
        print(df.shape)
        df.dropna(inplace=True)
        print(df.shape)



        for k,v in OTHERS.items():
            df=df.loc[df[k]>=v[0]]
            print(df.shape)
            df=df.loc[df[k]<=v[1]]
            print(df.shape)
        filename=str(['%s_%s'%(k,v) for k,v in OTHERS.items() ])
        df.to_csv(PATH+'%s%stimes%s%s.csv'%(l,PERIOD,TIMES,filename))

        wool=sheep.wool(df,data)
        wool.to_csv(PATH+'%s%stimes%s%shuisuxiaoguo.csv'%(l,PERIOD,TIMES,filename))


z=dis(data,label='lowma5',kv=100,tv=20)