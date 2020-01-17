
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

XISHU=0.998
DAYS=12
# TIMES=5
OTHERS={'pct_chg':[1,2]}


today = datetime.datetime.today().date()
tool=basic.basic()
pro=ts.pro_api()
# 获得基础数据


while (not os.path.isfile(PATH + str(today) + '\data.csv')) or (
        not os.path.isfile(PATH + str(today) + '\daily-basic.csv')) or (
        not os.path.isfile(PATH + str(today) + '\stock-label.csv'))or (
        not os.path.isfile(PATH + str(today) + '\history_name.csv')):
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，
PATH=PATH+str(today)
data = pd.read_csv(PATH+ '\data.csv', index_col=0, dtype={'trade_date': object})[['ts_code','trade_date','close','pct_chg','low','ma1','ma5']]
# data=data[data['trade_date']>='20190101']
stock_baisc = pd.read_csv(PATH+ '\daily-basic.csv', index_col=0, dtype={'trade_date': object})[
    ['ts_code', 'trade_date', 'turnover_rate','total_mv']]
history = pd.read_csv(PATH + '\history_name.csv', index_col=0, dtype={'trade_date': object})
history['name'] = 'st'
print(data.shape)
print(history.shape)
PATH=PATH+'idea9\\'
if not os.path.isdir(PATH):
    os.makedirs(PATH)
    print(PATH)


print(data.shape)
# PERIOD=5
# TIMES=5
# OTHERS={'pct_chg':[1,2]}
def dis(data,label='',period=5,times=0,**OTHERS):
    data.sort_values(by='trade_date',inplace=True)

    if 'lowma5'==label:
        data['lowma5']=data.apply(lambda x:1 if x['low']>(XISHU*x['ma5']) else 0 ,axis=1)

        c_times = data.groupby('ts_code')[label].rolling(period).sum()
        c_times.index = c_times.index.droplevel()
        c_times = pd.DataFrame(c_times)
        c_times.rename(columns={'lowma5': 'count_%s'%label}, inplace=True)
        # count_times=count_times[count_times['%s_%s_uptimes'%(label,period)]>=up_period]
        data = data.join(c_times)


    else:
        c_data=tool.up_times(data,period=period,up_times=times,label=label)
        data=data.merge(c_data,on=['ts_code','trade_date'],how='left')
    print(data.shape)
    df=data[data['count_%s'%label]>=times]
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

    return df

    # filename=str(['%s_%s'%(k,v) for k,v in OTHERS.items() ])
    # df.to_csv(PATH+'%s%stimes%s%s.csv'%(label,period,times,filename))
    #
    # wool=sheep.wool(df,data)
    # wool.to_csv(PATH+'%s%stimes%s%shuisuxiaoguo.csv'%(label,period,times,filename))
    #
    #
res=pd.DataFrame()
for ll in LABEL:
    for day in range(DAYS):
        if day==0:
            up_data = dis(data, label=ll, period=1)
            up_data=up_data[up_data['count_%s'%ll]==day][['ts_code','trade_date']]

        else:
            up_data=dis(data,label=ll,period=day)
            up_data=up_data[up_data['count_%s'%ll]==day][['ts_code','trade_date']]
        up_data.to_csv(PATH+'%s%stimes%s.csv'%(ll,day,day))
        up_wool=sheep.wool(up_data,data)
        res.loc[day,ll]=0 if  up_wool.empty else up_wool.iloc[-1,-1]
        print(res)
        print(day,ll)
res.to_csv(PATH+'%s%stimes%shuisu.csv'%(ll,day,day))