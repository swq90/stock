import os
import datetime
import pandas as pd
import tushare as ts
import util.basic as basic
tool=basic.basic()

label_roof='high'
label_floor='low'
periods=20
times=17
others={}
pct=5
path = 'D:\\workgit\\stock\\util\\stockdata\\'
today = datetime.datetime.today().date()
tool = basic.basic()
pro = ts.pro_api()
while (not os.path.isfile(path + str(today) + '\data.csv')) :
    today = today - datetime.timedelta(1)
# 基础数据，市值信息，
df=pd.DataFrame()
data = pd.read_csv(path + str(today) + '\data.csv', index_col=0, dtype={'trade_date': object})
# data=data[['ts_code','trade_date',label_roof,label_floor,'ma1']].sort_values(by='trade_date')
# df=data.copy()
# df['roof_chg']=data.groupby('ts_code')[label_roof]-data.groupby('ts_code')[label_roof].shift(-1)
# df['floor_chg']=data.groupby('ts_code')[label_floor]-data.groupby('ts_code')[label_floor].shift(-1)

def pct_chg(data, periods=10, up_times=1, label='ma1', low=0,high=None):
    def func(df):
        if high is not None:
            return len(df[df <= high])
        return len(df[df >= low])

    data = data[['ts_code', 'trade_date',label]].copy().sort_values(by=['ts_code','trade_date'])

    data['%s_up' % label] = data.groupby('ts_code')[[label]].pct_change(periods=periods-1).shift(-periods+1)

    count_times = data.groupby('ts_code')['%s_up' % label].rolling(periods, min_periods=up_times).apply(func,
                                                                                                        raw=True)
    count_times.index = count_times.index.droplevel()
    count_times=pd.DataFrame(count_times)
    count_times.rename(columns={'%s_up' % label: 'count_%s' % label}, inplace=True)
    # count_times=count_times[count_times['%s_%s_uptimes'%(label,period)]>=up_times]
    data = data.join(count_times)

    return data[['ts_code','trade_date', 'count_%s' % label]]


pct_chg(data.head(300),label='close',)