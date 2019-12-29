#连续大涨后大阴线or其他情况，后续情况回溯

import  os
import datetime
import pandas as pd
import tushare as ts

# import util.basic as basic
import util.sheep as sheep
pro=ts.pro_api()
today=datetime.datetime.today().date()

path_source=os.getcwd() +'\\stockdata\\'+str(today)+'\\'
# path_input='D:\workgit\stock\model\\'
query_days=50
errorTimes=0
interval_day=1
print(today,today-datetime.timedelta(query_days))

data=pd.read_csv(path_source + 'data.csv', index_col=0, dtype={'trade_date': object})
print(data.head(10),data.info())
# 日历区间有问题？
# tradecal=pro.trade_cal(start_date=str(today-datetime.timedelta(query_days)),is_open=1)
# print(tradecal.columns)
input_data=pd.DataFrame()



for day in range(1,query_days+1):
    query_date=str(today-datetime.timedelta(day))

    print(query_date)
    if query_date.replace('-','') in data['trade_date'].unique():
        filename=path_input+query_date+'10dayUp1-5%.txt'
        print(filename)
        if os.path.isfile(filename):
            df=pd.read_csv(filename,sep='\t')[['ts_code']]
            df['trade_date']=query_date.replace('-','')
            print(df)
            input_data=pd.concat([df,input_data],ignore_index=True)
        else:
            errorTimes += 1
            if errorTimes>3:
                break
res=sheep.wool(input_data,data,days=interval_day,PRICEB = "close",PRICES = "close")
print(res)

res.to_csv('1-5%huisu.csv')