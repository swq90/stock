import pandas as pd
from stock.util.basic import basic
from stock.sql.data import read_data,save_data
days=['m_ma','or_limit','m_ma','m_ma','m_ma']
end_date='20201231'

guoli = pd.read_csv('D:\\workgit\\stock\\data\\break_limit\\' + 'lianchuang.csv' , index_col=0,
                 dtype={'trade_date': object})
print(guoli.shape)
list_days=basic().list_days(guoli,list_days=30)


guoli=guoli.merge(list_days,on=['ts_code','trade_date'])

print(guoli.shape[0])
guoli['ma']=guoli['amount']/guoli['vol']*10
save_data(guoli,'lianchuang%s%s.csv'%(','.join(days),end_date))
for ps in ['high','low','ma','open','close']:
    guoli['%s/pre_close'%ps]=100*(guoli[ps]/guoli['pre_close']-1)
    print(ps,guoli['%s/pre_close'%ps].mean())
print('close_limit_up:',guoli.loc[guoli['close']==guoli['up_limit']].shape[0]/guoli.shape[0])
print('open_limitup:',guoli.loc[guoli['open']==guoli['up_limit']].shape[0]/guoli.shape[0])
print('line_red:',guoli.loc[(guoli['open']==guoli['up_limit'])&(guoli['close']==guoli['up_limit'])&(guoli['low']==guoli['up_limit'])].shape[0]/guoli.shape[0])
