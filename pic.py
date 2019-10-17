import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import data.get_data as gd

l_list = pd.read_csv(r'D:\workgit\stock\data\d1016c100203.csv')['ts_code']

daily = pd.read_csv(r'D:\workgit\stock\data\daily.csv')
x = [np.sort(daily[["trade_date"]]['trade_date'].unique())]
for i in l_list:
    # print(i)
    pass
s_d = daily[daily['ts_code']==i]
s_d = s_d[['trade_date', 'pct_chg']].sort_values(by='trade_date')
print(s_d)
s_d = s_d .T
print(s_d)
line_data = list(s_d[1:].values)
print(line_data)
# plt.plot(list(x), line_data, marker='o', mec='r', mfc='w',label=i)
# plt.plot(x, y_test, marker='*', ms=10,label='uniprot90_test')

plt.plot(range(s_d[0:1].values),(s_d[1:].values), marker='o', mec='r', mfc='w',label=i)

plt.show()