import pandas as pd
import matplotlib.pyplot as plt

import data.get_data as gd

#数据读取
data = pd.read_csv('涨停股票.csv')
print(1,data.shape)
data = data[data['ts_code'].isin(gd.get_limit_rec())]
print(data.shape)
# info 'ts_code,symbol,name,area,market,industry,list_date'
info = pd.read_csv(r'D:\workgit\stock\data\stock_basic.csv')
data = data[['high', 'vol', 'amount','list_date', 'turnover_rate',  'total_mv' ]]
# print(data.shape)

# data = data[['close','vol','amount']]
# data = data.values
print(list(data))
# print(type(data.head().values))

# data.plot(x=data)
# plt.show()
# amount 成交额，vol 成交量
# plt.scatter(data['total_mv']/100000000, data['turnover_rate'],c=data['high'],alpha=0.4,cmap='Reds')

# plt.scatter(data['pct_chg'],data['vol'],s=data['pre_close'],c=data['amount'],alpha=0.4,cmap='Reds')
# data_sort = data.sort_values(by='pct_chg',ascending='False')
# data_sort.plot
# plt.figure()
# plt.grid()

# whitened = vq.whiten(data)
# book = np.array((whitened[0],whitened[2]))
# a = vq.kmeans(whitened,book)
# # plt.plot(r0 * np.cos(theta), r0 * np.sin(theta))
# print(a)
# # plt.show()
#
# plt.plot(data[''])

# estimator = vq.kmeans(data,k_or_guess=2)
# estimator.fit(data)
# label_pred = estimator.labels_ #获取聚类标签
# centroids = estimator.cluster_centers_ #获取聚类中心
# inertia = estimator.inertia_ # 获取聚类准则的总和
# mark = ['or', 'ob', 'og', 'ok', '^r', '+r', 'sr', 'dr', '<r', 'pr']
# color = 0
# j = 0
# for i in label_pred:
#     plt.plot([data[j:j+1,0]], [data[j:j+1,1]], mark[i], markersize = 5)
#     j +=1
# plt.show()
# m = data['total_mv'].mode()
# # 众数
# med =m.median()
# mean = data['total_mv'].mean()
# mean_w=data['total_mv'].mean()
# data['total_mv'].plot(kind = 'kde',style = '--k',grid = True)
# plt.axvline(mean,color='r',linestyle="--",alpha=0.8)
# # 简单算数平均值
#
# plt.axvline(mean_w,color='b',linestyle="--",alpha=0.8)
# # 加权算数平均值
#
# plt.axvline(med,color='g',linestyle="--",alpha=0.8)
# # 中位数

pd.plotting.scatter_matrix(data,figsize=(8,8),
                  c = 'k',
                 marker = '+',
                 diagonal='hist',
                 alpha = 0.8,
                 range_padding=0.1)
data.head()

plt.show()
