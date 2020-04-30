import tensorflow as tf
from tensorflow import keras
from keras import layers, Sequential, optimizers, losses
import numpy as np
import pandas as pd
from stock.util.basic import basic

import tensorflow as tf
from tensorflow import keras
from stock.sql.data import save_data,read_data


start_date='20170101'
end_date='20170131'

raw_dataset =read_data('daily',start_date=start_date,end_date=end_date)
df=basic().pre_data(raw_dataset, label=['close'], pre_days=5)

data=raw_dataset.merge(df[['ts_code','trade_date','pre_5_close']],on=['ts_code','trade_date'])
data['pct']=data['close']/data['pre_5_close']-1
data=data.loc[data['pct']>0.5][['ts_code','trade_date','pct']]
print(data.shape)
datatest=pd.DataFrame()
for i in range(data.shape[0]):
    print(data.iloc[i])
    # df=data.iloc[i,:].copy().sort_values('trade')
    df=raw_dataset.loc[(raw_dataset['ts_code']==data.iloc[i,0])&(raw_dataset['trade_date']<data.iloc[i,1])].sort_values('trade_date').iloc[-5:]
    if df.shape[0]>=5:
        df=np.array(df.drop(columns=['ts_code','trade_date','pre_close'])).reshape(1,-1)
        df=np.append(df,np.array(data.iloc[i,-1]))



dataset = df.copy().drop(columns=['ts_code','trade_date'])
print(dataset.shape)
dataset = dataset.dropna()
print(dataset.shape)


# 切分为训练集和测试集
train_dataset = dataset.sample(frac=0.8, random_state=1)
test_dataset = dataset.drop(train_dataset.index)

train_labels = train_dataset.pop('pct_chg')
test_labels = test_dataset.pop('pct_chg')

# 查看x的统计数据？
train_stats = train_dataset.describe()
train_stats = train_stats.transpose()


# 标准化数据
def norm(x):
    return (x - train_stats['mean']) / train_stats['std']


normed_train_data = norm(train_dataset)
normed_test_data = norm(test_dataset)

print(normed_train_data.shape, train_labels.shape)
print(normed_test_data.shape, test_labels.shape)

# 利用切分的训练集数据构建数据集对象
train_db = tf.data.Dataset.from_tensor_slices((normed_train_data.values, train_labels.values))  # 构建Dataset对象
train_db = train_db.shuffle(100).batch(32)  # 随机打散，批量化


# 自定义网络层，通过继承keras.Model基类
class Network(keras.Model):
    # 回归网络
    def __init__(self):
        super(Network, self).__init__()
        # 创建3个全连接层
        self.fc1 = keras.layers.Dense(64, activation='relu')
        self.fc2 = keras.layers.Dense(64, activation='relu')
        self.fc3 = keras.layers.Dense(1)

    def call(self, inputs, training=None, mask=None):
        # 依次通过三个全连接层
        x = self.fc1(inputs)
        x = self.fc2(x)
        x = self.fc3(x)
        return x


model = Network()
# 通过 build 函数完成内部张量的创建，其中 4 为任意的 batch 数量，9 为输入特征长度
model.build(input_shape=(4, 8))
model.summary()  # 打印网络信息
# 创建优化器，指定学习率
optimizer = tf.keras.optimizers.RMSprop(0.001)

# 网络训练部分。通过 Epoch 和 Step 的双层循环训练网络，共训练 200 个 epoch:
for epoch in range(200):  # 200 个 Epoch
    for step, (x, y) in enumerate(train_db):  # 遍历一次训练集
        # 梯度记录器
        with tf.GradientTape() as tape:
            out = model(x)  # 通过网络获得输出
            loss = tf.reduce_mean(keras.losses.MSE(y, out))  # 计算 MSE
            mae_loss = tf.reduce_mean(keras.losses.MAE(y, out))  # 计算 MAE

        if step % 10 == 0:
            print(epoch, step, float(loss))

        # 计算梯度 并更新
        grads = tape.gradient(loss, model.trainable_variables)
        optimizer.apply_gradients(zip(grads, model.trainable_variables))

