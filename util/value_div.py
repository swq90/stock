import tensorflow as tf
import numpy.random
import pandas as pd

tf.compat.v1.disable_v2_behavior()


INPUT_NODE = 2
OUTPUT_NODE = 1
BATCH_SIZE = 10
LEARNING_RATE_BASE = 0.8
LEARNING_RATE_DECAY = 0.99
LAYER1_NODE = 3
REGULARIZATION_RATE = 0.0001
TRAINING_STEPS = 30000
# 滑动平均值？

# 回归问题,训练过程
x = tf.compat.v1.placeholder(tf.float32, shape=(None, INPUT_NODE), name="x-input")
y_ = tf.compat.v1.placeholder(tf.float32, shape=(None, OUTPUT_NODE), name="y-input")


# layer1 参数
weights1 = tf.Variable(tf.compat.v1.truncated_normal([INPUT_NODE, LAYER1_NODE], stddev=0.1))
biases1 = tf.Variable(tf.constant(0.1, shape=[LAYER1_NODE]))

# 输出层 参数
weights2 = tf.Variable(tf.compat.v1.truncated_normal([LAYER1_NODE, OUTPUT_NODE], stddev=0.1))
biases2 = tf.Variable(tf.constant(0.1, shape=[OUTPUT_NODE]))


# 向前传播
layer1 = tf.nn.relu(tf.matmul(x, weights1) + biases1)
y = tf.matmul(layer1, weights2) + biases2




# 正则，损失函数
# regularizer = tf.compat.v1.layers.l2_regularizer(REGULARIZATION_RATE)
# regularization = regularizer(weights1) + regularizer(weights2)
loss = -tf.reduce_sum(y_-y)
# 反向传播的参数 学习率0.001
train_step = tf.compat.v1.train.AdamOptimizer(0.001).minimize(loss)

# 数据集
# data = pd.read_csv("xxx.csv")
dataset_size = 100
# X = data
#
# Y = data

rdm = numpy.random.RandomState(1)
dataset_size = 128
X = rdm.rand(dataset_size, INPUT_NODE)
Y = [[x1 + rdm.rand()/10.0-0.05] for (x1, x2) in X]


# 训练神经网络
with tf.compat.v1.Session() as sess:
    init_op = tf.compat.v1.global_variables_initializer()
    sess.run(init_op)
    STEPS = 5000

    for i in range(STEPS):
        start = (i*BATCH_SIZE) % dataset_size
        end = min(start+BATCH_SIZE, dataset_size)
        sess.run(train_step, feed_dict={x: X[start:end], y_: Y[start:end]})
        if i %1000 == 0:
            print(i,'轮训练后', sess.run(weights1), sess.run(weights2))
            print('loss',sess.run(loss,feed_dict={x:X,y_:Y}))
    print(sess.run(loss,feed_dict={x:X,y_:Y}))