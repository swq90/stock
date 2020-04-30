#正负样本
import tensorflow as tf
from tensorflow import keras
# from tensorflow.contrib.factorization import KMeans
from keras import layers, Sequential, optimizers, losses
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import stock.util.basic as basic

import tensorflow as tf