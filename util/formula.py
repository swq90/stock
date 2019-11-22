import numpy as np
import pandas as pd


def MA(DF, N):
    return pd.Series.rolling(DF, N,min_periods=N).mean()