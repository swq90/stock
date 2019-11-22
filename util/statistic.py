import pandas as pd
import tushare as ts

pro = ts.pro_api()

pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('display.width', None)
# 指定dtype为object，读取数据就会保持原样，不然str日期会被读作int
df = pd.read_csv("D:\workgit\stock\model\五天上涨超50.csv",dtype=object)
print(df)

print(df.info())
