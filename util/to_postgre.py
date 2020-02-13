from sqlalchemy import create_engine
import numpy as np
import pandas as pd
engine=create_engine('postgresql://nezha:nezha@10.0.0.4:5432/stock',echo=False)
df=pd.DataFrame(np.arange(12).reshape(3,4),columns=list('abcd'))
print(df)
df.to_sql('test',con=engine,if_exists='append',index=False)
data=pd.read_sql_table('test',con=engine)
print(data)
df=pd.DataFrame(np.arange(20).reshape(5,4),columns=list('abcd'))
print(df)
df.to_sql('test',con=engine,if_exists='append',index=False)
data=pd.read_sql_table('test',con=engine)
print(data)