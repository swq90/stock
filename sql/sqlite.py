import sqlite3

conn = sqlite3.connect('limit_up.db')
cursor = conn.cursor()
# 建表
# cursor.execute("create table stock_limit (ts_code varchar(10) ,"
#                "trade_date varchar(10),close float,pre_close float,change float,pct_chg float)")
# 插入记录
# cursor.execute("insert into stock_limit (ts_code, date, close,pre_close,change,pct_chg) values ")

# cursor.execute('select * from stock_limit')
# values = cursor.fetchall()
# print(values)
# 关闭cursor




# cursor.execute("create table st (id varchar(10) ,name varchar(20)) ")
# cursor.execute("insert into st ")
cursor.execute('insert into st (id, name) values (\'1\', \'Michael\')')
cursor.execute("select * from st")
print(cursor.fetchall())




cursor.close()
# 提交事务
conn.commit()
# 关闭connection
conn.close()