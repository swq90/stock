import datetime
import os
import matplotlib.pyplot as plt
import pandas as pd

PATH=os.getcwd()


def pic_dis(data1,data2,file_name="",folder_name="",axis=0):
    if axis:
        for col_name in list(data1):
            data1[col_name].plot(label="all")
            data2[col_name].plot(label="up", title=col_name, grid=True)
            if not file_name:
                file_name=col_name
            plt.savefig(str(datetime.datetime.today())[:10]+file_name + '.png')
            plt.show()



        # if file_name:
        #         if folder_name:
        #             dir_new = os.path.join(PATH, folder_name)
        #             if os.path.isdir(PATH):
        #                 path = os.mkdir(dir_new)
        #         plt.savefig(path+str(datetime.datetime.today())[:10]+file_name+".png")
        #     plt.show()
    else:
        print(range(len(data1)))
        for row in range(len(data1)):
            data1.iloc[row].plot(label="all")
            data2.iloc[row].plot(label="up", title=data2.iat[row, 0], grid=True)
            plt.savefig(os.getcwd() + "\\pic\\" + str(datetime.datetime.today())[:10] + "range_pct.png")
            plt.show()


import numpy as np

df1=pd.DataFrame(np.random.randint(0,10,(5,2)),columns=list("ab"))
df2=pd.DataFrame(np.random.randint(0,10,(5,2)),columns=list("ab"))
pic_dis(df1,df2,file_name="aaa",axis=1)




