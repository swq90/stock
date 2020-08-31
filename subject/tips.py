import os
import jieba
import pandas as pd
import tushare as ts
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from stock import vars
from stock.sql.data import read_data, save_data

pro = ts.pro_api()
FORMAT = lambda x: '%.4f' % x
def group_ana(data, by, count_var=vars.TS_CODE):
    df=pd.DataFrame(data.groupby(by)[count_var].count()).rename(columns={count_var:'%s-count'%by})
    df["%s-pct"%by]=(df["%s-count"%by]/(df["%s-count"%by].sum())).map(FORMAT)
    return df.sort_values("%s-pct"%by,ascending=False)

class Stock:
    def __init__(self,start,end):
        self.start=start
        self.end=end
        self.raw_data = read_data('daily', start_date=start, end_date=end)
        self.growth_data=None

    def stock_increse(self,n,growth=2):
        data=self.raw_data.sort_values(vars.TRADE_DATE)
        data['%s-date'%n]=data.groupby(vars.TS_CODE)[vars.TRADE_DATE].shift(n)
        data['%s-close'%n]=data.groupby(vars.TS_CODE)[vars.CLOSE].shift(n)
        data['%s-growth'%n]=data[vars.CLOSE]/data['%s-close'%n]

        self.growth_data=data[data['%s-growth'%n]>=growth].sort_values('%s-growth'%n,ascending=False)
    def analyse_basic(self,):
        self.basic=read_data('stock_basic')
        self.basic=self.basic[self.basic['list_status']=='L']
        growth_basic = self.basic[self.basic[vars.TS_CODE].isin(self.growth_data[vars.TS_CODE])]
        # vs_table=pd.DataFrame()
        for key in ['industry','area','is_hs']:
            count_allmarket=group_ana(self.basic,key)
            count_growth=group_ana(growth_basic,key)
            df=pd.concat([count_growth,count_allmarket],ignore_index=True,axis=1)
            save_data(df,'股票%s对比.csv'%key,encoding='utf_8_sig')
    def analyse_company(self,fields='province,main_business,business_scope'):
        company_data=pro.stock_company( fields='ts_code,'+fields)
        company_data=company_data[company_data[vars.TS_CODE].isin(self.growth_data[vars.TS_CODE])]
        for name in fields.split(','):
            scope=''.join(x for x in company_data[name])
            seg_list = jieba.cut(scope ,cut_all=False)
            counter = dict()
            for seg in seg_list:
                counter[seg] = counter.get(seg, 1) + 1
            counter_sort = sorted(counter.items(), key=lambda value: value[1], reverse=True)
        return counter_sort
    def wc(self):
        data=self.analyse_company()
        counter={}
        for row in data:
            if row[0] in ('、', '，', '的', '；', '', '：', '和', '。','1','2','3','4','5','6','7','8','9','0','有','与','或','及','并','/'):
                continue
            counter[row[0]] = counter.get(row[0], int(row[1]))
        # pprint(counter)
        file_path = os.path.join("font", "msyh.ttc")
        wc = WordCloud(
            font_path=file_path, max_words=100, height=600, width=1200
        ).generate_from_frequencies(
            counter
        )
        plt.imshow(wc)
        plt.axis("off")
        plt.show()
        plt.savefig( "wc.jpg")
        print()







days,growth=270,2
if __name__=='__main__':
    stock=Stock('20190701','20202828')
    increase=stock.stock_increse(days,growth)
    # save_data(increase,'%s增长股票.csv')
    # stock.analyse_basic()
    stock.wc()
    print()
