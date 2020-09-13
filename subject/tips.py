import os
import time
import jieba
import pandas as pd
import tushare as ts
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from stock import vars
from stock.sql.data import read_data, save_data,save_to_sql
from stock.util.basic import basic
pro = ts.pro_api()
FORMAT = lambda x: '%.4f' % x


def group_ana(data, by, count_var=vars.TS_CODE):
    df = pd.DataFrame(data.groupby(by)[count_var].count()).rename(columns={count_var: '%s-count' % by})
    df["%s-pct" % by] = (df["%s-count" % by] / (df["%s-count" % by].sum())).map(FORMAT).astype(float)
    return df.sort_values("%s-pct" % by, ascending=False)


class Stock:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.raw_data = read_data('daily', start_date=start, end_date=end)

    def stock_increse(self, n, growth=2):
        data = self.raw_data.sort_values(vars.TRADE_DATE)
        data['%s-date' % n] = data.groupby(vars.TS_CODE)[vars.TRADE_DATE].shift(n)
        data['%s-close' % n] = data.groupby(vars.TS_CODE)[vars.CLOSE].shift(n)
        data['%s-growth' % n] = data[vars.CLOSE] / data['%s-close' % n]

        self.growth_data = data[data['%s-growth' % n] >= growth].sort_values('%s-growth' % n, ascending=False)

    def analyse_basic(self, ):
        self.basic = read_data('stock_basic')
        self.basic = self.basic[self.basic['list_status'] == 'L']
        growth_basic = self.basic[self.basic[vars.TS_CODE].isin(self.growth_data[vars.TS_CODE])]
        # vs_table=pd.DataFrame()
        for key in ['industry', 'area', 'is_hs']:
            count_allmarket = group_ana(self.basic, key)
            count_growth = group_ana(growth_basic, key)
            df = pd.concat([count_growth, count_allmarket], ignore_index=True, axis=1)
            save_data(df, '股票%s对比.csv' % key, encoding='utf_8_sig')

    def analyse_company(self, fields='province,main_business,business_scope'):
        company_data = pro.stock_company(fields='ts_code,' + fields)
        company_data = company_data[company_data[vars.TS_CODE].isin(self.growth_data[vars.TS_CODE])]
        for name in fields.split(','):
            scope = ''.join(x for x in company_data[name])
            seg_list = jieba.cut(scope, cut_all=False)
            counter = dict()
            for seg in seg_list:
                counter[seg] = counter.get(seg, 1) + 1
            counter_sort = sorted(counter.items(), key=lambda value: value[1], reverse=True)
            self.wc(counter_sort, name)
        return counter_sort

    def wc(self, data, name):
        # data=self.analyse_company()
        counter = {}
        for row in data:
            if row[0] in (
            '、', '，', '的', '；', '', '：', '和', '。', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '有', '与', '或', '及',
            '并', '/'):
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
        plt.savefig("%swc.jpg" % name)
        print()

    def mv(self):

        df = pro.daily_basic(ts_code='', trade_date=start, fields='ts_code,trade_date,total_mv,circ_mv')
        growth_mv = df[df[vars.TS_CODE].isin(self.growth_data[vars.TS_CODE])]
        # ceil=1000*(int(df['total_mv'].max()//1000)+1)
        df_count_mv = pd.DataFrame()
        for col in 'total_mv,circ_mv'.split(','):
            # print(df[col].index)
            bins = [int(df.sort_values(col).iloc[x, df.columns.get_loc(col)]) + 1 for x in range(0, df.shape[0], 200) ]+[
                int(df.sort_values(col).iloc[-1, df.columns.get_loc(col)]) + 100]
            # bins=bins.sort()
            df['all-%s-scope' % col] = pd.cut(df[col], bins)
            growth_mv['growth-%s-scope' % col] = pd.cut(growth_mv[col], bins)
            df_count_mv = pd.concat([group_ana(df, 'all-%s-scope' % col, count_var='all-%s-scope' % col),
                                     group_ana(growth_mv, 'growth-%s-scope' % col, count_var='growth-%s-scope' % col)],
                                    axis=1)
            df_count_mv['rate'] = df_count_mv['growth-%s-scope-pct' % col] / df_count_mv['all-%s-scope-pct' % col]
            df_count_mv.reset_index(inplace=True, )
            save_data(df_count_mv, '%s 分析占比.csv' % col)
    def fina(self):
        '''
        主营业务构成
        @return:
        '''
        self.fina_mainbz=read_data('fina_mainbz',)
        count=1
        for vars.TS_CODE in self.raw_data[vars.TS_CODE]:
            count+=1
            if count%50==0:
                time.sleep(60)
            self.fina_mainbz.append(pro.fina_mainbz(period='20200630',ts_code=vars.TS_CODE, type='P'))
        counter_all,counter_growth = dict(),dict()
        for seg in self.fina_mainbz['bz_item']:
            counter_all[seg] = counter_all.get(seg, 1) + 1
        counter_all = sorted(counter_all.items(), key=lambda value: value[1], reverse=True)
        for seg in self.fina_mainbz[self.fina_mainbz[vars.TS_CODE].isin(self.growth_data[vars.TS_CODE])]['bz_item']:
            counter_growth[seg] = counter_growth.get(seg, 1) + 1
        counter_growth = sorted(counter_growth.items(), key=lambda value: value[1], reverse=True)

        self.wc(counter_all, '主营业务.jpg')
        self.wc(counter_growth,'增长股票主营业务.jpg')
        print()


# start, end = '20190701', '20202828'
# days, growth = 270, 2

start, end = '20160901', '20201231'
days, growth = 60, 2

if __name__ == '__main__':
    stock = Stock(start, end)
    list_days = basic().list_days(stock.raw_data, list_days=30)
    stock.raw_data = stock.raw_data.merge(list_days, on=[vars.TS_CODE, vars.TRADE_DATE])
    stock.stock_increse(days, growth)
    save_data(stock.growth_data,'%s-%s-%s增长股票.csv'%(start,days,growth))
    increase_info=stock.growth_data.groupby(vars.TRADE_DATE)[vars.TS_CODE].count()

    save_to_sql(increase_info,'growth_%s_%s'%(days,growth))
    save_data(increase_info,'%s-%s-%s增长-数量.csv'%(start,days,growth))
    # save_data(increase,'%s增长股票.csv')
    # stock.analyse_basic()
    # stock.analyse_company()
    # stock.mv()
    # stock.fina()
    print()
