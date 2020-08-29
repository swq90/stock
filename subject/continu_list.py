from stock.subject import continu_limit as cl
from stock import vars
# ts_code = '300209'
# conditon = [n_n, up_limit, red_t_limit,
#             lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[OPEN] != x[UP_LIMIT]) & (x[PCT_CHG] >= -5) & (
#                         x[PCT_CHG] <= 0)]
# ts_code='600075'
# condition=[n_n, up_limit, red_t_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1) & (x[HIGH]== x[UP_LIMIT])]
# ts_code='6030693'
# condition=[n_n, up_limit, up_limit, up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1) & (x[CLOSE]== x[UP_LIMIT])& (x[OPEN]!= x[UP_LIMIT])]
# condition=[n_n,ord_limit,ord_limit,ord_limit,ord_limit]
# ts_code='300117'
# condition=[n_n, up_limit,up_limit, red_t_limit]
# ts_code='300464'
# condition=[n_n, up_limit, up_limit,  up_limit,  up_limit,  red_line_limit,red_t_limit]
# ts_code='000698'
# condition=[n_n, up_limit,red_line_limit]
# ts_code='000796'
# condition=[red_line_limit,  red_line_limit,red_line_limit,lambda x: (x[CLOSE]  != x[UP_LIMIT]) ,lambda x:x[CLOSE]==x[DOWN_LIMIT]]
# ts_code = '002614'
# condition = [lambda x: (x[PCT_CHG] > 9) & (x[CLOSE] != x[UP_LIMIT]),
#              lambda x: (x[PCT_CHG] < -9) & (x[CLOSE] != x[DOWN_LIMIT])]
# ts_code='002613'
# condition=[n_n, up_limit,up_limit,up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1.02) & (x[OPEN] != x[UP_LIMIT]) & (x[PCT_CHG] >= -8) & (
#                         x[PCT_CHG] <= 0)& (x[LOW] != x[DOWN_LIMIT]),up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE]< 0.98)]

# ts_code='300301-2'
# condition=[n_n, red_line_limit,red_line_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] <= 1.05) &(x[OPEN] / x[PRE_CLOSE] >= 1)  & (x[PCT_CHG] >= -6) & (
#                         x[PCT_CHG] <= -3)]

# ts_code='000573-2'
# condition=[n_n, ord_limit,red_t_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] <= 1.05) &(x[OPEN] / x[PRE_CLOSE] >= 1)  & (x[PCT_CHG] >= -6) & (
#                         x[PCT_CHG] <=-3)]
# ts_code='600961'
# condition=[n_n, up_limit, up_limit,red_t_limit]
# ts_code='000698'
# condition=[n_n, up_limit,red_line_limit,red_line_limit]
# ts_code='603633,303178'
# condition=[n_n, up_limit,red_line_limit]
# ts_code = '300178'
# condition = [up_limit, red_t_limit,
#              lambda x: (x[OPEN] != x[UP_LIMIT]) & (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[PCT_CHG] >= 4) & (
#                      x[PCT_CHG] <= 7)]
# ts_code='300084'
# condition=[n_n,  red_line_limit,  red_line_limit]
# ts_code='300052'
# condition=[n_n,  ord_limit,  red_t_limit]
# ts_code='603690'
# condition=[n_n,  ord_limit,  red_line_limit]
# ts_code='603020'
# condition=[n_n,  ord_limit,  lambda x:(x[OPEN] ==x[UP_LIMIT] ) & (x[PCT_CHG] >= 6) & (x[CLOSE]!=x[UP_LIMIT])]
# ts_code='300148'
# condition=[n_n,  up_limit,red_line_limit,  lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[CLOSE]==x[UP_LIMIT])]
# ts_code='601615'
# condition=[n_n,  lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[HIGH]==x[UP_LIMIT]) & (x[CLOSE]!=x[UP_LIMIT])]
# ts_code='002960'
# condition=[n_n,  ord_limit,ord_limit]
# ts_code='300842'
# condition=[ord_limit,ord_limit,red_t_limit,lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[OPEN] / x[PRE_CLOSE] >1) & (x[PCT_CHG] >= -7) & (
#                      x[PCT_CHG] <=-4)]
# ts_code='000011'
# condition=[n_n,up_limit,up_limit,red_line_limit,red_t_limit]
# ts_code='600127'
# condition=[n_n,ord_limit,ord_limit]
# ts_code='000026a'
# condition=[up_limit,red_line_limit or red_t_limit]
# ts_code='300556'
# condition=[n_n,up_limit,red_line_limit,lambda x:(x[HIGH]==x[UP_LIMIT])& (x[OPEN] / x[PRE_CLOSE] >1.03) & (x[OPEN] / x[PRE_CLOSE] <1.08)& (x[PCT_CHG] >= 1) & (
#                      x[PCT_CHG] <=8)]
# ts_code='002208'
# condition=[n_n,ord_limit,red_line_limit,lambda x:(x[vars.HIGH]!=x[vars.UP_LIMIT])& (x[OPEN] / x[PRE_CLOSE] >1.05) & (x[PCT_CHG] >= -5) & (
#                      x[PCT_CHG] <-1)]
#
# ts_code='300183'
# condition=[up_limit,red_t_limit,lambda x:(x[vars.OPEN]==x[vars.UP_LIMIT]) & (x[PCT_CHG] >0) & (
#                      x[PCT_CHG] <5)]

# ts_code='002751'
# condition=[n_n,ord_limit,red_line_limit,lambda x:(x[OPEN] / x[PRE_CLOSE] >1) & (x[OPEN] / x[PRE_CLOSE] <1.05)&(x[PCT_CHG] >-5) & (
#                      x[PCT_CHG] <0)]
# ts_code='300779'
# condition=[n_n,ord_limit,red_line_limit,ord_limit]
# ts_code='600697'
# condition=[n_n,red_line_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >=1.02) &(x[OPEN] / x[PRE_CLOSE] <=1.05) &(x[PCT_CHG] >= -1) & (
#                      x[PCT_CHG] <2)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code='002285'
# condition=[ord_limit,red_line_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >=1) &(x[OPEN] / x[PRE_CLOSE] <=1.03) &(x[PCT_CHG] >= 2) & (
#                      x[PCT_CHG] <5)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# condition=[up_limit,NR,lambda x: (x[OPEN] / x[PRE_CLOSE] >=1) &(x[OPEN] / x[PRE_CLOSE] <=1.03) &(x[PCT_CHG] >= 2) & (
#                      x[PCT_CHG] <5)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code=continu_list.ts_code
# condition=continu_list.condition

# condition=[n_n,
#
#            up_limit,red_line_limit]
# ts_code='600267'
# condition=[n_n,red_line_limit,red_line_limit,red_t_limit]

# ts_code='test000026a'
# # condition=[up_limit,red_line_limit,NR]
# condition=[up_limit,lambda x:(x[OPEN]==x[CLOSE])&(x[OPEN]==x[UP_LIMIT]),NR]
# ts_code='26nordt--open'
# condition=[n_n,ord_limit,red_t_limit,lambda x:  (x[OPEN] / x[PRE_CLOSE] >=1) &(x[OPEN] / x[PRE_CLOSE] <=1.03) &(x[PCT_CHG] >= 2) & (
#                      x[PCT_CHG] <5)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]

# ts_code='test'
# condition=[n_n,ord_limit,red_t_limit]
# ts_code='601038'
# condition=[n_n,ord_limit,ord_limit,ord_limit]
# ts_code = '000710'
# condition = [n_n, ord_limit, ord_limit,
#              lambda x: (x[OPEN] <= x[CLOSE]) & (x[HIGH] == x[UP_LIMIT]) & (x[PCT_CHG] >= 1) & (
#                      x[PCT_CHG] < 4)]
# ts_code = '300208'
#
# condition = [n_n, ord_limit, ord_limit,
#              lambda x: (x[OPEN] > x[CLOSE])  & (x[PCT_CHG] >= -4) & (
#                      x[PCT_CHG] < 0)]
# ts_code = '000822A'
#
# condition = [n_n, ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 3) & (
#                      x[PCT_CHG] < 8)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '300307'
# #
# condition = [n_n, ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 0) & (
#                      x[PCT_CHG] < 3)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '300086'
# #
# condition = [ red_line_limit,red_line_limit,
#              lambda x: (x[CLOSE]==x[DOWN_LIMIT])]
# ts_code = '002910'
# condition = [n_n, ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 0) & (
#                      x[PCT_CHG] < 5)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '002163'
# condition = [n_n, ord_limit,ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 3) & (
#                      x[PCT_CHG] < 8)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '002946'
# condition = [n_n, ord_limit,ord_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 4) & (
#                      x[PCT_CHG] < 9)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '300183'
# condition = [n_n, ord_limit,
#              lambda x: (x[OPEN] >x[CLOSE]) ,lambda x: (x[LOW] ==x[DOWN_LIMIT])&(x[CLOSE]!=x[DOWN_LIMIT])]
# ts_code = '002315'
# condition = [n_n, ord_limit,ord_limit,
#              lambda x: (x[OPEN] > x[CLOSE])  & (x[PCT_CHG] >= -5) & (
#                      x[PCT_CHG] < 0)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '603898-222'
# condition = [red_line_limit,red_t_limit,
#              lambda x: (x[CLOSE] > x[OPEN])  & (x[PCT_CHG] >= 5) & (x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]
# ts_code = '600187'
# condition = [n_n, ord_limit,ord_limit,ord_limit]


# ts_code = '002009'
# condition = [n_n, ord_limit,red_line_limit,
#              lambda x: (x[OPEN] < x[CLOSE])  & (x[PCT_CHG] >= 1) & (
#                      x[PCT_CHG] < 6)&(x[HIGH]!=x[UP_LIMIT])&(x[LOW]!=x[DOWN_LIMIT])]

# ts_code = '002614C-O'
# condition = [n_n,
#              lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1.01)  & (x[OPEN] / x[PRE_CLOSE] <= 1.02)  &(x[PCT_CHG] >= 5) & (
#                      x[PCT_CHG] < 6)&(x[HIGH]/x[PRE_CLOSE]<1.07),
#              lambda x:(x[OPEN] / x[PRE_CLOSE] >= 1.03)  & (x[OPEN] / x[PRE_CLOSE] <= 1.04) & (x[PCT_CHG] >= 4) & (
#                      x[PCT_CHG] < 5)&(x[LOW]/x[PRE_CLOSE]>1.02)]

# ts_code = '002151'
# condition = [up_limit,red_line_limit, red_line_limit, red_line_limit, ord_limit]
# ts_code = '600351'
# condition = [red_line_limit,lambda x: (x[OPEN]<x[CLOSE])& (x[PCT_CHG] >= 0) & (
#                      x[PCT_CHG] < 5)
#              ]
# ts_code = '002550'
# condition = [n_n, ord_limit, red_t_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= -10) & (
#         x[PCT_CHG] < -5) & (x[HIGH] != x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '002651'
# condition = [n_n,up_limit,up_limit,red_line_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= 6) & (
#         x[PCT_CHG] < 10) & (x[HIGH] == x[UP_LIMIT]) &(x[CLOSE] != x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '600166'
# condition = [n_n,ord_limit,ord_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= -6) & (
#         x[PCT_CHG] < -1) & (x[HIGH] != x[UP_LIMIT])  & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '002226'
# condition = [n_n,red_line_limit,red_t_limit, lambda x: (x[OPEN] > x[CLOSE])  & (
#         x[PCT_CHG] < -7) & (x[LOW] == x[DOWN_LIMIT])  & (x[CLOSE] != x[DOWN_LIMIT])]
# ts_code = '300776'
# condition = [n_n,ord_limit,red_line_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= -7) & (
#         x[PCT_CHG] < -2) & (x[HIGH] != x[UP_LIMIT])  & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '000597'
# condition = [n_n,red_line_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= 0) & (
#         x[PCT_CHG] < 4) & (x[HIGH] != x[UP_LIMIT])  & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '002442'
# condition = [n_n,ord_limit, lambda x: (x[OPEN] < x[CLOSE]) & (x[PCT_CHG] >= 0) & (
#         x[PCT_CHG] < 5) & (x[HIGH] == x[UP_LIMIT])  & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '603105'
# condition = [n_n,ord_limit, lambda x: (x[OPEN] < x[CLOSE]) & (x[PCT_CHG] >= 2) & (
#         x[PCT_CHG] < 7) & (x[HIGH] != x[UP_LIMIT])  & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '300278'
# condition = [n_n,ord_limit,ord_limit,red_line_limit,ord_limit]
# ts_code = '300401'
# condition = [n_n,ord_limit, lambda x: (x[OPEN] < x[CLOSE]) & (x[PCT_CHG] >= 0) & (
#         x[PCT_CHG] < 5) & (x[HIGH] != x[UP_LIMIT])  & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '002126'
# condition = [n_n, ord_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= 0) & (
#         x[PCT_CHG] < 5) & (x[HIGH] != x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '002451'
# condition = [red_line_limit,red_line_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= 0) & (
#         x[PCT_CHG] < 5) & (x[HIGH] == x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '000028'
# condition = [n_n,
# ord_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= -4) & (
#         x[PCT_CHG] < 0) & (x[HIGH] != x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '002748'
# condition = [n_n, ord_limit, lambda x: (x[OPEN] > x[CLOSE]) & (x[PCT_CHG] >= -9) & (
#         x[PCT_CHG] < -4) & (x[HIGH] != x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]
# ts_code = '002027'
# condition = [n_n, ord_limit, ord_limit,lambda x: (x[OPEN] < x[CLOSE]) & (x[PCT_CHG] >= 0) & (
#         x[PCT_CHG] < 3) & (x[HIGH] != x[UP_LIMIT]) & (x[LOW] != x[DOWN_LIMIT])]
# ts_code=''
# condition=[]
# ts_code = '300209'
# conditon = [n_n, up_limit, red_t_limit,
#             lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[OPEN] != x[UP_LIMIT]) & (x[PCT_CHG] >= -5) & (
#                         x[PCT_CHG] <= 0)]
# ts_code='600075'
# condition=[n_n, up_limit, red_t_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1) & (x[HIGH]== x[UP_LIMIT])]
# ts_code='6030693'
# condition=[n_n, up_limit, up_limit, up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1) & (x[CLOSE]== x[UP_LIMIT])& (x[OPEN]!= x[UP_LIMIT])]
# condition=[n_n,ord_limit,ord_limit,ord_limit,ord_limit]
# ts_code='300117'
# condition=[n_n, up_limit,up_limit, red_t_limit]
# ts_code='300464'
# condition=[n_n, up_limit, up_limit,  up_limit,  up_limit,  red_line_limit,red_t_limit]
# ts_code='000698'
# condition=[n_n, up_limit,red_line_limit]
# ts_code='000796'
# condition=[red_line_limit,  red_line_limit,red_line_limit,lambda x: (x[CLOSE]  != x[UP_LIMIT]) ,lambda x:x[CLOSE]==x[DOWN_LIMIT]]
# ts_code = '002614'
# condition = [lambda x: (x[PCT_CHG] > 9) & (x[CLOSE] != x[UP_LIMIT]),
#              lambda x: (x[PCT_CHG] < -9) & (x[CLOSE] != x[DOWN_LIMIT])]
# ts_code='002613'
# condition=[n_n, up_limit,up_limit,up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] >= 1.02) & (x[OPEN] != x[UP_LIMIT]) & (x[PCT_CHG] >= -8) & (
#                         x[PCT_CHG] <= 0)& (x[LOW] != x[DOWN_LIMIT]),up_limit,lambda x: (x[OPEN] / x[PRE_CLOSE]< 0.98)]

# ts_code='300301-2'
# condition=[n_n, red_line_limit,red_line_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] <= 1.05) &(x[OPEN] / x[PRE_CLOSE] >= 1)  & (x[PCT_CHG] >= -6) & (
#                         x[PCT_CHG] <= -3)]

# ts_code='000573-2'
# condition=[n_n, ord_limit,red_t_limit,lambda x: (x[OPEN] / x[PRE_CLOSE] <= 1.05) &(x[OPEN] / x[PRE_CLOSE] >= 1)  & (x[PCT_CHG] >= -6) & (
#                         x[PCT_CHG] <=-3)]
# ts_code='600961'
# condition=[n_n, up_limit, up_limit,red_t_limit]
# ts_code='000698'
# condition=[n_n, up_limit,red_line_limit,red_line_limit]
# ts_code='603633,303178'
# condition=[n_n, up_limit,red_line_limit]
# ts_code = '300178'
# condition = [up_limit, red_t_limit,
#              lambda x: (x[OPEN] != x[UP_LIMIT]) & (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[PCT_CHG] >= 4) & (
#                      x[PCT_CHG] <= 7)]
# ts_code='300084'
# condition=[n_n,  red_line_limit,  red_line_limit]
# ts_code='300052'
# condition=[n_n,  ord_limit,  red_t_limit]
# ts_code='603690'
# condition=[n_n,  ord_limit,  red_line_limit]
# ts_code='603020'
# condition=[n_n,  ord_limit,  lambda x:(x[OPEN] ==x[UP_LIMIT] ) & (x[PCT_CHG] >= 6) & (x[CLOSE]!=x[UP_LIMIT])]
# ts_code='300148'
# condition=[n_n,  up_limit,red_line_limit,  lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[OPEN] / x[PRE_CLOSE] >= 1.05) & (x[CLOSE]==x[UP_LIMIT])]
# ts_code='601615'
# condition=[n_n,  lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[HIGH]==x[UP_LIMIT]) & (x[CLOSE]!=x[UP_LIMIT])]
# ts_code='002960'
# condition=[n_n,  ord_limit,ord_limit]
# ts_code='300842'
# condition=[ord_limit,ord_limit,red_t_limit,lambda x:(x[OPEN] !=x[UP_LIMIT] ) & (x[OPEN] / x[PRE_CLOSE] >1) & (x[PCT_CHG] >= -7) & (
#                      x[PCT_CHG] <=-4)]
# ts_code='000011'
# condition=[n_n,up_limit,up_limit,red_line_limit,red_t_limit]
# ts_code='600127'
# condition=[n_n,ord_limit,ord_limit]
# ts_code='000026a'
# condition=[cl.up_limit,cl.red_line_limit or cl.red_t_limit]
# ts_code='002208'
# condition=[n_n,ord_limit,red_line_limit,lambda x:(x[vars.HIGH]!=x[vars.UP_LIMIT])& (x[OPEN] / x[PRE_CLOSE] >1.05) & (x[PCT_CHG] >= -5) & (
#                      x[PCT_CHG] <-1)]
ts_code='test'
condition=[cl.red_line_limit]
