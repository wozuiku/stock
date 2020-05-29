
from src.util.sqlutil import SqlUtil;
import time
import operator
from datetime import datetime



class MaUtil(object):


    def cal_ma(self, rows_all, day_count):

        sqlUtil = SqlUtil()

        rows_all_count = len(rows_all)

        print('rows_all_count = '+str(rows_all_count))

        ma_5 = 0
        ma_13 = 0
        ma_21 = 0

        array_5 = []
        array_13 = []
        array_21 = []



        index = 0
        for row in rows_all:
            print(row)
            print(index)

            # ma_5 = 0
            # if index >= 5:
            #     array_5 = rows_all[index - 5 : index]
            #     print(array_5)
            #     close_sum_5 = 0
            #     for array_5_row in array_5:
            #         close = float(array_5_row[3])
            #         close_sum_5 = close_sum_5 + close
            #     ma_5 = close_sum_5 / 5
            #
            # print('ma_5 = ' + str(ma_5))

            ma_5 = 0

            array_5 = rows_all[index: index + 5]
            print(array_5)
            close_sum_5 = 0
            for array_5_row in array_5:
                close = float(array_5_row[3])
                close_sum_5 = close_sum_5 + close
            ma_5 = close_sum_5 / 5

            print('ma_5 = ' + str(ma_5))

            # ma_13 = 0
            # if index >= 13:
            #     array_13 = rows_all[index - 13: index]
            #     #print(array_13)
            #     close_sum_13 = 0
            #     for array_13_row in array_13:
            #         close = float(array_13_row[3])
            #         close_sum_13 = close_sum_13 + close
            #     ma_13 = close_sum_13 / 13
            #
            # print('ma_13 = ' + str(ma_13))
            #
            # ma_21 = 0
            # if index >= 21:
            #     array_21 = rows_all[index - 21: index]
            #
            #     close_sum_21 = 0
            #     for array_21_row in array_21:
            #         close = float(array_21_row[3])
            #         close_sum_21 = close_sum_21 + close
            #     ma_21 = close_sum_21 / 21
            #
            # print('ma_21 = ' + str(ma_21))
            #
            #
            #
            #
            index = index + 1

















if __name__ == "__main__":

    sqlUtil = SqlUtil()
    ruleUtil = MaUtil()

    rows = sqlUtil.select_stock()

    # print('开始时间：%s' % datetime.now())
    #
    # for row in rows:
    #     code = row[0]
    #     rows_all = sqlUtil.select_stock_history_by_code(code)
    #     ruleUtil.rule_1Win9(rows_all)
    #
    # print('结束时间：%s' % datetime.now())

    rows_all = sqlUtil.select_stock_history_by_code_desc('000858')
    ruleUtil.cal_ma(rows_all, 5)







