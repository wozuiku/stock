
from src.util.sqlutil import SqlUtil;
import time
import operator
from datetime import datetime



class RuleUtil(object):


    def check_turn_over_rate(self, rows):

        count = 0;
        for row in rows:
            if row >= 3:
                count = count + 1

        if  count == len(rows):
            return True
        else:
            return False



    def rule_3DayUp(self, rows_all, batch):
        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()

        rule_vol_n = 3

        rows_all_count = len(rows_all)
        rows_target = rows_all[rows_all_count - rule_vol_n: rows_all_count]
        # rows_target = rows_all [209 : 213]



        ruleUtil = RuleUtil()

        opens = []
        closes = [];
        volumes = [];
        turn_over_rates = [];

        for row in rows_target:
            code = row[1]
            name = row[2]
            close = float(row[3])
            open = float(row[6])
            turn_over_rate = float(row[10])
            bargain_volume = float(row[11])

            if close > open:
                opens.append(open)
                closes.append(close)
                volumes.append(bargain_volume)
                turn_over_rates.append(float(turn_over_rate))

        sort_closes = sorted(closes)
        sort_volumes = sorted(volumes)

        if len(closes) >= 3:
            if ruleUtil.check_turn_over_rate(turn_over_rates) and operator.eq(closes, sort_closes) and operator.eq(volumes, sort_volumes):
                remark = '连续3天换手率>=3，开始温和放量 # 连续3天股价逐渐升高 # 连续3天成交量依次放大'

                exist_rows = sqlUtil.check_stock_select(code, batch)
                exist_count = exist_rows[0][0]

                if exist_count == 0:
                    sqlUtil.insert_stock_select(batch, code, name, '1', remark)
                print(remark)

        else:
            print('需要3天数据，实际只有'+ str(len(closes)) + "天")



    def rule_1WinN(self, rows_all):

        now = int(time.time())
        timeStruct = time.localtime(now)
        batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()
        rule_win_n = 4

        rows_all_count = len(rows_all)
        rows_target = rows_all[rows_all_count - rule_win_n : rows_all_count]

        rows_target_count = len(rows_target)

        if rows_target_count > 3:

            close_target = float(rows_target[rows_target_count - 1][3])
            open_target = float(rows_target[rows_target_count - 1][6])

            rule_count1 = 0
            rule_count2 = 0
            rule_count3 = 0

            if close_target > open_target:

                for i in range(0, rows_target_count - 1):

                    close = float(rows_target[i][3])
                    open = float(rows_target[i][6])

                    if close < open:
                        rule_count1 = rule_count1 + 1;


                for i in range(0, rows_target_count - 1):
                    close = float(rows_target[i][3])
                    open = float(rows_target[i][6])

                    if open_target < close:
                        rule_count2 = rule_count2 + 1

                    if close_target > open:
                        rule_count3 = rule_count3 + 1




            if (rule_count1 == rows_target_count - 1) and (rule_count2 == rows_target_count - 1) and (rule_count3 == rows_target_count - 1):

                remark = '当天阳线吞没前' + str(rows_target_count - 1) + '天震荡K线'
                code = rows_target[rows_target_count - 1][1]
                name = rows_target[rows_target_count - 1][2]


                sqlUtil.insert_stock_select(batch, code, name, '2', remark)
                print(remark)
                print(code)
                print(name)


    def rule_1Win9(self, rows_all, batch):

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()
        rule_win_n = 10


        rows_all_count = len(rows_all)

        rows_target = rows_all[rows_all_count - rule_win_n : rows_all_count]
        #rows_target = rows_all[233 : 243]
        rows_target_count = len(rows_target)

        for row in rows_target:
            print(row)



        if rows_target_count > 9:

            close_target = float(rows_target[rows_target_count - 1][3])
            high_target = float(rows_target[rows_target_count - 1][4])
            low_target = float(rows_target[rows_target_count - 1][5])
            open_target = float(rows_target[rows_target_count - 1][6])




            rule_count1 = 0
            rule_count2 = 0


            if close_target > open_target:


                for i in range(0, rows_target_count - 1):
                    close = float(rows_target[i][3])
                    open = float(rows_target[i][6])

                    if (low_target <= open) and (low_target <= close):
                        rule_count1 = rule_count1 + 1

                    if (close_target > open) and (close_target > close):
                        rule_count2 = rule_count2 + 1



            if (rule_count1 == rows_target_count - 1) and (rule_count2 == rows_target_count - 1):

                remark = '当天阳线吞没前' + str(rows_target_count - 1) + '天震荡K线'
                code = rows_target[rows_target_count - 1][1]
                name = rows_target[rows_target_count - 1][2]

                sqlUtil.insert_stock_select(batch, code, name, '9', remark)
                print(remark)
                print(code)
                print(name)



    def rule_dayangLine(self, rows_all, batch):

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()


        rows_all_count = len(rows_all)

        if rows_all_count > 9:

            close = float(rows_all[rows_all_count - 1][3])
            high = float(rows_all[rows_all_count - 1][4])
            low = float(rows_all[rows_all_count - 1][5])
            open = float(rows_all[rows_all_count - 1][6])


            if open > 0:
                upRate = (close - open) / open;


            if close > open:

                if (close == high) and (low == open):

                    if upRate >= 0.05:
                        remark = '当天大阳线，且涨幅超过5%'
                        code = rows_all[rows_all_count - 1][1]
                        name = rows_all[rows_all_count - 1][2]

                        sqlUtil.insert_stock_select(batch, code, name, '2', remark)
                        print(remark)
                        print(code)
                        print(name)





    def rule_limitUp(self, rows_all, batch):

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()


        rows_all_count = len(rows_all)

        if rows_all_count > 2:

            close = float(rows_all[rows_all_count - 1][3])

            close_pre = float(rows_all[rows_all_count - 2][3])

            upRate = 0

            if close_pre > 0:
                upRate = (close - close_pre) / close_pre;

            if upRate >= 0.095:

                remark = '今天涨停'
                code = rows_all[rows_all_count - 1][1]
                name = rows_all[rows_all_count - 1][2]

                sqlUtil.insert_stock_select(batch, code, name, '101', remark)
                print(remark)
                print(code)
                print(name)



    def rule_50PercentUp(self, rows_all, batch):

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()

        rows_all_count = len(rows_all)

        if rows_all_count > 30:

            close_end = float(rows_all[rows_all_count - 1][3])

            close_begin = float(rows_all[rows_all_count - 30][3])

            upRate = 0

            if close_begin > 0:
                upRate = (close_end - close_begin) / close_begin;

            if upRate >= 0.5:
                remark = '30天累计涨幅超过50%'
                code = rows_all[rows_all_count - 1][1]
                name = rows_all[rows_all_count - 1][2]

                sqlUtil.insert_stock_select(batch, code, name, '102', remark)
                print(remark)
                print(code)
                print(name)

    def rule_20PercentUp(self, rows_all, batch):

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()

        rows_all_count = len(rows_all)

        if rows_all_count > 5:

            close_end = float(rows_all[rows_all_count - 1][3])

            close_begin = float(rows_all[rows_all_count - 5][3])

            upRate = 0

            if close_begin > 0:
                upRate = (close_end - close_begin) / close_begin;

            if upRate >= 0.2:
                remark = '1周累计涨幅超过20%'
                code = rows_all[rows_all_count - 1][1]
                name = rows_all[rows_all_count - 1][2]

                sqlUtil.insert_stock_select(batch, code, name, '103', remark)
                print(remark)
                print(code)
                print(name)



    def rule_bigDown(self, rows_all, batch):

        now = int(time.time())
        timeStruct = time.localtime(now)
        batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()
        rule_day_count = 10
        rows_all_count = len(rows_all)
        rows_target = rows_all[rows_all_count - rule_day_count: rows_all_count]
        rows_target_count = len(rows_target)

        index = 0
        for row in rows_target:
            print(row)
            print(index)
            index = index + 1


        if rows_target_count > 9:

            closes = []

            for row in rows_all:
                closes.append(float(row[3]))

            close_max = max(closes)
            close_min = min(closes)

            print('close_max = ' + str(close_max))
            print('close_min = ' + str(close_min))

            close_targets = []

            for row in rows_target:
                close_targets.append(float(row[3]))

            close_target_max = max(close_targets)
            close_target_min = min(close_targets)

            print('close_target_max = ' + str(close_target_max))
            print('close_target_min = ' + str(close_target_min))


            rateDown = 0

            if close_max > 0:
                rateDown = (close_max - close_min) / close_max

            print('rateDown = ' + str(rateDown))

            rateUp_target = 0
            if rateDown > 0.5:


                if close_target_min > 0:

                    rateUp_target = (close_target_max - close_target_min) / close_target_min

                print('rateUp_target = ' + str(rateUp_target))


                if rateUp_target < 0.2:

                    close = float(rows_all[rows_all_count - 1][3])
                    high = float(rows_all[rows_all_count - 1][4])
                    low = float(rows_all[rows_all_count - 1][5])
                    open = float(rows_all[rows_all_count - 1][6])

                    turn_over_rate = float(rows_all[rows_all_count - 1][10])

                    rateUp_today = 0

                    if open > 0:
                        rateUp_today = (close - open) / open;

                    print('rateUp_today = ' + str(rateUp_today))

                    if close > open:


                        if rateUp_today >= 0.05:
                            remark = '累计跌幅超过50%，最近9天涨幅小于20%，今天涨幅超过5%'
                            code = rows_all[rows_all_count - 1][1]
                            name = rows_all[rows_all_count - 1][2]

                            sqlUtil.insert_stock_select(batch, code, name, '4', remark)
                            print(remark)
                            print(code)
                            print(name)

                        # if turn_over_rate >= 3:
                        #     remark = '累计跌幅超过50%，最近9天涨幅小于20%，今天换手率超过3%'
                        #     code = rows_all[rows_all_count - 1][1]
                        #     name = rows_all[rows_all_count - 1][2]
                        #
                        #     sqlUtil.insert_stock_select(batch, code, name, '4', remark)
                        #     print(remark)
                        #     print(code)
                        #     print(name)




    def rule_littleUp(self, rows_all, batch):

        # now = int(time.time())
        # timeStruct = time.localtime(now)
        # batch = time.strftime("%Y%m%d", timeStruct)

        sqlUtil = SqlUtil()
        rule_day_count = 10
        rows_all_count = len(rows_all)
        rows_target = rows_all[rows_all_count - rule_day_count: rows_all_count]
        rows_target_count = len(rows_target)

        close_targets = []
        open_targets = []

        for row in rows_target:
            print(row)
            close_targets.append(float(row[3]))
            open_targets.append(float(row[6]))

        print('******')

        print(close_targets)
        print('******')
        print(open_targets)
        print('******')

        if rows_target_count > 9:


            close_target_max = max(close_targets)
            close_target_min = min(close_targets)
            open_target_max = max(open_targets)
            open_target_min = min(open_targets)

            target_max = max([close_target_max, close_target_min, open_target_max, open_target_min])
            target_min = min([close_target_max, close_target_min, open_target_max, open_target_min])



            print('close_target_max = ' + str(close_target_max))
            print('close_target_min = ' + str(close_target_min))
            print('open_target_max = ' + str(open_target_max))
            print('open_target_min = ' + str(open_target_min))

            print('target_max = ' + str(target_max))
            print('target_min = ' + str(target_min))




            if target_max > 0:

                rateUp_target = (target_max - target_min) / target_max

                if rateUp_target < 0.1:

                    close = float(rows_all[rows_all_count - 1][3])
                    high = float(rows_all[rows_all_count - 1][4])
                    low = float(rows_all[rows_all_count - 1][5])
                    open = float(rows_all[rows_all_count - 1][6])

                    turn_over_rate = float(rows_all[rows_all_count - 1][10])

                    upRate = 0

                    if open > 0:
                        upRate = (close - open) / open;

                    if close > open:



                        if upRate >= 0.05:
                            remark = '最近9天涨幅小于20%，今天涨幅超过5%'
                            code = rows_all[rows_all_count - 1][1]
                            name = rows_all[rows_all_count - 1][2]

                            sqlUtil.insert_stock_select(batch, code, name, '5', remark)
                            print(remark)
                            print(code)
                            print(name)

                        if turn_over_rate >= 3:
                            remark = '最近9天涨幅小于20%，今天换手率超过3%'
                            code = rows_all[rows_all_count - 1][1]
                            name = rows_all[rows_all_count - 1][2]

                            sqlUtil.insert_stock_select(batch, code, name, '5', remark)
                            print(remark)
                            print(code)
                            print(name)



if __name__ == "__main__":

    sqlUtil = SqlUtil()
    ruleUtil = RuleUtil()

    rows = sqlUtil.select_stock()

    # print('开始时间：%s' % datetime.now())
    #
    # for row in rows:
    #     code = row[0]
    #     rows_all = sqlUtil.select_stock_history_by_code(code)
    #     ruleUtil.rule_1Win9(rows_all)
    #
    # print('结束时间：%s' % datetime.now())

    rows_all = sqlUtil.select_stock_history_by_code('002385')
    ruleUtil.rule_bigDown(rows_all)







