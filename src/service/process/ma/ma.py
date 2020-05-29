
from src.util.sqlutil import SqlUtil;
import time
import operator
from datetime import datetime



class Ma(object):

    # FUNCTION
    #
    # NAME :        get_date_start_end
    #
    # DESCRIPTION:  获取计算ma的起始时间
    #
    # ARGUMENT:     n  天数
    #               date 要计算的日期
    #
    # RETURN:       date_start 起始时间
    #               date_end   截止时间
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def get_date_start_end(self, n, date):

        sqlUtil = SqlUtil()

        sqlUtil.connect();

        cursor = sqlUtil.db.cursor();

        select_sql = "SELECT code, date  FROM stock_history WhERE code = '000001' AND date <= \'" + date + "\' ORDER BY date desc LIMIT " + str(n)  # 002752、300208

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        sqlUtil.db.close()


        rows_count = len(rows)

        date_start = rows[rows_count - 1][1]
        date_end = rows[0][1]


        return date_start, date_end



    # FUNCTION
    #
    # NAME :        select_stock
    #
    # DESCRIPTION:  获取所有股票代码
    #
    # ARGUMENT:
    #
    # RETURN:       rows 所有股票数组
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def select_stock(self):

        sqlUtil = SqlUtil()

        sqlUtil.connect();

        cursor = sqlUtil.db.cursor();

        select_sql = 'SELECT code, name, industry  FROM stock '  # 002752、300208

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        sqlUtil.db.close()

        return rows




    #
    # FUNCTION
    #
    # NAME :        select_stock_history_by_code
    #
    # DESCRIPTION:  按照时间逆序的方式，选出1支股票历史记录，该函数暂时只用来测试
    #
    # ARGUMENT:
    #
    # RETURN:       rows 表stock_history数组记录
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def select_stock_history_by_code(self, code):

        sqlUtil = SqlUtil()

        sqlUtil.connect();

        cursor = sqlUtil.db.cursor();

        select_sql = "SELECT id, date, code, name,  close, high, low, open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count FROM stock_history WHERE code = \'" + code + "\'  ORDER BY date desc";

        print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        sqlUtil.db.close()

        return rows



    #
    # FUNCTION
    #
    # NAME :        select_stock_history_by_codes
    #
    # DESCRIPTION:  按照时间逆序的方式，选出多支股票历史记录，现在是默认一次取100支股票的历史数据
    #
    # ARGUMENT:     codes，比如('00001', '00002',...)
    #
    # RETURN:       rows 表stock_history数组记录
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def select_stock_history_by_codes(self, codes):

        sqlUtil = SqlUtil()

        sqlUtil.connect();

        cursor = sqlUtil.db.cursor();

        select_sql = "SELECT id, date, code, name,  close, high, low, open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count FROM stock_history WHERE code in " + codes + "  ORDER BY code, date desc";

        #print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except Exception:
            print("select_stock_history_by_codes error：" + Exception)

            sqlUtil.db.close()

        return rows



    #
    # FUNCTION
    #
    # NAME :        select_stock_history_by_days
    #
    # DESCRIPTION:  按照时间逆序的方式，选出参数时间范围内多支股票历史记录
    #
    # ARGUMENT:     codes，比如('00001', '00002',...)
    #               date_start，比如'2020-05-01'
    #               date_end，比如'2020-05-08'
    #
    # RETURN:       rows 表stock_history数组记录
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def select_stock_history_by_days(self, codes, date_start, date_end):

        sqlUtil = SqlUtil()

        sqlUtil.connect();

        cursor = sqlUtil.db.cursor();

        select_sql = "SELECT id, date, code, name,  close, high, low, open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count FROM stock_history WHERE code in " + codes + " AND date >= \'" + date_start + "\' AND date <= \'" + date_end + "\' ORDER BY code, date desc";

        #print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except Exception:
            print("select_stock_history_by_codes error：" + Exception)

            sqlUtil.db.close()

        return rows


    #
    # FUNCTION
    #
    # NAME :        update_ma
    #
    # DESCRIPTION:  将计算好的ma数据，批量更新到表stock_history
    #               这里采用when case方式，效率较低，测试下来更新2万条数据，10秒左右
    #               参考：https://www.cnblogs.com/lgqtecng/p/6415938.html
    #
    # ARGUMENT:     array_ma，比如[[id，ma5], [id, ma5],...]
    #               n，ma周期，比如5
    #
    # RETURN:
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def update_ma(self, array_ma, n):

        sqlUtil = SqlUtil()
        sqlUtil.connect();
        cursor = sqlUtil.db.cursor();

        #print(array_ma)

        array_ma_count = len(array_ma)

        if array_ma_count:

            ma_n = "ma" + str(n)

            update_sql = "UPDATE stock_history SET " + ma_n + " = CASE id "

            when_str = ""
            id_str = ""

            for array_item in array_ma:
                #print(array_item)

                when_item = "WHEN " + str(array_item[0]) + " THEN " + str(array_item[1]) + " "

                when_str = when_str + when_item

                id_str = id_str + str(array_item[0]) + ","

            id_str = id_str[0: len(id_str) - 1]

            update_sql = update_sql + when_str + " END WHERE id IN  (" + id_str + ")"

            try:

                cursor.execute(update_sql);

                sqlUtil.db.commit();
                #print("update_ma sucess");
            except Exception as e:
                # 发生错误时回滚
                sqlUtil.db.rollback();
                #print("update_ma error：" + e);

            sqlUtil.db.close()


    #
    # FUNCTION
    #
    # NAME :        cal_ma
    #
    # DESCRIPTION:  计算一支股票，所有历史记录的的ma
    #
    # ARGUMENT:     rows_all，该支股票所有历史记录
    #               n，ma周期，比如5
    #
    # RETURN:       array_ma，比如[[id，ma5], [id, ma5],...]
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def cal_ma(self, rows_all, n):

        rows_all_count = len(rows_all)

        #print('rows_all_count = '+str(rows_all_count))

        array_ma = []
        item_ma = []

        #历史记录少于5个的不计算
        if rows_all_count > 5:

            ma_n = 0
            array_n = []

            index = 0
            for row in rows_all:

                ma_n = 0
                array_n = rows_all[index: index + n]
                #print(array_n)
                close_sum_n = 0
                for array_n_row in array_n:
                    close = float(array_n_row[4])
                    close_sum_n = close_sum_n + close

                rows_n_count = len(array_n)
                if rows_n_count == n:
                    ma_n = close_sum_n / n
                else:
                    ma_n = 0

                close = float(row[4])

                #print('close = ' + str(close))
                #print('ma_n = ' + str(ma_n))

                item_ma = []
                item_ma.append(row[0])
                item_ma.append(ma_n)

                array_ma.append(item_ma)

                index = index + 1

        return array_ma





    #
    # FUNCTION
    #
    # NAME :        cal_ma_day
    #
    # DESCRIPTION:  计算一支股票，最新一天的ma
    #
    # ARGUMENT:     rows_all，该支股票n天的历史记录
    #               n，ma周期，比如5
    #
    # RETURN:       array_ma，比如[[id，ma5], [id, ma5],...]
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def cal_ma_day(self, rows_all, n):

        #print('cal_ma_day 111111111111')


        rows_all_count = len(rows_all)



        #print('rows_all_count :'+str(rows_all_count))
        #print('id :'+str(id))


        array_ma = []
        item_ma = []

        # 历史记录少于5个的不计算
        if rows_all_count >= 5:

            id = rows_all[0][0]
            ma_n = 0
            array_n = [] #存放计算周期时间段的历史数据，比如计算5天ma，则存放5天数据

            array_n = rows_all[0: n]

            close_sum_n = 0 #计算周期内收盘价综合，比如计算5天ma，则5天收盘价和
            for array_n_row in array_n:
                close = float(array_n_row[4]) #第5个元素是收盘价close
                close_sum_n = close_sum_n + close

            rows_n_count = len(array_n)
            if rows_n_count == n:
                ma_n = close_sum_n / n
            else:
                ma_n = 0


            # print('close = ' + str(close))
            # print('ma_n = ' + str(ma_n))

            item_ma = []
            item_ma.append(id)
            item_ma.append(ma_n)

            array_ma.append(item_ma)


        #print('cal_ma_day 2222222222')


        return array_ma



    #
    # FUNCTION
    #
    # NAME :        cal_ma_all
    #
    # DESCRIPTION:  计算所有股票的ma，为了加快速度，每100支股票计算一次
    #
    # ARGUMENT:     n，ma周期，比如5
    #
    # RETURN:
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def cal_ma_all(self, n):
        ma = Ma()
        rows = ma.select_stock()
        print('开始时间：%s' % datetime.now())
        stock_codes_array = []
        stock_index = 0
        stock_codes_str = "("
        array_ma_100 = []  # 100支股票的ma数据

        for row in rows:
            code = row[0]
            stock_codes_array.append(code)
            stock_index = stock_index + 1
            div_left = stock_index % 100;  # 每次取100支股票的历史记录
            # print('stock_index = ' + str(stock_index) + ' div_left = ' + str(div_left))
            stock_codes_str = stock_codes_str + "\'" + code + "\'" + ","
            if div_left == 0:
                stock_codes_str = stock_codes_str[0: len(stock_codes_str) - 1] + ")"
                print('stock_index：' + str(stock_index))
                print('time1：%s' % datetime.now())
                his_rows_all = ma.select_stock_history_by_codes(stock_codes_str)
                print('time2：%s' % datetime.now())
                # his_rows_all有100支股票的历史记录，按股票代码取出每只股票的历史记录
                for stock_code in stock_codes_array:
                    stock_his_rows = []
                    for his_row in his_rows_all:
                        his_stock_code = his_row[2]

                        if stock_code == his_stock_code:
                            stock_his_rows.append(his_row)

                    array_ma = ma.cal_ma(stock_his_rows, n)
                    array_ma_100 = array_ma_100 + array_ma

                print('time3：%s' % datetime.now())
                array_ma_100_count = len(array_ma_100)
                print('array_ma_100_count: ' + str(array_ma_100_count))
                ma.update_ma(array_ma_100, n)
                print('time4：%s' % datetime.now())
                stock_codes_str = "("
                stock_codes_array = []
                array_ma_100 = []

        print('结束时间：%s' % datetime.now())





    #
    # FUNCTION
    #
    # NAME :        cal_ma_day_all
    #
    # DESCRIPTION:  计算所有股票，最新一天的ma
    #
    # ARGUMENT:     n，ma周期，比如5
    #               date 要计算的日期
    #
    # RETURN:
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def cal_ma_day_all(self, n, date):
        ma = Ma()

        date_start, date_end = ma.get_date_start_end(n, date)

        rows = ma.select_stock()
        print('开始时间：%s' % datetime.now())
        stock_codes_array = []
        stock_index = 0
        stock_codes_str = "("
        array_ma_100 = []  # 100支股票的ma数据

        for row in rows:
            code = row[0]
            stock_codes_array.append(code)
            stock_index = stock_index + 1
            div_left = stock_index % 1000;  # 每次取100支股票的历史记录
            # print('stock_index = ' + str(stock_index) + ' div_left = ' + str(div_left))
            stock_codes_str = stock_codes_str + "\'" + code + "\'" + ","
            if div_left == 0:
                stock_codes_str = stock_codes_str[0: len(stock_codes_str) - 1] + ")"
                print('stock_index：' + str(stock_index))
                print('time1：%s' % datetime.now())
                his_rows_all = ma.select_stock_history_by_days(stock_codes_str, date_start, date_end)
                # print('his_rows_all :')
                # print(his_rows_all)
                print('time2：%s' % datetime.now())
                # his_rows_all有100支股票的历史记录，按股票代码取出每只股票的历史记录
                for stock_code in stock_codes_array:
                    stock_his_rows = []
                    for his_row in his_rows_all:
                        his_stock_code = his_row[2]

                        if stock_code == his_stock_code:
                            stock_his_rows.append(his_row)






                    array_ma = ma.cal_ma_day(stock_his_rows, n)

                    # print('array_ma:')
                    # print(array_ma)


                    array_ma_100 = array_ma_100 + array_ma

                print('time3：%s' % datetime.now())
                array_ma_100_count = len(array_ma_100)
                print('array_ma_100_count: ' + str(array_ma_100_count))
                # print('array_ma_100:')
                # print(array_ma_100)
                ma.update_ma(array_ma_100, n)
                print('time4：%s' % datetime.now())
                stock_codes_str = "("
                stock_codes_array = []
                array_ma_100 = []

        print('结束时间：%s' % datetime.now())







if __name__ == "__main__":


    ma = Ma()

    #计算当天所有股票5天均线
    ma.cal_ma_day_all(5, '2020-05-13')
    # 计算当天所有股票13天均线
    ma.cal_ma_day_all(13, '2020-05-13')
    # 计算当天所有股票21天均线
    ma.cal_ma_day_all(21, '2020-05-13')












