
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
    #
    # RETURN:       date_start 起始时间
    #               date_end   截止时间
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def get_date_start_end(self, n):

        sqlUtil = SqlUtil()

        sqlUtil.connect();

        cursor = sqlUtil.db.cursor();

        select_sql = "SELECT code, date  FROM stock_history WhERE code = '000001' ORDER BY date desc LIMIT " + str(n)  # 002752、300208

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
    # NAME :        update_vma
    #
    # DESCRIPTION:  将计算好的vma数据，批量更新到表stock_history
    #               这里采用when case方式，效率较低，测试下来更新2万条数据，10秒左右
    #               参考：https://www.cnblogs.com/lgqtecng/p/6415938.html
    #
    # ARGUMENT:     array_ma，比如[[id，vma50], [id, vma50],...]
    #               n，ma周期，比如5
    #
    # RETURN:
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def update_vma(self, array_vma, n):

        sqlUtil = SqlUtil()
        sqlUtil.connect();
        cursor = sqlUtil.db.cursor();

        # print(array_ma)

        array_vma_count = len(array_vma)

        if array_vma_count:

            vma_n = "vma" + str(n)

            update_sql = "UPDATE stock_history SET " + vma_n + " = CASE id "

            when_str = ""
            id_str = ""

            for array_item in array_vma:
                # print(array_item)

                when_item = "WHEN " + str(array_item[0]) + " THEN " + str(array_item[1]) + " "

                when_str = when_str + when_item

                id_str = id_str + str(array_item[0]) + ","

            id_str = id_str[0: len(id_str) - 1]

            update_sql = update_sql + when_str + " END WHERE id IN  (" + id_str + ")"

            try:

                cursor.execute(update_sql);

                sqlUtil.db.commit();
                # print("update_ma sucess");
            except Exception as e:
                # 发生错误时回滚
                sqlUtil.db.rollback();
                # print("update_ma error：" + e);

            sqlUtil.db.close()




    #
    # FUNCTION
    #
    # NAME :        cal_vma
    #
    # DESCRIPTION:  计算一支股票，所有历史记录的成交量均值
    #
    # ARGUMENT:     rows_all，该支股票所有历史记录
    #               n，成交量周期，比如50
    #
    # RETURN:       array_vma，比如[[id，vma50], [id, vma50],...]
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def cal_vma(self, rows_all, n):

        rows_all_count = len(rows_all)

        #print('rows_all_count = '+str(rows_all_count))

        array_vma = []
        item_vma = []

        #历史记录需要大于要计算的成交量周期
        if rows_all_count >= n:

            vma_n = 0  #周期为n的移动平均成交量
            array_n = []  #用于计算周期n的平均成交量的数据记录

            index = 0
            for row in rows_all:

                vma_n = 0
                array_n = rows_all[index: index + n]
                #print(array_n)
                volume_sum_n = 0
                for array_n_row in array_n:
                    volume = float(array_n_row[12])
                    volume_sum_n = volume_sum_n + volume

                rows_n_count = len(array_n)
                if rows_n_count == n:
                    vma_n = volume_sum_n / n
                else:
                    vma_n = 0

                close = float(row[4])

                #print('close = ' + str(close))
                #print('ma_n = ' + str(ma_n))

                item_vma = []
                item_vma.append(row[0]) #id
                item_vma.append(vma_n)  #平均成交量

                array_vma.append(item_vma)

                index = index + 1

        return array_vma




    #
    # FUNCTION
    #
    # NAME :        cal_vma_day
    #
    # DESCRIPTION:  计算一支股票，最新一天的，周期为n的移动平均成交量
    #
    # ARGUMENT:     rows_all，该支股票n天的历史记录
    #               n，移动平均成交量周期，比如50
    #
    # RETURN:       array_vma，比如[[id，vma50], [id, vma50],...]
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def cal_vma_day(self, rows_all, n):

        #print('cal_ma_day 111111111111')


        rows_all_count = len(rows_all)



        #print('rows_all_count :'+str(rows_all_count))
        #print('id :'+str(id))


        array_vma = []
        item_vma = []

        # 历史记录少于5个的不计算
        if rows_all_count >= n:

            id = rows_all[0][0]
            vma_n = 0    #周期为n的移动平均成交量
            array_n = [] #存放计算周期时间段的历史数据，比如计算50天移动平均成交量，则存放50天数据

            array_n = rows_all[0: n]

            volume_sum_n = 0 #计算周期内收盘价综合，比如计算5天ma，则5天收盘价和
            for array_n_row in array_n:
                volume = float(array_n_row[12]) #第5个元素是收盘价close
                volume_sum_n = volume_sum_n + volume

            rows_n_count = len(array_n)
            if rows_n_count == n:
                vma_n = volume_sum_n / n
            else:
                vma_n = 0


            # print('close = ' + str(close))
            # print('ma_n = ' + str(ma_n))

            item_vma = []
            item_vma.append(id)
            item_vma.append(vma_n)

            array_vma.append(item_vma)


        #print('cal_ma_day 2222222222')


        return array_vma



    #
    # FUNCTION
    #
    # NAME :        cal_vma_all
    #
    # DESCRIPTION:  计算所有股票的vma，为了加快速度，每100支股票计算一次
    #
    # ARGUMENT:     n，vma周期，比如50
    #
    # RETURN:
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def cal_vma_all(self, n):
        ma = Ma()
        rows = ma.select_stock()
        print('开始时间：%s' % datetime.now())
        stock_codes_array = []
        stock_index = 0
        stock_codes_str = "("
        array_vma_sum = []  # 多支股票的vma数据，比如100支

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

                    array_vma = ma.cal_vma(stock_his_rows, n)
                    array_vma_sum = array_vma_sum + array_vma

                print('time3：%s' % datetime.now())
                array_vma_sum_count = len(array_vma_sum)
                print('array_vma_sum_count: ' + str(array_vma_sum_count))
                ma.update_vma(array_vma_sum, n)
                print('time4：%s' % datetime.now())
                stock_codes_str = "("
                stock_codes_array = []
                array_vma_sum = []

        print('结束时间：%s' % datetime.now())




    #
    # FUNCTION
    #
    # NAME :        cal_vma_day_all
    #
    # DESCRIPTION:  计算所有股票，最新一天的vma
    #
    # ARGUMENT:     n，vma周期，比如50
    #
    # RETURN:
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def cal_vma_day_all(self, n):
        ma = Ma()

        date_start, date_end = ma.get_date_start_end(n)


        rows = ma.select_stock()
        print('开始时间：%s' % datetime.now())
        stock_codes_array = []
        stock_index = 0
        stock_codes_str = "("
        array_vma_sum = []  # 多支股票的vma数据，比如100支

        for row in rows:
            code = row[0]
            stock_codes_array.append(code)
            stock_index = stock_index + 1
            div_left = stock_index % 1000;  # 每次取1000支股票的历史记录
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






                    array_vma = ma.cal_vma_day(stock_his_rows, n)

                    # print('array_ma:')
                    # print(array_ma)

                    array_vma_sum = array_vma_sum + array_vma

                print('time3：%s' % datetime.now())
                array_vma_sum_count = len(array_vma_sum)
                print('array_vma_sum_count: ' + str(array_vma_sum_count))
                # print('array_ma_100:')
                # print(array_ma_100)
                ma.update_vma(array_vma_sum, n)
                print('time4：%s' % datetime.now())
                stock_codes_str = "("
                stock_codes_array = []
                array_vma_sum = []

        print('结束时间：%s' % datetime.now())




if __name__ == "__main__":


    ma = Ma()

    # 计算当天所有股票50天成交量均线
    ma.cal_vma_day_all(50)











