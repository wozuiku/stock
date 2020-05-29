
from src.util.sqlutil import SqlUtil;
import time
import operator
from datetime import datetime
from src.util.emailutil import EmailUtil




class Ma5(object):


    #
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
    def select_stock_history_by_ma(self, date):

        sqlUtil = SqlUtil()

        sqlUtil.connect();

        cursor = sqlUtil.db.cursor();

        select_sql = "SELECT date, code, name,  close, high, low, open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count FROM stock_history WHERE date = \'" + date + "\'  AND open < ma5  AND close > ma5 ORDER BY code, date";

        print('select_sql:' + select_sql)

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
    # NAME :        insert_stock_select
    #
    # DESCRIPTION:  将符合规则的股票新增到stock_select
    #
    # ARGUMENT:     batch  批次号
    #               code   股票代码
    #               name   股票名称
    #               type   规则类型
    #               remark 规则备注说明
    #
    # RETURN:
    #
    # HISTORY:
    #               1.0  2020-05-11  zhixian.zhu  Creation
    #
    def insert_stock_select(self, batch, code, name, type, remark):

        sqlUtil = SqlUtil()

        sqlUtil.connect();
        cursor = sqlUtil.db.cursor();


        insert_sql = "INSERT INTO stock_select(batch, code, name, type, remark)  VALUES ( \'" + batch + "\',  \'" + code + "\' , \'" + name + "\' , \'" + type + "\' , \'" + remark + "\')"

        print(insert_sql)

        try:

            cursor.execute(insert_sql);

            sqlUtil.db.commit();
            #print("insert_stock_history sucess");
        except Exception as e:
            # 发生错误时回滚
            sqlUtil.db.rollback();
            print("insert_stock_history error：" + e);

        # 关闭数据库连接
        sqlUtil.db.close()



    def check_turn_over_rate(self, rows):

        count = 0;
        for row in rows:
            if row >= 3:
                count = count + 1

        if  count == len(rows):
            return True
        else:
            return False



    def ma_5(self, rows_all, batch):

        ma5 = Ma5()




        for row in rows_all:
            remark = '股票刚突破5天均线'
            code = row[1]
            name = row[2]

            ma5.insert_stock_select(batch, code, name, '1', remark)














if __name__ == "__main__":

    ma5 = Ma5()
    emailUtil = EmailUtil()
    rows = ma5.select_stock_history_by_ma('2020-05-11')
    batch = '2020051102'
    ma5.ma_5(rows, batch)
    emailUtil.send_email(batch)








