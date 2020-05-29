import pymysql

from datetime import datetime
import csv;
import re;




class SqlUtil(object):

    def __init__(self):
        self.db = None

    def connect(self):

        self.db = pymysql.connect("localhost", "root", "zzx363711", "stock");



    def insert_stock_nows(self, now_values):

        self.connect();
        cursor = self.db.cursor();


        insert_sql = "INSERT INTO stock_now(time, code, name, price, high, low,  open, pre_close, bargain_volume, bargain_amount)  VALUES " + now_values + ""

        print('insert_stock_nows insert_sql = ' + insert_sql)

        try:

            cursor.execute(insert_sql);

            self.db.commit();
            #print("insert_stock_history sucess");
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback();
            print("insert_stock_nows error：" + e);

        # 关闭数据库连接
        self.db.close()




    def insert_stock(self, code, name, industry, exchange, url,  status):

        self.connect();
        cursor = self.db.cursor();


        insert_sql = "INSERT INTO stock(code, name, industry, exchange, url,  status)  VALUES ( \'" + code + "\', \'" + name + "',\'" + industry + "\',\'" + exchange + "',\'" + url + "',\'" + status + "\')"


        try:

            cursor.execute(insert_sql);

            self.db.commit();
            print("insert_stock sucess");
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback();
            print("insert_stock error：" + e);

        # 关闭数据库连接
        self.db.close()




    def check_stock_history(self, code, date):

        self.connect();

        cursor = self.db.cursor();

        select_sql = "SELECT count(*) FROM stock_history sh WHERE sh.code = \'" + code + "\' AND sh.date =  \'" + date + "\'";

        print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        self.db.close()

        return rows




    def insert_stock_concept(self, concept, code, name):

        self.connect();
        cursor = self.db.cursor();


        insert_sql = "INSERT INTO stock_concept(concept, code, name)  VALUES ( \'" + concept + "\', \'" + code + "',\'" + name + "\')"


        try:

            cursor.execute(insert_sql);

            self.db.commit();
            print("insert_stock_concept sucess");
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback();
            print("insert_stock_concept error：" + e);

        # 关闭数据库连接
        self.db.close()



    def insert_stock_history(self, date, code, name, close, high, low,  open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count):

        self.connect();
        cursor = self.db.cursor();


        insert_sql = "INSERT INTO stock_history(date, code, name, close, high, low,  open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count)  VALUES ( \'" + date + "\', \'" + code + "\',\'" + name + "\',\'" + close + "\',\'" + high + "\',\'" + low + "\',\'" + open + "\',\'" + pre_close + "\',\'" + up_down_price + "\',\'" + up_down_range + "\',\'" + turn_over_rate + "\',\'" + bargain_volume + "\',\'" + bargain_amount + "\',\'" + total_market_value + "\',\'" + flow_market_value + "\',\'" + bargain_ticket_count + "\')"


        try:

            cursor.execute(insert_sql);

            self.db.commit();
            #print("insert_stock_history sucess");
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback();
            print("insert_stock_history error：" + e);

        # 关闭数据库连接
        self.db.close()


    def insert_stock_historys(self, history_values):

        self.connect();
        cursor = self.db.cursor();


        insert_sql = "INSERT INTO stock_history(date, code, name, close, high, low,  open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count)  VALUES " + history_values + ""

        print('insert_stock_historys insert_sql = ' + insert_sql)

        try:

            cursor.execute(insert_sql);

            self.db.commit();
            #print("insert_stock_history sucess");
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback();
            print("insert_stock_history error：" + e);

        # 关闭数据库连接
        self.db.close()



    def insert_stock_select(self, batch, code, name, type, remark):

        self.connect();
        cursor = self.db.cursor();


        insert_sql = "INSERT INTO stock_select(batch, code, name, type, remark)  VALUES ( \'" + batch + "\',  \'" + code + "\' , \'" + name + "\' , \'" + type + "\' , \'" + remark + "\')"

        print(insert_sql)

        try:

            cursor.execute(insert_sql);

            self.db.commit();
            #print("insert_stock_history sucess");
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback();
            print("insert_stock_history error：" + e);

        # 关闭数据库连接
        self.db.close()



    def insert_stock_industry(self, code, name, industry, segment):

        self.connect();
        cursor = self.db.cursor();


        insert_sql = "INSERT INTO stock_industry(code, name, industry, segment)  VALUES ( \'" + code + "\', \'" + name + "\',\'" + industry + "\',\'" + segment + "\')"


        try:

            cursor.execute(insert_sql);

            self.db.commit();
            print("insert_stock_industry sucess");
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback();
            print("insert_stock_industry error：" + e);

        # 关闭数据库连接
        self.db.close()



    def check_stock_select(self, code, batch):

        self.connect();

        cursor = self.db.cursor();

        select_sql = "SELECT count(*) FROM stock_select ss WHERE ss.code = \'" + code + "\' AND ss.batch =  \'" + batch + "\'";

        print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        self.db.close()

        return rows



    def select_stock(self):

        self.connect();

        cursor = self.db.cursor();

        select_sql = 'SELECT code, name, industry  FROM stock '#002752、300208

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        self.db.close()



        return rows



    def select_stock_history_by_code(self, code):

        self.connect();

        cursor = self.db.cursor();

        select_sql = "SELECT date, code, name,  close, high, low, open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count FROM stock_history WHERE code = \'" + code + "\'  ORDER BY date";

        print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        self.db.close()

        return rows



    def select_stock_history_by_code_desc(self, code):

        self.connect();

        cursor = self.db.cursor();

        select_sql = "SELECT date, code, name,  close, high, low, open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count FROM stock_history WHERE code = \'" + code + "\'  ORDER BY date desc";

        print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        self.db.close()

        return rows




    def select_stock_history_by_codes(self, codes):

        self.connect();

        cursor = self.db.cursor();

        select_sql = "SELECT date, code, name,  close, high, low, open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count FROM stock_history WHERE code in " + codes + "  ORDER BY code, date";

        print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        self.db.close()

        return rows



    def select_stock_select(self, batch):

        self.connect();

        cursor = self.db.cursor();

        select_sql = "SELECT si.industry, si.segment, s.code, s.name, s.sector, sh.turn_over_rate, sh.close, ss.remark, ss.type FROM stock s, stock_industry si, stock_select ss, stock_history sh WHERE s.code = si.code AND s.code = ss.code AND s.code = sh.code AND replace(sh.date, '-', '') = ss.batch AND ss.batch = \'" + batch + "\' AND s.sector <> \'科创板\' AND s.name NOT LIKE '%ST%' ORDER BY ss.type, si.industry, si.segment";

        print('select_sql:'+select_sql)

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        self.db.close()

        return rows


    def update_stock_history_ma(self, date, code, ma5, ma13, ma21, vma50):

        self.connect();
        cursor = self.db.cursor();


        update_sql = "UPDATE stock_history SET ma5 = \'" + ma5 + "\', ma13 = \'" + ma13 + "', ma21 = \'" + ma21 + "\', vma50 = \'" + vma50 + "' WHERE date = \'" + date + "' AND code = \'" + code + "\'"

        print('update_sql = ' + update_sql)

        try:

            cursor.execute(update_sql);

            self.db.commit();
            print("update_stock_history_ma sucess");
        except Exception as e:
            # 发生错误时回滚
            self.db.rollback();
            print("update_stock_history_ma error：" + e);

        # 关闭数据库连接
        self.db.close()



    def import_csv_data(self, data_path):



        sqlUtil = SqlUtil()
        rows = sqlUtil.select_stock()

        for row in rows:
            csv_name = data_path + row[0] + ".csv";

            try:

                print('处理文件：' + csv_name)

                with open(csv_name, 'r', encoding='gb2312') as f:
                    reader = csv.reader(f)
                    next(reader)

                    for row in reader:
                        date = row[0]

                        matchObj = re.search(r'[0-9]+', row[1], re.M | re.I)

                        if matchObj:
                            code = matchObj.group()
                        else:
                            code = ''
                        name = row[2]
                        close = row[3]
                        high = row[4]
                        low = row[5]
                        open2 = row[6]
                        pre_close = row[7]
                        up_down_price = row[8]
                        up_down_range = row[9]
                        turn_over_rate = row[10]
                        bargain_volume = row[11]
                        bargain_amount = row[12]
                        total_market_value = row[13]
                        flow_market_value = row[14]
                        bargain_ticket_count = row[15]

                        exist_rows = sqlUtil.check_stock_history(code, date)
                        exist_count = exist_rows[0][0]

                        if exist_count == 0:
                            sqlUtil.insert_stock_history(date, code, name, close, high, low, open2, pre_close,
                                                         up_down_price,
                                                         up_down_range, turn_over_rate, bargain_volume, bargain_amount,
                                                         total_market_value, flow_market_value, bargain_ticket_count)


            except:

                print('处理文件：' + csv_name + '出错')










if __name__ == "__main__":

    sqlUtil = SqlUtil()

    # exist_rows = sqlUtil.check_stock_select('603279', '20200104')
    # exist_count = exist_rows[0][0]
    #
    # print(exist_count)

    #sqlUtil.update_stock_history_ma('2020-01-07', '601318', '85.90599999999999', '85.26769230769233', '85.15809523809526', '45966234.0')

    # his_rows_all = sqlUtil.select_stock_history_by_code('603983')
    # print(his_rows_all)
    # print('his_rows_all_count = ' + str(len(his_rows_all)))



    # 批量规则匹配

    # stock_rows = sqlUtil.select_stock()
    # #print(stock_all)
    # stock_count = len(stock_rows)
    #
    # stock_codes_str = "("
    # stock_codes_array = []
    #
    # stock_index = 0
    #
    # for row in stock_rows:
    #     code = row[0]
    #     #print(code)
    #
    #     stock_codes_array.append(code)
    #
    #     stock_index = stock_index + 1
    #
    #     div_left = stock_index % 100;
    #
    #     print('stock_index = ' + str(stock_index) + ' div_left = ' + str(div_left))
    #
    #     stock_codes_str = stock_codes_str + "\'" + code + "\'" + ","
    #
    #     if div_left == 0:
    #
    #         stock_codes_str = stock_codes_str[0: len(stock_codes_str) - 1] + ")"
    #         print('stock_codes_str = ' + stock_codes_str)
    #
    #         print('当前时间：%s' % datetime.now())
    #
    #         his_rows_all = sqlUtil.select_stock_history_by_codes(stock_codes_str)
    #         print('his_rows_all_count = ' + str(len(his_rows_all)))
    #
    #
    #         print(his_rows_all)
    #
    #
    #
    #         stock_his_rows = []
    #
    #         for stock_code in stock_codes_array:
    #             #print('stock_code = ' + stock_code)
    #             stock_his_rows = []
    #             for his_row in his_rows_all:
    #                 his_stock_code = his_row[1]
    #                 #print('his_stock_code = ' + his_stock_code)
    #                 if stock_code == his_stock_code :
    #                     stock_his_rows.append(his_row)
    #
    #             print('stock_code = ' + stock_code)
    #             print('stock_his_rows_count = ' + str(len(stock_his_rows)))
    #             print(stock_his_rows)
    #
    #
    #
    #
    #         stock_codes_str = "("
    #         stock_codes_array = []

    stock_rows = sqlUtil.select_stock()
    stock_count = len(stock_rows)
    stock_historys_str = ""
    stock_index = 0

    for row in stock_rows:
        code = row[0]
        print(code)
        stock_index = stock_index + 1
        div_left = stock_index % 100;
        print('stock_index = ' + str(stock_index) + ' div_left = ' + str(div_left))

        data_path = '/Users/xianxiaoge/PycharmProjects/stock/data/today/'

        csv_name = data_path + row[0] + ".csv";

        try:

            print('处理文件：' + csv_name)

            with open(csv_name, 'r', encoding='gb2312') as f:
                reader = csv.reader(f)
                next(reader)

                for row in reader:
                    date = row[0]

                    matchObj = re.search(r'[0-9]+', row[1], re.M | re.I)

                    if matchObj:
                        code = matchObj.group()
                    else:
                        code = ''
                    name = row[2]
                    close = row[3]
                    high = row[4]
                    low = row[5]
                    open2 = row[6]
                    pre_close = row[7]
                    up_down_price = row[8]
                    up_down_range = row[9]
                    turn_over_rate = row[10]
                    bargain_volume = row[11]
                    bargain_amount = row[12]
                    total_market_value = row[13]
                    flow_market_value = row[14]
                    bargain_ticket_count = row[15]

                    stock_history_item =   "(" + "\'" + date + "\'" + "," + "\'" + code + "\'" + "," + "\'" + name + "\'" + "," + "\'" + close + "\'" + "," + "\'" + high + "\'" + "," + "\'" + low + "\'" + "," + "\'" + open2 + "\'" + "," + "\'" + pre_close + "\'" + "," + "\'" + up_down_price + "\'" + "," + "\'" + up_down_range + "\'" + "," + "\'" + turn_over_rate + "\'" + "," + "\'" + bargain_volume + "\'" + "," + "\'" + bargain_amount + "\'" + "," + "\'" + total_market_value + "\'" + "," + "\'" + flow_market_value + "\'" + "," + "\'" + bargain_ticket_count + "\'" + ")"


                    stock_historys_str = stock_historys_str + stock_history_item + ","

        except Exception as e:



            print('处理文件：' + csv_name + '出错')






        if div_left == 0:
            stock_historys_str = stock_historys_str[0: len(stock_historys_str) - 1]
            print('stock_historys_str = ' + stock_historys_str)

            sqlUtil.insert_stock_historys(stock_historys_str)

            stock_historys_str = ""

            #INSERT INTO stock_history(date, code, name, close, high, low,  open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count)  VALUES history_values)"




