import pymysql


class SqlUtil(object):

    def __init__(self):
        self.db = None

    def connect(self):

        self.db = pymysql.connect("localhost", "root", "zzx363711", "stock");




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


    def select_stock_select(self, batch):

        self.connect();

        cursor = self.db.cursor();

        select_sql = "SELECT s.industry, s.code, s.name, s.sector, s.url, ss.remark FROM stock s, stock_select ss WHERE s.code = ss.code AND ss.batch = \'" + batch + "\' AND s.sector <> \'科创板\' ORDER BY s.industry, s.code";

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





if __name__ == "__main__":

    sqlUtil = SqlUtil()

    # exist_rows = sqlUtil.check_stock_select('603279', '20200104')
    # exist_count = exist_rows[0][0]
    #
    # print(exist_count)

    sqlUtil.update_stock_history_ma('2020-01-07', '601318', '85.90599999999999', '85.26769230769233', '85.15809523809526', '45966234.0')
