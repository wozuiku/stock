

from src.util.sqlutil import SqlUtil;

from datetime import datetime
import csv;
import re;
import requests;
import os

from src.api.netease import NetEase;
import json;
import time;








class NowData(object):

    #api
    def now_data_api(self, stock_codes):

        url = 'http://api.money.126.net/data/feed/'+stock_codes+'money.api'
        resp = requests.get(url)

        stock_data_source = resp.content

        start_len = len('_ntes_quote_callback(')
        total_len = len(resp.content)


        stock_data_source = stock_data_source[start_len : total_len - 2]



        return stock_data_source


    # 获取所有股票
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





    #stock_now新增记录
    def insert_stock_nows(self, now_values):

        sqlUtil = SqlUtil()

        sqlUtil.connect();
        cursor = sqlUtil.db.cursor();


        insert_sql = "INSERT INTO stock_now(time, code, name, price, high, low,  open, pre_close, bargain_volume, bargain_amount)  VALUES " + now_values + ""

        print('insert_stock_nows insert_sql = ' + insert_sql)

        try:

            cursor.execute(insert_sql);

            sqlUtil.db.commit();
            #print("insert_stock_history sucess");
        except Exception as e:
            # 发生错误时回滚
            sqlUtil.db.rollback();
            print("insert_stock_nows error：" + e);

        # 关闭数据库连接
            sqlUtil.db.close()





    def get_now_data(self):

        nowData = NowData();

        print('获取实时数据，当前时间：%s' % datetime.now())

        stock_rows = nowData.select_stock()
        stock_count = len(stock_rows)
        stock_codes_str = ""
        stock_index = 0

        for row in stock_rows:
            code = row[0]
            #print(code)

            prefix = code[0:1]
            stock_index = stock_index + 1
            div_left = stock_index % 100;
            #print('stock_index = ' + str(stock_index) + ' div_left = ' + str(div_left))

            if prefix == '0':
                code = '1' + code;

            if prefix == '3':
                code = '1' + code;

            if prefix == '6':
                code = '0' + code;


            stock_codes_str = stock_codes_str + code + ",";


            if div_left == 0:

                #print('stock_codes_str = ' + stock_codes_str)
                stock_data_source = nowData.now_data_api(stock_codes_str)
                print(stock_data_source)

                stock_data_dict = json.loads(stock_data_source)

                stock_now_items_str = ""

                for i in stock_data_dict:

                    try:
                        timeStr = stock_data_dict[i]['time']
                        code = stock_data_dict[i]['code']
                        name = stock_data_dict[i]['name']
                        # type = stock_data_dict[i]['type']
                        price = stock_data_dict[i]['price']

                        high = stock_data_dict[i]['high']
                        low = stock_data_dict[i]['low']
                        open = stock_data_dict[i]['open']
                        pre_close = stock_data_dict[i]['yestclose']

                        bargain_volume = stock_data_dict[i]['volume']
                        bargain_amount = stock_data_dict[i]['turnover']

                    except:
                        print(stock_data_dict[i])


                    # print('timeStr = ' + timeStr)
                    # print('code = ' + code)
                    # print('name = ' + name)
                    # print('price = ' + str(price))
                    # print('high = ' + str(high))
                    # print('low = ' + str(low))
                    # print('open = ' + str(open))
                    # print('pre_close = ' + str(pre_close))
                    #
                    # print('bargain_volume = ' + str(bargain_volume))
                    # print('bargain_amount = ' + str(bargain_amount))

                    timeStruct = time.strptime(timeStr, "%Y/%m/%d %H:%M:%S")
                    timeStr = time.strftime("%Y%m%d%H%M%S", timeStruct)
                    stock_now_item = "(" + "\'" + timeStr + "\'" + "," + "\'" + code + "\'" + "," + "\'" + name + "\'" + "," + "\'" + str(
                        price) + "\'" + "," + "\'" + str(high) + "\'" + "," + "\'" + str(low) + "\'" + "," + "\'" + str(
                        open) + "\'" + "," + "\'" + str(pre_close) + "\'" + "," + "\'" + str(
                        bargain_volume) + "\'" + "," + "\'" + str(bargain_amount) + "\'" + ")"

                    stock_now_items_str = stock_now_items_str + stock_now_item + ","

                stock_now_items_str = stock_now_items_str[0: len(stock_now_items_str) - 1]


                print(stock_now_items_str)

                nowData.insert_stock_nows(stock_now_items_str)

                stock_codes_str = ""







if __name__ == "__main__":

    nowData = NowData()

    data_path = '/Users/xianxiaoge/PycharmProjects/stock/data/now/'

    nowData.get_now_data()



