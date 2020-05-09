

from src.util.sqlutil import SqlUtil;

from datetime import datetime
import csv;
import re;
import requests;
import os





class HistoryData(object):


    #csv接口api
    def his_data_api(self, stock_code, date_start, date_end):

        url = 'http://quotes.money.163.com/service/chddata.html'

        if stock_code >= '600000':
            url = url + "?code=0" + stock_code + "&start=" + date_start + "&end=" + date_end;


        else:
            url = url + "?code=1" + stock_code + "&start=" + date_start + "&end=" + date_end;


        resp = requests.get(url)

        with open("../../../../data/history/" + stock_code + ".csv", "wb") as code:
            code.write(resp.content)

        print("url:"+url)

        print("file :"+"data/history/" + stock_code + ".csv")



    #获取所有股票
    def select_stock(self):

        sqlUtil = SqlUtil()

        sqlUtil.connect();

        cursor = sqlUtil.db.cursor();

        select_sql = 'SELECT code, name, industry  FROM stock '#002752、300208

        rows = [];

        try:
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        except:
            print("Error: unable to fetch data")

        sqlUtil.db.close()



        return rows



    #stock_history新增记录
    def insert_stock_historys(self, history_values):

        sqlUtil = SqlUtil()

        sqlUtil.connect();
        cursor = sqlUtil.db.cursor();

        insert_sql = "INSERT INTO stock_history(date, code, name, close, high, low,  open, pre_close, up_down_price, up_down_range, turn_over_rate, bargain_volume, bargain_amount, total_market_value, flow_market_value, bargain_ticket_count)  VALUES " + history_values + ""

        print('insert_stock_historys insert_sql = ' + insert_sql)

        try:

            cursor.execute(insert_sql);

            sqlUtil.db.commit();
            # print("insert_stock_history sucess");
        except Exception as e:
            # 发生错误时回滚
            sqlUtil.db.rollback();
            print("insert_stock_history error：" + e);

        # 关闭数据库连接
        sqlUtil.db.close()



    #下载csv数据
    def download_csv_data(self, date_start, date_end):
        historyData = HistoryData()

        rows = historyData.select_stock()
        for row in rows:
            code = row[0];
            historyData.his_data_api(code, date_start, date_end)






    #将csv数据导入数据库
    def import_csv_data(self, data_path):

        historyData = HistoryData()



        stock_rows = historyData.select_stock()
        stock_count = len(stock_rows)
        stock_historys_str = ""
        stock_index = 0

        for row in stock_rows:
            code = row[0]
            print(code)
            stock_index = stock_index + 1
            div_left = stock_index % 100;
            print('stock_index = ' + str(stock_index) + ' div_left = ' + str(div_left))


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

                        stock_history_item = "(" + "\'" + date + "\'" + "," + "\'" + code + "\'" + "," + "\'" + name + "\'" + "," + "\'" + close + "\'" + "," + "\'" + high + "\'" + "," + "\'" + low + "\'" + "," + "\'" + open2 + "\'" + "," + "\'" + pre_close + "\'" + "," + "\'" + up_down_price + "\'" + "," + "\'" + up_down_range + "\'" + "," + "\'" + turn_over_rate + "\'" + "," + "\'" + bargain_volume + "\'" + "," + "\'" + bargain_amount + "\'" + "," + "\'" + total_market_value + "\'" + "," + "\'" + flow_market_value + "\'" + "," + "\'" + bargain_ticket_count + "\'" + ")"

                        stock_historys_str = stock_historys_str + stock_history_item + ","

            except Exception as e:

                print('处理文件：' + csv_name + '出错')

            if div_left == 0:
                stock_historys_str = stock_historys_str[0: len(stock_historys_str) - 1]
                print('stock_historys_str = ' + stock_historys_str)

                historyData.insert_stock_historys(stock_historys_str)

                stock_historys_str = ""



    #删除csv数据
    def delete_csv_data(self, data_path):

        files = os.listdir(data_path)

        for file in files:
            file_path = os.path.join(data_path, file)

            print('删除文件：' + file_path)

            os.remove(file_path)





if __name__ == "__main__":

    historyData = HistoryData()
    date_start = '20200506'
    date_end = '20200508'
    data_path = '/Users/xianxiaoge/PycharmProjects/stock/data/history/'

    historyData.download_csv_data(date_start, date_end)
    historyData.import_csv_data(data_path)
    historyData.delete_csv_data(data_path)





