

from src.util.sqlutil import SqlUtil;
from src.api.netease import NetEase;
from src.util.ruleutil import RuleUtil
from src.util.emailutil import EmailUtil
from src.util.marketutil import MarketUtil

from src.util.threadutil import JobThread



from datetime import datetime
import csv;
import re;
import threading;
import json;
import time;



class Job(object):

    def get_now_data(self):

        netEase = NetEase();
        sqlUtil = SqlUtil()

        print('获取实时数据，当前时间：%s' % datetime.now())

        stock_rows = sqlUtil.select_stock()
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
                stock_data_source = netEase.get_now_data(stock_codes_str)
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

                print('*********')
                print('*********')
                print('*********')
                print('*********')
                print('*********')
                print('*********')
                print(stock_now_items_str)

                sqlUtil.insert_stock_nows(stock_now_items_str)

                stock_codes_str = ""




    def get_history_data(self):
        sqlUtil = SqlUtil()
        netEase = NetEase()

        print('开始任务')
        print('step1：下载csv数据文件，当前时间：%s'% datetime.now())

        rows = sqlUtil.select_stock()
        for row in rows:
            code = row[0];
            netEase.get_his_data_1year(code)

        print('step2：将csv数据导入数据库，当前时间：%s' % datetime.now())

        for row in rows:
            csv_name = "../data/1year/" + row[0] + ".csv";

            print('处理文件：' + csv_name)

            with open(csv_name, 'r', encoding='gb2312') as f:
                reader = csv.reader(f)
                next(reader)

                for row in reader:
                    date = row[0]

                    matchObj = re.search(r'[0-9]+',row[1], re.M | re.I)

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


                    sqlUtil.insert_stock_history(date, code, name, close, high, low, open2, pre_close, up_down_price,
                                                 up_down_range, turn_over_rate, bargain_volume, bargain_amount,
                                                 total_market_value, flow_market_value, bargain_ticket_count)


    def get_today_data(self, data_path, today):

        sqlUtil = SqlUtil()
        netEase = NetEase()
        rows = sqlUtil.select_stock()

        print('下载当天csv数据文件, 当前时间：%s' % datetime.now())

        for row in rows:
            code = row[0];
            netEase.get_today_data(code, data_path, today)


    def get_today_data_thread(self, data_path, today):


        # 创建新线程
        thread1 = JobThread(1, "Thread-1", data_path, today)
        thread2 = JobThread(2, "Thread-2", data_path, today)
        thread3 = JobThread(3, "Thread-3", data_path, today)
        thread4 = JobThread(4, "Thread-4", data_path, today)
        thread5 = JobThread(5, "Thread-5", data_path, today)
        thread6 = JobThread(6, "Thread-6", data_path, today)
        thread7 = JobThread(7, "Thread-7", data_path, today)
        thread8 = JobThread(8, "Thread-8", data_path, today)
        thread9 = JobThread(9, "Thread-9", data_path, today)
        thread10 = JobThread(10, "Thread-10", data_path, today)

        # 开启新线程
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()
        thread5.start()
        thread6.start()
        thread7.start()
        thread8.start()
        thread9.start()
        thread10.start()

        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
        thread5.join()
        thread6.join()
        thread7.join()
        thread8.join()
        thread9.join()
        thread10.join()
        print("退出主线程")



    def import_csv_data(self, data_path):

        print('将csv数据导入数据库，当前时间：%s' % datetime.now())


        sqlUtil = SqlUtil()

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

                sqlUtil.insert_stock_historys(stock_historys_str)

                stock_historys_str = ""



    def calculate_moving_average(self):
        sqlUtil = SqlUtil()
        marketUtil = MarketUtil()

        rows = sqlUtil.select_stock()
        for row in rows:
            code = row[0]
            rows_all = sqlUtil.select_stock_history_by_code(code)
            rows_all_count = len(rows_all)

            if rows_all_count >= 5:
                date = rows_all[rows_all_count - 1][0]
                code = rows_all[rows_all_count - 1][1]
                ma5, ma13, ma21, vma50 = marketUtil.cal_ma(rows_all)
                print('date = ' + date)
                print('code = ' + code)
                print('ma5 = ' + str(ma5))
                print('ma13 = ' + str(ma13))
                print('ma21 = ' + str(ma21))
                print('vma50 = ' + str(vma50))

                sqlUtil.update_stock_history_ma(date, code, str(ma5), str(ma13), str(ma21), str(vma50))






    def rule_match(self, batch):

        print('开始规则匹配，当前时间：%s' % datetime.now())

        sqlUtil = SqlUtil()
        ruleUtil = RuleUtil()
        emailUtil = EmailUtil()
        marketUtil = MarketUtil()


        stock_rows = sqlUtil.select_stock()
        stock_count = len(stock_rows)
        stock_codes_str = "("
        stock_codes_array = []
        stock_index = 0

        for row in stock_rows:
            code = row[0]
            # print(code)

            stock_codes_array.append(code)

            stock_index = stock_index + 1

            div_left = stock_index % 100;

            print('stock_index = ' + str(stock_index) + ' div_left = ' + str(div_left))

            stock_codes_str = stock_codes_str + "\'" + code + "\'" + ","

            if div_left == 0:

                stock_codes_str = stock_codes_str[0: len(stock_codes_str) - 1] + ")"


                his_rows_all = sqlUtil.select_stock_history_by_codes(stock_codes_str)




                for stock_code in stock_codes_array:

                    stock_his_rows = []
                    for his_row in his_rows_all:
                        his_stock_code = his_row[1]
                        # print('his_stock_code = ' + his_stock_code)
                        if stock_code == his_stock_code:
                            stock_his_rows.append(his_row)



                    print('规则1：超跌品种，当前时间：%s' % datetime.now())
                    ruleUtil.rule_bigDown(stock_his_rows, batch)

                    print('规则2：2天累计涨幅超过百分之10，当前时间：%s' % datetime.now())
                    ruleUtil.rule_2Day10Up(stock_his_rows, batch)

                    print('规则3：3天累计涨幅超过百分之15，当前时间：%s' % datetime.now())
                    ruleUtil.rule_3Day15Up(stock_his_rows, batch)

                    print('规则4：5天累计涨幅超过百分之30，当前时间：%s' % datetime.now())
                    ruleUtil.rule_5Day30Up(stock_his_rows, batch)





                stock_codes_str = "("
                stock_codes_array = []




if __name__ == "__main__":

    job = Job()

    print('******开始任务******')

    job_type = 2 # 获取实时数据


    #获取实时数据
    if job_type == 1:
        job.get_now_data()

    # 获取当天数据
    if job_type == 2:

        data_path = '/Users/xianxiaoge/PycharmProjects/stock/data/today/'
        today = '20200415'
        emailUtil = EmailUtil()

        job.get_today_data(data_path, today)

        #job.get_today_data_thread(data_path, today)

        job.import_csv_data(data_path)
        #job.calculate_moving_average()

        job.rule_match(today)

        emailUtil.send_email(today)

        print('结束时间：%s' % datetime.now())

