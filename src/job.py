

from src.util.sqlutil import SqlUtil;
from src.api.netease import NetEase;
from src.util.ruleutil import RuleUtil
from src.util.emailutil import EmailUtil
from src.util.marketutil import MarketUtil


from datetime import datetime
import csv;
import re;


class Job(object):

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

        print('下载当天csv数据文件, 当前时间：%s'% datetime.now())

        for row in rows:
            code = row[0];
            netEase.get_today_data(code, data_path, today)



    def import_csv_data(self, data_path):

        print('将csv数据导入数据库，当前时间：%s' % datetime.now())

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

        rows = sqlUtil.select_stock()
        for row in rows:
            code = row[0]
            rows_all = sqlUtil.select_stock_history_by_code(code)

            # print('规则101：今天涨停，当前时间：%s' % datetime.now())
            # ruleUtil.rule_limitUp(rows_all, batch)
            #
            # print('规则102：30天累计涨幅超过百分之五十，当前时间：%s' % datetime.now())
            # ruleUtil.rule_50PercentUp(rows_all, batch)

            print('规则103：1周累计涨幅超过百分之二十，当前时间：%s' % datetime.now())
            ruleUtil.rule_20PercentUp(rows_all, batch)

            # print('规则1：连续3天放量增长，当前时间：%s' % datetime.now())
            # ruleUtil.rule_3DayUp(rows_all, batch)
            #
            # print('规则2：1吃9，当前时间：%s' % datetime.now())
            # ruleUtil.rule_1Win9(rows_all, batch)
            #
            # print('规则3：大阳线，当前时间：%s' % datetime.now())
            # ruleUtil.rule_dayangLine(rows_all, batch)
            #
            # print('规则4：超跌品种，当前时间：%s' % datetime.now())
            # ruleUtil.rule_bigDown(rows_all, batch)
            #
            # print('规则5：最近9天涨幅不超过百分之20，当前时间：%s' % datetime.now())
            # ruleUtil.rule_littleUp(rows_all, batch)

        emailUtil.send_email(batch)

if __name__ == "__main__":

    job = Job()

    data_path = '/Users/xianxiaoge/PycharmProjects/stock/data/today/'

    print('******开始任务******')

    today = '20200117'

    #job.get_today_data(data_path, today)
    #job.import_csv_data(data_path)
    #job.calculate_moving_average()
    job.rule_match(today)

