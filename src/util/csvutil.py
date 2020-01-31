
import csv;
import re;

from src.util.sqlutil import SqlUtil;

if __name__ == "__main__":



    sqlUtil = SqlUtil()
    rows = sqlUtil.select_stock()

    for row in rows:
        csv_name = "data_1year/" + row[0] + ".csv";

        print(csv_name)

        with open(csv_name, 'r', encoding='gb2312') as f:
            reader = csv.reader(f)
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

                # print(date)
                # print(code)
                # print(name)
                # print(close)
                # print(high)
                # print(low)
                # print(pre_close)
                # print(up_down_price)
                # print(up_down_range)
                # print(turn_over_rate)
                # print(bargain_volume)
                # print(bargain_amount)
                # print(total_market_value)
                # print(flow_market_value)
                # print(bargain_ticket_count)

                sqlUtil.insert_stock_history_1year(date, code, name, close, high, low, open2, pre_close, up_down_price,
                                             up_down_range, turn_over_rate, bargain_volume, bargain_amount,
                                             total_market_value, flow_market_value, bargain_ticket_count)





