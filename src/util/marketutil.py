
from src.util.sqlutil import SqlUtil;

class MarketUtil(object):

    def cal_ma(self, rows_all):
        print('计算k线均线')



        rows_all_count = len(rows_all)
        rows_ma5 = rows_all[rows_all_count - 5 : rows_all_count]
        close_sum5 = 0
        ma5 = 0

        rows_ma13 = rows_all[rows_all_count - 13: rows_all_count]
        close_sum13 = 0
        ma13 = 0

        rows_ma21 = rows_all[rows_all_count - 21: rows_all_count]
        close_sum21 = 0
        ma21 = 0

        rows_vma50 = rows_all[rows_all_count - 50: rows_all_count]
        volume_sum50 = 0
        vma50 = 0


        for row in rows_ma5:
            print(row)
            close = float(row[3])
            close_sum5 = close_sum5 + close

        if len(rows_ma5) == 5:
            ma5 = close_sum5 / 5
        else:
            ma5 = ''

        for row in rows_ma13:
            print(row)
            close = float(row[3])
            close_sum13 = close_sum13 + close

        if len(rows_ma13) == 13:
            ma13 = close_sum13 / 13
        else:
            ma13 = ''

        for row in rows_ma21:
            print(row)
            close = float(row[3])
            close_sum21 = close_sum21 + close
        if len(rows_ma21) == 21:
            ma21 = close_sum21 / 21
        else:
            ma21 = ''

        for row in rows_vma50:
            print(row)
            volume = float(row[11])
            volume_sum50 = volume_sum50 + volume

        if len(rows_vma50) == 50:
            vma50 = volume_sum50 / 50
        else:
            vma50 = ''


        # print('ma5 = ' + str(ma5))
        # print('ma13 = ' + str(ma13))
        # print('ma21 = ' + str(ma21))
        # print('vma50 = ' + str(vma50))

        date = rows_all[rows_all_count - 1][0]
        code = rows_all[rows_all_count - 1][1]


        return ma5, ma13, ma21, vma50










if __name__ == "__main__":

    sqlUtil = SqlUtil()
    rows_all = sqlUtil.select_stock_history_by_code('601318')

    marketUtil = MarketUtil()
    ma5, ma13, ma21, vma50 = marketUtil.cal_ma(rows_all)

    print('ma5 = ' + str(ma5))
    print('ma13 = ' + str(ma13))
    print('ma21 = ' + str(ma21))
    print('vma50 = ' + str(vma50))


    #marketUtil.cal_vma(rows_all)
