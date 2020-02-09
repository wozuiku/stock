import pymysql

from datetime import datetime
import csv;
import re;
import threading
from src.util.sqlutil import SqlUtil;
from src.api.netease import NetEase;



class ThreadUtil(object):

    def get_row_index(self, rowCount, threadCount, threadId):

        rowFrom = 0;
        rowTo = 0;

        targetCount = int(rowCount / threadCount);
        leftCount = rowCount % threadCount;

        rowFrom = (threadId - 1) * targetCount;
        rowTo = rowFrom + targetCount;

        if threadCount == threadId:
            rowTo = rowTo + leftCount;

        return rowFrom, rowTo;



    def get_today_data(self, thread_name, data_path, today, row_from, row_to):

        sqlUtil = SqlUtil()
        netEase = NetEase()
        rows = sqlUtil.select_stock()

        target_rows = rows[row_from : row_to];


        for row in target_rows:
            code = row[0];
            netEase.get_today_data_thread(thread_name, code, data_path, today)



class JobThread (threading.Thread):
    def __init__(self, threadID, threadName, data_path, today):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.data_path = data_path
        self.today = today

    def run(self):
        print ("开始线程：" + self.threadName)

        threadUtil = ThreadUtil()

        sqlUtil = SqlUtil()
        rows = sqlUtil.select_stock()
        rowCount = len(rows)
        threadCount = 10;
        rowFrom, rowTo = threadUtil.get_row_index(rowCount, threadCount, self.threadID)

        print(self.threadName + ' rowFrom = ' + str(rowFrom))
        print(self.threadName + ' rowTo = ' + str(rowTo))

        threadUtil.get_today_data(self.threadName, self.data_path, self.today, rowFrom, rowTo)


        print ("退出线程：" + self.threadName)


if __name__ == "__main__":

    data_path = '/Users/xianxiaoge/PycharmProjects/stock/data/today/'
    today = '20200207'

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


    print('结束时间：%s' % datetime.now())

