import threading
import time

exitFlag = 0

class jobThread (threading.Thread):
    def __init__(self, threadID, threadName):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName

    def run(self):
        print ("开始线程：" + self.threadName)

        for i in (1, 10):
            print(self.threadName + '---' + str(i));


        print ("退出线程：" + self.threadName)



# 创建新线程
thread1 = jobThread(1, "Thread-1")
thread2 = jobThread(2, "Thread-2")
thread3 = jobThread(3, "Thread-3")
thread4 = jobThread(4, "Thread-4")
thread5 = jobThread(5, "Thread-5")
thread6 = jobThread(6, "Thread-6")
thread7 = jobThread(7, "Thread-7")
thread8 = jobThread(8, "Thread-8")
thread9 = jobThread(9, "Thread-9")
thread10 = jobThread(10, "Thread-10")


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
print ("退出主线程")

