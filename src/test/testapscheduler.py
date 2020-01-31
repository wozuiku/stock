
from datetime import datetime
import os
import time
from apscheduler.schedulers.blocking import BlockingScheduler


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == "__main__":

    scheduler = BlockingScheduler() #新建scheduler调度器

    #scheduler.add_job(tick, 'interval', seconds=3) #向调度器添加一个job调度任务
    scheduler.add_job(tick, 'cron', day_of_week='mon-fri', hour=11, minute=30, end_date='2020-03-30')

    scheduler.start() #运行job调度任务

    # try:
    #     scheduler.start()
    # except(KeyboardInterrupt, SystemExit):
    #     pass









# if __name__ == '__main__':
#     scheduler = BlockingScheduler()
#     scheduler.add_job(tick, 'interval', seconds=3)
#     print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
#     try:
#         scheduler.start()
#     except (KeyboardInterrupt, SystemExit):
#         pass

