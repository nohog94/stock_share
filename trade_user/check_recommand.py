import pymysql
import time
from datetime import datetime, timedelta
import multiprocessing


def check_recommand(test_date=None, queue=None):
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    if test_date is None:
        test_date = datetime.now()
    else:
        test_date = datetime.strptime(test_date, '%Y-%m-%d').date()
    test_date = test_date.strftime('%Y-%m-%d')
    cursor = connection.cursor()
    cursor.execute(f"SELECT code, action from daily_order where trade_date = '{test_date}';")
    for row in cursor:
        code, action = row
        queue.put([code, action])
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    return

if __name__ == '__main__':
    start_time = time.time()
    queue = multiprocessing.Queue()
    check_recommand(queue)
    end_time = time.time()
    elapsed_time = end_time - start_time  # 실행 시간 계산
    print(f"실행 시간: {elapsed_time} 초")