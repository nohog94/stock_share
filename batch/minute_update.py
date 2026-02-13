import sys
from pathlib import Path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

import win32com.client
import pymysql
import time
import pythoncom
from datetime import datetime, date, timedelta
import pandas as pd
from sqlalchemy import create_engine
from trade_user.index_exists_check import check_five_minute_index_exists

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy connectable.*')

class Cybos:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def stock_minute_price_update(self):
        pythoncom.CoInitialize()
        print('stock minute price update')
        # 연결 여부 체크
        objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
        bConnect = objCpCybos.IsConnect
        if (bConnect == 0):
            print("PLUS가 정상적으로 연결되지 않음. ")
            exit()

        today = date.today()
        day_of_week = today.weekday()
        if day_of_week == 5 or day_of_week == 6:
            exit()

        connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                     autocommit=True)
        engine = create_engine(f"mysql+pymysql://root:P@ssw0rd@127.0.0.1:3306/investar")

        count = 1
        epoch = 0
        interval = 5
        with connection.cursor() as cursor:
            today = date.today()
            formatted_date1 = today.strftime('%Y-%m-%d')
            formatted_date2 = today.strftime('%Y%m%d')
            check_partition_query = f"""
                        SELECT COUNT(*)
                        FROM information_schema.partitions
                        WHERE table_schema = 'investar'
                          AND table_name = 'five_minute_price'
                          AND partition_name = 'p{formatted_date2}';
                        """
            cursor.execute(check_partition_query)
            partition_exists = cursor.fetchone()[0]

            if not partition_exists:
                sql = f"ALTER TABLE five_minute_price ADD PARTITION (PARTITION p{formatted_date2} VALUES LESS THAN (TO_DAYS('{formatted_date1}')+1));"
                cursor.execute(sql)
            else:
                print('partition table exists')
            check_index_test_partition_query = f"""
                                    SELECT COUNT(*)
                                    FROM information_schema.partitions
                                    WHERE table_schema = 'investar'
                                      AND table_name = 'five_minute_index_test'
                                      AND partition_name = 'p{formatted_date2}';
                                    """
            cursor.execute(check_index_test_partition_query)
            index_test_partition_exists = cursor.fetchone()[0]

            if not index_test_partition_exists:
                sql = f"ALTER TABLE five_minute_index_test ADD PARTITION (PARTITION p{formatted_date2} VALUES LESS THAN (TO_DAYS('{formatted_date1}')+1));"
                cursor.execute(sql)
            else:
                print('partition table exists')

        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM five_minute_price WHERE CODE IN (SELECT CODE FROM company_info where del_yn = 'Y' );")
            cursor.execute(f"DELETE FROM five_minute_index_test WHERE CODE IN (SELECT CODE FROM company_info where del_yn = 'Y' );")
            cursor.execute(f"SELECT code from company_info where del_yn = 'N';")
            result = cursor.fetchall()

        sql = f"SELECT distinct trade_date FROM daily_price where trade_date BETWEEN '{self.start_date}' AND '{self.end_date}';"
        df_date = pd.read_sql(sql, engine)
        df_date['trade_date'] = df_date['trade_date'].apply(lambda x: x.strftime('%Y%m%d'))


        df = pd.DataFrame(result, columns=['code'])
        for code in df["code"]:
            # try:
                # 차트 객체 구하기
                objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")
                objStockChart.SetInputValue(0, code)  # 종목 코드 - 삼성전자
                objStockChart.SetInputValue(1, ord('1'))  # 기간조회1/개수조회2
                objStockChart.SetInputValue(2, self.end_date)  # 요청종료일 최근
                objStockChart.SetInputValue(3, self.start_date)  # 요청시작일 이전
                # objStockChart.SetInputValue(4, 77)  # 최근 100일 치 뒤에가 일 # 77
                objStockChart.SetInputValue(5, [0,1, 2, 3, 4, 5, 8])  # 날짜,시간,시가,고가,저가,종가,거래량
                objStockChart.SetInputValue(6, ord('m'))  # '차트 주가 - 분간 차트 요청
                objStockChart.SetInputValue(7, interval)  # 차트 주기
                objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용
                objStockChart.BlockRequest()

                request_len = objStockChart.GetHeaderValue(3)
                day_pre = df_date['trade_date'].iloc[-1]
                day_time_pre = 1530
                with connection.cursor() as cursor:
                    for i in range(request_len):
                        day = objStockChart.GetDataValue(0, i)
                        day_time = objStockChart.GetDataValue(1, i)
                        open = objStockChart.GetDataValue(2, i)
                        high = objStockChart.GetDataValue(3, i)
                        low = objStockChart.GetDataValue(4, i)
                        close = objStockChart.GetDataValue(5, i)
                        vol = objStockChart.GetDataValue(6, i)
                        if self.start_date > str(day):
                            break
                        if i == request_len-1:
                            day = df_date['trade_date'].iloc[0]
                            day_time = 905
                        if (day_time == 1530 and day_pre == str(day)) or \
                                (day_pre == str(day) and (datetime.strptime(f"{day_time_pre:04d}", "%H%M")-timedelta(minutes=5)).strftime("%H%M") == str(f"{day_time:04d}")):
                            day_pre = str(day)
                            if day_time == 1530:
                                day_time_pre = 1525
                            else:
                                day_time_pre = day_time
                            trade_date = datetime.strptime(str(day) + str(day_time), '%Y%m%d%H%M')
                            cursor.execute(f"REPLACE INTO five_minute_price(code, trade_date, high, low, open, close, volume) VALUES('{code}','{trade_date}',{high},{low},{open},{close},{vol});")
                        elif (df_date['trade_date'].shift(1)[df_date['trade_date'] == day_pre].iloc[0] == str(day)) and \
                                (day_time_pre == 905 and day_time == 1530):
                            day_pre = str(day)
                            day_time_pre = 1525
                            trade_date = datetime.strptime(str(day) + str(day_time), '%Y%m%d%H%M')
                            cursor.execute(f"REPLACE INTO five_minute_price(code, trade_date, high, low, open, close, volume) VALUES('{code}','{trade_date}',{high},{low},{open},{close},{vol});")
                        else:
                            s_day = str(day)
                            s_day_time = day_time
                            while s_day != day_pre or s_day_time != day_time_pre:
                                if self.start_date > day_pre:
                                    break
                                if day_time_pre == 905:
                                    day = df_date['trade_date'].shift(1)[df_date['trade_date'] == day_pre].iloc[0]
                                    day_time = 1530
                                elif day_time_pre == 1530:
                                    day = day_pre
                                    day_time = 1520
                                else:
                                    day = day_pre
                                    day_time = datetime.strptime(f"{day_time_pre:04d}", "%H%M") - timedelta(minutes=5)
                                    day_time = int(day_time.strftime("%H%M"))
                                trade_date = datetime.strptime(str(day) + str(day_time), '%Y%m%d%H%M')
                                day_pre = str(day)
                                day_time_pre = day_time
                                cursor.execute(f"REPLACE INTO five_minute_price(code, trade_date, high, low, open, close, volume) VALUES('{code}','{trade_date}',{high},{low},{open},{close},{vol});")

                # Update Minute Index Data
                today_str = datetime.today().strftime('%Y-%m-%d')
                check_five_minute_index_exists(today_str, today_str, code, [97])

                count+=1
                if count % 60 == 0:
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] epoch:#{epoch+1:04d} count:#{count:06d} date:{day}")
                    epoch+=1
                    time.sleep(15)
            # except Exception as e:
            #     print(code, e)

if __name__ == "__main__":
    # cybos = Cybos('20250326', '20250326')
    cybos = Cybos(datetime.now().strftime("%Y%m%d"), datetime.now().strftime("%Y%m%d"))
    # cybos = Cybos(datetime.now().strftime("20250827"), datetime.now().strftime("20250902"))
    cybos.stock_minute_price_update()
