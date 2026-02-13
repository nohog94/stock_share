import win32com.client
import pymysql
import time
import pythoncom
from datetime import datetime, date
class Cybos:
    def test(self):
        pythoncom.CoInitialize()
        print('hi')
        # 연결 여부 체크
        objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
        bConnect = objCpCybos.IsConnect
        if (bConnect == 0):
            print("PLUS가 정상적으로 연결되지 않음. ")
            exit()

        today = date.today()
        day_of_week = today.weekday()
        # if day_of_week == 5 or day_of_week == 6:
        #     exit()

        connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                     autocommit=True)
        count = 1
        epoch = 0
        interval = 5
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM daily_price WHERE CODE IN (SELECT CODE FROM company_info where del_yn = 'Y' );")
            cursor.execute(f"SELECT code from company_info where del_yn = 'N';")

            for code_num in cursor:
                try:
                    code = code_num[0]
                    #code = 'A005930'
                    # 차트 객체 구하기
                    objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")

                    objStockChart.SetInputValue(0, code)  # 종목 코드 - 삼성전자
                    objStockChart.SetInputValue(1, ord('1'))  # 기간조회1/개수조회2
                    objStockChart.SetInputValue(2, '20240123')  # 요청종료일 최근
                    objStockChart.SetInputValue(3, '20240123')  # 요청시작일 이전
                    #objStockChart.SetInputValue(4, 6)  # 최근 100일 치 뒤에가 일
                    objStockChart.SetInputValue(5, [0, 1, 2, 3, 4, 5, 8])  # 날짜,시간,시가,고가,저가,종가,거래량
                    objStockChart.SetInputValue(6, ord('m'))  # '차트 주가 - 분간 차트 요청
                    objStockChart.SetInputValue(7, interval)  # 차트 주기
                    objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용
                    objStockChart.BlockRequest()

                    len = objStockChart.GetHeaderValue(3)

                    with connection.cursor() as cursor:
                        for i in range(len):
                            day = objStockChart.GetDataValue(0, i)
                            day_time = objStockChart.GetDataValue(1, i)
                            open = objStockChart.GetDataValue(2, i)
                            high = objStockChart.GetDataValue(3, i)
                            low = objStockChart.GetDataValue(4, i)
                            close = objStockChart.GetDataValue(5, i)
                            vol = objStockChart.GetDataValue(6, i)

                            trade_date = datetime.strptime(str(day) + str(day_time), '%Y%m%d%H%M')

                            cursor.execute(f"REPLACE INTO five_minute_price(code, trade_date, high, low, open, close, volume) VALUES('{code}','{trade_date}',{high},{low},{open},{close},{vol});")

                    count+=1
                    if count % 60 == 0:
                        tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                        print(f"[{tmnow}] epoch:#{epoch+1:04d} count:#{count:06d} date:{day}")
                        epoch+=1
                    #exit()

                except Exception as e:
                    print(code, e)



if __name__ == "__main__":
    cybos = Cybos()
    cybos.test()
