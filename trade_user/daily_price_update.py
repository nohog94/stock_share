import win32com.client
import pymysql
import pythoncom
from datetime import datetime
import time


def etf_price_update():
    pythoncom.CoInitialize()
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    # 연결 여부 체크
    objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")

    bConnect = objCpCybos.IsConnect
    if (bConnect == 0):
        print("PLUS가 정상적으로 연결되지 않음. ")
        exit()
    with connection.cursor() as cursor:
        cursor.execute(f"DELETE FROM daily_price WHERE CODE IN (SELECT CODE FROM etf_info where del_yn = 'Y' );")
        cursor.execute(f"SELECT code from etf_info where del_yn = 'N';")
        code_list = []
        for code_num in cursor:
            code_list.append(code_num[0])
        for code in code_list:
            try:
                count = 1
                epoch = 0
                objStockChart = win32com.client.dynamic.Dispatch("CpSysDib.StockChart")
                objStockChart.SetInputValue(0, code)  # 종목 코드 - 삼성전자
                objStockChart.SetInputValue(1, ord('2'))  # 기간조회1/개수조회2
                objStockChart.SetInputValue(4, 1)  # 최근 100일 치
                objStockChart.SetInputValue(5, [0, 2, 3, 4, 5, 8])  # 날짜,시가,고가,저가,종가,거래량
                objStockChart.SetInputValue(6, ord('D'))  # '차트 주가 - 일간 차트 요청
                objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용
                objStockChart.BlockRequest()

                len = objStockChart.GetHeaderValue(3)

                for i in range(len):
                    close = objStockChart.GetDataValue(4, i)
                    cursor.execute(f"UPDATE etf_info SET price = '{close}' WHERE code = '{code}';")
                count += 1
                if count % 60 == 0:
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] market cap epoch:#{epoch + 1:04d} count:#{count:06d}")
                    epoch += 1
                    time.sleep(15)
            except:
                print(code)


if __name__ == "__main__":
    etf_price_update()
