import sys
from pathlib import Path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

import win32com.client
import pymysql
import time
import pythoncom
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy connectable.*')
from datetime import datetime, date
from trade_user.index_exists_check import check_daily_index_exists

class Cybos:
    def stock_price_update(self):
        pythoncom.CoInitialize()
        print('stock price update')
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
        count = 1
        epoch = 0
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM daily_price WHERE CODE IN (SELECT CODE FROM company_info where del_yn = 'Y' );")
            cursor.execute(f"SELECT code from company_info where del_yn = 'N';")
            code_list = []
            for code_num in cursor:
                code_list.append(code_num[0])

            for code in code_list:
                try:
                    # 차트 객체 구하기
                    objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")

                    objStockChart.SetInputValue(0, code)  # 종목 코드 - 삼성전자
                    objStockChart.SetInputValue(1, ord('2'))  # 기간조회1/개수조회2
                    # objStockChart.SetInputValue(2, '20250902')  # 요청종료일 최근
                    # objStockChart.SetInputValue(3, '20250827')  # 요청시작일 이전
                    objStockChart.SetInputValue(4, 1)  # 최근 100일 치 뒤에가 일
                    objStockChart.SetInputValue(5, [0, 2, 3, 4, 5, 8])  # 날짜,시가,고가,저가,종가,거래량
                    objStockChart.SetInputValue(6, ord('D'))  # '차트 주가 - 일간 차트 요청
                    objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용
                    objStockChart.BlockRequest()

                    len = objStockChart.GetHeaderValue(3)

                    with connection.cursor() as cursor:
                        for i in range(len):
                            day = objStockChart.GetDataValue(0, i)
                            open = objStockChart.GetDataValue(1, i)
                            high = objStockChart.GetDataValue(2, i)
                            low = objStockChart.GetDataValue(3, i)
                            close = objStockChart.GetDataValue(4, i)
                            vol = objStockChart.GetDataValue(5, i)
                            cursor.execute(f"REPLACE INTO daily_price(code, trade_date, high, low, open, close, volume) VALUES('{code}','{day}',{high},{low},{open},{close},{vol});")
                    
                    # Update Index Data
                    today_str = datetime.today().strftime('%Y-%m-%d')
                    check_daily_index_exists(today_str, today_str, code, [1, 46, 97, 113])
                    
                    count+=1
                    if count % 60 == 0:
                        tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                        print(f"[{tmnow}] price update epoch:#{epoch+1:04d} count:#{count:06d} date:{day}")
                        epoch+=1
                        time.sleep(15)
                except Exception as e:
                    print(code, e)
                    # cursor.execute(f"UPDATE company_info set del_yn='Y'' where code = 'A{code}'';")

        with connection.cursor() as cursor:
            for code in code_list:
                try:
                    count = 1
                    epoch = 0
                    objRq = win32com.client.Dispatch("CpSysDib.MarketEye")
                    g_objCodeMgr = win32com.client.Dispatch('CpUtil.CpCodeMgr')
                    rqField = [0, 4, 20]
                    objRq.SetInputValue(0, rqField)  # 요청 필드
                    objRq.SetInputValue(1, code)  # 종목코드 or 종목코드 리스트
                    objRq.BlockRequest()
                    cnt = objRq.GetHeaderValue(2)
                    for i in range(cnt):
                        code = objRq.GetDataValue(0, i)  # 코드
                        cur = objRq.GetDataValue(1, i)  # 종가
                        listedStock = objRq.GetDataValue(2, i)  # 상장주식수
                        maketAmt = listedStock * cur
                        if g_objCodeMgr.IsBigListingStock(code):
                            maketAmt *= 1000

                    cursor.execute(f"update company_info set market_cap = {maketAmt} where code = '{code}';")

                    count += 1
                    if count % 60 == 0:
                        tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                        print(f"[{tmnow}] market cap epoch:#{epoch + 1:04d} count:#{count:06d}")
                        epoch += 1
                        time.sleep(15)

                except Exception as e:
                    print(code, e)

    def etf_price_update(self):
        pythoncom.CoInitialize()
        print('etf price update')
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
        count = 1
        epoch = 0
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM etf_daily_price WHERE CODE IN (SELECT CODE FROM etf_info where del_yn = 'Y' );")
            cursor.execute(f"SELECT code from etf_info where del_yn = 'N';")
            code_list = []
            for code_num in cursor:
                code_list.append(code_num[0])

            for code in code_list:
                try:
                    # 차트 객체 구하기
                    objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")

                    objStockChart.SetInputValue(0, code)  # 종목 코드 - 삼성전자
                    objStockChart.SetInputValue(1, ord('1'))  # 기간조회1/개수조회2
                    objStockChart.SetInputValue(2, '20250312')  # 요청종료일 최근
                    objStockChart.SetInputValue(3, '20250312')  # 요청시작일 이전
                    objStockChart.SetInputValue(4, 1)  # 최근 100일 치 뒤에가 일
                    objStockChart.SetInputValue(5, [0, 2, 3, 4, 5, 8])  # 날짜,시가,고가,저가,종가,거래량
                    objStockChart.SetInputValue(6, ord('D'))  # '차트 주가 - 일간 차트 요청
                    objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용
                    objStockChart.BlockRequest()

                    len = objStockChart.GetHeaderValue(3)

                    with connection.cursor() as cursor:
                        for i in range(len):
                            day = objStockChart.GetDataValue(0, i)
                            open = objStockChart.GetDataValue(1, i)
                            high = objStockChart.GetDataValue(2, i)
                            low = objStockChart.GetDataValue(3, i)
                            close = objStockChart.GetDataValue(4, i)
                            vol = objStockChart.GetDataValue(5, i)
                            cursor.execute(f"REPLACE INTO etf_daily_price(code, trade_date, high, low, open, close, volume) VALUES('{code}','{day}',{high},{low},{open},{close},{vol});")
                    count+=1
                    if count % 60 == 0:
                        tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                        print(f"[{tmnow}] price update epoch:#{epoch+1:04d} count:#{count:06d} date:{day}")
                        epoch+=1
                        time.sleep(15)
                except:
                    print(code)
                    # cursor.execute(f"UPDATE company_info set del_yn='Y'' where code = 'A{code}'';")

        with connection.cursor() as cursor:
            for code in code_list:
                try:
                    count = 1
                    epoch = 0
                    objRq = win32com.client.Dispatch("CpSysDib.MarketEye")
                    g_objCodeMgr = win32com.client.Dispatch('CpUtil.CpCodeMgr')
                    rqField = [0, 4, 20]
                    objRq.SetInputValue(0, rqField)  # 요청 필드
                    objRq.SetInputValue(1, code)  # 종목코드 or 종목코드 리스트
                    objRq.BlockRequest()
                    cnt = objRq.GetHeaderValue(2)
                    for i in range(cnt):
                        code = objRq.GetDataValue(0, i)  # 코드
                        cur = objRq.GetDataValue(1, i)  # 종가
                        cursor.execute(f"select cu_amount from etf_info where code = '{code}';")
                        cu_amount = cursor.fetchone()[0]
                        maketAmt = cu_amount * cur
                    cursor.execute(f"update etf_info set price = {cur} where code = '{code}';")
                    cursor.execute(f"update etf_info set market_cap = {maketAmt} where code = '{code}';")


                    count += 1
                    if count % 60 == 0:
                        tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                        print(f"[{tmnow}] market cap epoch:#{epoch + 1:04d} count:#{count:06d}")
                        epoch += 1
                        time.sleep(15)

                except Exception as e:
                    print(code, e)


if __name__ == "__main__":
    cybos = Cybos()
    cybos.stock_price_update()
    # cybos.etf_price_update()
