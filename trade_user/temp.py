import sys
import win32com.client
import ctypes
import pandas as pd
import pymysql
from datetime import datetime, timedelta
import requests
from sqlalchemy import create_engine


################################################
# PLUS 공통 OBJECT
g_objCodeMgr = win32com.client.Dispatch('CpUtil.CpCodeMgr')
g_objCpStatus = win32com.client.Dispatch('CpUtil.CpCybos')
g_objCpTrade = win32com.client.Dispatch('CpTrade.CpTdUtil')


def balance():
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
    bConnect = objCpCybos.IsConnect
    if (bConnect == 0):
        print("PLUS가 정상적으로 연결되지 않음. ")
        return False
    objTrade = win32com.client.Dispatch("CpTrade.CpTdUtil")
    initCheck = objTrade.TradeInit(0)
    if (initCheck != 0):
        print("주문 초기화 실패")
        exit()

    acc = objTrade.AccountNumber[0]  # 계좌번호
    accFlag = objTrade.GoodsList(acc, 1)  # 주식상품 구분
    objRq = win32com.client.Dispatch("CpTrade.CpTd6033")
    objRq.SetInputValue(0, acc)  # 계좌번호
    objRq.SetInputValue(1, accFlag[0])  # 상품구분 - 주식 상품 중 첫번째
    objRq.SetInputValue(2, 50)  # 요청 건수(최대 50)
    objRq.BlockRequest()
    cnt = objRq.GetHeaderValue(7)
    sql = f"UPDATE stock_hold SET STK_CNT = 0;"
    cursor.execute(sql)
    for i in range(cnt):
        code = objRq.GetDataValue(12, i)
        stk_cnt = objRq.GetDataValue(15, i)
        sql = f"UPDATE stock_hold SET STK_CNT = '{stk_cnt}' WHERE CODE = '{code}';"
        cursor.execute(sql)
        print(i, code, stk_cnt)
    engine = create_engine(f"mysql+pymysql://root:P@ssw0rd@127.0.0.1:3306/investar")


    sql_stock_hold_total = f"SELECT avg_price, stk_cnt FROM stock_hold where stk_cnt > 0;"
    df_stock_hold_total = pd.read_sql(sql_stock_hold_total, engine)
    print(df_stock_hold_total.shape[0])
    cursor.close()
    connection.close()

def sell():
    objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
    bConnect = objCpCybos.IsConnect
    if (bConnect == 0):
        print("PLUS가 정상적으로 연결되지 않음. ")
        return False
    objTrade = win32com.client.Dispatch("CpTrade.CpTdUtil")
    initCheck = objTrade.TradeInit(0)
    if (initCheck != 0):
        print("주문 초기화 실패")
        exit()

    acc = objTrade.AccountNumber[0]  # 계좌번호
    accFlag = objTrade.GoodsList(acc, 1)  # 주식상품 구분
    objStockOrder = win32com.client.Dispatch("CpTrade.CpTd0311")
    objStockOrder.SetInputValue(0, "1")  # 1: 매도
    objStockOrder.SetInputValue(1, acc)  # 계좌번호
    objStockOrder.SetInputValue(2, accFlag[0])  # 상품구분 - 주식 상품 중 첫번째
    objStockOrder.SetInputValue(3, 'A005930')  # 종목코드 - A003540 - 대신증권 종목
    objStockOrder.SetInputValue(4, 1)  # 매수수량 10주
    objStockOrder.SetInputValue(5, 0)  # 주문단가  - 14,100원
    objStockOrder.SetInputValue(7, "0")  # 주문 조건 구분 코드, 0: 기본 1: IOC 2:FOK
    objStockOrder.SetInputValue(8, "13")  # 주문호가 구분코드 - 01: 보통 13:최우선
    objStockOrder.BlockRequest()

def temp():

    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    engine = create_engine(f"mysql+pymysql://root:P@ssw0rd@127.0.0.1:3306/investar")
    index_list = {46: [0.7, 1, 0.5, 0.8], 97: [0.7, 1, 0.5, 0.8], 113: [0.7, 1, 0.5, 0.8]}

    query = """
    SELECT idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,  
           idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high,  
           idx3_idx_low, idx3_idx_high, idx3_sig_low, idx3_sig_high 
    FROM test_index_list
    WHERE test_index_kind = 3;
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        idx1_values = row[0:4]  # idx1 값들만 사용 예시
        idx2_values = row[4:8]  # idx1 값들만 사용 예시
        idx3_values = row[8:12]  # idx1 값들만 사용 예시

        index_list[46] = list(idx1_values)
        index_list[97] = list(idx2_values)
        index_list[113] = list(idx3_values)
    print(index_list)


    if cursor:
        cursor.close()
    if connection:
        connection.close()


def temp2():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 6, 17)  # 마지막 파티션의 다음날

    partition_statements = []
    current_date = start_date

    while current_date < end_date:
        partition_name = current_date.strftime('p%Y%m%d')
        partition_end = current_date + timedelta(days=1)
        partition_statements.append(
            f"PARTITION {partition_name} VALUES LESS THAN (TO_DAYS('{partition_end.strftime('%Y-%m-%d')}'))")
        current_date += timedelta(days=1)

    partition_sql = ",\n    ".join(partition_statements)
    create_table_sql = f"""
    CREATE TABLE five_minute_index_test (
        code VARCHAR(50),
        trade_date DATE,
        minute_order INT,
        idx_id INT,
        val FLOAT,
        PRIMARY KEY (code, trade_date, idx_id)
    )
    PARTITION BY RANGE (TO_DAYS(trade_date)) (
        {partition_sql}
    );
    """

    print(create_table_sql)

def price():
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
    bConnect = objCpCybos.IsConnect
    if (bConnect == 0):
        print("PLUS가 정상적으로 연결되지 않음. ")
        return False
    objTrade = win32com.client.Dispatch("CpTrade.CpTdUtil")
    initCheck = objTrade.TradeInit(0)
    if (initCheck != 0):
        print("주문 초기화 실패")
        exit()

    objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")
    code = 'A466810'
    objStockChart.SetInputValue(0, code)  # 종목 코드 - 삼성전자
    objStockChart.SetInputValue(1, ord('2'))  # 기간조회1/개수조회2
    # objStockChart.SetInputValue(2, '20241004')  # 요청종료일 최근
    # objStockChart.SetInputValue(3, '20241003')  # 요청시작일 이전
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
            print(f"'{code}','{day}',{high},{low},{open},{close},{vol});")

    cursor.close()
    connection.close()

def etf():
    url = "http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd"
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201030104'
    }
    # data = {
    #     'code': 'value1',  # 개발자 도구에서 확인한 폼 데이터의 key-value 쌍
    #     'name': 'value2',
    #     # 필요한 다른 폼 데이터 추가
    # }

    # POST 요청 보내기
    response = requests.post(url, headers=headers)

    # 엑셀 파일 저장
    if response.status_code == 200:
        with open('downloaded_file.xlsx', 'wb') as file:
            file.write(response.content)
        print("엑셀 파일이 성공적으로 다운로드되었습니다.")
    else:
        print(f"요청 실패: {response.status_code}")
        print(response.text)


def index_test():
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    test_date = datetime.now()
    sql = f"WITH previous_dates AS (" \
            f"SELECT DISTINCT trade_date " \
            f"FROM daily_price " \
            f"WHERE trade_date < '{test_date}' " \
            f"AND code = 'A005930'" \
            f"ORDER BY trade_date DESC " \
            f"LIMIT 80" \
            f")" \
            f"SELECT * " \
            f"FROM daily_price " \
            f"WHERE CODE = 'A005930' AND trade_date IN (SELECT trade_date FROM previous_dates) ORDER BY trade_date;"

    df = pd.read_sql(sql, connection)
    objSeries = win32com.client.Dispatch("CpIndexes.CpSeries")
    for _, row_data in df.iterrows():
        objSeries.Add(row_data["close"], row_data["open"], row_data["high"], row_data["low"], row_data["volume"])
    objIndex = win32com.client.Dispatch("CpIndexes.CpIndex")
    objIndex.series = objSeries        
    objIndex.put_IndexKind('Bollinger Band')  # 계산할 지표: Bollinger Band
    objIndex.put_IndexDefault('Bollinger Band')  # Bollinger Band 지표 기본 변수 default로 세팅
    objIndex.Calculate()
    cnt = objIndex.GetCount(0)
    idx = objIndex.GetResult(0, cnt - 1)
    sig = objIndex.GetResult(1, cnt - 1)
    print(idx, sig)
    
    # 전날 close 값 확인
    last_close = df.iloc[-1]["close"]
    mid_point = (sig + idx) / 2
    
    # 두 번째 데이터가 (sig, (sig+idx)/2) 구간에 있는 경우
    if sig < last_close < mid_point:
        print(f"두 번째 데이터({last_close})가 (sig={sig}, 중간점={mid_point}) 구간에 있습니다.")
        # 여기에 해당 구간에 있을 때 수행할 작업 추가
    
    # 두 번째 데이터가 ((sig+idx)/2, idx) 구간에 있는 경우
    elif mid_point < last_close < idx:
        print(f"두 번째 데이터({last_close})가 (중간점={mid_point}, idx={idx}) 구간에 있습니다.")
        # 여기에 해당 구간에 있을 때 수행할 작업 추가
    
    # 두 번째 데이터가 다른 구간에 있는 경우
    else:
        print(f"두 번째 데이터({last_close})가 지정된 구간에 없습니다.")
    
    cursor.close()
    connection.close()


if __name__ == "__main__":
    # buy()
    # sell()
    # balance()
    # price()
    # etf()
    index_test()