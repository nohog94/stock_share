import win32com.client
import pymysql
import time
from datetime import datetime, timedelta, date
import multiprocessing
import pandas as pd
import numpy as np
from .order_cancel import CpRPOrder, Cp5339
from sqlalchemy import create_engine

g_objCodeMgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
g_objCpStatus = win32com.client.Dispatch("CpUtil.CpCybos")
g_objCpTrade = win32com.client.Dispatch("CpTrade.CpTdUtil")

def five_minute_action(queue):
    engine = create_engine(f"mysql+pymysql://root:P@ssw0rd@127.0.0.1:3306/investar")
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()

    # 지표 초기화
    local_index_list = {97: [0.5, 0.7, 0.3, 0.6, 0.5, 0.7, 0.3, 0.6]}
    query = """
                        SELECT idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,
                        idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high                             
                        FROM test_index_list
                        WHERE test_index_kind = 2;
                        """
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        idx1_values = row[0:8]
        local_index_list[97] = list(idx1_values)
    result = {key: [] for key in local_index_list.keys()}

    index_list_str = ", ".join(map(str, local_index_list.keys()))
    sql_idx_nm = f"SELECT IDX_ID, IDX_NM FROM stock_index WHERE IDX_ID IN({(index_list_str)});"
    df_idx_nm = pd.read_sql(sql_idx_nm, connection)
    index_name_dict = {row['IDX_ID']: row['IDX_NM'] for _, row in df_idx_nm.iterrows()}

    objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
    bConnect = objCpCybos.IsConnect
    if (bConnect == 0):
        print("PLUS가 정상적으로 연결되지 않음. ")
        return False
    objTrade = win32com.client.Dispatch("CpTrade.CpTdUtil")
    initCheck = objTrade.TradeInit(0)
    if (initCheck != 0):
        print("주문 초기화 실패")
        return False

    diOrderList = {}
    orderList = []
    # 주식 미체결 항목 조회
    diOrder = Cp5339()
    diOrder.Request5339(diOrderList, orderList)
    for item in orderList:
        # 주식 미체결 항목 취소
        orderCancel = CpRPOrder()
        orderCancel.BlockRequestCancel(item.orderNum, item.code, item.amount)

    acc = objTrade.AccountNumber[0]  # 계좌번호
    accFlag = objTrade.GoodsList(acc, 1)  # 주식상품 구분
    objRq = win32com.client.Dispatch("CpTrade.CpTd6033")
    objRq.SetInputValue(0, acc)  # 계좌번호
    objRq.SetInputValue(1, accFlag[0])  # 상품구분 - 주식 상품 중 첫번째
    objRq.SetInputValue(2, 50)  # 요청 건수(최대 50)
    objRq.BlockRequest()
    cnt = objRq.GetHeaderValue(7)
    stk_cnt_list = {}
    sql = f"UPDATE stock_hold SET STK_CNT = 0;"
    cursor.execute(sql)
    for i in range(cnt):
        code = objRq.GetDataValue(12, i)
        stk_cnt = objRq.GetDataValue(15, i)
        stk_cnt_list[code] = stk_cnt
        sql = f"UPDATE stock_hold SET STK_CNT = '{stk_cnt}' WHERE CODE = '{code}';"
        cursor.execute(sql)

    acc = objTrade.AccountNumber[0]  # 계좌번호
    accFlag = objTrade.GoodsList(acc, 1)  # 주식상품 구분

    test_date = datetime.now()
    test_date_YYMMDD = test_date.strftime('%Y-%m-%d')
    test_date_HHMMSS = test_date.strftime('%Y-%m-%d %H:%M:%S')
    print(f"current time : {test_date_HHMMSS}")


    formatted_date1 = test_date.strftime('%Y-%m-%d')
    formatted_date2 = test_date.strftime('%Y%m%d')

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
        None    


    code_results = []
    # 오늘 매매 종목 확인
    sql = f"SELECT code, action FROM daily_order WHERE trade_date =" \
        f" (SELECT MAX(trade_date) FROM daily_order WHERE trade_date < '{test_date_YYMMDD}');"
    df_order = pd.read_sql(sql, connection)

    for index, row in df_order.iterrows():
        code_action_list = [row['code'], row['action']]
        # stk_cnt_list에 있는 코드는 'H'를, 없는 코드는 'N'을 추가
        if row['code'] in stk_cnt_list:
            code_action_list.append('H')
        else:
            code_action_list.append('N')
        code_results.append(code_action_list)
    
    # stk_cnt_list에 있는 코드들을 code_results에 추가
    for code in stk_cnt_list:
        # 이미 code_results에 있는 코드는 추가하지 않음
        if not any(item[0] == code for item in code_results):
        # if not any(item[0] == code for item in code_results) and code != 'A347700':
            code_results.append([code, 'S', 'H'])

    interval = 5
    daily_total_cnt = 77

    buy_tax_rate = 0.0088 / 100
    sell_tax_rate = (0.0088 + 0.18) / 100
    trade_money = 40000

    # 5분 데이터 신규 업데이트
    objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")
    for daily_code in code_results:
        sql_stock_hold = f"SELECT avg_price, stk_cnt FROM stock_hold where code = '{daily_code[0]}';"
        df_stock_hold = pd.read_sql(sql_stock_hold, connection)

        sql_stock_hold_total = f"SELECT avg_price, stk_cnt FROM stock_hold where stk_cnt > 0;"
        df_stock_hold_total = pd.read_sql(sql_stock_hold_total, connection)

        if df_stock_hold.shape[0] == 0 and len(df_stock_hold_total) >= 50:
            continue
        objStockChart.SetInputValue(0, daily_code[0])  # 종목 코드 - 삼성전자
        objStockChart.SetInputValue(1, ord('2'))  # 기간조회1/개수조회2
        objStockChart.SetInputValue(4, 1)  # 최근 100일 치 뒤에가 일
        objStockChart.SetInputValue(5, [0, 1, 2, 3, 4, 5, 8])  # 날짜,시간,시가,고가,저가,종가,거래량
        objStockChart.SetInputValue(6, ord('m'))  # '차트 주가 - 분간 차트 요청
        objStockChart.SetInputValue(7, interval)  # 차트 주기
        objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용
        objStockChart.BlockRequest()

        day = objStockChart.GetDataValue(0, 0)
        day_time = objStockChart.GetDataValue(1, 0)
        open = objStockChart.GetDataValue(2, 0)
        high = objStockChart.GetDataValue(3, 0)
        low = objStockChart.GetDataValue(4, 0)
        close = objStockChart.GetDataValue(5, 0)
        vol = objStockChart.GetDataValue(6, 0)
        trade_date = datetime.strptime(str(day) + str(day_time), '%Y%m%d%H%M')
        # print(f"code:{daily_code[0]}/ trade_date:{trade_date}/ high:{high}/ low:{low}/open:{open}/close:{close}/vol:{vol}")
        cursor.execute(f"REPLACE INTO five_minute_price(code, trade_date, high, low, open, close, volume)"
                    f" VALUES('{daily_code[0]}','{trade_date}',{high},{low},{open},{close},{vol});")
        # 매매 대상 여부 확인
        # try:
        sql = f"WITH previous_dates AS (" \
            f"SELECT DISTINCT trade_date " \
            f"FROM five_minute_price " \
            f"WHERE trade_date <= '{test_date_YYMMDD} 23:59:59'" \
            f"AND code = '{daily_code[0]}'" \
            f"ORDER BY trade_date DESC " \
            f"LIMIT 160" \
            f")" \
            f"SELECT * " \
            f"FROM five_minute_price " \
            f"WHERE CODE = '{daily_code[0]}' AND trade_date IN (SELECT trade_date FROM previous_dates) ORDER BY trade_date;"
        
        df = pd.read_sql(sql, connection)

        # 지표 계산
        objSeries = win32com.client.Dispatch("CpIndexes.CpSeries")
        for _, row_data in df.iterrows():
            objSeries.Add(row_data["close"], row_data["open"], row_data["high"], row_data["low"], row_data["volume"])
        objIndex = win32com.client.Dispatch("CpIndexes.CpIndex")
        objIndex.series = objSeries

        # 지표 데이터 계산 하기
        for key, value in local_index_list.items():
            sig_max = 100
            objIndex.put_IndexKind(index_name_dict[key])  # 계산할 지표: MACD
            objIndex.put_IndexDefault(index_name_dict[key])  # MACD 지표 기본 변수 default로 세팅
            objIndex.Calculate()
            cnt = objIndex.GetCount(0)
            idx = objIndex.GetResult(0, cnt - 1)
            sig = objIndex.GetResult(1, cnt - 1)
            result[key].append(idx / sig_max)
            result[key].append(sig / sig_max)
        
        # 종목당 거래할 수 있는 상한
        trade_amount = int(trade_money / close)
        
        # 직전 거래에서 취소된 물량이 있으면 해당 물량으로 판매량 설정
        for order in orderList:
            if order.code == daily_code[0]:
                trade_amount += order.amount

        # 매도 조건
        sql = f"WITH previous_dates AS (" \
        f"SELECT DISTINCT trade_date " \
        f"FROM daily_price " \
        f"WHERE trade_date < '{test_date_YYMMDD}' " \
        f"AND code = '{daily_code[0]}'" \
        f"ORDER BY trade_date DESC " \
        f"LIMIT 80" \
        f")" \
        f"SELECT * " \
        f"FROM daily_price " \
        f"WHERE CODE = '{daily_code[0]}' AND trade_date IN (SELECT trade_date FROM previous_dates) ORDER BY trade_date;"

        df_price_hold = pd.read_sql(sql, connection)
        objSeries = win32com.client.Dispatch("CpIndexes.CpSeries")
        for _, row_data in df_price_hold.iterrows():
            objSeries.Add(row_data["close"], row_data["open"], row_data["high"], row_data["low"], row_data["volume"])
        objIndex = win32com.client.Dispatch("CpIndexes.CpIndex")
        objIndex.series = objSeries        
        objIndex.put_IndexKind('Bollinger Band')  # 계산할 지표: Bollinger Band
        objIndex.put_IndexDefault('Bollinger Band')  # Bollinger Band 지표 기본 변수 default로 세팅
        try:
            objIndex.Calculate()
        except:
            continue 
        cnt = objIndex.GetCount(0)
        idx = objIndex.GetResult(0, cnt - 1)
        sig = objIndex.GetResult(1, cnt - 1)            
        
        # 전날 close 값 확인
        last_close = df.iloc[-1]["close"]
        mid_point = (sig + idx) / 2
        if trade_amount == 0:
            # trade_money / close를 저장
            # 저장한 결과가 1보다 크면 1 뺀거를 저장 후 trade_amount = 1
            #
            sql_trade_amount = "SELECT amount AS total_amount " \
                            f"FROM daily_trade WHERE code = '{daily_code[0]}' AND trade_date = '{test_date_YYMMDD}'"
            df_trade_amount = pd.read_sql(sql_trade_amount, connection)

            if df_trade_amount.empty or pd.isnull(df_trade_amount['total_amount'].iloc[0]):
                total_amount = 0
            else:
                total_amount = df_trade_amount['total_amount'].iloc[0]
            total_amount = total_amount + trade_money / close
            if total_amount > 1:
                cursor.execute(f"REPLACE INTO daily_trade(code, trade_date, amount)"
                            f" VALUES('{daily_code[0]}','{trade_date}',{total_amount-1});")
                trade_amount = 1
            else:
                cursor.execute(f"REPLACE INTO daily_trade(code, trade_date, amount)"
                            f" VALUES('{daily_code[0]}','{trade_date}',{total_amount});")
                continue
        # 전날 데이터가 (sig, (sig+idx)/2) 구간에 있는 경우
        if sig < last_close < mid_point and sig < close < mid_point:                
            pass
        # 전날 데이터가 ((sig+idx)/2, idx) 구간에 있는 경우
        elif mid_point < last_close < idx and mid_point < close < idx:
            pass
        else:                        
            # 보유한 양보다 매도할 양이 많으면 보유한 양으로 매도            
            trade_amount = trade_amount*3
            try:
                if stk_cnt_list[daily_code[0]] < trade_amount:
                    trade_amount = stk_cnt_list[daily_code[0]]
            except:
                None    
            
            objStockOrder = win32com.client.Dispatch("CpTrade.CpTd0311")
            objStockOrder.SetInputValue(0, "1")  # 1: 매도
            objStockOrder.SetInputValue(1, acc)  # 계좌번호
            objStockOrder.SetInputValue(2, accFlag[0])  # 상품구분 - 주식 상품 중 첫번째
            objStockOrder.SetInputValue(3, daily_code[0])  # 종목코드 - A003540 - 대신증권 종목
            objStockOrder.SetInputValue(4, trade_amount)  # 매수수량 10주
            objStockOrder.SetInputValue(5, 0)  # 주문단가  - 14,100원
            objStockOrder.SetInputValue(7, "0")  # 주문 조건 구분 코드, 0: 기본 1: IOC 2:FOK
            objStockOrder.SetInputValue(8, "13")  # 주문호가 구분코드 - 01: 보통 13:최우선
            objStockOrder.BlockRequest()

            if len(df_stock_hold) == 0 or df_stock_hold.loc[0, 'stk_cnt'] == 0:
                continue
            elif df_stock_hold.loc[0, 'stk_cnt'] > 0:
                df_stock_hold.loc[0, 'stk_cnt'] = max(0, df_stock_hold.loc[0, 'stk_cnt'] - trade_amount)
                cursor.execute(f"UPDATE stock_hold SET stk_cnt ="
                            f" {df_stock_hold.loc[0, 'stk_cnt']} where code = '{daily_code[0]}';")
            queue.put(f"{daily_code[0]} has been sell at {close}, {trade_amount} in {test_date_HHMMSS}")
            continue
        # 매수 조건
        if daily_code[1] == 'B' and all(result[key][0] >= local_index_list[key][0] for key, _ in result.items()) and \
                all(result[key][0] < local_index_list[key][1] for key, _ in result.items()) and \
                all(result[key][1] >= local_index_list[key][2] for key, _ in result.items()) and \
                all(result[key][1] < local_index_list[key][3] for key, _ in result.items()):
            
            
           

            objStockOrder = win32com.client.Dispatch("CpTrade.CpTd0311")
            objStockOrder.SetInputValue(0, "2")  # 2: 매수
            objStockOrder.SetInputValue(1, acc)  # 계좌번호
            objStockOrder.SetInputValue(2, accFlag[0])  # 상품구분 - 주식 상품 중 첫번째
            objStockOrder.SetInputValue(3, daily_code[0])  # 종목코드 - A003540 - 대신증권 종목
            objStockOrder.SetInputValue(4, trade_amount)  # 매수수량 10주
            objStockOrder.SetInputValue(5, 0)  # 주문단가  - 14,100원
            objStockOrder.SetInputValue(7, "0")  # 주문 조건 구분 코드, 0: 기본 1: IOC 2:FOK
            objStockOrder.SetInputValue(8, "13")  # 주문호가 구분코드 - 01: 보통 13:최우선
            objStockOrder.BlockRequest()

            if len(df_stock_hold) == 0:
                buy_price = close * (1 + buy_tax_rate + sell_tax_rate)
                cursor.execute(
                    f"INSERT INTO stock_hold(code, avg_price, stk_cnt, first_trade_date)"
                    f" VALUES('{daily_code[0]}',{buy_price},{trade_amount}, '{test_date}');")
                df_stock_hold.loc[0] = [buy_price, trade_amount]
            else:
                buy_price = close * (1 + buy_tax_rate + sell_tax_rate)
                avg_price = (df_stock_hold.loc[0, 'avg_price'] * df_stock_hold.loc[0, 'stk_cnt'] + buy_price) / (
                            df_stock_hold.loc[0, 'stk_cnt'] + 1)
                if df_stock_hold.loc[0, 'stk_cnt'] == 0:
                    cursor.execute(f"UPDATE stock_hold SET avg_price = {avg_price},"
                                   f" stk_cnt = {df_stock_hold.loc[0, 'stk_cnt'] + trade_amount},"
                                   f" first_trade_date = '{test_date}' where code = '{daily_code[0]}';")
                else:
                    cursor.execute(f"UPDATE stock_hold SET avg_price = {avg_price},"
                                   f" stk_cnt = {df_stock_hold.loc[0, 'stk_cnt'] + trade_amount}"
                                   f" where code = '{daily_code[0]}';")
                df_stock_hold.loc[0, 'avg_price'] = avg_price
                df_stock_hold.loc[0, 'stk_cnt'] += trade_amount
            queue.put(f"{daily_code[0]} has been bought at {close}, {trade_amount} in {test_date_HHMMSS}")
        
        else:
            None
        # except Exception as e:
        #     print(e)
        #     queue.put("error occurs")
    if cursor:
        cursor.close()
    if connection:
        connection.close()




if __name__ == '__main__':
    start_time = time.time()
    queue = multiprocessing.Queue()
    five_minute_action(queue)
    end_time = time.time()
    elapsed_time = end_time - start_time  # 실행 시간 계산
    print(f"실행 시간: {elapsed_time} 초")


