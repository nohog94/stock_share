import win32com.client
import pymysql
import time
from datetime import datetime
import pandas as pd
import pythoncom
from sqlalchemy import create_engine
import sys


def check_recommand(trade_date):
    pythoncom.CoInitialize()
    engine = create_engine(f"mysql+pymysql://root:P@ssw0rd@127.0.0.1:3306/investar")
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)
    cursor = connection.cursor()
    local_index_list = {46: None, 97: None, 113: None}
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
        idx1_values = row[0:4]
        idx2_values = row[4:8]
        idx3_values = row[8:12]
        local_index_list[46] = list(idx1_values)
        local_index_list[97] = list(idx2_values)
        local_index_list[113] = list(idx3_values)

    result = {46: [], 97: [], 113: []}
    index_list_str = ", ".join(map(str, local_index_list.keys()))
    sql_idx_nm = f"SELECT IDX_ID, IDX_NM FROM stock_index WHERE IDX_ID IN({(index_list_str)});"
    df_idx_nm = pd.read_sql(sql_idx_nm, engine)
    index_name_dict = {row['IDX_ID']: row['IDX_NM'] for _, row in df_idx_nm.iterrows()}

    objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
    bConnect = objCpCybos.IsConnect
    if (bConnect == 0):
        print("PLUS가 정상적으로 연결되지 않음. ")
        exit()

    recommand = []

    sql_code_info = """
            SELECT code, market_cap 
            FROM company_info 
            WHERE industry NOT LIKE %s 
            AND industry NOT LIKE %s 
            AND products NOT LIKE %s 
            AND products NOT LIKE %s
            AND company NOT LIKE %s    
            AND market_cap > 200000000000 
            AND del_yn = 'N'
            AND rmd_block_start_date <= DATE_SUB(NOW(), INTERVAL 15 DAY)
            ORDER BY market_cap DESC;
            """
    params = ('%의약품%', '%의료%', '%지주%', '%리츠%', '%리츠%')
    df_code_info = pd.read_sql(sql_code_info, engine, params=params)
    df_code_info_sorted = df_code_info.sort_values(by='market_cap', ascending=False)
    count = 1
    epoch = 0

    for i, row in df_code_info_sorted.iterrows():
        result = {46: [], 97: [], 113: []}
        daily_code = row[0]
        sql = f"WITH previous_dates AS (" \
            f"SELECT DISTINCT trade_date " \
            f"FROM daily_price " \
            f"WHERE trade_date < '{trade_date}' " \
            f"AND code = '{daily_code}'" \
            f"ORDER BY trade_date DESC " \
            f"LIMIT 80" \
            f")" \
            f"SELECT * " \
            f"FROM daily_price " \
            f"WHERE CODE = '{daily_code}' AND trade_date IN (SELECT trade_date FROM previous_dates) ORDER BY trade_date;"
        dfPrice = pd.read_sql(sql, engine)
        if dfPrice.shape[0] != 80:
            continue
        objSeries = win32com.client.Dispatch("CpIndexes.CpSeries")
        for _, row_data in dfPrice.iterrows():
            objSeries.Add(row_data["close"], row_data["open"], row_data["high"], row_data["low"], row_data["volume"])
        objIndex = win32com.client.Dispatch("CpIndexes.CpIndex")
        objIndex.series = objSeries
        no_index_flag = False
        for key, _ in local_index_list.items():
            sql_max_daily_index = f"SELECT MAX(ABS(val)) " \
                f"FROM daily_index_test " \
                f"WHERE code = %s AND IDX_ID = %s AND idx_term = 1 AND val IS NOT NULL"
            cursor.execute(sql_max_daily_index, (daily_code, key))
            max_daily_index = cursor.fetchone()
            if max_daily_index[0] is None:
                no_index_flag = True
                break
            sig_max = max_daily_index[0] + 1e-5
            # 저장된 게 없으면 지표 계산 후 insert 수행
            objIndex.put_IndexKind(index_name_dict[key])  # 계산할 지표: RSI
            objIndex.put_IndexDefault(index_name_dict[key])  # RSI 지표 기본 변수 default로 세팅
            # 지표 데이터 계산 하기 이 부분 최적화 필요
            objIndex.Calculate()
            # 첫번째 지표 계산값의 예) MACD선택 시 MACD선 개수
            cnt = objIndex.GetCount(0)
            # 이전 날과 비교하기 위해 비교대상 데이터 먼저 입력
            idx = objIndex.GetResult(0, cnt - 1)
            sig = objIndex.GetResult(1, cnt - 1)
            if key == 97:
                sig_max = 100
            result[key].append(idx / sig_max)
            result[key].append(sig / sig_max)

        if no_index_flag:
            continue
        if all(result[key][0] >= local_index_list[key][0] for key, _ in result.items()) and \
                all(result[key][0] < local_index_list[key][1] for key, _ in result.items()) and \
                all(result[key][1] >= local_index_list[key][2] for key, _ in result.items()) and \
                all(result[key][1] < local_index_list[key][3] for key, _ in result.items()):
            print(daily_code)
            recommand.append(daily_code)
        count += 1
        if count % 60 == 0:
            tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
            print(f"[{tmnow}] epoch:#{epoch + 1:04d} count:#{count:06d} ")
            epoch += 1
        if len(recommand) > 20:
            break

    if recommand:
        company_value = []
        tuple_recommand = tuple(recommand)
        placeholders = ', '.join(['%s'] * len(tuple_recommand))
        sql = f"SELECT code, market_cap FROM company_info WHERE code in ({placeholders})"
        cursor.execute(sql, tuple_recommand)
        for row in cursor:
            company_value.append([row[0], row[1]])
        # value를 기준으로 내림차순 정렬
        sorted_company_value = sorted(company_value, key=lambda x: x[1], reverse=True)
        # 상위 20개의 code 추출
        top_20_codes = [x[0] for x in sorted_company_value[:min(20, len(sorted_company_value))]]
        for code in top_20_codes:
            cursor.execute(f"REPLACE INTO daily_recommand (trade_date, code) VALUES ('{trade_date}', '{code}');")
            cursor.execute(f"REPLACE INTO daily_order (CODE, trade_date, action) VALUES ('{code}', '{trade_date}', 'B');")

        cursor.execute(f"SELECT MAX(trade_date) FROM daily_order WHERE trade_date < '{trade_date}'")
        prev_date_row = cursor.fetchone()
        if prev_date_row and prev_date_row[0]:
            prev_date = prev_date_row[0].strftime('%Y-%m-%d') if hasattr(prev_date_row[0], 'strftime') else str(prev_date_row[0])
            # 직전 거래일의 daily_order 데이터 가져오기
            cursor.execute(f"SELECT code, action FROM daily_order WHERE trade_date = '{prev_date}'")
            prev_orders = cursor.fetchall()
            for code, action in prev_orders:
                # 오늘 날짜로 INSERT (중복 방지 위해 REPLACE 사용)
                cursor.execute(f"REPLACE INTO daily_order (CODE, trade_date, action) VALUES ('{code}', '{trade_date}', '{action}');")        

        # 보유 종목 중에 내일 추매 대상 여부 확인 / 백 테스트 시 주석처리
        sql_stock_hold = f"SELECT code FROM stock_hold where stk_cnt > 0;"
        df_stock_hold = pd.read_sql(sql_stock_hold, engine)
        for code in df_stock_hold['code']:
            buy_recommand = check_buy_hold_recommand(trade_date, code, 'stock', 0)
            if buy_recommand:
                cursor.execute(f"REPLACE INTO daily_order (CODE, trade_date, action)"
                               f" VALUES ('{code}', '{trade_date}', 'B');")            
    else:
        return
    if cursor:
        cursor.close()
    if connection:
        connection.close()


def check_buy_hold_recommand(test_date, daily_code, stock_type, test_index_kind):
    # test_index_kind가 0인경우 매수, 1인 경우 매도
    engine = create_engine(f"mysql+pymysql://root:P@ssw0rd@127.0.0.1:3306/investar")
    cursor = connection.cursor()
    local_index_list = {46: None, 97: None, 113: None}
    query = f"""
            SELECT idx1_idx_low, idx1_idx_high, idx1_sig_low, idx1_sig_high,  
                   idx2_idx_low, idx2_idx_high, idx2_sig_low, idx2_sig_high,  
                   idx3_idx_low, idx3_idx_high, idx3_sig_low, idx3_sig_high 
            FROM test_index_list
            WHERE test_index_kind = {test_index_kind};
            """
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        idx1_values = row[0:4]
        idx2_values = row[4:8]
        idx3_values = row[8:12]
        local_index_list[46] = list(idx1_values)
        local_index_list[97] = list(idx2_values)
        local_index_list[113] = list(idx3_values)

    result = {46: [], 97: [], 113: []}
    index_list_str = ", ".join(map(str, local_index_list.keys()))
    sql_idx_nm = f"SELECT IDX_ID, IDX_NM FROM stock_index WHERE IDX_ID IN({(index_list_str)});"
    df_idx_nm = pd.read_sql(sql_idx_nm, engine)
    index_name_dict = {row['IDX_ID']: row['IDX_NM'] for _, row in df_idx_nm.iterrows()}
    recommand = False
    if stock_type == 'stock':
        sql = f"WITH previous_dates AS (" \
            f"SELECT DISTINCT trade_date " \
            f"FROM daily_price " \
            f"WHERE trade_date < '{test_date}' " \
            f"AND code = '{daily_code}'" \
            f"ORDER BY trade_date DESC " \
            f"LIMIT 80" \
            f")" \
            f"SELECT * " \
            f"FROM daily_price " \
            f"WHERE CODE = '{daily_code}' AND trade_date IN (SELECT trade_date FROM previous_dates) ORDER BY trade_date;"
    else:
        sql = f"WITH previous_dates AS (" \
            f"SELECT DISTINCT trade_date " \
            f"FROM etf_daily_price " \
            f"WHERE trade_date < '{test_date}' " \
            f"AND code = '{daily_code}'" \
            f"ORDER BY trade_date DESC " \
            f"LIMIT 80" \
            f")" \
            f"SELECT * " \
            f"FROM etf_daily_price " \
            f"WHERE CODE = '{daily_code}' AND trade_date IN (SELECT trade_date FROM previous_dates) ORDER BY trade_date;"
    df = pd.read_sql(sql, engine)
    if df.shape[0] != 80:
        return False
    objSeries = win32com.client.Dispatch("CpIndexes.CpSeries")
    for _, row_data in df.iterrows():
        objSeries.Add(row_data["close"], row_data["open"], row_data["high"], row_data["low"], row_data["volume"])
    objIndex = win32com.client.Dispatch("CpIndexes.CpIndex")
    objIndex.series = objSeries

    for key, _ in local_index_list.items():
        sql_max_daily_index = f"SELECT MAX(ABS(val)) " \
            f"FROM daily_index_test " \
            f"WHERE code = %s AND IDX_ID = %s AND idx_term = 1 AND val IS NOT NULL"
        cursor.execute(sql_max_daily_index, (daily_code, key))
        max_daily_index = cursor.fetchone()
        if max_daily_index[0] is None:
            return False
        sig_max = max_daily_index[0] + 1e-5
        # 저장된 게 없으면 지표 계산 후 insert 수행
        objIndex.put_IndexKind(index_name_dict[key])  # 계산할 지표: RSI
        objIndex.put_IndexDefault(index_name_dict[key])  # RSI 지표 기본 변수 default로 세팅
        # 지표 데이터 계산 하기 이 부분 최적화 필요
        objIndex.Calculate()
        # 첫번째 지표 계산값의 예) MACD선택 시 MACD선 개수
        cnt = objIndex.GetCount(0)
        # 이전 날과 비교하기 위해 비교대상 데이터 먼저 입력
        idx = objIndex.GetResult(0, cnt - 1)
        sig = objIndex.GetResult(1, cnt - 1)
        if key == 97:
            sig_max = 100
        result[key].append(idx / sig_max)
        result[key].append(sig / sig_max)
    if all(result[key][0] >= local_index_list[key][0] for key, _ in result.items()) and \
            all(result[key][0] < local_index_list[key][1] for key, _ in result.items()) and \
            all(result[key][1] >= local_index_list[key][2] for key, _ in result.items()) and \
            all(result[key][1] < local_index_list[key][3] for key, _ in result.items()):
        recommand = True
    return recommand


if __name__ == '__main__':
    if len(sys.argv) == 1:
        start_date = datetime.now().strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
    else:
        start_date = sys.argv[1]
        end_date = sys.argv[1]
    start_time = time.time()
    engine = create_engine(f"mysql+pymysql://root:P@ssw0rd@127.0.0.1:3306/investar")
    connection = pymysql.connect(host='localhost', port=3306, db='investar', user='root', passwd='P@ssw0rd',
                                 autocommit=True)


    sql = f"SELECT distinct trade_date FROM daily_price where trade_date BETWEEN '{start_date}' AND '{end_date}';"
    # 수동으로 돌릴 땐 check_buy_hold_recommand 실행부분 주석 처리
    # sql = f"SELECT distinct trade_date FROM daily_price where trade_date BETWEEN '2025-10-20' AND '2025-10-20';"
    df = pd.read_sql(sql, engine)
    df['trade_date'] = df['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    for trade_date in df['trade_date']:
        check_recommand(trade_date)
        # check_etf_recommand(trade_date)
    end_time = time.time()
    elapsed_time = end_time - start_time  # 실행 시간 계산
    print(f"실행 시간: {elapsed_time} 초")



