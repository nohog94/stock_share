import pymysql
import pandas as pd
import win32com.client

g_objCodeMgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
g_objCpStatus = win32com.client.Dispatch("CpUtil.CpCybos")
g_objCpTrade = win32com.client.Dispatch("CpTrade.CpTdUtil")

# 지표별 라인(시리즈) 개수 정의
# 지표별 라인(시리즈) 개수 정의
INDEX_SERIES_COUNT = {
    1: 2,
    46: 2,
    97: 2,
    113: 2
}

# daily_index_test에 데이터가 있는지 확인하는 함수
# start_date와 end_date 사이에 80개 이상의 데이터가 있으면 잘못된 지표값이 있으므로 유의
# daily_index_test에 데이터가 있는지 확인하는 함수
# start_date와 end_date 사이에 80개 이상의 데이터가 있으면 잘못된 지표값이 있으므로 유의
def check_daily_index_exists(start_date, end_date, daily_code, idx_id_list):
    # Compatibility wrapper related to idx_id_list vs single idx_id in user snippet
    # The user snippet uses single idx_id. We must adapt to the calling convention of back_test.py
    # back_test.py passes a list. We will iterate here.
    
    if not isinstance(idx_id_list, list):
        idx_id_list = [idx_id_list]

    connection = get_connection()
    cursor = connection.cursor()
    
    # User snippet logic adaptation
    local_index_list = {idx_id: 0 for idx_id in idx_id_list}
    # Pre-fetch index names (optimization kept from user snippet or just use snippet logic?)
    # User snippet fetches all names.
    index_list_str = ", ".join(map(str, local_index_list.keys()))
    sql_idx_nm = f"SELECT IDX_ID, IDX_NM FROM stock_index WHERE IDX_ID IN({(index_list_str)});"
    cursor.execute(sql_idx_nm)
    df_idx_nm = pd.DataFrame(cursor.fetchall(), columns=['IDX_ID', 'IDX_NM'])
    index_name_dict = {row['IDX_ID']: row['IDX_NM'] for _, row in df_idx_nm.iterrows()}

    # Check dates range
    sql = f"SELECT distinct trade_date FROM daily_price where trade_date BETWEEN '{start_date}' AND '{end_date}'" \
        f" ORDER BY trade_date DESC;"
    cursor.execute(sql)
    df_date = pd.DataFrame(cursor.fetchall(), columns=['trade_date'])
    df_date['trade_date'] = df_date['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    
    objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
    objTrade = win32com.client.Dispatch("CpTrade.CpTdUtil") 
    
    for idx_id in idx_id_list: # Iterate over list as per calling convention
        cnt = 0
        objIndex = None
        
        num_series = INDEX_SERIES_COUNT.get(idx_id, 2)

        for i, row in df_date.iterrows():

            test_date = row[0]
            
            # Using a new cursor per iteration as per user snippet (though one is fine, we follow logic)
            # User snippet uses `with connection.cursor()`. We will just use `cursor`.
            
            query = f"""
            SELECT COUNT(*) 
            FROM daily_index_test 
            WHERE trade_date = '{test_date}' AND code = '{daily_code}' AND IDX_ID = {idx_id}
            """
            cursor.execute(query)
            result = cursor.fetchone()
            
            # User logic: If count is 2 (or num_series), continue.
            # We use INDEX_SERIES_COUNT for generality or 2 as per user request?
            # User explicitly said "전부 2,2,2 이어야 해".            
            
            if result[0] == num_series: 
                continue
            else:                                
                # Fetch Data if not ready
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
                df = pd.read_sql(sql, connection)
                
                if df.shape[0] != 80:
                    print(f"code : {daily_code} 충분한 daily_price 데이터 없음. 필요: 80, 실제: {df.shape[0]}")
                    # Delete invalid data
                    query = f"DELETE FROM daily_index_test WHERE trade_date = '{test_date}' AND code = '{daily_code}' AND IDX_ID = {idx_id}"
                    cursor.execute(query)                    
                    return

                if (objCpCybos.IsConnect == 0):
                    print("PLUS가 정상적으로 연결되지 않음..")
                    return
                if (objTrade.TradeInit(0) != 0):
                    print("주문 초기화 실패")
                    return

                objSeries = win32com.client.Dispatch("CpIndexes.CpSeries")
                for _, row_data in df.iterrows():
                    objSeries.Add(row_data["close"], row_data["open"], row_data["high"], row_data["low"], row_data["volume"])
                
                objIndex = win32com.client.Dispatch("CpIndexes.CpIndex")
                objIndex.series = objSeries
                
                if idx_id not in index_name_dict: continue

                objIndex.put_IndexKind(index_name_dict[idx_id]) 
                objIndex.put_IndexDefault(index_name_dict[idx_id]) 
                objIndex.Calculate()
                cnt = objIndex.GetCount(0)
                
                for j in range(num_series):
                    val = objIndex.GetResult(j, cnt - 1)
                    sql = f"REPLACE INTO daily_index_test(code, trade_date, idx_id, idx_term, val)" \
                        f" VALUES('{daily_code}','{test_date}',{idx_id},{j},{val});"
                    cursor.execute(sql)


# five_minute_index_test에 데이터가 있는지 확인하는 함수
# five_minute_index_test에 데이터가 있는지 확인하는 함수
def check_five_minute_index_exists(start_date, end_date, daily_code, idx_id_list):
    if not isinstance(idx_id_list, list):
        idx_id_list = [idx_id_list]

    connection = get_connection()
    cursor = connection.cursor()

    local_index_list = {97: 0}
    index_list_str = ", ".join(map(str, local_index_list.keys()))
    sql_idx_nm = f"SELECT IDX_ID, IDX_NM FROM stock_index WHERE IDX_ID IN({(index_list_str)});"
    cursor.execute(sql_idx_nm)
    df_idx_nm = pd.DataFrame(cursor.fetchall(), columns=['IDX_ID', 'IDX_NM'])
    index_name_dict = {row['IDX_ID']: row['IDX_NM'] for _, row in df_idx_nm.iterrows()}

    # Check date range using subquery logic from snippet
    sql = f"""
    SELECT DISTINCT trade_date
    FROM daily_price
    WHERE trade_date BETWEEN (    
            SELECT DISTINCT trade_date
            FROM daily_price
            WHERE trade_date < '{start_date}' 
            ORDER BY trade_date DESC
            LIMIT 1    
    ) AND '{end_date}'
    ORDER BY trade_date DESC;
    """
    cursor.execute(sql)
    df_date = pd.DataFrame(cursor.fetchall(), columns=['trade_date'])
    df_date['trade_date'] = df_date['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    

    
    daily_total_cnt = 77
    
    for idx_id in idx_id_list: # Iterate requests
        
        for i, row in df_date.iterrows():
            if i == df_date.shape[0] - 1: # Last day check skip as per snippet
                continue
            objCpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
            objTrade = win32com.client.Dispatch("CpTrade.CpTdUtil")
            test_date = row[0]
            
            query = f"""
            SELECT COUNT(*) 
            FROM five_minute_index_test 
            WHERE trade_date = '{test_date}' AND code = '{daily_code}' AND IDX_ID = {idx_id}
            """
            cursor.execute(query)
            result = cursor.fetchone()
            
            # Using 154 (77*2) as per user snippet. 
            # Note: This is specific to indexes with 2 outputs (MACD, RSI, OBV were set to 2).
            # If we change series count, this calculation needs update. 
            # (daily_total_cnt * num_series)
            num_series = INDEX_SERIES_COUNT.get(idx_id, 2)
            expected_count = daily_total_cnt * num_series
            
            if result[0] == expected_count:
                continue
            else:
                base_date = df_date['trade_date'].shift(-1)[df_date['trade_date'] == test_date].values[0]
                
                sql = f"WITH previous_dates AS (" \
                    f"SELECT DISTINCT trade_date " \
                    f"FROM five_minute_price " \
                    f"WHERE trade_date BETWEEN '{base_date} 00:00:00' AND '{test_date} 23:59:59'" \
                    f"AND code = '{daily_code}'" \
                    f"ORDER BY trade_date DESC " \
                    f"LIMIT 154" \
                    f")" \
                    f"SELECT * " \
                    f"FROM five_minute_price " \
                    f"WHERE CODE = '{daily_code}' " \
                    f"AND trade_date IN (SELECT trade_date FROM previous_dates) ORDER BY trade_date;"
                    
                df = pd.read_sql(sql, connection)
                if df.shape[0] != 154:
                    print(f"code : {daily_code}, 날짜 {base_date}와 {test_date} 사이 five_minute_price 데이터 없음")
                    query = f"DELETE FROM five_minute_index_test WHERE trade_date = '{test_date}' AND code = '{daily_code}' AND IDX_ID = {idx_id}"
                    cursor.execute(query)
                    continue

                if (objCpCybos.IsConnect == 0):
                    print("PLUS가 정상적으로 연결되지 않음...")
                    return
                if (objTrade.TradeInit(0) != 0):
                    print("주문 초기화 실패")
                    return

                objSeries = win32com.client.Dispatch("CpIndexes.CpSeries")
                for _, row_data in df.iterrows():
                    objSeries.Add(row_data["close"], row_data["open"], row_data["high"], row_data["low"], row_data["volume"])
                
                objIndex = win32com.client.Dispatch("CpIndexes.CpIndex")
                objIndex.series = objSeries

                if idx_id not in index_name_dict: continue

                objIndex.put_IndexKind(index_name_dict[idx_id]) 
                objIndex.put_IndexDefault(index_name_dict[idx_id]) 
                objIndex.Calculate()
                
                cnt = objIndex.GetCount(0)

                # Reverse fill logic from user snippet
                for daily_cnt in range(daily_total_cnt, 0, -1):
                    minute_order = daily_total_cnt - daily_cnt
                    
                    for j in range(num_series):
                        try:
                            val = objIndex.GetResult(j, cnt - daily_cnt)
                            sql = f"REPLACE INTO five_minute_index_test(code, trade_date, minute_order, idx_id, idx_term, val)" \
                                f" VALUES('{daily_code}','{test_date}',{minute_order},{idx_id},{j}, {val});"
                            cursor.execute(sql)
                        except Exception as e:
                            # print(f'{daily_code}, {test_date}, {minute_order}, {j} Error: {e}')
                            pass


# 데이터베이스 연결 설정
def get_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='P@ssw0rd',
        db='investar',
        charset='utf8',
        autocommit=True
    )
